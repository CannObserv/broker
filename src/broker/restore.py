"""Restore a shipped snapshot so Redis loads it (CannObserv/broker#4).

This module exists for one fact about Redis 7 under ``appendonly yes``: **it
loads the AOF manifest and nothing else.** A ``dump.rdb`` beside an absent
``appendonlydir`` is ignored - the server starts empty and writes a fresh base
over the silence. So a restored snapshot has to *become* the base of a
multi-part AOF: ``appendonly.aof.1.base.rdb`` is the RDB,
``appendonly.aof.1.incr.aof`` is empty, and the manifest names both. Redis
recognises the base by its ``REDIS`` magic, loads it, opens the incr file, and
the next write appends there. ``tests/deploy/test_backup_restore_rehearsal.py``
proves it against a real server on every run, positions and PEL included.

``python -m src.broker.restore --latest --into /var/lib/redis`` is the whole
data half of a rebuild; docs/RECOVERY.md is the runbook around it.
"""

from __future__ import annotations

import argparse
import gzip
import os
import shutil
import socket
import sys
import tempfile
from pathlib import Path

from google.api_core.exceptions import GoogleAPICallError
from google.cloud import storage

from src.broker.backup import (
    LIST_TIMEOUT_SECONDS,
    OBJECT_SUFFIX,
    UPLOAD_TIMEOUT_SECONDS,
    BackupError,
    parse_rdb_header,
)
from src.broker.logging import configure_logging, get_logger

logger = get_logger("src.broker.restore")

APPENDONLY_DIRNAME = "appendonlydir"
BASE_FILE = "appendonly.aof.1.base.rdb"
INCR_FILE = "appendonly.aof.1.incr.aof"
MANIFEST_FILE = "appendonly.aof.manifest"
# Exactly what a 7.0 server writes for a freshly enabled AOF, with the base
# swapped for the snapshot. One entry per line and a trailing newline; the
# manifest parser is strict about both.
MANIFEST = f"file {BASE_FILE} seq 1 type b\nfile {INCR_FILE} seq 1 type i\n"
DEFAULT_REDIS_DIR = Path("/var/lib/redis")


class RestoreError(Exception):
    """Anything that means nothing was staged."""


def stage_appendonlydir(rdb: Path, redis_dir: Path) -> Path:
    """Write the three files under ``redis_dir/appendonlydir``; return that path.

    Refuses to touch an existing directory: the server that owns it may still
    hold data, and moving it aside is the operator's decision (docs/RECOVERY.md).
    The RDB is copied, never moved - the backup file stays where it was.
    """
    with rdb.open("rb") as handle:
        parse_rdb_header(handle.read(9))
    target = redis_dir / APPENDONLY_DIRNAME
    if target.exists():
        raise RestoreError(f"{target} exists; refusing to overwrite a directory that may hold data")
    target.mkdir(parents=True)
    shutil.copyfile(rdb, target / BASE_FILE)
    (target / INCR_FILE).write_bytes(b"")
    (target / MANIFEST_FILE).write_text(MANIFEST)
    return target


def list_objects(client: storage.Client, bucket: str, prefix: str) -> list[str]:
    """Snapshot names under ``prefix``, newest first - the names sort by time."""
    blobs = client.list_blobs(bucket, prefix=f"{prefix.strip('/')}/", timeout=LIST_TIMEOUT_SECONDS)
    return sorted((b.name for b in blobs if b.name.endswith(OBJECT_SUFFIX)), reverse=True)


def newest_object(client: storage.Client, bucket: str, prefix: str) -> str | None:
    names = list_objects(client, bucket, prefix)
    return names[0] if names else None


def download(client: storage.Client, bucket: str, name: str, dest: Path) -> Path:
    client.bucket(bucket).blob(name).download_to_filename(str(dest), timeout=UPLOAD_TIMEOUT_SECONDS)
    return dest


def gunzip_file(src: Path, dst: Path) -> None:
    with gzip.open(src, "rb") as inp, dst.open("wb") as out:
        shutil.copyfileobj(inp, out)


def _next_steps(redis_dir: Path, source: str) -> str:
    staged = redis_dir / APPENDONLY_DIRNAME
    return (
        f"staged {source}\n"
        f"  as {staged / BASE_FILE}\n"
        "next, as root - Redis must own what it will write to:\n"
        f"  chown -R redis:redis {staged}\n"
        f"  chmod 0750 {staged} && chmod 0640 {staged}/*\n"
        "  systemctl start redis-server\n"
        "then verify the positions, not just the key count - docs/RECOVERY.md\n"
    )


def _stage(rdb: Path, into: Path, source: str) -> int:
    stage_appendonlydir(rdb, into)
    print(_next_steps(into, source))
    return 0


def _stage_local(file: Path, into: Path) -> int:
    if file.suffix != ".gz":
        return _stage(file, into, str(file))
    with tempfile.TemporaryDirectory(prefix="broker-restore-") as work:
        rdb = Path(work) / "snapshot.rdb"
        gunzip_file(file, rdb)
        return _stage(rdb, into, str(file))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="stage a broker snapshot for Redis to load")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--latest", action="store_true", help="the newest snapshot under the prefix"
    )
    source.add_argument("--object", help="one object name under the bucket")
    source.add_argument("--file", type=Path, help="a local .rdb or .rdb.gz")
    source.add_argument("--list", action="store_true", help="print the snapshots, newest first")
    parser.add_argument("--into", type=Path, default=DEFAULT_REDIS_DIR, help="Redis's `dir`")
    parser.add_argument("--bucket", default=os.environ.get("BROKER_BACKUP_BUCKET"))
    parser.add_argument(
        "--prefix", default=os.environ.get("BROKER_BACKUP_PREFIX") or socket.gethostname()
    )
    args = parser.parse_args(argv)

    configure_logging()

    try:
        if args.file is not None:
            return _stage_local(args.file, args.into)
        if not args.bucket:
            logger.error("no bucket: pass --bucket or set BROKER_BACKUP_BUCKET")
            return 2
        client = storage.Client()
        if args.list:
            for name in list_objects(client, args.bucket, args.prefix):
                print(f"gs://{args.bucket}/{name}")
            return 0
        name = args.object or newest_object(client, args.bucket, args.prefix)
        if name is None:
            logger.error(f"no snapshots under gs://{args.bucket}/{args.prefix.strip('/')}/")
            return 1
        with tempfile.TemporaryDirectory(prefix="broker-restore-") as work:
            gz = download(client, args.bucket, name, Path(work) / "snapshot.rdb.gz")
            rdb = Path(work) / "snapshot.rdb"
            gunzip_file(gz, rdb)
            return _stage(rdb, args.into, f"gs://{args.bucket}/{name}")
    except (BackupError, RestoreError, GoogleAPICallError, OSError) as exc:
        logger.error(f"restore failed: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
