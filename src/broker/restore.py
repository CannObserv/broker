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

``--digests /etc/redis/broker-acl-passwords`` is the admission half
(CannObserv/broker#72): the newest ACL digests the backup shipped, whichever
snapshot is staged. Rolling data back is not rolling back who is admitted.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import os
import shutil
import socket
import sys
import tempfile
from pathlib import Path

from google.api_core.exceptions import GoogleAPICallError
from google.cloud import storage

from src.broker.backup import (
    DIGESTS_SUFFIX,
    LIST_TIMEOUT_SECONDS,
    OBJECT_SUFFIX,
    UPLOAD_TIMEOUT_SECONDS,
    BackupError,
    parse_rdb_header,
    project_digests,
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
# The passwords file render-acl.sh reads: root's alone, as the install makes it.
DIGESTS_FILE_MODE = 0o400
# What `--list` prints beside each name, in this order.
_LISTED_METADATA = ("snapshot_at", "keys", "size_bytes", "redis_version", "sha256")


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


def describe_objects(client: storage.Client, bucket: str, prefix: str) -> list[tuple[str, dict]]:
    """Snapshots under ``prefix`` as ``(name, metadata)``, newest first.

    A listing returns each object's metadata with it, so this is one request
    under ``objectViewer`` - which is what lets an operator on the node, which
    has no gcloud and an identity that cannot read bucket metadata, see a
    snapshot's ``sha256`` and ``keys`` before staging it and check the staged
    base against them after.
    """
    blobs = client.list_blobs(bucket, prefix=f"{prefix.strip('/')}/", timeout=LIST_TIMEOUT_SECONDS)
    described = [(b.name, dict(b.metadata or {})) for b in blobs if b.name.endswith(OBJECT_SUFFIX)]
    return sorted(described, key=lambda pair: pair[0], reverse=True)


def list_objects(client: storage.Client, bucket: str, prefix: str) -> list[str]:
    """Snapshot names under ``prefix``, newest first - the names sort by time."""
    return [name for name, _meta in describe_objects(client, bucket, prefix)]


def newest_object(client: storage.Client, bucket: str, prefix: str) -> str | None:
    names = list_objects(client, bucket, prefix)
    return names[0] if names else None


def newest_digests(client: storage.Client, bucket: str, prefix: str) -> tuple[str, dict] | None:
    """The last run's digests object under ``prefix``, with its metadata. Each
    run creates one named by its time, so that is the greatest name."""
    blobs = client.list_blobs(bucket, prefix=f"{prefix.strip('/')}/", timeout=LIST_TIMEOUT_SECONDS)
    described = [(b.name, dict(b.metadata or {})) for b in blobs if b.name.endswith(DIGESTS_SUFFIX)]
    return max(described, default=None)


def write_digests(body: bytes, dest: Path, *, expected_sha256: str | None) -> None:
    """Create ``dest`` 0400 holding ``body``, or refuse and write nothing.

    Refused: an existing file (it may be the better copy - appendonlydir's
    rule), bytes that are not what the object's metadata says was shipped, and
    anything the backup itself would not ship. The bucket is not trusted to
    have been written by this job alone, so a plaintext or a node user's line
    stops here rather than in ``/etc/redis``.
    """
    if expected_sha256 and hashlib.sha256(body).hexdigest() != expected_sha256:
        raise RestoreError("download does not match the sha256 the backup recorded")
    try:
        text = body.decode()
        if project_digests(text) != text:
            raise RestoreError("not service digest lines alone, as the backup ships them")
    except (UnicodeDecodeError, BackupError) as exc:
        raise RestoreError(f"not a digests file the backup ships: {exc}") from exc
    try:
        fd = os.open(dest, os.O_WRONLY | os.O_CREAT | os.O_EXCL, DIGESTS_FILE_MODE)
    except FileExistsError as exc:
        raise RestoreError(f"{dest} exists; refusing to overwrite it") from exc
    with os.fdopen(fd, "wb") as out:
        out.write(body)
    os.chmod(dest, DIGESTS_FILE_MODE)


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


def _digests_next_steps(dest: Path, source: str, users: str) -> str:
    return (
        f"wrote {source}\n"
        f"  to {dest} - {users or 'users not recorded'}\n"
        "next, as root, before the first render - the lines a rebuild mints, not restores:\n"
        "  default's tombstone, the digest of a value nobody keeps:\n"
        "    printf '__DEFAULT_PW_SHA256__=%s\\n' \"$(LC_ALL=C tr -dc A-Za-z0-9 </dev/urandom"
        f" | head -c 40 | sha256sum | cut -d' ' -f1)\" >> {dest}\n"
        "  acladmin and brokeradmin: docs/NODE-CREDENTIALS.md, 'On a new or rebuilt node'\n"
        "then render and install users.acl - deploy/README.md, 'Installing the ACL users'\n"
    )


def _restore_digests(client: storage.Client, bucket: str, prefix: str, dest: Path) -> int:
    found = newest_digests(client, bucket, prefix)
    if found is None:
        logger.error(f"no ACL digests under gs://{bucket}/{prefix.strip('/')}/")
        return 1
    name, meta = found
    with tempfile.TemporaryDirectory(prefix="broker-restore-") as work:
        body = download(client, bucket, name, Path(work) / "acl.digests").read_bytes()
    write_digests(body, dest, expected_sha256=meta.get("sha256"))
    print(_digests_next_steps(dest, f"gs://{bucket}/{name}", meta.get("users", "")))
    return 0


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
    source.add_argument(
        "--digests",
        type=Path,
        metavar="PATH",
        help="write the newest ACL digests to PATH (the passwords file); never overwrites",
    )
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
        if args.digests is not None:
            return _restore_digests(client, args.bucket, args.prefix, args.digests)
        if args.list:
            for name, meta in describe_objects(client, args.bucket, args.prefix):
                described = "  ".join(f"{k}={meta[k]}" for k in _LISTED_METADATA if meta.get(k))
                print(f"gs://{args.bucket}/{name}  {described}".rstrip())
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
