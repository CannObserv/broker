"""Ship the broker's RDB snapshot to a bucket, hourly (CannObserv/broker#4).

``/var/lib/redis`` is the only copy of every stream, every consumer group's
position and PEL, and replicator's expiring command state. ``appendonly yes``
protects against a process restart, not against losing the disk. This job puts
a copy of the server's own snapshot off-node, and it is built to be dumb in the
ways that keep a backup honest:

- **It reads a file and holds no Redis credential.** ``dump.rdb`` is rewritten
  by the server at its ``save`` points, atomically (a temp file, then a
  rename), so a copy taken at any moment is a consistent point in time. The job
  never asks the broker for anything: no ACL user exists for it, and nothing it
  does can be mistaken for a client.
- **It verifies before it ships.** The private copy goes through
  ``redis-check-rdb`` - the format, every record, the CRC64 trailer - and a file
  that fails is refused, never uploaded under a name that says snapshot.
- **The object is named by the snapshot's own time**, the ``ctime`` the server
  wrote into the file, so a listing reads as a timeline and the newest name is
  the newest data. Shipping the same snapshot twice is a 412 on the create's
  precondition and reported as ``unchanged`` - a success.
- **It creates, and never overwrites or deletes.** ``if_generation_match=0`` in
  code; ``objectCreator`` + ``objectViewer`` and no ``delete`` at IAM. Retention
  is the bucket's lifecycle rule, so a compromised node cannot destroy its own
  history. The same property as replicator's replicate writer, for the same
  reason.
- **Failure is loud.** A oneshot that exits 0 after shipping nothing is the
  silent backup every incident write-up warns about. A failed run is a failed
  unit *and* a line in the state file the probe reads
  (``bus_health.evaluate_backup``), which is how a bucket typo becomes a
  notifier alert within the hour rather than a discovery during a restore.

Restore is ``src.broker.restore``; docs/RECOVERY.md is the runbook around both.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from google.api_core.exceptions import NotFound, PreconditionFailed
from google.cloud import storage

from src.broker.logging import configure_logging, get_logger

# Literal rather than __name__: the timer runs this module via ``python -m``.
logger = get_logger("src.broker.backup")

RDB_MAGIC = b"REDIS"
OBJECT_SUFFIX = ".rdb.gz"
# Basic-format ISO 8601, UTC. Sorts as it reads, and no ':' for a shell to trip on.
KEY_TIME_FORMAT = "%Y%m%dT%H%M%SZ"
CONTENT_TYPE = "application/gzip"
DEFAULT_RDB = Path("/var/lib/redis/dump.rdb")

# Bounds on the two network calls. A snapshot is under a megabyte today and
# the maxmemory cap bounds it at a few hundred; two minutes is generous for the
# upload and short enough that a wedged one is a failed unit rather than a hang.
UPLOAD_TIMEOUT_SECONDS = 120.0
LIST_TIMEOUT_SECONDS = 30.0

CHECKER = "redis-check-rdb"
# Read by the probe, which runs as another user.
STATE_FILE_MODE = 0o644

# The two structured lines in redis-check-rdb's report:
#   [offset 27] AUX FIELD redis-ver = '7.0.15'
#   [info] 2 keys read
_AUX_RE = re.compile(r"AUX FIELD (\S+) = '([^']*)'")
_KEYS_RE = re.compile(r"\[info\] (\d+) keys read")


class BackupError(Exception):
    """Anything that means the snapshot was not shipped."""


@dataclass(frozen=True)
class Snapshot:
    """A verified private copy of the server's RDB, and what it says about itself."""

    path: Path
    size_bytes: int
    sha256: str
    snapshot_at: datetime
    rdb_version: int
    redis_version: str | None
    keys: int | None


# --- pure ---


def parse_rdb_header(head: bytes) -> int:
    """The RDB format version from the file's first nine bytes, or a refusal.

    ``REDIS`` then four ASCII digits - ``REDIS0010`` is Redis 7.0. Checked
    before the checker runs, so a wrong ``--rdb`` (a text file, an AOF, an
    empty path) is refused by name rather than by a subprocess's stderr.
    """
    if len(head) < 9 or not head.startswith(RDB_MAGIC) or not head[5:9].isdigit():
        raise BackupError(f"not an RDB file: header {head[:9]!r}")
    return int(head[5:9])


def parse_check_report(text: str) -> dict[str, str]:
    """The AUX fields and the key count out of ``redis-check-rdb``'s report."""
    report = dict(_AUX_RE.findall(text))
    keys = _KEYS_RE.search(text)
    if keys:
        report["keys"] = keys.group(1)
    return report


def snapshot_time(report: dict[str, str], *, mtime: float) -> datetime:
    """When the snapshot was taken: the ``ctime`` the server wrote into the
    file, else the file's mtime. ``ctime`` survives a copy made without ``-p``
    and a restore; an mtime survives neither."""
    raw = report.get("ctime")
    seconds = float(raw) if raw and raw.isdigit() else mtime
    return datetime.fromtimestamp(seconds, tz=UTC).replace(microsecond=0)


def object_key(prefix: str, snapshot_at: datetime) -> str:
    stamp = snapshot_at.astimezone(UTC).strftime(KEY_TIME_FORMAT)
    return f"{prefix.strip('/')}/{stamp}{OBJECT_SUFFIX}"


def iso(at: datetime) -> str:
    """ISO 8601, UTC, second precision, ``Z``: the form the state file and the
    object metadata carry, and the one ``bus_health`` parses back."""
    return at.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


# --- effects on the file ---


def check_rdb(path: Path) -> str:
    """Run ``redis-check-rdb`` on the private copy and return its report.

    The checker walks every record and verifies the CRC64 trailer, which is the
    one thing a header check cannot see. A non-zero exit refuses the backup,
    and so does a missing binary: shipping an unverified file under a name that
    says snapshot is worse than shipping nothing and saying so.
    """
    binary = shutil.which(CHECKER)
    if binary is None:
        raise BackupError(f"{CHECKER} not installed; refusing to ship an unverified snapshot")
    result = subprocess.run([binary, str(path)], capture_output=True, text=True, check=False)
    if result.returncode != 0:
        tail = (result.stdout + result.stderr).strip().splitlines()[-3:]
        raise BackupError(f"{CHECKER} rejected {path.name}: {' | '.join(tail)}")
    return result.stdout


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def gzip_file(src: Path, dst: Path) -> int:
    """Stream-compress ``src`` into ``dst``; return the bytes written. Streamed
    so the job's memory does not scale with the snapshot."""
    with src.open("rb") as inp, gzip.open(dst, "wb") as out:
        shutil.copyfileobj(inp, out)
    return dst.stat().st_size


def take_snapshot(
    rdb: Path, workdir: Path, *, checker: Callable[[Path], str] = check_rdb
) -> Snapshot:
    """Copy the live file into ``workdir``, verify the copy, describe it.

    The copy is what everything downstream touches. ``shutil.copyfile`` holds
    the source open for the duration, so if the server renames a fresh
    ``dump.rdb`` into place mid-copy the old inode is read to its end - a
    complete snapshot either way, never a torn one.
    """
    with rdb.open("rb") as handle:
        rdb_version = parse_rdb_header(handle.read(9))
    source_mtime = rdb.stat().st_mtime
    workdir.mkdir(parents=True, exist_ok=True)
    copy = workdir / "snapshot.rdb"
    shutil.copyfile(rdb, copy)
    report = parse_check_report(checker(copy))
    keys = report.get("keys")
    return Snapshot(
        path=copy,
        size_bytes=copy.stat().st_size,
        sha256=sha256_file(copy),
        snapshot_at=snapshot_time(report, mtime=source_mtime),
        rdb_version=rdb_version,
        redis_version=report.get("redis-ver"),
        keys=int(keys) if keys is not None and keys.isdigit() else None,
    )


# --- effects on the bucket ---


def preflight(client: storage.Client, bucket: str, prefix: str) -> None:
    """Prove the bucket is there and listable by this identity.

    A one-object listing, not ``exists()``: the SDK swallows the 404 a missing
    bucket answers with and returns ``False``, which is exactly the
    misconfiguration this check exists to catch (replicator#7 CR #1). The
    listing is lazy, so the iterator is advanced or no request is made at all.
    ``storage.objects.list`` is in ``objectViewer``; this widens no grant.
    """
    listing = client.list_blobs(
        bucket, max_results=1, prefix=f"{prefix.strip('/')}/", timeout=LIST_TIMEOUT_SECONDS
    )
    try:
        next(iter(listing), None)
    except NotFound as exc:
        raise BackupError(
            f"bucket {bucket!r} not found, or not listable by this identity: {exc}"
        ) from exc


def upload(
    client: storage.Client, bucket: str, key: str, gz: Path, snapshot: Snapshot, *, host: str
) -> str:
    """Create the object, or learn that this snapshot is already there.

    ``if_generation_match=0`` makes it a create and never a put. The already-
    there case arrives as a 412, and it is a success: the object at that name
    holds these bytes by construction, because the name is the snapshot time.
    The metadata is what a restore reads without downloading anything.
    """
    blob = client.bucket(bucket).blob(key)
    blob.metadata = {
        "snapshot_at": iso(snapshot.snapshot_at),
        "sha256": snapshot.sha256,
        "size_bytes": str(snapshot.size_bytes),
        "rdb_version": str(snapshot.rdb_version),
        "redis_version": snapshot.redis_version or "",
        "keys": "" if snapshot.keys is None else str(snapshot.keys),
        "source_host": host,
    }
    try:
        blob.upload_from_filename(
            str(gz),
            content_type=CONTENT_TYPE,
            if_generation_match=0,
            timeout=UPLOAD_TIMEOUT_SECONDS,
        )
    except PreconditionFailed:
        return "unchanged"
    return "uploaded"


# --- state file (read by the probe) ---


def load_backup_state(path: Path) -> dict:
    try:
        raw = json.loads(path.read_text())
    except (FileNotFoundError, ValueError):
        return {}
    return raw if isinstance(raw, dict) else {}


def save_backup_state(path: Path, state: dict) -> None:
    """Atomic and world-readable. The probe reads this as another user, and a
    half-written file would read as corrupt state - which it reports as never
    having backed up, loudly, so the write must not be interruptible."""
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")
    os.chmod(tmp, STATE_FILE_MODE)
    os.replace(tmp, path)


def record_failure(state_path: Path, error: str, *, at: datetime) -> None:
    """A failure never erases the last success - the probe's staleness rule
    needs it to stay visible through a run of failures - but ``outcome`` is
    the last RUN's, so it must not keep saying ``uploaded`` from the hour
    before. An operator reading the file after a failed start would otherwise
    take the previous success for this run's."""
    state = load_backup_state(state_path)
    state.update({"outcome": "failed", "last_failure_at": iso(at), "last_error": error})
    save_backup_state(state_path, state)


# --- orchestration ---


def run_backup(
    *,
    rdb: Path,
    bucket: str,
    prefix: str,
    client: storage.Client,
    state_path: Path,
    workdir: Path,
    checker: Callable[[Path], str] = check_rdb,
    host: str | None = None,
    now: datetime | None = None,
) -> dict:
    """One run: snapshot, verify, compress, preflight, create; record the result.

    Returns the state written. Raises ``BackupError`` after recording a
    failure, so the unit fails and the probe can say why.
    """
    host = host or socket.gethostname()
    at = now or datetime.now(UTC)
    try:
        snapshot = take_snapshot(rdb, workdir, checker=checker)
        key = object_key(prefix, snapshot.snapshot_at)
        gz = workdir / "snapshot.rdb.gz"
        gzip_bytes = gzip_file(snapshot.path, gz)
        preflight(client, bucket, prefix)
        outcome = upload(client, bucket, key, gz, snapshot, host=host)
    except Exception as exc:
        # Broad on purpose. A revoked key surfaces from google.auth as a
        # RefreshError and a transport fault as a TransportError - neither a
        # GoogleAPICallError nor an OSError - and whatever the type, the run
        # failed and the probe must be able to say so from the state file. A
        # traceback in the journal with no record is the silent backup again,
        # by a narrower door. Recorded, then re-raised as the one type main()
        # maps to a failed unit.
        error = f"{type(exc).__name__}: {exc}"
        record_failure(state_path, error, at=at)
        logger.error(f"Backup failed: {error}", extra={"rdb": str(rdb), "bucket": bucket})
        raise BackupError(error) from exc

    state = load_backup_state(state_path)
    state.update(
        {
            "last_success_at": iso(at),
            "outcome": outcome,
            "object": f"gs://{bucket}/{key}",
            "snapshot_at": iso(snapshot.snapshot_at),
            "size_bytes": snapshot.size_bytes,
            "gzip_bytes": gzip_bytes,
            "sha256": snapshot.sha256,
            "rdb_version": snapshot.rdb_version,
            "redis_version": snapshot.redis_version,
            "keys": snapshot.keys,
            "source_host": host,
        }
    )
    save_backup_state(state_path, state)
    logger.info(
        f"Backup {outcome}: {state['object']}",
        extra={
            "outcome": outcome,
            "snapshot_at": state["snapshot_at"],
            "size_bytes": snapshot.size_bytes,
            "gzip_bytes": gzip_bytes,
            "keys": snapshot.keys,
        },
    )
    return state


def main(argv: list[str] | None = None) -> int:
    """Timer entrypoint. Exit 0 only when the snapshot is in the bucket (or was
    already); anything else is a failed unit, on purpose."""
    parser = argparse.ArgumentParser(description="broker RDB snapshot to GCS")
    parser.add_argument("--rdb", type=Path, default=DEFAULT_RDB)
    parser.add_argument("--state-file", type=Path, required=True)
    args = parser.parse_args(argv)

    configure_logging()

    bucket = os.environ.get("BROKER_BACKUP_BUCKET")
    if not bucket:
        # No default bucket. Guessing one is how bytes land somewhere nobody
        # reads - the rule replicator applies to REPLICATOR_BLOB_BUCKET.
        logger.error("BROKER_BACKUP_BUCKET not set - nowhere to ship the snapshot")
        return 2
    host = socket.gethostname()
    prefix = os.environ.get("BROKER_BACKUP_PREFIX") or host

    with tempfile.TemporaryDirectory(prefix="broker-backup-") as work:
        try:
            # Built here so a missing or unreadable key fails before the
            # snapshot is taken, and is recorded like any other failure.
            client = storage.Client()
        except Exception as exc:  # google.auth raises its own hierarchy
            error = f"{type(exc).__name__}: {exc}"
            record_failure(args.state_file, error, at=datetime.now(UTC))
            logger.error(f"Backup failed before it could start: {error}")
            return 1
        try:
            run_backup(
                rdb=args.rdb,
                bucket=bucket,
                prefix=prefix,
                client=client,
                state_path=args.state_file,
                workdir=Path(work),
                host=host,
            )
        except BackupError:
            return 1  # recorded and logged where it happened
    return 0


if __name__ == "__main__":
    sys.exit(main())
