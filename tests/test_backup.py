"""The RDB backup job (CannObserv/broker#4).

Every decision here runs against a fake client: which key, which precondition,
what a snapshot is called, what the job refuses to upload, what it writes for
the probe to read. None of that needs a network to be wrong. What a fake cannot
answer - whether the SDK accepts these arguments and whether the unit's sandbox
lets it reach the bucket - is answered by the first real run, recorded in
docs/RECOVERY.md.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import shutil
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.broker import backup, bus_health
from src.broker.backup import (
    OBJECT_SUFFIX,
    BackupError,
    check_rdb,
    gzip_file,
    object_key,
    parse_check_report,
    parse_rdb_header,
    run_backup,
    snapshot_time,
    take_snapshot,
)
from tests.gcs_fakes import FakeBucket, FakeClient

# A syntactically valid RDB header, one aux field, EOF, and a WRONG checksum (a
# zero trailer means "checksum disabled" and the checker would wave it through).
# Enough for every pure check, and rejected by the real one - a test relies on that.
RDB_HEADER = b"REDIS0010"
FAKE_RDB = RDB_HEADER + b"\xfa\x09redis-ver\x066.2.99" + b"\xff" + b"\x01" * 8

# What redis-check-rdb printed for a real 7.0.15 snapshot on the node, verbatim.
CHECK_REPORT = """\
[offset 0] Checking RDB file backup.rdb
[offset 27] AUX FIELD redis-ver = '7.0.15'
[offset 41] AUX FIELD redis-bits = '64'
[offset 53] AUX FIELD ctime = '1789054988'
[offset 68] AUX FIELD used-mem = '764592'
[offset 80] AUX FIELD aof-base = '0'
[offset 82] Selecting DB ID 0
[offset 284] Checksum OK
[offset 284] \\o/ RDB looks OK! \\o/
[info] 2 keys read
[info] 1 expires
[info] 0 already expired
"""

SNAPSHOT_AT = datetime.fromtimestamp(1789054988, tz=UTC)
NOW = SNAPSHOT_AT + timedelta(minutes=10)


def _fake_checker(report: str = CHECK_REPORT) -> MagicMock:
    return MagicMock(return_value=report)


@pytest.fixture
def rdb(tmp_path) -> Path:
    path = tmp_path / "dump.rdb"
    path.write_bytes(FAKE_RDB)
    return path


@pytest.fixture
def bucket() -> FakeBucket:
    return FakeBucket("a-backup-bucket")


@pytest.fixture
def client(bucket) -> FakeClient:
    return FakeClient(bucket)


def _run(rdb: Path, client: FakeClient, tmp_path: Path, **overrides) -> dict:
    kwargs = dict(
        rdb=rdb,
        bucket="a-backup-bucket",
        prefix="co-broker",
        client=client,
        state_path=tmp_path / "state" / "state.json",
        workdir=tmp_path / "work",
        checker=_fake_checker(),
        host="co-broker",
        now=NOW,
    )
    kwargs.update(overrides)
    kwargs["state_path"].parent.mkdir(exist_ok=True)
    return run_backup(**kwargs)


def _state(tmp_path: Path) -> dict:
    return json.loads((tmp_path / "state" / "state.json").read_text())


# --- what a snapshot is ---


def test_parse_rdb_header_reads_the_format_version() -> None:
    assert parse_rdb_header(RDB_HEADER + b"anything") == 10


@pytest.mark.parametrize(
    "head", [b"", b"REDIS", b"REDIS00", b"REDIS00xx", b"NOTRDB0010", b"redis0010"]
)
def test_parse_rdb_header_refuses_anything_that_is_not_an_rdb(head: bytes) -> None:
    with pytest.raises(BackupError):
        parse_rdb_header(head)


def test_parse_check_report_extracts_the_aux_fields_and_key_count() -> None:
    report = parse_check_report(CHECK_REPORT)
    assert report["redis-ver"] == "7.0.15"
    assert report["ctime"] == "1789054988"
    assert report["keys"] == "2"


def test_snapshot_time_is_the_ctime_the_server_wrote_into_the_file() -> None:
    """``ctime`` is stamped by the save itself, so it survives a copy without
    ``-p``, a restore, and a clock that was wrong when the file was read.
    ``mtime`` is the fallback for a report that carries none."""
    assert snapshot_time({"ctime": "1789054988"}, mtime=0.0) == SNAPSHOT_AT
    assert snapshot_time({}, mtime=1789054988.0) == SNAPSHOT_AT


def test_object_key_is_the_snapshot_time_under_the_host_prefix() -> None:
    assert object_key("co-broker", SNAPSHOT_AT) == "co-broker/20260910T154308Z.rdb.gz"


def test_object_keys_sort_chronologically() -> None:
    """``newest`` on the restore side is ``max()`` over names, so the name has
    to carry the order - which a basic-format UTC timestamp does and a
    friendlier one with locale-dependent pieces would not."""
    times = [SNAPSHOT_AT.replace(hour=h) for h in (23, 1, 12)]
    keys = [object_key("p", t) for t in times]
    assert sorted(keys) == [object_key("p", t) for t in sorted(times)]
    assert all(k.endswith(OBJECT_SUFFIX) for k in keys)


# --- taking the snapshot ---


def test_take_snapshot_copies_verifies_and_describes(rdb, tmp_path) -> None:
    checker = _fake_checker()
    snap = take_snapshot(rdb, tmp_path / "work", checker=checker)

    assert snap.path != rdb
    assert snap.path.read_bytes() == FAKE_RDB
    # Verified on the private copy, never on the file Redis is writing.
    checker.assert_called_once_with(snap.path)
    assert snap.size_bytes == len(FAKE_RDB)
    assert snap.sha256 == hashlib.sha256(FAKE_RDB).hexdigest()
    assert snap.snapshot_at == SNAPSHOT_AT
    assert snap.rdb_version == 10
    assert snap.redis_version == "7.0.15"
    assert snap.keys == 2


def test_take_snapshot_refuses_a_file_that_is_not_an_rdb(tmp_path) -> None:
    """Refused before the checker runs and before anything is uploaded: a wrong
    ``--rdb`` must not ship a text file under a name that says snapshot."""
    not_rdb = tmp_path / "dump.rdb"
    not_rdb.write_bytes(b"this is not a snapshot")
    checker = _fake_checker()
    with pytest.raises(BackupError):
        take_snapshot(not_rdb, tmp_path / "work", checker=checker)
    checker.assert_not_called()


def test_take_snapshot_refuses_when_the_checker_does(rdb, tmp_path) -> None:
    checker = MagicMock(side_effect=BackupError("checksum mismatch"))
    with pytest.raises(BackupError):
        take_snapshot(rdb, tmp_path / "work", checker=checker)


@pytest.mark.skipif(shutil.which("redis-check-rdb") is None, reason="redis-check-rdb not installed")
def test_the_real_checker_rejects_a_wrong_checksum(rdb) -> None:
    """The seam, exercised against the binary on the one input every other test
    treats as valid. If the checker accepted this, the ``checker`` argument
    would be decoration."""
    with pytest.raises(BackupError):
        check_rdb(rdb)


def test_gzip_round_trips(rdb, tmp_path) -> None:
    out = tmp_path / "dump.rdb.gz"
    written = gzip_file(rdb, out)
    assert written == out.stat().st_size > 0
    assert gzip.decompress(out.read_bytes()) == FAKE_RDB


# --- the job ---


def test_run_backup_uploads_a_new_snapshot_as_a_create(rdb, client, bucket, tmp_path) -> None:
    state = _run(rdb, client, tmp_path)

    key = "co-broker/20260910T154308Z.rdb.gz"
    assert list(bucket.objects) == [key]
    assert gzip.decompress(bucket.objects[key]) == FAKE_RDB
    # A create, never a put. The identity holds no delete, so this is also the
    # only shape the bucket would accept; the precondition says so in code.
    assert bucket.preconditions == [0]
    assert bucket.content_types[key] == "application/gzip"
    assert bucket.timeouts == [backup.UPLOAD_TIMEOUT_SECONDS]
    assert state["outcome"] == "uploaded"
    assert state["object"] == f"gs://a-backup-bucket/{key}"


def test_run_backup_describes_the_snapshot_in_object_metadata(
    rdb, client, bucket, tmp_path
) -> None:
    """The restore side reads these without downloading anything: which server
    wrote it, when, how many keys, and the digest to verify the download."""
    _run(rdb, client, tmp_path)
    (meta,) = bucket.metadata.values()
    assert meta["snapshot_at"] == "2026-09-10T15:43:08Z"
    assert meta["sha256"] == hashlib.sha256(FAKE_RDB).hexdigest()
    assert meta["redis_version"] == "7.0.15"
    assert meta["rdb_version"] == "10"
    assert meta["keys"] == "2"
    assert meta["source_host"] == "co-broker"


def test_run_backup_preflights_the_bucket_with_a_listing(rdb, client, tmp_path) -> None:
    """A one-object listing, not an existence check: ``Blob.exists()`` swallows
    the 404 a missing bucket answers with (replicator#7 CR #1)."""
    _run(rdb, client, tmp_path)
    assert client.listings == [
        {"max_results": 1, "prefix": "co-broker/", "timeout": backup.LIST_TIMEOUT_SECONDS}
    ]


def test_run_backup_reports_unchanged_when_this_snapshot_is_already_there(
    rdb, client, bucket, tmp_path
) -> None:
    """Redis rewrites dump.rdb only at a ``save`` point, so an hourly job will
    often find the file it shipped last time. The key is the snapshot time, the
    create's precondition answers 412, and that is a success: the object is
    there. Not an error, and not a second object."""
    first = _run(rdb, client, tmp_path)
    second = _run(rdb, client, tmp_path, now=NOW + timedelta(hours=1))

    assert len(bucket.objects) == 1
    assert second["outcome"] == "unchanged"
    assert second["object"] == first["object"]
    assert second["snapshot_at"] == first["snapshot_at"]
    assert second["last_success_at"] > first["last_success_at"]


def test_run_backup_writes_state_the_probe_can_read(rdb, client, tmp_path) -> None:
    _run(rdb, client, tmp_path)
    on_disk = _state(tmp_path)
    assert on_disk["last_success_at"] == "2026-09-10T15:53:08Z"
    assert on_disk["snapshot_at"] == "2026-09-10T15:43:08Z"
    assert on_disk["size_bytes"] == len(FAKE_RDB)
    assert on_disk["sha256"] == hashlib.sha256(FAKE_RDB).hexdigest()
    assert on_disk["keys"] == 2
    # The unit runs as root; the probe runs as exedev. World-readable is the contract.
    assert stat.S_IMODE((tmp_path / "state" / "state.json").stat().st_mode) == 0o644


def test_the_probe_accepts_the_state_the_job_writes(rdb, client, tmp_path) -> None:
    """The one cross-module contract: the keys written here are the keys
    ``evaluate_backup`` reads. Fresh on both clocks is healthy."""
    state = _run(rdb, client, tmp_path)
    assert bus_health.evaluate_backup(state, now=NOW) == []


def test_run_backup_fails_loudly_on_a_missing_bucket_and_records_it(rdb, bucket, tmp_path) -> None:
    """A misspelled BROKER_BACKUP_BUCKET must be a failed unit *and* a line in
    the state the probe reads - not a job that ran and shipped nothing."""
    with pytest.raises(BackupError, match="a-backup-bucket"):
        _run(rdb, FakeClient(bucket, missing=True), tmp_path)
    state = _state(tmp_path)
    assert "last_success_at" not in state
    assert state["last_failure_at"] == "2026-09-10T15:53:08Z"
    assert "not found" in state["last_error"]
    assert state["outcome"] == "failed"  # the last RUN, not the last success
    assert bucket.objects == {}


def test_a_failure_keeps_the_previous_success_on_record(rdb, bucket, tmp_path) -> None:
    """The probe's staleness rule needs the last good backup to stay visible
    through a run of failures; overwriting it would turn "stale since 03:00"
    into "never backed up"."""
    good = _run(rdb, FakeClient(bucket), tmp_path)
    with pytest.raises(BackupError):
        _run(rdb, FakeClient(bucket, missing=True), tmp_path, now=NOW + timedelta(hours=1))
    state = _state(tmp_path)
    assert state["last_success_at"] == good["last_success_at"]
    assert state["object"] == good["object"]
    assert state["outcome"] == "failed"
    assert state["last_failure_at"] > state["last_success_at"]


def test_run_backup_refuses_to_upload_a_corrupt_snapshot(client, bucket, tmp_path) -> None:
    bad = tmp_path / "dump.rdb"
    bad.write_bytes(b"not an rdb at all")
    with pytest.raises(BackupError):
        _run(bad, client, tmp_path)
    assert bucket.objects == {}
    assert client.listings == []  # refused before the bucket was touched
    assert "last_error" in _state(tmp_path)


# --- the entrypoint ---


@pytest.fixture
def stub_main(monkeypatch, tmp_path, rdb):
    """Neutralise everything main() touches outside the job itself."""
    monkeypatch.setattr(backup, "configure_logging", lambda: None)
    run = MagicMock(return_value={"outcome": "uploaded"})
    monkeypatch.setattr(backup, "run_backup", run)
    client_factory = MagicMock(return_value=object())
    monkeypatch.setattr(backup.storage, "Client", client_factory)
    monkeypatch.setenv("BROKER_BACKUP_BUCKET", "a-backup-bucket")
    monkeypatch.delenv("BROKER_BACKUP_PREFIX", raising=False)
    return SimpleNamespace(
        run=run,
        client_factory=client_factory,
        argv=["--rdb", str(rdb), "--state-file", str(tmp_path / "state.json")],
    )


def test_main_needs_a_bucket_and_will_not_guess_one(stub_main, monkeypatch) -> None:
    """No default bucket. Guessing one is how bytes land somewhere nobody reads,
    the rule replicator applies to REPLICATOR_BLOB_BUCKET."""
    monkeypatch.delenv("BROKER_BACKUP_BUCKET")
    error_spy = MagicMock()
    monkeypatch.setattr(backup.logger, "error", error_spy)
    assert backup.main(stub_main.argv) == 2
    stub_main.client_factory.assert_not_called()
    error_spy.assert_called_once()


def test_main_exits_zero_on_success_and_prefixes_by_host(stub_main, monkeypatch) -> None:
    monkeypatch.setattr(backup.socket, "gethostname", lambda: "co-broker")
    assert backup.main(stub_main.argv) == 0
    kwargs = stub_main.run.call_args.kwargs
    assert kwargs["bucket"] == "a-backup-bucket"
    assert kwargs["prefix"] == "co-broker"
    assert kwargs["host"] == "co-broker"


def test_main_exits_nonzero_when_the_backup_fails(stub_main) -> None:
    """Unlike the probe, this unit's failure IS the signal: a oneshot that exits
    0 after shipping nothing is the silent backup every incident write-up warns
    about. systemd marks the unit failed and the probe reports the state file;
    both need the non-zero."""
    stub_main.run.side_effect = BackupError("bucket not found")
    assert backup.main(stub_main.argv) == 1


def test_run_backup_records_any_failure_not_only_the_anticipated_ones(
    rdb, client, tmp_path
) -> None:
    """A revoked key surfaces from google.auth as RefreshError, a transport
    fault as TransportError - neither is a GoogleAPICallError nor an OSError.
    Whatever the type, the run failed and the probe must be able to say so from
    the state file; a traceback in the journal with no record is the silent
    backup again, by a narrower door."""
    checker = MagicMock(side_effect=RuntimeError("something nobody anticipated"))
    with pytest.raises(BackupError, match="nobody anticipated"):
        _run(rdb, client, tmp_path, checker=checker)
    state = _state(tmp_path)
    assert state["last_error"] == "RuntimeError: something nobody anticipated"
    assert state["last_failure_at"] == "2026-09-10T15:53:08Z"
