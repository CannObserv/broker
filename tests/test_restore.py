"""The restore side of CannObserv/broker#4: staging a snapshot so Redis loads it.

``tests/deploy/test_backup_restore_rehearsal.py`` proves against a real server
that what this module writes is what Redis 7 reads. These tests own the
decisions - which three files, what the manifest says, what is refused, which
object is newest - and need no server to be wrong.
"""

from __future__ import annotations

import gzip
import hashlib
import os
import stat
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.broker import backup, restore
from src.broker.restore import (
    APPENDONLY_DIRNAME,
    BASE_FILE,
    INCR_FILE,
    MANIFEST,
    MANIFEST_FILE,
    RestoreError,
    download,
    gunzip_file,
    newest_digests,
    newest_object,
    stage_appendonlydir,
    write_digests,
)
from tests.gcs_fakes import FakeBucket, FakeClient

RDB = b"REDIS0010" + b"\xfa\x09redis-ver\x066.2.99\xff" + b"\x01" * 8

# What the backup ships (CannObserv/broker#72): service digest lines only.
DIGESTS = b"".join(
    f"__{u.upper()}_PW_SHA256__={hashlib.sha256(u.encode()).hexdigest()}\n".encode()
    for u in ("archiver", "watcher")
)
DIGESTS_SHA = hashlib.sha256(DIGESTS).hexdigest()


def _put_digests(bucket: FakeBucket, name: str, body: bytes, taken_at: str) -> None:
    bucket.objects[name] = body
    bucket.metadata[name] = {
        "taken_at": taken_at,
        "sha256": hashlib.sha256(body).hexdigest(),
        "users": "archiver,watcher",
    }


@pytest.fixture
def rdb(tmp_path) -> Path:
    path = tmp_path / "backup.rdb"
    path.write_bytes(RDB)
    return path


def test_stage_writes_the_three_files_redis_reads_on_start(rdb, tmp_path) -> None:
    """Under ``appendonly yes`` Redis 7 loads the AOF manifest and nothing else.
    A ``dump.rdb`` beside an absent ``appendonlydir`` is ignored: the server
    starts EMPTY and silently creates a fresh base (CannObserv/broker#4 had
    that backwards; the rehearsal test pins the real behaviour). So the
    snapshot has to *become* the base: the manifest names it, the incr file
    exists and is empty, and the next write appends there."""
    redis_dir = tmp_path / "redis"
    redis_dir.mkdir()

    staged = stage_appendonlydir(rdb, redis_dir)

    assert staged == redis_dir / APPENDONLY_DIRNAME
    assert (staged / BASE_FILE).read_bytes() == RDB
    assert (staged / INCR_FILE).read_bytes() == b""
    assert (staged / MANIFEST_FILE).read_text() == MANIFEST
    assert MANIFEST == (
        "file appendonly.aof.1.base.rdb seq 1 type b\nfile appendonly.aof.1.incr.aof seq 1 type i\n"
    )
    assert rdb.exists()  # copied, never consumed - the backup file stays


def test_stage_refuses_to_clobber_an_existing_appendonlydir(rdb, tmp_path) -> None:
    """The one thing a restore must never do is overwrite the directory of a
    server that still has data. Moving it aside is the operator's decision,
    made by hand; docs/RECOVERY.md says how."""
    redis_dir = tmp_path / "redis"
    (redis_dir / APPENDONLY_DIRNAME).mkdir(parents=True)
    with pytest.raises(RestoreError, match="exists"):
        stage_appendonlydir(rdb, redis_dir)


def test_stage_refuses_a_file_that_is_not_an_rdb(tmp_path) -> None:
    not_rdb = tmp_path / "x.rdb"
    not_rdb.write_bytes(b"nope")
    redis_dir = tmp_path / "redis"
    redis_dir.mkdir()
    with pytest.raises(backup.BackupError):
        stage_appendonlydir(not_rdb, redis_dir)
    assert not (redis_dir / APPENDONLY_DIRNAME).exists()


def test_newest_object_is_the_greatest_key_under_the_prefix() -> None:
    bucket = FakeBucket("a-backup-bucket")
    for key in (
        "co-broker/20260910T153511Z.rdb.gz",
        "co-broker/20260910T163511Z.rdb.gz",
        "co-broker/20260909T233511Z.rdb.gz",
        "other-host/20260911T000000Z.rdb.gz",  # another node's prefix
        "co-broker/notes.txt",  # not a snapshot
    ):
        bucket.objects[key] = b""
    assert newest_object(FakeClient(bucket), "a-backup-bucket", "co-broker") == (
        "co-broker/20260910T163511Z.rdb.gz"
    )


def test_newest_object_is_none_when_nothing_has_been_backed_up() -> None:
    client = FakeClient(FakeBucket("a-backup-bucket"))
    assert newest_object(client, "a-backup-bucket", "co-broker") is None


def test_download_and_gunzip_round_trip(tmp_path) -> None:
    bucket = FakeBucket("a-backup-bucket")
    key = "co-broker/20260910T153511Z.rdb.gz"
    bucket.objects[key] = gzip.compress(RDB)
    gz = download(FakeClient(bucket), "a-backup-bucket", key, tmp_path / "dl.rdb.gz")
    out = tmp_path / "dl.rdb"
    gunzip_file(gz, out)
    assert out.read_bytes() == RDB


def test_the_snapshot_listing_never_offers_a_digests_object() -> None:
    """Both live under the prefix, and a digests object sorts after its
    snapshot's name; ``--latest`` staging one as an RDB would be refused by the
    header check, but it must not be offered at all."""
    bucket = FakeBucket("a-backup-bucket")
    bucket.objects["co-broker/20260910T153511Z.rdb.gz"] = b"x"
    _put_digests(
        bucket, "co-broker/20260910T153511Z.0123abcd.digests", DIGESTS, "2026-09-10T15:40:00Z"
    )
    client = FakeClient(bucket)
    assert newest_object(client, "a-backup-bucket", "co-broker") == (
        "co-broker/20260910T153511Z.rdb.gz"
    )


def test_newest_digests_is_the_greatest_name() -> None:
    """Each run creates one, named by its time, so the greatest name is the
    last run's - whatever snapshot names sort beside them."""
    bucket = FakeBucket("a-backup-bucket")
    _put_digests(bucket, "co-broker/20260910T154000Z.digests", b"old", "2026-09-10T15:40:00Z")
    _put_digests(bucket, "co-broker/20260910T164000Z.digests", DIGESTS, "2026-09-10T16:40:00Z")
    bucket.objects["co-broker/20260910T173511Z.rdb.gz"] = b"x"
    name, meta = newest_digests(FakeClient(bucket), "a-backup-bucket", "co-broker")
    assert name == "co-broker/20260910T164000Z.digests"
    assert meta["sha256"] == DIGESTS_SHA


def test_newest_digests_is_none_when_none_were_shipped() -> None:
    bucket = FakeBucket("a-backup-bucket")
    bucket.objects["co-broker/20260910T163511Z.rdb.gz"] = b"x"
    assert newest_digests(FakeClient(bucket), "a-backup-bucket", "co-broker") is None


def test_write_digests_creates_the_passwords_file_root_only(tmp_path) -> None:
    dest = tmp_path / "broker-acl-passwords"
    write_digests(DIGESTS, dest, expected_sha256=DIGESTS_SHA)
    assert dest.read_bytes() == DIGESTS
    assert stat.S_IMODE(dest.stat().st_mode) == 0o400


def test_write_digests_refuses_to_overwrite(tmp_path) -> None:
    """The file already there may be the better copy - appendonlydir's rule."""
    dest = tmp_path / "broker-acl-passwords"
    dest.write_text("kept\n")
    with pytest.raises(RestoreError, match="exists"):
        write_digests(DIGESTS, dest, expected_sha256=DIGESTS_SHA)
    assert dest.read_text() == "kept\n"


def test_write_digests_refuses_a_download_that_is_not_what_was_shipped(tmp_path) -> None:
    dest = tmp_path / "broker-acl-passwords"
    with pytest.raises(RestoreError, match="sha256"):
        write_digests(DIGESTS, dest, expected_sha256="0" * 64)
    assert not dest.exists()


def test_write_digests_leaves_nothing_when_the_write_fails(tmp_path, monkeypatch) -> None:
    """A partial file would refuse every retry as "exists" - a disk full
    mid-rebuild turned into a wedge nothing explains."""
    dest = tmp_path / "broker-acl-passwords"

    def fail(fd, mode):
        os.close(fd)
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(restore.os, "fdopen", fail)
    with pytest.raises(OSError):
        write_digests(DIGESTS, dest, expected_sha256=DIGESTS_SHA)
    assert not dest.exists()


def test_write_digests_refuses_anything_but_service_digest_lines(tmp_path) -> None:
    """The bucket is not trusted to have been written by this job alone. A
    plaintext or a node user's line is refused before it reaches /etc/redis."""
    dest = tmp_path / "broker-acl-passwords"
    body = DIGESTS + b"__ACLADMIN_PW_SHA256__=" + b"a" * 64 + b"\n"
    with pytest.raises(RestoreError):
        write_digests(body, dest, expected_sha256=hashlib.sha256(body).hexdigest())
    assert not dest.exists()


# --- the entrypoint ---


@pytest.fixture
def stub_main(monkeypatch, tmp_path):
    monkeypatch.setattr(restore, "configure_logging", lambda: None)
    bucket = FakeBucket("a-backup-bucket")
    client = FakeClient(bucket)
    monkeypatch.setattr(restore.storage, "Client", MagicMock(return_value=client))
    monkeypatch.setenv("BROKER_BACKUP_BUCKET", "a-backup-bucket")
    monkeypatch.setenv("BROKER_BACKUP_PREFIX", "co-broker")
    redis_dir = tmp_path / "redis"
    redis_dir.mkdir()
    return SimpleNamespace(bucket=bucket, client=client, redis_dir=redis_dir)


def test_main_latest_stages_the_newest_snapshot(stub_main, capsys) -> None:
    stub_main.bucket.objects["co-broker/20260910T153511Z.rdb.gz"] = gzip.compress(b"old")
    stub_main.bucket.objects["co-broker/20260910T163511Z.rdb.gz"] = gzip.compress(RDB)

    assert restore.main(["--latest", "--into", str(stub_main.redis_dir)]) == 0

    staged = stub_main.redis_dir / APPENDONLY_DIRNAME
    assert (staged / BASE_FILE).read_bytes() == RDB
    out = capsys.readouterr().out
    assert "20260910T163511Z" in out
    # The follow-ups the operator runs next are printed, not remembered.
    assert "chown" in out and "redis:redis" in out


def test_main_refuses_when_the_bucket_holds_nothing(stub_main) -> None:
    assert restore.main(["--latest", "--into", str(stub_main.redis_dir)]) == 1
    assert not (stub_main.redis_dir / APPENDONLY_DIRNAME).exists()


def test_main_list_prints_newest_first_with_what_the_metadata_says(stub_main, capsys) -> None:
    """The node has no gcloud and its identity cannot read bucket metadata, but
    a listing carries each object's metadata for free. Printing it is what
    lets an operator verify a staged base's sha256 against the object it came
    from, on the node, during a rebuild."""
    newest = "co-broker/20260910T163511Z.rdb.gz"
    stub_main.bucket.objects["co-broker/20260910T153511Z.rdb.gz"] = b"old"
    stub_main.bucket.objects[newest] = b"new"
    stub_main.bucket.metadata[newest] = {
        "snapshot_at": "2026-09-10T16:35:11Z",
        "sha256": "abc123",
        "keys": "37",
        "size_bytes": "498485",
    }
    assert restore.main(["--list"]) == 0
    lines = [ln for ln in capsys.readouterr().out.splitlines() if ln.strip()]
    assert lines[0].startswith(f"gs://a-backup-bucket/{newest}")
    assert "snapshot_at=2026-09-10T16:35:11Z" in lines[0]
    assert "keys=37" in lines[0]
    assert "sha256=abc123" in lines[0]
    assert lines[1].startswith("gs://a-backup-bucket/co-broker/20260910T153511Z.rdb.gz")


def test_main_file_stages_a_local_snapshot_without_a_client(stub_main, rdb) -> None:
    assert restore.main(["--file", str(rdb), "--into", str(stub_main.redis_dir)]) == 0
    restore.storage.Client.assert_not_called()
    assert (stub_main.redis_dir / APPENDONLY_DIRNAME / BASE_FILE).read_bytes() == RDB


def test_main_file_accepts_a_gzipped_snapshot(stub_main, tmp_path) -> None:
    gz = tmp_path / "backup.rdb.gz"
    gz.write_bytes(gzip.compress(RDB))
    assert restore.main(["--file", str(gz), "--into", str(stub_main.redis_dir)]) == 0
    assert (stub_main.redis_dir / APPENDONLY_DIRNAME / BASE_FILE).read_bytes() == RDB


def test_main_digests_writes_the_newest_whatever_snapshot_is_staged(
    stub_main, tmp_path, capsys
) -> None:
    """Rolling the data back is not rolling back who is admitted: observo was
    minted on 2026-09-24, and an older snapshot's paired digests would lock it
    out. So ``--digests`` takes no snapshot argument at all."""
    _put_digests(
        stub_main.bucket,
        "co-broker/20260910T153511Z.aaaaaaaa.digests",
        b"old",
        "2026-09-10T15:40:00Z",
    )
    _put_digests(
        stub_main.bucket,
        "co-broker/20260910T163511Z.bbbbbbbb.digests",
        DIGESTS,
        "2026-09-10T16:40:00Z",
    )
    dest = tmp_path / "broker-acl-passwords"

    assert restore.main(["--digests", str(dest)]) == 0

    assert dest.read_bytes() == DIGESTS
    out = capsys.readouterr().out
    assert "20260910T163511Z.bbbbbbbb.digests" in out
    assert "archiver,watcher" in out
    # The node lines a rebuild appends next are named, not remembered.
    assert "NODE-CREDENTIALS.md" in out and "__DEFAULT_PW_SHA256__" in out
    # Runnable as the operator: the file is 0400 root, and a `>>` under `sudo`
    # is the caller's shell redirecting, not root's.
    (tombstone,) = [ln for ln in out.splitlines() if "__DEFAULT_PW_SHA256__" in ln]
    assert tombstone.rstrip().endswith(f"| sudo tee -a {dest} >/dev/null")
    assert ">>" not in tombstone


def test_main_digests_fails_when_none_were_shipped(stub_main, tmp_path) -> None:
    dest = tmp_path / "broker-acl-passwords"
    assert restore.main(["--digests", str(dest)]) == 1
    assert not dest.exists()
