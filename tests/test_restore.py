"""The restore side of CannObserv/broker#4: staging a snapshot so Redis loads it.

``tests/deploy/test_backup_restore_rehearsal.py`` proves against a real server
that what this module writes is what Redis 7 reads. These tests own the
decisions - which three files, what the manifest says, what is refused, which
object is newest - and need no server to be wrong.
"""

from __future__ import annotations

import gzip
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
    newest_object,
    stage_appendonlydir,
)
from tests.gcs_fakes import FakeBucket, FakeClient

RDB = b"REDIS0010" + b"\xfa\x09redis-ver\x066.2.99\xff" + b"\x01" * 8


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


def test_main_list_prints_newest_first(stub_main, capsys) -> None:
    for key in ("co-broker/20260910T153511Z.rdb.gz", "co-broker/20260910T163511Z.rdb.gz"):
        stub_main.bucket.objects[key] = b""
    assert restore.main(["--list"]) == 0
    lines = [ln for ln in capsys.readouterr().out.splitlines() if ln.strip()]
    assert lines[0].endswith("20260910T163511Z.rdb.gz")
    assert lines[1].endswith("20260910T153511Z.rdb.gz")


def test_main_file_stages_a_local_snapshot_without_a_client(stub_main, rdb) -> None:
    assert restore.main(["--file", str(rdb), "--into", str(stub_main.redis_dir)]) == 0
    restore.storage.Client.assert_not_called()
    assert (stub_main.redis_dir / APPENDONLY_DIRNAME / BASE_FILE).read_bytes() == RDB


def test_main_file_accepts_a_gzipped_snapshot(stub_main, tmp_path) -> None:
    gz = tmp_path / "backup.rdb.gz"
    gz.write_bytes(gzip.compress(RDB))
    assert restore.main(["--file", str(gz), "--into", str(stub_main.redis_dir)]) == 0
    assert (stub_main.redis_dir / APPENDONLY_DIRNAME / BASE_FILE).read_bytes() == RDB
