"""The restore, rehearsed against a real server on every test run (broker#4).

An untested backup is a second thing to discover during an incident. This
module takes a snapshot the way the job does, restores it the way the runbook
says, and asserts what an operator would check first: the stream is there, the
group is at the position it was at, the PEL still names what was delivered and
not acked, and a TTL survived. The group positions are the whole point of the
backup - they are recoverable from nowhere else.

It also pins the trap CannObserv/broker#4 stated backwards: under
``appendonly yes``, a ``dump.rdb`` with no ``appendonlydir`` beside it is not
loaded. The server starts empty and creates a fresh base, silently.

Skips where ``redis-server`` is absent. Binds loopback on free ports and never
reads BROKER_REDIS_URL, so it cannot touch ``co-broker``.
"""

from __future__ import annotations

import shutil
import socket
import subprocess
import time
from pathlib import Path

import pytest
import redis as redis_pkg

from src.broker import backup, restore
from tests.gcs_fakes import FakeBucket, FakeClient

pytestmark = pytest.mark.skipif(
    shutil.which("redis-server") is None, reason="redis-server not installed"
)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Scratch:
    """One throwaway redis-server over ``directory``."""

    def __init__(self, directory: Path, *, appendonly: bool) -> None:
        self.directory = directory
        self.port = _free_port()
        self.proc = subprocess.Popen(
            [
                "redis-server",
                "--port",
                str(self.port),
                "--bind",
                "127.0.0.1",
                "--save",
                "",
                "--appendonly",
                "yes" if appendonly else "no",
                "--dir",
                str(directory),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        deadline = time.time() + 10
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"redis-server exited:\n{self.proc.stdout.read()}")
            with socket.socket() as s:
                s.settimeout(0.2)
                if s.connect_ex(("127.0.0.1", self.port)) == 0:
                    break
            time.sleep(0.05)
        else:
            self.proc.terminate()
            raise RuntimeError("redis-server did not start")
        self.client = redis_pkg.Redis(
            host="127.0.0.1", port=self.port, decode_responses=True, socket_timeout=5
        )

    def stop(self) -> None:
        self.client.close()
        self.proc.terminate()
        self.proc.wait(timeout=10)


@pytest.fixture
def scratch(tmp_path):
    servers: list[Scratch] = []

    def start(name: str, *, appendonly: bool) -> Scratch:
        directory = tmp_path / name
        directory.mkdir(exist_ok=True)
        server = Scratch(directory, appendonly=appendonly)
        servers.append(server)
        return server

    yield start
    for server in servers:
        server.stop()


def _seed(client: redis_pkg.Redis) -> dict:
    """A stream with a group midway through it: three entries, two delivered,
    one acked - so last-delivered-id, entries-read, the PEL and a TTL all have
    something to lose."""
    ids = [client.xadd("s1", {"k": f"v{i}"}) for i in range(3)]
    client.xgroup_create("s1", "g1", id="0")
    client.xreadgroup("g1", "c1", {"s1": ">"}, count=2)
    client.xack("s1", "g1", ids[0])
    client.set("ttlkey", "v", ex=3600)
    return {"groups": client.xinfo_groups("s1"), "pending": client.xpending("s1", "g1")}


def _snapshot_from(scratch, tmp_path: Path) -> tuple[dict, Path]:
    """Seed a server, SAVE, stop it; hand back what was seeded and the file."""
    source = scratch("a", appendonly=False)
    before = _seed(source.client)
    source.client.save()
    source.stop()  # --save '' so the shutdown writes nothing over the SAVE
    return before, source.directory / "dump.rdb"


def test_a_snapshot_backed_up_by_the_job_restores_with_every_position_intact(
    scratch, tmp_path
) -> None:
    before, dump = _snapshot_from(scratch, tmp_path)

    bucket = FakeBucket("a-backup-bucket")
    state = backup.run_backup(
        rdb=dump,
        bucket="a-backup-bucket",
        prefix="co-broker",
        client=FakeClient(bucket),
        state_path=tmp_path / "state.json",
        workdir=tmp_path / "work",
        host="rehearsal",
    )
    assert state["outcome"] == "uploaded"
    assert state["keys"] == 2  # the real checker counted them
    assert state["redis_version"]

    target = tmp_path / "b"
    target.mkdir()
    client = FakeClient(bucket)
    newest = restore.newest_object(client, "a-backup-bucket", "co-broker")
    assert newest is not None
    fetched = restore.download(client, "a-backup-bucket", newest, tmp_path / "fetched.rdb.gz")
    unpacked = tmp_path / "fetched.rdb"
    restore.gunzip_file(fetched, unpacked)
    restore.stage_appendonlydir(unpacked, target)

    restored = scratch("b", appendonly=True).client
    assert restored.xlen("s1") == 3
    (group,) = restored.xinfo_groups("s1")
    (group_before,) = before["groups"]
    for field in ("name", "pending", "last-delivered-id", "entries-read"):
        assert group[field] == group_before[field], field
    assert restored.xpending("s1", "g1") == before["pending"]
    assert 0 < restored.ttl("ttlkey") <= 3600

    # The AOF is live: a write lands in the incr file the manifest named.
    restored.xadd("s1", {"k": "v3"})
    time.sleep(0.3)
    assert (target / restore.APPENDONLY_DIRNAME / restore.INCR_FILE).stat().st_size > 0


def test_without_staging_redis_ignores_the_snapshot_and_starts_empty(scratch, tmp_path) -> None:
    """The behaviour docs/RECOVERY.md is written around, pinned so a Redis
    upgrade that changes it is noticed rather than assumed."""
    _before, dump = _snapshot_from(scratch, tmp_path)

    bare = tmp_path / "c"
    bare.mkdir()
    shutil.copyfile(dump, bare / "dump.rdb")

    started = scratch("c", appendonly=True).client
    assert started.dbsize() == 0
    # ...and it created a fresh, empty base in the snapshot's place.
    assert (bare / restore.APPENDONLY_DIRNAME / restore.BASE_FILE).exists()
