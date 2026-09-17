"""The undelivered-age check, against a real server and the real ACL (broker#20).

The check it covers exists because of a state fakeredis cannot produce and
this repo had no other way to reason about. After the broker VM's reboot on
2026-09-16 replicator never reconnected, ``replicator.fetch`` held one
undelivered command from 15:27:00Z onwards, and the probe reported **0
findings** on every tick from 15:38. It was found by hand.

Two things therefore have to be true against a real ``redis-server``, and
neither is checkable in the main suite:

**``lag`` is not the signal, and a reload is why.** Its input, the group's
``entries-read``, does not survive one; the position does.
``test_a_reload_keeps_the_position_and_loses_lags_input`` restarts a server to
show it, which is a thing only a real one can be asked. The three unusable lag
readings taken that morning are in that test's docstring.

**It costs no new privilege.** The fixture is the server loading *this repo's
tracked ACL file*, so the probe runs here as the real ``brokeradmin`` - the
same rules the node has - and a check needing a grant nobody has fails as
``NOPERM`` rather than passing in a test that granted itself ``~*``. The
converse, that the probe still cannot join a group, is asserted on the ACL
itself in ``test_redis_acl.py``.
"""

from __future__ import annotations

import shutil
import socket
import subprocess
import time
from pathlib import Path

import pytest
import redis as redis_pkg
from co_core.pure.adapters.bus.streams import CONTENT_REVISIONS
from redis.asyncio import Redis as AsyncRedis

from src.broker.bus_health import (
    GROUP_WARN_UNDELIVERED_AGE_SECONDS,
    STREAM_CHECKS,
    collect_broker_findings,
)
from tests.deploy.conftest import PASSWORD, _free_port

#: The one stream these run against, and its group as the probe derives it.
CHECK = next(c for c in STREAM_CHECKS if c.topic == CONTENT_REVISIONS)
GROUP = CHECK.pending_group


@pytest.fixture
async def probe(tracked_acl_broker):
    """An async ``brokeradmin`` on the tracked-ACL server, as the timer runs it.

    Function-scoped over a module-scoped server, so each test starts from a
    keyspace it cleans up rather than from whatever the last one left - and the
    clean-up is a seeder's job, because ``brokeradmin`` deliberately cannot
    write.
    """
    client = AsyncRedis(
        host="127.0.0.1",
        port=tracked_acl_broker.port,
        username="brokeradmin",
        password=PASSWORD,
        decode_responses=True,
    )
    try:
        yield client
    finally:
        await client.aclose()


@pytest.fixture
def seeder(tracked_acl_broker):
    """A throwaway publisher with the rights the probe must not have.

    ``brokeradmin`` cannot XADD, XTRIM or create a group by design, which is the
    property under test elsewhere; setting up a stream for it needs an identity
    that can. Deleted afterwards, along with the keys it wrote, so the
    module-scoped server is left as the tracked file describes it.
    """
    admin = tracked_acl_broker("acladmin")
    admin.execute_command("ACL", "SETUSER", "seed", "on", f">{PASSWORD}", "~*", "+@all")
    client = tracked_acl_broker("seed")
    try:
        yield client
    finally:
        client.delete(CONTENT_REVISIONS)
        admin.execute_command("ACL", "DELUSER", "seed")


def _ms_ago(seconds: float) -> int:
    return int(time.time() * 1000) - int(seconds * 1000)


async def undelivered(probe) -> list:
    """This tick's findings about a group's position, and nothing else.

    ``collect_broker_findings`` is called whole rather than reaching for the
    private collector, so what is exercised is the path the timer takes.
    """
    findings, _ = await collect_broker_findings(probe, previous_state={})
    return [f for f in findings if f.check.startswith("group-undelivered")]


async def test_a_group_with_a_reader_that_never_came_back_is_reported(probe, seeder) -> None:
    """The 2026-09-16 state itself: a group, entries, and no XREADGROUP ever.

    ``XPENDING`` is 0 here - the healthy value - because nothing was delivered.
    That is the whole reason this check was owed.
    """
    seeder.xgroup_create(CONTENT_REVISIONS, GROUP, id="0", mkstream=True)
    stale = GROUP_WARN_UNDELIVERED_AGE_SECONDS + 60
    seeder.xadd(CONTENT_REVISIONS, {"k": "v"}, id=f"{_ms_ago(stale)}-0")

    (finding,) = await undelivered(probe)
    assert finding.check == "group-undelivered"
    assert finding.subject == f"{CONTENT_REVISIONS}/{GROUP}"


async def test_a_read_clears_it(probe, seeder) -> None:
    """Delivery is what the check measures, so delivery is what silences it -
    with no ack, which is where ``XPENDING`` takes over."""
    seeder.xgroup_create(CONTENT_REVISIONS, GROUP, id="0", mkstream=True)
    stale = GROUP_WARN_UNDELIVERED_AGE_SECONDS + 60
    seeder.xadd(CONTENT_REVISIONS, {"k": "v"}, id=f"{_ms_ago(stale)}-0")
    assert await undelivered(probe)

    seeder.xreadgroup(GROUP, "c1", {CONTENT_REVISIONS: ">"}, count=10)
    assert await undelivered(probe) == []


async def test_a_group_that_joined_at_the_end_has_nothing_undelivered(probe, seeder) -> None:
    """A group created at ``$`` on a stream with history is caught up, not behind.

    It will never be offered anything older than its own creation - that is the
    contract its consumer asked for - so the entries before it are not
    undelivered, they are not its. A check counting what a group has not read
    would report every one of them; comparing positions reports nothing, which
    is the answer.
    """
    stale = GROUP_WARN_UNDELIVERED_AGE_SECONDS + 600
    for i in range(3):
        seeder.xadd(CONTENT_REVISIONS, {"k": "v"}, id=f"{_ms_ago(stale) + i}-0")
    seeder.xgroup_create(CONTENT_REVISIONS, GROUP, id="$")

    assert await undelivered(probe) == []


async def test_entries_trimmed_before_delivery_are_their_own_condition(probe, seeder) -> None:
    """A stream trimmed past its group's position, which is not an age.

    There is nothing left to date and nothing that will ever be delivered, so
    the finding names what happened instead of how old it is. ``XTRIM`` leaves
    ``last-generated-id`` where it was, which is what makes the condition
    visible at all.
    """
    seeder.xgroup_create(CONTENT_REVISIONS, GROUP, id="0", mkstream=True)
    seeder.xadd(CONTENT_REVISIONS, {"k": "v"}, id=f"{_ms_ago(60)}-0")
    seeder.xtrim(CONTENT_REVISIONS, maxlen=0)

    (finding,) = await undelivered(probe)
    assert finding.check == "group-undelivered-lost"
    assert finding.subject == f"{CONTENT_REVISIONS}/{GROUP}"


async def test_a_fresh_entry_is_not_a_finding(probe, seeder) -> None:
    """Every delivery is momentarily undelivered. The threshold is what
    separates that from a consumer that is gone, and a check that fired on the
    former would be one nobody reads."""
    seeder.xgroup_create(CONTENT_REVISIONS, GROUP, id="0", mkstream=True)
    seeder.xadd(CONTENT_REVISIONS, {"k": "v"})

    assert await undelivered(probe) == []


# --- why not `lag`: the reload the incident began with ---


class _Restartable:
    """A throwaway ``redis-server`` with an AOF, stopped and started over one dir.

    Local rather than the ACL fixture above, which runs without persistence and
    is module-scoped: what this needs is a server that can be **restarted**, so
    the reload is the thing under test. Local rather than the rehearsal module's
    equivalent for the reason ``conftest`` gives about sharing: importing that
    one drags the storage SDK into a module with no use for it.
    """

    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self.port = _free_port()
        self.proc: subprocess.Popen | None = None
        self.start()

    def start(self) -> None:
        self.proc = subprocess.Popen(
            # fmt: off
            [
                "redis-server",
                "--port",
                str(self.port),
                "--bind",
                "127.0.0.1",
                "--save",
                "",
                "--appendonly",
                "yes",
                "--dir",
                str(self.directory),
            ],
            # fmt: on
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
        deadline = time.time() + 10
        while time.time() < deadline:
            with socket.socket() as s:
                s.settimeout(0.2)
                if s.connect_ex(("127.0.0.1", self.port)) == 0:
                    return
            time.sleep(0.05)
        raise RuntimeError("redis-server did not start")

    def restart(self) -> None:
        """Through ``SHUTDOWN``, not ``SIGTERM``: what is being reproduced is a
        clean stop and a load from the AOF, which is what the node did on
        2026-09-16."""
        subprocess.run(
            ["redis-cli", "-p", str(self.port), "SHUTDOWN", "NOSAVE"], capture_output=True
        )
        assert self.proc is not None
        self.proc.wait(timeout=10)
        self.start()

    def stop(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
            self.proc.wait(timeout=10)


@pytest.fixture
def restartable(tmp_path):
    if shutil.which("redis-server") is None:
        pytest.skip("redis-server not installed")
    server = _Restartable(tmp_path)
    try:
        yield server
    finally:
        server.stop()


async def test_a_reload_keeps_the_position_and_loses_lags_input(restartable) -> None:
    """Why the check compares positions rather than reading ``lag``.

    ``lag`` is ``entries-added`` minus the group's ``entries-read``, and
    **``entries-read`` does not survive a reload**: measured here, it is a
    number before the restart and ``None`` after it, on a group that read and
    acked everything. ``last-delivered-id`` comes back exactly where it was.

    That is not a detail of a test server. It is the state the node was in on
    2026-09-16, and it is why the three lag readings taken that morning were
    unusable: ``watcher.blobs`` 152, ``archiver.revisions`` 147,
    ``replicator.fetch`` 153, while the first two stood at their stream's
    ``last-generated-id`` and the third was behind by exactly one entry. A
    lag-based check would have raised three findings, two false, and misstated
    the third - in the hour when a consumer was genuinely gone.

    A Redis that learns to carry ``entries-read`` across a reload is a good
    reason for this to go red: the counter becoming trustworthy is worth
    knowing, and it still would not make it the better signal - a position is
    the thing the consumer's next read actually resumes from.
    """
    client = redis_pkg.Redis(port=restartable.port, decode_responses=True)
    for _ in range(3):
        client.xadd(CONTENT_REVISIONS, {"k": "v"})
    client.xgroup_create(CONTENT_REVISIONS, GROUP, id="0")
    client.xreadgroup(GROUP, "c1", {CONTENT_REVISIONS: ">"}, count=10)
    client.xack(GROUP, CONTENT_REVISIONS, *[e[0] for e in client.xrange(CONTENT_REVISIONS)])
    before = client.xinfo_groups(CONTENT_REVISIONS)[0]
    assert before["entries-read"] is not None
    client.close()

    restartable.restart()

    client = redis_pkg.Redis(port=restartable.port, decode_responses=True)
    after = client.xinfo_groups(CONTENT_REVISIONS)[0]
    stream = client.xinfo_stream(CONTENT_REVISIONS)
    client.close()
    assert after["last-delivered-id"] == before["last-delivered-id"], "the position survives"
    assert after["last-delivered-id"] == stream["last-generated-id"], "and it is caught up"
    assert after["entries-read"] is None, (
        "entries-read survived the reload on this Redis - re-read this test's docstring "
        "and the lag paragraph in docs/BUS-HEALTH.md before trusting lag anyway"
    )

    probe = AsyncRedis(port=restartable.port, decode_responses=True)
    try:
        assert await undelivered(probe) == []
    finally:
        await probe.aclose()
