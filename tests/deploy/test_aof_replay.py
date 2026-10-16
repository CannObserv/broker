"""The AOF replay keeps every ``MULTI``/``EXEC`` block (CannObserv/broker#101).

During AOF load, ``EXEC`` re-checks each queued command against **``default``**
- the AOF-loading client's identity - command *and* key rules, whatever the
user that originally wrote it and whether ``default`` is ``on`` or ``off``. A
refused command is logged ``== CRITICAL ==`` once and skipped; the load does not
abort. So while ``default`` was the bare ``-@all`` tombstone, an unplanned
restart silently dropped every block in the incr file, and a block can tear:
under ``~a`` alone, a transaction's ``a`` write survives and its ``b`` write
does not. Measured on 7.0.15 and Valkey 7.2.13.

No participant writes a transaction today; the engine writes the blocks itself:

- ``XREADGROUP`` delivering 2+ entries, and ``XAUTOCLAIM``/``XCLAIM`` moving 2+
  (one ``XCLAIM`` per entry). Archiver's reclaim pages at count 10.
- a write to a key past its TTL that active expiry has not yet reaped:
  ``DEL`` + the write. Replicator's ``replicator:cmd:*`` dedupe keys are the
  shape; no participant contract can prevent this one.

Built on ``tracked_acl_broker``'s server with ``--appendonly yes`` and one
orchestration user, ``ops``, appended to the rendered file: it writes, and the
replay ignores who wrote, which is the point. Restarted on the same ``dir``.

The control - the bare tombstone still loses on this engine - is the exit
condition, not decoration. Valkey >= 8.0.11 (valkey#3984) and Redis >= 8.4.4
(redis#14545) skip the re-check while loading. The day the control fails, the
engine no longer needs ``default``'s grant, and it can come off.
"""

import time

from co_core.pure.adapters.bus.streams import CONTENT_REVISIONS

from tests.deploy.conftest import PASSWORD, acl_server, render_tracked_acl

BARE_TOMBSTONE = "user default off >unknown resetchannels -@all"
# A Replicator dedupe key, by shape (deploy/redis-acl.conf, the replicator stanza).
DEDUPE_KEY = "replicator:cmd:aof-replay"
GROUP = "replay"
# Each workload step's propagation: delivery of 2, a claim of 2, a lazy-expired SET.
BLOCKS = 3


def _acl(tmp_path, default_line: str | None):
    acl = render_tracked_acl(tmp_path)
    lines = acl.read_text().splitlines()
    if default_line is not None:
        lines = [default_line if ln.startswith("user default ") else ln for ln in lines]
    lines.append(f"user ops on >{PASSWORD} ~* &* +@all")
    acl.write_text("\n".join(lines) + "\n")
    return acl


def _state(client) -> dict:
    (group,) = client.xinfo_groups(CONTENT_REVISIONS)
    owners = sorted(
        (p["message_id"], p["consumer"])
        for p in client.xpending_range(CONTENT_REVISIONS, GROUP, "-", "+", 10)
    )
    return {
        "xlen": client.xlen(CONTENT_REVISIONS),
        "last-delivered-id": group["last-delivered-id"],
        "owners": owners,
        "dedupe": client.get(DEDUPE_KEY),
    }


def _replay(tmp_path, default_line: str | None) -> tuple[dict, dict, str]:
    acl = _acl(tmp_path, default_line)
    args = ("--appendonly", "yes", "--enable-debug-command", "yes")
    with acl_server(acl, tmp_path, args) as connect:
        ops = connect("ops")
        ops.xadd(CONTENT_REVISIONS, {"n": "1"})
        ops.xadd(CONTENT_REVISIONS, {"n": "2"})
        ops.xgroup_create(CONTENT_REVISIONS, GROUP, id="0")
        ops.xreadgroup(GROUP, "c1", {CONTENT_REVISIONS: ">"}, count=2)
        ops.xautoclaim(CONTENT_REVISIONS, GROUP, "c2", min_idle_time=0, count=10)
        ops.execute_command("DEBUG", "SET-ACTIVE-EXPIRE", "0")
        ops.set(DEDUPE_KEY, "old", px=1)
        time.sleep(0.02)
        ops.set(DEDUPE_KEY, "new", nx=True, ex=600)
        before = _state(ops)
    incr = b"".join(f.read_bytes() for f in (tmp_path / "appendonlydir").glob("*.incr.aof"))
    assert incr.count(b"\r\nMULTI\r\n") == BLOCKS, (
        "the workload no longer makes the engine write its blocks - re-measure #101's shapes"
    )
    with acl_server(acl, tmp_path, args) as connect:
        after = _state(connect("ops"))
    return before, after, (tmp_path / "redis-server.log").read_text()


def test_the_tracked_default_line_replays_every_block(tmp_path) -> None:
    before, after, log = _replay(tmp_path, None)
    assert after == before
    assert "ACLs rules changed" not in log


def test_the_bare_tombstone_still_drops_them_on_this_engine(tmp_path) -> None:
    """The control. Failing here is good news: the engine skips the re-check
    while loading, and ``default``'s grant can come off (#101)."""
    before, after, log = _replay(tmp_path, BARE_TOMBSTONE)
    assert before["owners"] and {o for _, o in before["owners"]} == {"c2"}
    assert after != before, (
        "the bare tombstone replayed every block: this engine no longer re-checks "
        "EXEC's queued commands during AOF load. Retire default's grant (#101)."
    )
    assert after["dedupe"] is None
    assert log.count("ACLs rules changed") == 2 * BLOCKS
