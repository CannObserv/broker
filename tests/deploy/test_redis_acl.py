"""The per-service ACL users must parse, and must say what CannObserv/broker#2 decided.

The file is `deploy/redis-acl.conf`, installed as `/etc/redis/users.acl`. These
tests exist because every mistake this file can make is quiet:

- **A syntax error is a cutover-window discovery.** `aclfile` is an immutable
  config, so enabling it costs a restart of an instance three services depend on
  - the worst possible place to learn that a line does not parse. The load test
  below turns that into a test failure on a throwaway server.
- **A typo in a key pattern denies silently.** `~content.revision` grants nothing
  and looks right. Every pattern is therefore checked against co-core's stream
  constants rather than read.
- **A missing command grant degrades quietly.** `+info` and `+ping` were both
  absent from the draft, and both fail in ways that read as something else - a
  warn-only version check going blind, and idle-connection health checks failing.

The one mistake these cannot catch is a grant that is too *wide*, which is why
the archiver/`content.blobs` assertion is written as an explicit denial rather
than as a property of the pattern list.
"""

import fnmatch
import re
import shutil
import socket
import subprocess
import time
from pathlib import Path

import pytest
import redis as redis_pkg
from co_core.pure.adapters.bus.streams import (
    CONTENT_ARTIFACTS,
    CONTENT_BLOBS,
    CONTENT_FETCH,
    CONTENT_FETCH_POLICY,
    CONTENT_REPLICATE,
    CONTENT_REVISIONS,
    INFO_CHANGES,
    INFO_REGISTRY,
    INFO_WATCH_STATUS,
    dlq_name,
    group_name,
    stream_kind,
)

DEPLOY = Path(__file__).resolve().parents[2] / "deploy"
ACL_FILE = DEPLOY / "redis-acl.conf"
RENDER_SCRIPT = DEPLOY / "render-acl.sh"

SERVICE_USERS = ("archiver", "watcher", "replicator")

# Throwaway, for the spawned server below. Never a real credential.
PASSWORD = "throwaway-password"

CANONICAL_STREAMS = frozenset(
    {
        INFO_CHANGES,
        INFO_REGISTRY,
        INFO_WATCH_STATUS,
        CONTENT_FETCH,
        CONTENT_FETCH_POLICY,
        CONTENT_BLOBS,
        CONTENT_REVISIONS,
        CONTENT_ARTIFACTS,
        CONTENT_REPLICATE,
    }
)

# Patterns that are legitimately not a canonical stream or its DLQ.
NON_STREAM_PATTERNS = frozenset({"*", "replicator:cmd:*", "probe.*", "replicator.itest.*"})

# The command streams, which is what makes the dedupe keyspace plural. Derived
# from co-core's taxonomy rather than listed, so a third command stream added
# upstream fails ``test_replicator_can_name_every_dedupe_namespace`` here rather
# than wedging its loop on the node.
COMMAND_STREAMS = tuple(s for s in sorted(CANONICAL_STREAMS) if stream_kind(s) == "command")


def dedupe_key(topic: str, command_id: str) -> str:
    """Replicator's de-duplication key for a command on ``topic``.

    ``replicator:cmd:<stream suffix>:<command_id>`` - and the suffix is the same
    one co-core's ``group_name`` puts after the service, so ``content.fetch``
    gives both ``replicator.fetch`` and ``replicator:cmd:fetch:<id>``. Derived
    through that helper rather than spelled, for the reason cannobserv#384
    exists: a convention change arrives with the wheel and trips a test, instead
    of being something someone has to notice.

    The keys themselves are Replicator's, documented in its
    ``docs/CONVENTIONS.md``; the inventory row is in ../docs/STREAMS.md.
    """
    _service, _, suffix = group_name(topic, "replicator").partition(".")
    return f"replicator:cmd:{suffix}:{command_id}"


def admits(patterns: set[str], key: str) -> bool:
    """Whether any granted ``~pattern`` admits ``key``.

    ``fnmatch`` rather than Redis's own matcher, which is only reachable from a
    running server - so the live test below is what proves this approximation
    honest. Both patterns in play here use ``*`` and nothing else.
    """
    return any(fnmatch.fnmatchcase(key, pattern) for pattern in patterns)


def parse_users(text: str) -> dict[str, list[str]]:
    """`user <name> <rule> <rule> ...`, one per line.

    Deliberately strict about the one-line rule: neither redis.conf nor an
    aclfile supports backslash continuation, and the version of this file
    drafted in the issue thread used it.
    """
    users: dict[str, list[str]] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        assert not line.endswith("\\"), f"aclfile has no line continuation: {line!r}"
        assert line.startswith("user "), f"not a user line: {line!r}"
        _, name, *rules = line.split()
        users[name] = rules
    return users


@pytest.fixture(scope="module")
def users() -> dict[str, list[str]]:
    return parse_users(ACL_FILE.read_text())


def key_patterns(rules: list[str]) -> set[str]:
    return {r[1:] for r in rules if r.startswith("~")}


# --- what the file says ---


def test_no_password_is_committed(users) -> None:
    """Every credential is a placeholder substituted at install time. A real one
    here is readable by everyone with repo access, which is a strictly larger set
    than everyone with root on the broker."""
    for name, rules in users.items():
        for rule in rules:
            if rule.startswith(">"):
                assert re.fullmatch(r"__[A-Z]+_PW__", rule[1:]), f"{name}: {rule!r}"


def test_archiver_cannot_name_content_blobs(users) -> None:
    """The single omission this whole file exists for.

    The `content.blobs` boundary was an unqualified role rule in archiver's own
    guidelines - documentation, enforceable by nobody. Omitting the pattern is
    what makes the broker refuse. Asserted as an explicit denial rather than as a
    property of the list, because the failure being guarded is somebody adding it
    back for a plausible-sounding reason.
    """
    assert CONTENT_BLOBS not in key_patterns(users["archiver"])
    assert dlq_name(CONTENT_BLOBS) not in key_patterns(users["archiver"])


def test_every_key_pattern_names_a_real_stream(users) -> None:
    """A typo denies silently: `~content.revision` grants nothing and reads
    right. Checked against co-core's constants, so a stream renamed upstream
    fails here rather than at a cutover."""
    allowed = CANONICAL_STREAMS | {dlq_name(s) for s in CANONICAL_STREAMS} | NON_STREAM_PATTERNS
    for name, rules in users.items():
        unknown = key_patterns(rules) - allowed
        assert not unknown, f"{name} names patterns that are not streams: {sorted(unknown)}"


def test_replicator_can_name_every_dedupe_namespace(users) -> None:
    """The dedupe keyspace is **per command stream**, and the draft granted one.

    CannObserv/broker#9 records what these keys are: `replicator:cmd:<stream>:
    <command_id>`, written after a handler completes and read by an `EXISTS`
    *before* the next one runs. There is one namespace per command stream, so
    today there are two - `fetch` and `replicate` - and the tracked grant named
    only `~replicator:cmd:fetch:*`.

    That is correction eleven in its key-pattern form. The command inventory was
    read off the wire, the replicate loop has never completed a command (no
    alias table is provisioned), so its namespace is **empty rather than
    absent** and nothing could have observed the gap. The moment that loop
    completes one - which is what broker#7 exists to make happen - the `EXISTS`
    is denied, replicator#82 classifies NOPERM transient, and the loop backs off
    and retries forever without ever running a handler. Nothing is lost and
    nothing progresses.

    Asserted over the taxonomy rather than over the two names, so a third
    command stream cannot arrive without either a grant or a red test.
    """
    patterns = key_patterns(users["replicator"])
    assert COMMAND_STREAMS, "co-core classified no stream as a command"
    for topic in COMMAND_STREAMS:
        key = dedupe_key(topic, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
        assert admits(patterns, key), (
            f"replicator cannot name {key!r} - the dedupe write and the EXISTS "
            f"before {topic}'s handler are both denied"
        )


def test_no_other_user_can_name_the_dedupe_keyspace(users) -> None:
    """They are Replicator's private state, and the broker's own sweep does not
    want them: `brokeradmin` holds `~*` for `INFO` and the DLQ scan, and that is
    the one exception. A second service naming this pattern would be reaching
    into another's dedupe window."""
    key = dedupe_key(COMMAND_STREAMS[0], "01ARZ3NDEKTSV4RRFFQ69G5FAV")
    for name, rules in users.items():
        if name in {"replicator", "brokeradmin", "acladmin", "default"}:
            continue
        assert not admits(key_patterns(rules), key), f"{name} can name {key!r}"


@pytest.mark.parametrize("user", SERVICE_USERS)
def test_service_users_can_read_the_version_and_answer_a_health_check(users, user) -> None:
    """Both were missing from the draft, and both fail as something else.

    `check_redis_floor.sh` reads `redis_version` (an `INFO server`) at each
    service's ExecStartPre and is warn-only, so without `+info` it degrades to
    "broker unreachable?" on every start, forever - the exact silent-blinding
    that CannObserv/archiver#195 already cost this cohort once, arriving by a
    different route. And redis-py's `health_check_interval` issues `PING` on idle
    connections; `PING` is an ordinary command subject to ACL, so without it the
    mechanism that exists to notice a dead connection is the thing that dies.
    """
    assert "+info" in users[user]
    assert "+ping" in users[user]


@pytest.mark.parametrize("user", SERVICE_USERS)
def test_a_dlq_writer_can_also_drain_it(users, user) -> None:
    """CannObserv/broker#1 Phase 5 moved DLQ triage from archiver to each
    stream's own consumer, and the draft predates that. Draining is *audit, back
    up, trim* - a user that can `XADD` a DLQ but not read or trim it can create a
    queue it is then unable to empty."""
    if not any(p.endswith(".dlq") for p in key_patterns(users[user])):
        pytest.skip(f"{user} writes no DLQ")
    for command in ("+xrange", "+xlen", "+xtrim", "+xinfo|stream"):
        assert command in users[user], f"{user} cannot drain its own DLQ: missing {command}"


def test_default_is_declared_disabled_and_still_carries_a_password(users) -> None:
    """The sharpest line in the file, and it has now been through both of its states.

    **Omitting `default` from an aclfile silently makes it `nopass`** - verified
    on a scratch instance: `requirepass` set, aclfile without a `default` line,
    and an anonymous client gets `PONG` while `CONFIG GET requirepass` still
    returns the password. The ACL subsystem takes ownership of `default` the
    moment an aclfile exists and defaults it to `nopass ~* &* +@all`. That is R2
    arriving as a side effect of turning on the mechanism meant to prevent it,
    and every check anyone would think to run still reports auth as on. So the
    line must exist.

    **It is `off` because CannObserv/broker#2 step 4 ran on 2026-09-10** - live,
    as `acladmin`, once every service and the probe were on their own
    credential. The shared password is no longer an identity anything can
    authenticate as, and the tracked file says so, so that a re-render onto a
    rebuilt node (broker#4) cannot quietly reopen it.

    **It keeps its password while disabled**, which looks redundant and is not.
    `off` is a flag; the password set is untouched by it, and the rollback is
    `ACL SETUSER default on` - which on a line carrying no password would enable
    a `nopass` user holding `+@all`. The password on a disabled user is what
    makes the rollback land somewhere safe.

    The one ordering caveat is for a NEW cluster whose services still say
    `default:` at the restart that enables `aclfile`: there this line must read
    `on` for that first load and go `off` live at the end, or the restart locks
    all three services out. That sequence is recorded in docs/RESTART-WINDOW.md
    and is history on this one.
    """
    assert "default" in users, "omitting default from an aclfile makes it nopass"
    rules = users["default"]
    assert rules[0] == "off", f"the shared password was retired on 2026-09-10; got {rules}"
    assert any(r.startswith(">") for r in rules), (
        "a disabled default must still carry a password: "
        "'ACL SETUSER default on' would otherwise roll back to nopass"
    )
    assert "nopass" not in rules


def test_a_grant_can_still_be_widened_after_default_is_disabled(users) -> None:
    """The recovery story the whole cutover rests on has to survive step 4.

    A rule that is too narrow produces `NOPERM`; all three participants classify
    that transient and back off rather than losing data; an operator widens the
    grant live with one `ACL SETUSER`. That last step needs `+acl`, and once
    `default` is disabled the only user holding it is this one - without it the
    documented recovery becomes "edit the file and restart", which on this
    instance is a cohort-wide event.

    Separate from `brokeradmin` on purpose. That user holds `~*` because `INFO`
    and the DLQ sweep need it, so its narrow command list is the only boundary
    it has, and `+acl` would let it grant itself `+xadd`. The observer cannot
    change; the changer cannot read.
    """
    assert "+acl" in users["acladmin"]
    assert "+acl" not in users["brokeradmin"]
    for user in SERVICE_USERS:
        assert "+acl" not in users[user]


def test_the_nodes_diagnostics_survive_disabling_default(users) -> None:
    """`user default off` must not take `CLIENT LIST` and `ACL LOG` with it.

    Both earned their place during the cutover. `ACL LOG` found archiver's
    `config|get` denial in one command, where the alternative was reading three
    services' journals on two hosts this node deliberately cannot reach. And
    `CLIENT LIST`'s `flags=b` is the only reliable way to tell a blocked
    consumer from a client that merely ran a read once - `XINFO CONSUMERS`
    ``idle`` does not update on an empty read, and ``consumers=0`` on a stream
    that has never carried a message means nothing at all.

    No service user holds either, and none should: connection shape and denial
    history are the node's business, not a participant's.
    """
    assert "+client|list" in users["brokeradmin"]
    assert "+acl|log" in users["brokeradmin"]
    for user in SERVICE_USERS:
        assert "+client|list" not in users[user]
        assert "+acl|log" not in users[user]


def test_the_probe_cannot_write_to_a_stream(users) -> None:
    """`brokeradmin` is instance-wide by necessity - `INFO memory` has no key and
    the DLQ sweep is `SCAN MATCH *.dlq` so it finds queues nobody declared. Wide
    keys make a narrow command list the only remaining boundary, so the one thing
    it must not be able to do is publish."""
    assert key_patterns(users["brokeradmin"]) == {"*"}
    assert "+xadd" not in users["brokeradmin"]
    assert "+@all" not in users["brokeradmin"]


# --- does it actually parse? ---


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def live_acl_broker(tmp_path_factory):
    """A throwaway redis-server running the tracked ACL file.

    This is the assertion that could otherwise only be made during the restart
    window. `aclfile` is an immutable config, so it is enabled by restarting an
    instance three services depend on - and redis **aborts startup** on an ACL
    error, refusing the whole file rather than the offending line. A syntax
    error found there is found with the broker down.

    It has already earned this twice. The first run caught that an aclfile
    permits no comments; the second that `+client|setinfo` does not exist before
    Redis 7.2, so the pre-emptive grant broker#2 recommended would have taken
    every user down with it.

    Binds loopback on a free port with no persistence and never reads
    BROKER_REDIS_URL, so it cannot reach `co-broker`.
    """
    binary = shutil.which("redis-server")
    if not binary:
        pytest.skip("redis-server not installed")

    tmp_path = tmp_path_factory.mktemp("acl")
    passwords = tmp_path / "passwords"
    placeholders = sorted(set(re.findall(r"__[A-Z]+_PW__", ACL_FILE.read_text())))
    passwords.write_text("".join(f"{m}={PASSWORD}\n" for m in placeholders))
    acl = tmp_path / "users.acl"
    # Rendered through the same script the install uses, so what is tested is
    # what is installed - including the comment strip, which is not cosmetic.
    acl.write_text(
        subprocess.run(
            [str(RENDER_SCRIPT), str(passwords)], capture_output=True, text=True, check=True
        ).stdout
    )

    port = _free_port()
    proc = subprocess.Popen(
        [
            binary,
            "--port",
            str(port),
            "--bind",
            "127.0.0.1",
            "--save",
            "",
            "--appendonly",
            "no",
            "--aclfile",
            str(acl),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    deadline = time.time() + 10
    while time.time() < deadline:
        if proc.poll() is not None:
            pytest.fail(f"redis-server refused the ACL file:\n{proc.stdout.read()}")
        with socket.socket() as s:
            s.settimeout(0.2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                break
        time.sleep(0.1)
    else:
        proc.terminate()
        pytest.fail("redis-server did not start")

    def connect(user: str | None = None):
        kwargs = {} if user is None else {"username": user, "password": PASSWORD}
        return redis_pkg.Redis(
            host="127.0.0.1",
            port=port,
            socket_connect_timeout=2,
            socket_timeout=2,
            decode_responses=True,
            **kwargs,
        )

    yield connect
    proc.terminate()
    proc.wait(timeout=10)


def test_anonymous_access_is_refused_at_first_load(live_acl_broker) -> None:
    """Startup alone proves the file parsed - redis aborts on an ACL error and
    refuses the whole file rather than one line. This adds the assertion that
    matters more: **the restart that enables `aclfile` must not open the broker.**

    An aclfile that omits `default` makes it `nopass`, so an anonymous client is
    served while `CONFIG GET requirepass` still reports a password. This test is
    what stands between that and a tailnet-bound broker with no door on it.

    The refusal arrives as an `AuthenticationError` on the handshake rather than
    a `NOAUTH` reply, because redis-py sends `HELLO` on connect. Pinned in that
    shape because it is what a service reports at the cutover if its credential
    is wrong.
    """
    with pytest.raises(redis_pkg.exceptions.AuthenticationError):
        live_acl_broker().ping()


def test_retiring_default_is_reversible_live_as_acladmin(live_acl_broker) -> None:
    """The last step of the cutover and its undo, exercised as the users that
    actually perform them.

    The tracked file ships `default off`, so on this throwaway server the shared
    identity is refused from the first load - the state the production broker
    has been in since 2026-09-10. The rollback is one live `ACL SETUSER` as
    `acladmin`, the only user holding `+acl`, and it is issued WITHOUT
    re-supplying a password: that is the assertion that `off` leaves the
    password set intact, which is what makes carrying a password on a disabled
    user worth its apparent redundancy. Then it is disabled again the same way.

    An earlier version of this test ran as `default` and disabled itself, which
    demonstrated the mechanism and nothing about the production path - there
    `default` is the user being disabled and cannot undo its own disabling.
    """
    with pytest.raises(redis_pkg.exceptions.AuthenticationError):
        live_acl_broker("default").ping()
    # The per-service users are untouched by it - that is the whole point.
    assert live_acl_broker("archiver").ping()

    admin = live_acl_broker("acladmin")
    try:
        admin.execute_command("ACL", "SETUSER", "default", "on")
        assert live_acl_broker("default").ping()
    finally:
        admin.execute_command("ACL", "SETUSER", "default", "off")
    with pytest.raises(redis_pkg.exceptions.AuthenticationError):
        live_acl_broker("default").ping()


def test_archiver_is_refused_content_blobs_but_served_its_own_streams(live_acl_broker) -> None:
    """The enforcement this whole file exists for, exercised rather than read.

    `content.blobs` was an unqualified role rule in archiver's guidelines -
    documentation nobody could enforce. Here the broker refuses it. A key-pattern
    denial is also the quieter of the two ACL mistakes, so it is the one worth an
    end-to-end assertion.
    """
    client = live_acl_broker("archiver")
    assert client.xadd(CONTENT_REVISIONS, {"k": "v"})
    with pytest.raises(redis_pkg.exceptions.NoPermissionError):
        client.xadd(CONTENT_BLOBS, {"k": "v"})


@pytest.mark.parametrize("user", SERVICE_USERS)
def test_each_service_can_read_the_version_and_be_health_checked(live_acl_broker, user) -> None:
    """+info and +ping, exercised. Both were absent from the draft and both fail
    as something other than a permissions problem: a warn-only floor check going
    permanently blind, and idle-connection health checks failing."""
    client = live_acl_broker(user)
    assert client.info("server")["redis_version"]
    assert client.ping()


def test_the_probe_can_sweep_but_cannot_publish(live_acl_broker) -> None:
    """`brokeradmin` holds `~*` because `INFO memory` has no key and the DLQ
    sweep must find queues nobody declared. Wide keys make the command list the
    only remaining boundary, so the assertion is on what it cannot do."""
    client = live_acl_broker("brokeradmin")
    assert client.info("memory")["maxmemory"] is not None
    assert list(client.scan_iter(match="*.dlq")) == []
    with pytest.raises(redis_pkg.exceptions.NoPermissionError):
        client.xadd(CONTENT_REVISIONS, {"k": "v"})


@pytest.mark.parametrize("topic", COMMAND_STREAMS)
def test_replicator_can_dedupe_a_command_on_every_command_stream(live_acl_broker, topic) -> None:
    """The pure test above matches globs with ``fnmatch``; this one uses Redis's
    own matcher, on both of the commands these keys ever see.

    ``SET .. NX EX`` is the write after a completed handler and ``EXISTS`` is the
    read before the next one. Nothing else touches them - no ``GET``, no
    ``DEL``, no ``TTL`` (CannObserv/broker#9, and Replicator's own CI AST-scans
    ``src/`` to keep that surface closed), which is why this asserts exactly two.
    """
    client = live_acl_broker("replicator")
    key = dedupe_key(topic, "01ARZ3NDEKTSV4RRFFQ69G5FAV")

    assert client.set(key, "some-message-id", nx=True, ex=86400) is True
    assert client.exists(key) == 1
    # The window is not extended by a redelivery: the second SET is a no-op.
    assert client.set(key, "another-message-id", nx=True, ex=86400) is None


def test_citest_cannot_name_a_production_topic(live_acl_broker) -> None:
    """R4, on the axis ACLs can actually enforce. The db-15 guard was never the
    enforcement - Redis ACLs cannot partition by database index at all - so a
    credential that cannot NAME a production topic is. The database-index axis is
    closed separately by `databases 1` (CannObserv/broker#5)."""
    client = live_acl_broker("citest")
    assert client.xadd("probe.scratch", {"k": "v"})
    for topic in (CONTENT_FETCH, CONTENT_REPLICATE, CONTENT_BLOBS):
        with pytest.raises(redis_pkg.exceptions.NoPermissionError):
            client.xadd(topic, {"k": "v"})
