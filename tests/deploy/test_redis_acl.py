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
NON_STREAM_PATTERNS = frozenset({"*", "replicator:cmd:fetch:*", "probe.*", "replicator.itest.*"})


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


def test_default_is_declared_and_enabled_at_first_load(users) -> None:
    """The two-sided trap that makes this the sharpest line in the file.

    **Omitting `default` from an aclfile silently makes it `nopass`** - verified
    on a scratch instance: `requirepass` set, aclfile without a `default` line,
    and an anonymous client gets `PONG` while `CONFIG GET requirepass` still
    returns the password. The ACL subsystem takes ownership of `default` the
    moment an aclfile exists and defaults it to `nopass ~* &* +@all`. That is R2
    arriving as a side effect of turning on the mechanism meant to prevent it,
    and every check anyone would think to run still reports auth as on.

    **And `off` here locks out all three services**, because the restart that
    enables `aclfile` lands before any service has moved onto its own
    credential - every URL still says `default:` at that instant.

    So the file must declare `default`, and must declare it enabled with a
    password. Disabling it is a live `ACL SETUSER` at the end of the cutover.
    """
    assert "default" in users, "omitting default from an aclfile makes it nopass"
    rules = users["default"]
    assert rules[0] == "on", f"default must stay enabled at first load, got {rules}"
    assert any(r.startswith(">") for r in rules), "default must carry a password, not nopass"
    assert "nopass" not in rules


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


def test_disabling_default_is_live_and_reversible(live_acl_broker) -> None:
    """The last step of the cutover, exercised rather than trusted.

    Retiring the shared password is the one genuinely irreversible-feeling step,
    so it is deliberately the one that needs no window: `ACL SETUSER` applies
    immediately and `ACL SETUSER default on >...` puts it back. Proving both
    directions here is what makes it safe to do live at the end, after every
    service is confirmed on its own credential.
    """
    admin = live_acl_broker("default")
    assert admin.ping()

    admin.execute_command("ACL", "SETUSER", "default", "off")
    with pytest.raises(redis_pkg.exceptions.AuthenticationError):
        live_acl_broker("default").ping()
    # The per-service users are untouched by it - that is the whole point.
    assert live_acl_broker("archiver").ping()

    admin.execute_command("ACL", "SETUSER", "default", "on", f">{PASSWORD}", "~*", "&*", "+@all")
    assert live_acl_broker("default").ping()


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
