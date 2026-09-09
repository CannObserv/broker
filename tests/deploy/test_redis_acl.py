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


def test_default_is_disabled(users) -> None:
    """What actually retires the shared password as an identity. `requirepass`
    sets the password for exactly this user."""
    assert users["default"] == ["off"]


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


def test_the_file_loads_and_default_is_really_off(live_acl_broker) -> None:
    """Startup alone proves the file parsed - redis aborts on an ACL error, and
    refuses the whole file rather than one line. The anonymous probe proves
    `user default off` took effect, which is the line that actually retires the
    shared password as an identity: until it lands, every per-service credential
    is an addition rather than a boundary.

    The refusal arrives as an `AuthenticationError` on the handshake, not as a
    `NOAUTH` reply to the command - redis-py sends `HELLO` on connect, so the
    connection never opens. Worth pinning in that shape, because it is what a
    service will report at the cutover if its own credential is wrong.
    """
    with pytest.raises(redis_pkg.exceptions.AuthenticationError):
        live_acl_broker().ping()


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
