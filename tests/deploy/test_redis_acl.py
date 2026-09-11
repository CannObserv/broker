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

import contextlib
import fnmatch
import re

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

from src.broker.bus_health import DLQ_DRAINERS
from tests.deploy.conftest import ACL_FILE, PASSWORD, parse_users, split_rules

SERVICE_USERS = ("archiver", "watcher", "replicator")

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
# `*.dlq` is the backstop's disposal pattern: it names every dead-letter queue,
# including the ones nobody declared, which is the whole point of a backstop.
NON_STREAM_PATTERNS = frozenset({"*", "*.dlq", "replicator:cmd:*", "probe.*", "replicator.itest.*"})

# The command streams, which is what makes the dedupe keyspace plural. Derived
# from co-core's taxonomy rather than listed, so a third command stream added
# upstream fails ``test_replicator_can_name_every_dedupe_namespace`` here rather
# than wedging its loop on the node.
COMMAND_STREAMS = tuple(s for s in sorted(CANONICAL_STREAMS) if stream_kind(s) == "command")

# A stand-in for the ULID replicator puts in the last segment. Any value works -
# what is under test is the namespace before it.
SAMPLE_COMMAND_ID = "01ARZ3NDEKTSV4RRFFQ69G5FAV"


def dedupe_key(topic: str, command_id: str) -> str:
    """Replicator's de-duplication key for a command on ``topic``.

    ``replicator:cmd:<stream suffix>:<command_id>`` - and the suffix is the same
    one co-core's ``group_name`` puts after the service, so ``content.fetch``
    gives both ``replicator.fetch`` and ``replicator:cmd:fetch:<id>``. Derived
    through that helper rather than spelled, for the reason cannobserv#384
    exists - though note what that does and does not buy here: the grant is now
    the whole ``replicator:cmd:*`` namespace, so no change to the suffix
    derivation can make the pattern tests below go red. What they still catch is
    the grant being re-narrowed to one segment, which is the regression
    broker#9 was.

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


@pytest.fixture(scope="module")
def users() -> dict[str, list[str]]:
    return parse_users(ACL_FILE.read_text())


# Redis spells a key grant four ways, and only one of them starts with `~`:
# `allkeys` is `~*` by another name, and `%R~`, `%W~` and `%RW~` are read-only,
# write-only and read-write selectors over the same glob. Any of them would be
# invisible to a `startswith("~")` filter - which would make every assertion in
# this file that reads "user X cannot name pattern P" pass while X names it, and
# would hide a typo'd `%R~content.revision` from
# `test_every_key_pattern_names_a_real_stream`.
_KEY_RULE = re.compile(r"^(?:%(?:R|W|RW)?)?~(?P<pattern>.+)$")


def key_patterns(rules: list[str]) -> set[str]:
    patterns = {"*"} if "allkeys" in rules else set()
    root, selectors = split_rules(rules)
    # Selector patterns count. A selector is a narrower grant than a root rule -
    # it carries its own command list - but it is still a pattern this user can
    # NAME, and every assertion below that a user cannot reach a key has to mean
    # "by any route". `root_key_patterns` is for the few places the distinction
    # is the point.
    for rule in [*root, *(r for selector in selectors for r in selector)]:
        match = _KEY_RULE.match(rule)
        if match:
            patterns.add(match.group("pattern"))
    return patterns


def root_key_patterns(rules: list[str]) -> set[str]:
    """Only the patterns on the root permission set, ignoring selectors."""
    root, _ = split_rules(rules)
    return key_patterns(root)


def selector_patterns(rules: list[str], command: str) -> set[str]:
    """The key patterns of every selector granting ``command``.

    Empty when no selector grants it, which is the state every user but two is
    in and the state this helper must not make look like a grant.
    """
    _root, selectors = split_rules(rules)
    return {
        match.group("pattern")
        for selector in selectors
        if command in selector
        for rule in selector
        if (match := _KEY_RULE.match(rule))
    }


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
        key = dedupe_key(topic, SAMPLE_COMMAND_ID)
        assert admits(patterns, key), (
            f"replicator cannot name {key!r} - the dedupe write and the EXISTS "
            f"before {topic}'s handler are both denied"
        )


def test_no_other_user_can_name_the_dedupe_keyspace(users) -> None:
    """They are Replicator's private state, and the broker's own sweep does not
    want them: `brokeradmin` holds `~*` for `INFO` and the DLQ scan, and that is
    the one exception. A second service naming this pattern would be reaching
    into another's dedupe window.

    Over every command stream's namespace rather than the first, for the reason
    the test above is written over the taxonomy: a grant reaching into one
    segment is exactly the shape of mistake broker#9 corrected, and checking
    only ``fetch`` would miss its mirror image.
    """
    assert COMMAND_STREAMS, "co-core classified no stream as a command"
    for name, rules in users.items():
        if name in {"replicator", "brokeradmin", "acladmin", "default"}:
            continue
        for topic in COMMAND_STREAMS:
            key = dedupe_key(topic, SAMPLE_COMMAND_ID)
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


def test_only_the_probe_can_read_an_acl(users) -> None:
    """`+acl|getuser` is the mirror test's grant, and it is also a credential read.

    `ACL GETUSER` returns the target user's password *hash* - unsalted sha256 -
    for every user on the instance, so it belongs only to the one identity that
    already reads every key and every stream. A service user acquiring it reads
    the whole cohort's credential material.

    Asserted here rather than only live, because
    `tests/deploy/test_live_acl_matches_tracked_acl.py` skips without
    `BROKER_REDIS_URL` - so without this, CI protects neither half of the grant
    that makes that test possible.
    """
    assert "+acl|getuser" in users["brokeradmin"], (
        "the mirror test needs it: tests/deploy/test_live_acl_matches_tracked_acl.py"
    )
    for name, rules in users.items():
        if name in {"brokeradmin", "acladmin", "default"}:
            continue
        assert "+acl|getuser" not in rules, f"{name} can read every user's password hash"


def test_no_user_holds_xdel_on_its_root_permission_set(users) -> None:
    """Deletion is selector-scoped or it is not granted, for every user.

    This is the assertion the `+set` stanza in the tracked file should have been
    able to make about itself. A Redis ACL key pattern applies to **every**
    command on the root permission set, so a root `+xdel` beside
    `~content.replicate` is `XDEL` on a command stream - deleting commands the
    consumer group has not delivered and orphaning the PEL entries naming them,
    which is the exact hazard `docs/STREAMS.md` carves that stream out of the
    trim set for. A selector is the only grammar that scopes a command to a
    pattern, so the rule is: never on the root.
    """
    for name, rules in users.items():
        root, _ = split_rules(rules)
        assert "+xdel" not in root, (
            f"{name} holds +xdel on its root permissions, which applies it to every key "
            f"pattern the user has: {sorted(root_key_patterns(rules))}"
        )


def test_no_xdel_grant_can_name_a_fact_or_command_stream(users) -> None:
    """What the selectors are *for*, asserted against co-core rather than read.

    A dead-letter entry is a copy - the fact carrying the outcome went to
    `content.blobs` or `content.artifacts` and is what issuers actually read - so
    deleting one destroys no unique record. That argument collapses the moment a
    deletion grant can reach the original, and a glob is how that happens
    silently: `~content.*` would admit both `content.fetch.dlq` and
    `content.fetch`.

    Derived from the canonical stream set, so a tenth stream added upstream is
    covered without an edit here.
    """
    for name, rules in users.items():
        for pattern in sorted(selector_patterns(rules, "+xdel")):
            reachable = sorted(s for s in CANONICAL_STREAMS if admits({pattern}, s))
            assert not reachable, (
                f"{name}'s +xdel selector names {pattern!r}, which admits {reachable} - "
                "a deletion grant that can reach the stream the DLQ is a copy OF"
            )


def test_the_drainer_can_delete_from_every_queue_it_drains_and_can_read(users) -> None:
    """The grant broker#12 found missing, derived from the assignment that made it owed.

    `DLQ_DRAINERS` settled who triages each queue in broker#1 Phase 5, and the
    comment above it claimed the assignment "costs nothing under D3 - each
    service already holds `~<its own topic>.dlq` in the draft ACL". The key
    pattern was there; the deletion command was not, so for a year the named
    drainer of `content.replicate.dlq` could fill it and not empty it
    (broker#12).

    **Conditioned on being able to READ the queue, which is not a loophole but
    the honest boundary.** `DLQ_DRAINERS` names replicator as the prospective
    drainer of `info.changes.dlq`, and replicator holds no `~info.changes`
    pattern at all - it cannot consume the stream, let alone triage its queue.
    Granting deletion ahead of the group would be granting it to a service that
    cannot read what it is deleting. The day replicator gets that group, it gets
    the pattern, and this test starts demanding the grant on its own.
    """
    for dlq, drainer in sorted(DLQ_DRAINERS.items()):
        rules = users.get(drainer)
        if rules is None or dlq not in key_patterns(rules):
            continue
        assert admits(selector_patterns(rules, "+xdel"), dlq), (
            f"{drainer} is the drainer of {dlq} and can read it, but holds no +xdel "
            f"selector that names it - it can fill the queue and not empty it"
        )


def test_the_probe_cannot_write_to_a_stream(users) -> None:
    """`brokeradmin` is instance-wide by necessity - `INFO memory` has no key and
    the DLQ sweep is `SCAN MATCH *.dlq` so it finds queues nobody declared. Wide
    keys make a narrow command list the only remaining boundary, so the one thing
    it must not be able to do is publish."""
    assert root_key_patterns(users["brokeradmin"]) == {"*"}
    assert "+xadd" not in users["brokeradmin"]
    assert "+@all" not in users["brokeradmin"]
    # It can DELETE, as the declared backstop for a queue nobody drains
    # (`DLQ_UNASSIGNED`), and that is the one exception - scoped by a selector to
    # dead-letter queues, so the instance-wide `~*` above cannot carry it onto a
    # fact or a command stream. Publishing remains the thing it cannot do.
    assert selector_patterns(users["brokeradmin"], "+xdel") == {"*.dlq"}


# --- does it actually parse? ---


def test_anonymous_access_is_refused_at_first_load(tracked_acl_broker) -> None:
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
        tracked_acl_broker().ping()


def test_retiring_default_is_reversible_live_as_acladmin(tracked_acl_broker) -> None:
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
        tracked_acl_broker("default").ping()
    # The per-service users are untouched by it - that is the whole point.
    assert tracked_acl_broker("archiver").ping()

    admin = tracked_acl_broker("acladmin")
    try:
        admin.execute_command("ACL", "SETUSER", "default", "on")
        assert tracked_acl_broker("default").ping()
    finally:
        admin.execute_command("ACL", "SETUSER", "default", "off")
    with pytest.raises(redis_pkg.exceptions.AuthenticationError):
        tracked_acl_broker("default").ping()


def test_archiver_is_refused_content_blobs_but_served_its_own_streams(tracked_acl_broker) -> None:
    """The enforcement this whole file exists for, exercised rather than read.

    `content.blobs` was an unqualified role rule in archiver's guidelines -
    documentation nobody could enforce. Here the broker refuses it. A key-pattern
    denial is also the quieter of the two ACL mistakes, so it is the one worth an
    end-to-end assertion.
    """
    client = tracked_acl_broker("archiver")
    assert client.xadd(CONTENT_REVISIONS, {"k": "v"})
    with pytest.raises(redis_pkg.exceptions.NoPermissionError):
        client.xadd(CONTENT_BLOBS, {"k": "v"})


@pytest.mark.parametrize("user", SERVICE_USERS)
def test_each_service_can_read_the_version_and_be_health_checked(tracked_acl_broker, user) -> None:
    """+info and +ping, exercised. Both were absent from the draft and both fail
    as something other than a permissions problem: a warn-only floor check going
    permanently blind, and idle-connection health checks failing."""
    client = tracked_acl_broker(user)
    assert client.info("server")["redis_version"]
    assert client.ping()


def test_the_probe_can_sweep_but_cannot_publish(tracked_acl_broker) -> None:
    """`brokeradmin` holds `~*` because `INFO memory` has no key and the DLQ
    sweep must find queues nobody declared. Wide keys make the command list the
    only remaining boundary, so the assertion is on what it cannot do."""
    client = tracked_acl_broker("brokeradmin")
    assert client.info("memory")["maxmemory"] is not None
    assert list(client.scan_iter(match="*.dlq")) == []
    with pytest.raises(redis_pkg.exceptions.NoPermissionError):
        client.xadd(CONTENT_REVISIONS, {"k": "v"})


@pytest.mark.parametrize("topic", COMMAND_STREAMS)
def test_replicator_can_dedupe_a_command_on_every_command_stream(tracked_acl_broker, topic) -> None:
    """The pure test above matches globs with ``fnmatch``; this one uses Redis's
    own matcher, on both of the commands these keys ever see.

    ``SET .. NX EX`` is the write after a completed handler and ``EXISTS`` is the
    read before the next one. Nothing else touches them - no ``GET``, no
    ``DEL``, no ``TTL`` (CannObserv/broker#9, and Replicator's own CI AST-scans
    ``src/`` to keep that surface closed), which is why this asserts exactly two
    and then asserts the third is refused: a grant that is too *wide* is the one
    mistake the pattern checks cannot see, so it is written as an explicit
    denial, the way the archiver/``content.blobs`` assertion is.
    """
    client = tracked_acl_broker("replicator")
    key = dedupe_key(topic, SAMPLE_COMMAND_ID)

    assert client.set(key, "some-message-id", nx=True, ex=86400) is True
    assert client.exists(key) == 1
    # The window is not extended by a redelivery: the second SET is a no-op.
    assert client.set(key, "another-message-id", nx=True, ex=86400) is None
    # And the surface stops there - the value is never read back, only its
    # existence, so the grant must not stretch to GET, DEL or TTL.
    for refused in (lambda: client.get(key), lambda: client.delete(key), lambda: client.ttl(key)):
        with pytest.raises(redis_pkg.exceptions.NoPermissionError):
            refused()


@contextlib.contextmanager
def _seeder(tracked_acl_broker):
    """A throwaway publisher, because no tracked user can write everywhere.

    These tests need bait on a stream the user under test cannot publish to -
    that denial being half of what is asserted - and `acladmin` holds `+acl` and
    `+ping` only. Created with the same throwaway password the fixture connects
    with, and deleted in a `finally` so the module-scoped server is left as the
    tracked file describes it.
    """
    admin = tracked_acl_broker("acladmin")
    admin.execute_command("ACL", "SETUSER", "seed", "on", f">{PASSWORD}", "~*", "+xadd")
    try:
        yield tracked_acl_broker("seed")
    finally:
        admin.execute_command("ACL", "DELUSER", "seed")


@pytest.mark.parametrize("topic", sorted(CANONICAL_STREAMS))
def test_a_drainer_can_empty_its_own_queue_and_not_the_stream_it_copies(
    tracked_acl_broker, topic
) -> None:
    """The selector, enforced by redis rather than asserted against the file.

    Parametrised over every canonical stream and skipped where no drainer holds
    the grant, so the pair under test is always (the queue it may empty, the
    stream that queue copies FROM) for one topic. That pairing is the property
    which makes "a dead-letter entry is only a copy" safe to rely on: the static
    test can say the pattern does not admit the stream, and this says redis
    agrees.
    """
    dlq = dlq_name(topic)
    drainer = DLQ_DRAINERS.get(dlq)
    if drainer is None:
        pytest.skip(f"no drainer assigned for {dlq}")
    rules = parse_users(ACL_FILE.read_text()).get(drainer)
    if rules is None or not admits(selector_patterns(rules, "+xdel"), dlq):
        pytest.skip(f"{drainer} holds no +xdel selector naming {dlq}")

    client = tracked_acl_broker(drainer)
    with _seeder(tracked_acl_broker) as seeder:
        dlq_id = seeder.xadd(dlq, {"k": "v"})
        topic_id = seeder.xadd(topic, {"k": "v"})
        assert client.xdel(dlq, dlq_id) == 1, f"{drainer} cannot drain {dlq}"
        with pytest.raises(redis_pkg.exceptions.NoPermissionError):
            client.xdel(topic, topic_id)


def test_the_probe_can_dispose_of_one_dlq_entry_and_nothing_else(tracked_acl_broker) -> None:
    """The backstop half, and the precision it exists to buy.

    `DLQ_UNASSIGNED` makes the broker the drainer of a `*.dlq` key nobody
    claimed, and before broker#12 its only tool was `XTRIM MAXLEN 0` - which on a
    queue holding more than the one triaged entry takes the rest with it. So the
    assertion is not just that a delete is permitted but that **the other entry
    survives it**. `~*.dlq` is deliberately a glob: the queue this exists for is
    by definition one nobody declared.
    """
    orphan = "nobody.claims.this.dlq"
    probe = tracked_acl_broker("brokeradmin")
    with _seeder(tracked_acl_broker) as seeder:
        doomed = seeder.xadd(orphan, {"k": "triaged"})
        kept = seeder.xadd(orphan, {"k": "keep"})
        stream_id = seeder.xadd(CONTENT_FETCH, {"k": "v"})
        assert probe.xdel(orphan, doomed) == 1
        assert probe.xlen(orphan) == 1, "a per-entry disposal must leave the rest"
        assert probe.xrange(orphan)[0][0] == kept
        with pytest.raises(redis_pkg.exceptions.NoPermissionError):
            probe.xdel(CONTENT_FETCH, stream_id)


def test_citest_cannot_name_a_production_topic(tracked_acl_broker) -> None:
    """R4, on the axis ACLs can actually enforce. The db-15 guard was never the
    enforcement - Redis ACLs cannot partition by database index at all - so a
    credential that cannot NAME a production topic is. The database-index axis is
    closed separately by `databases 1` (CannObserv/broker#5)."""
    client = tracked_acl_broker("citest")
    assert client.xadd("probe.scratch", {"k": "v"})
    for topic in (CONTENT_FETCH, CONTENT_REPLICATE, CONTENT_BLOBS):
        with pytest.raises(redis_pkg.exceptions.NoPermissionError):
            client.xadd(topic, {"k": "v"})
