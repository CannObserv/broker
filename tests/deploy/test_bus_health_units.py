"""The bus-health timer units must exist, carry their guards, and stay in sync.

Same failure class as the drop-in parity test: the deployed thing quietly
diverging from the documented thing. Both units are asserted for content in the
repo copy (runs everywhere) and for byte-parity against ``/etc/systemd/system/``
(skips on hosts that do not run the timer).
"""

from pathlib import Path

import pytest

_DEPLOY = Path(__file__).resolve().parents[2] / "deploy"
REPO_SERVICE = _DEPLOY / "broker-bus-health.service"
REPO_TIMER = _DEPLOY / "broker-bus-health.timer"
INSTALLED_SERVICE = Path("/etc/systemd/system/broker-bus-health.service")
INSTALLED_TIMER = Path("/etc/systemd/system/broker-bus-health.timer")


def _read_if_installed(path: Path) -> str | None:
    """Only ``FileNotFoundError`` means "not installed" - a ``PermissionError``
    propagates rather than silently passing."""
    try:
        return path.read_text()
    except FileNotFoundError:
        return None


def _comment_block_holding(text: str, phrase: str) -> str:
    """The contiguous run of ``#`` lines carrying ``phrase``.

    A whole-file substring test cannot tell a rule stated from a rule deleted:
    the words would still be somewhere in the unit. Anchoring to the block lets
    the sentence be rewrapped - #31 moved a word between lines already - while
    still failing if the sentence itself goes.
    """
    lines = text.splitlines()
    matches = [i for i, line in enumerate(lines) if phrase in line]
    assert len(matches) == 1, (
        f"expected exactly one line holding {phrase!r}, found {len(matches)} - "
        "a deleted sentence is the failure this helper exists to report, so it "
        "says so rather than raising out of an unpack"
    )
    (index,) = matches
    start = index
    while start > 0 and lines[start - 1].startswith("#"):
        start -= 1
    end = index
    while end + 1 < len(lines) and lines[end + 1].startswith("#"):
        end += 1
    return "\n".join(lines[start : end + 1])


def test_service_is_a_oneshot_probe() -> None:
    text = REPO_SERVICE.read_text()
    assert "Type=oneshot" in text
    assert "src.broker.bus_health" in text


def test_service_holds_no_database_opt_in() -> None:
    """The half of the old combined probe that queried ``changes_outbox``
    stayed in archiver, and so did its ``ARCHIVER_ALLOW_PRODUCTION_DB`` opt-in
    (archiver#193 D6). Nothing on this node has a reason to reach a service's
    database; an opt-in appearing here would mean the split leaked back.

    Keyed on the ``Environment=`` assignment, not the bare name, for the same
    reason as the consumer-group test below: the unit's comment names the
    variable precisely to say it is absent, and a substring test would forbid
    saying so.
    """
    assert "Environment=ARCHIVER_ALLOW_PRODUCTION_DB" not in REPO_SERVICE.read_text()


def test_service_never_joins_a_consumer_group() -> None:
    """XINFO GROUPS is read-only group introspection. Joining a group from a probe
    would silently swallow another service's messages. The unit may mention the
    variable in a comment saying exactly that - only an ``Environment=``
    assignment is the hazard.

    It does not today: the rule above ``StateDirectory`` states the rule without
    naming ``ARCHIVER_BUS_CONSUMER``, which is why this says *may* rather than
    the *does* its database sibling can say of its own variable."""
    assert "Environment=ARCHIVER_BUS_CONSUMER" not in REPO_SERVICE.read_text()


def test_service_bounds_its_own_runtime() -> None:
    """The per-call socket timeouts bound each Redis command, but ~25 commands
    each hitting their ceiling could in theory outlast systemd's default
    TimeoutStartSec (90s) - the pathological case the socket bounds were added
    to rule out. An explicit, shorter bound keeps a wedged probe visibly killed
    rather than hanging: a tick that cannot finish inside it has nothing useful
    left to report anyway."""
    text = REPO_SERVICE.read_text()
    assert "TimeoutStartSec=" in text
    (line,) = [ln for ln in text.splitlines() if ln.startswith("TimeoutStartSec=")]
    assert int(line.split("=", 1)[1].rstrip("s")) < 90


def test_service_is_not_bound_to_the_broker_it_probes() -> None:
    """The probe exists to report while redis-server is down. A ``Requires=``
    or ``BindsTo=`` on it would silence the probe in exactly the state it was
    written for."""
    text = REPO_SERVICE.read_text()
    assert "Requires=redis-server.service" not in text
    assert "BindsTo=redis-server.service" not in text


def test_timer_ticks_periodically() -> None:
    text = REPO_TIMER.read_text()
    assert "OnUnitActiveSec=" in text
    assert "OnBootSec=" in text


def test_installed_service_matches_repo() -> None:
    installed = _read_if_installed(INSTALLED_SERVICE)
    if installed is None:
        pytest.skip(f"{INSTALLED_SERVICE} not present - not a host running the timer")
    assert installed == REPO_SERVICE.read_text(), (
        f"{INSTALLED_SERVICE} has drifted from {REPO_SERVICE}.\n"
        "Reinstall with:\n"
        f"  sudo cp {REPO_SERVICE} {INSTALLED_SERVICE} && sudo systemctl daemon-reload"
    )


def test_installed_timer_matches_repo() -> None:
    installed = _read_if_installed(INSTALLED_TIMER)
    if installed is None:
        pytest.skip(f"{INSTALLED_TIMER} not present - not a host running the timer")
    assert installed == REPO_TIMER.read_text(), (
        f"{INSTALLED_TIMER} has drifted from {REPO_TIMER}.\n"
        "Reinstall with:\n"
        f"  sudo cp {REPO_TIMER} {INSTALLED_TIMER} && sudo systemctl daemon-reload"
    )


def test_the_notifier_credential_has_a_file_of_its_own() -> None:
    """CannObserv/broker#3's key must not ride in /etc/broker/.env.

    That file is 0640 root:exedev, so the unit's own `User=` can read it at any
    time, running or not. systemd reads an EnvironmentFile as root before
    dropping privileges, so a 0400 root:root file still reaches the process
    while staying unreadable to the account. The process seeing its own
    environment is unavoidable and fine; a second copy sitting in a file the
    account can `cat` is not.

    The leading `-` is part of the contract: absent is the supported state, and
    a node not yet wired to notifier must still start.
    """
    text = REPO_SERVICE.read_text()
    assert "EnvironmentFile=-/etc/broker/notifier.env" in text
    assert "Environment=NOTIFIER_API_KEY" not in text, "a credential never belongs in the unit"


def test_the_units_name_the_group_read_the_probe_issues() -> None:
    """Both units describe a read the probe stopped issuing.

    Since CannObserv/broker#29 there is no ``XPENDING`` in the tick at all: a
    group's existence, its ``pending`` count and its ``last-delivered-id`` all
    come out of one ``XINFO GROUPS`` per grouped stream. The rules the units
    state are unchanged - the probe joins no consumer group, and the two-tick
    pending rule still carries a count between oneshot runs - so only the name
    was wrong, and ``XINFO GROUPS`` is now the read-only introspection the
    service's sentence is about (CannObserv/broker#31).

    These units state live rules, which is why the bare string is forbidden
    here and nowhere else. ``deploy/redis-acl.conf`` still says ``XPENDING``
    and keeps every one: two of those sentences narrate the 2026-09 ``EXISTS``
    incident, where the two-tick rule genuinely was an ``XPENDING`` rule, and
    the third names the triage query ``+xpending`` is held for (broker#32) - a
    command an operator issues at a ``redis-cli``, which is a caller this unit
    is not.
    """
    rule = _comment_block_holding(REPO_SERVICE.read_text(), "must never join")
    assert "XINFO GROUPS" in rule, (
        "the sentence stating the rule is what has to name the read - a match "
        "anywhere else in the unit would pass with the sentence deleted"
    )
    for path in (REPO_SERVICE, REPO_TIMER):
        assert "XPENDING" not in path.read_text(), (
            f"{path.name} names a command the probe has not issued since broker#29"
        )
