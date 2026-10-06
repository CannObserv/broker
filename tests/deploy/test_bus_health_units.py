"""The bus-health timer units must exist, carry their guards, and stay in sync.

Same failure class as the drop-in parity test: the deployed thing quietly
diverging from the documented thing. Both units are asserted for content in the
repo copy (runs everywhere) and for byte-parity against ``/etc/systemd/system/``
on the node. Off it they skip, by ``on_broker_node`` rather than by the unit's own
absence: on the node a missing unit, or ``/etc/broker/.env``, fails
(CannObserv/broker#81).
"""

import re
import sys
from pathlib import Path
from urllib.parse import urlsplit

import pytest

from src.broker.bus_health import BROKER_CREDENTIAL
from tests.deploy.conftest import outcome_of, pretend_node, read_installed

_ROOT = Path(__file__).resolve().parents[2]
_DEPLOY = _ROOT / "deploy"
REPO_SERVICE = _DEPLOY / "broker-bus-health.service"
REPO_TIMER = _DEPLOY / "broker-bus-health.timer"
INSTALLED_SERVICE = Path("/etc/systemd/system/broker-bus-health.service")
INSTALLED_TIMER = Path("/etc/systemd/system/broker-bus-health.timer")
PROBE_SOURCE = _ROOT / "src" / "broker" / "bus_health.py"
# The file the probe shares with the operator: 0640 root:exedev, so readable by
# the account these tests run as, by design.
SHARED_ENV = Path("/etc/broker/.env")
WHEELHOUSE_KEY = "GOOGLE_APPLICATION_CREDENTIALS"
# systemd's own search path for encrypted credentials, 0700 root. The probe's
# password is here, encrypted to this host (CannObserv/broker#52).
CREDSTORE = Path("/etc/credstore.encrypted")


def _comment_block_holding(text: str, phrase: str) -> str:
    """The contiguous run of ``#`` lines carrying ``phrase``.

    A whole-file substring test cannot tell a rule stated from a rule deleted:
    the words would still be somewhere in the unit. Anchoring to the block lets
    the sentence be rewrapped - CannObserv/broker#31 moved a word between lines
    already - while still failing if the sentence itself goes.
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


def _directive(text: str, key: str) -> list[str]:
    return [ln.split("=", 1)[1] for ln in text.splitlines() if ln.startswith(f"{key}=")]


def _names_assigned_in(path: Path) -> set[str]:
    """The variable names an ``EnvironmentFile`` assigns.

    A helper rather than inline, so the file's text - which held a password in
    ``BROKER_REDIS_URL`` until CannObserv/broker#52, and would again were it put
    back - is never a local of the test frame: ``pytest -l`` prints a failing
    test's locals, and this frame has returned by then.
    """
    text = path.read_text()
    return {
        line.split("=", 1)[0].strip()
        for line in text.splitlines()
        if "=" in line and not line.lstrip().startswith(("#", ";"))
    }


def _url_carries_a_password(path: Path, name: str) -> bool | None:
    """Whether ``name`` in ``path`` is a URL with a password, or ``None`` if unset.

    A helper for the same reason as ``_names_assigned_in``: the value never
    becomes a local of the test frame, and only the verdict crosses back.
    """
    text = path.read_text()
    # Commented lines count: a copy "kept as the rollback" is still plaintext
    # at rest. A comment with no password in it is prose and says nothing.
    assignment = re.compile(rf"^\s*(?P<comment>#\s*)?(?:export\s+)?{name}\s*=\s*(?P<value>\S*)")
    live, commented = [], []
    for line in text.splitlines():
        if match := assignment.match(line):
            value = match.group("value").strip("\"'")
            (commented if match.group("comment") else live).append(value)
    carrying = [value for value in live + commented if urlsplit(value).password is not None]
    if carrying:
        return True
    return False if live else None


def _variables_the_probe_reads() -> set[str]:
    """Every name ``src/broker/bus_health.py`` looks up in its environment."""
    source = PROBE_SOURCE.read_text()
    return set(re.findall(r'os\.(?:environ\.get|getenv)\(\s*"(\w+)"', source)) | set(
        re.findall(r'os\.environ\[\s*"(\w+)"\s*\]', source)
    )


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

    It does not today: the unit states the rule in a comment that names no
    variable at all, which is why this says *may* rather than the *does* its
    database sibling can say of its own."""
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
    installed = read_installed(INSTALLED_SERVICE)
    assert installed == REPO_SERVICE.read_text(), (
        f"{INSTALLED_SERVICE} has drifted from {REPO_SERVICE}.\n"
        "Reinstall with:\n"
        f"  sudo cp {REPO_SERVICE} {INSTALLED_SERVICE} && sudo systemctl daemon-reload"
    )


def test_installed_timer_matches_repo() -> None:
    installed = read_installed(INSTALLED_TIMER)
    assert installed == REPO_TIMER.read_text(), (
        f"{INSTALLED_TIMER} has drifted from {REPO_TIMER}.\n"
        "Reinstall with:\n"
        f"  sudo cp {REPO_TIMER} {INSTALLED_TIMER} && sudo systemctl daemon-reload"
    )


def test_the_checkin_credential_has_a_file_of_its_own() -> None:
    """CannObserv/broker#3's key must not ride in /etc/broker/.env.

    That file is 0640 root:exedev, so the unit's own `User=` can read it at any
    time, running or not. systemd reads an EnvironmentFile as root before
    dropping privileges, so a 0400 root:root file still reaches the process
    while staying unreadable to the account. The process seeing its own
    environment is unavoidable and fine; a second copy sitting in a file the
    account can `cat` is not.

    The leading `-` is part of the contract: absent is the supported state, and
    a node not yet wired to co-status must still start.

    And only the one file: ``notifier.env`` was kept through the handover for
    the key that disabled notifier's copy (#66), was never the probe's, and was
    shredded as ``notifier.env.pre-status-66`` once notifier deleted the
    ``co-broker`` tenant (#70). The substring assert covers either spelling.
    """
    text = REPO_SERVICE.read_text()
    assert "EnvironmentFile=-/etc/broker/status.env" in text
    assert "notifier.env" not in text, "notifier's monitor is retired (#66)"
    assert "Environment=STATUS_API_KEY" not in text, "a credential never belongs in the unit"


def test_the_units_name_only_reads_the_probe_issues() -> None:
    """Each unit names the group read the probe makes, and no other.

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
    the third names the triage query ``+xpending`` is held for
    (CannObserv/broker#32) - a command an operator issues at a ``redis-cli``,
    which is a caller this unit is not.
    """
    rule = _comment_block_holding(REPO_SERVICE.read_text(), "must never join")
    assert "XINFO GROUPS" in rule, (
        "the sentence stating the rule is what has to name the read - a match "
        "anywhere else in the unit would pass with the sentence deleted"
    )
    for path in (REPO_SERVICE, REPO_TIMER):
        assert "XPENDING" not in path.read_text(), (
            f"{path.name} names a command the probe has not issued since CannObserv/broker#29"
        )


def test_no_unit_runs_the_wheelhouse_sync() -> None:
    """The node's wheelhouse is refreshed by hand, and the docs say so
    (CannObserv/broker#37).

    ``scripts/sync_wheelhouse.py`` came from archiver, whose service does run
    it as an ``ExecStartPre``; no broker unit ever has, and the docstring that
    said otherwise was archiver's. Adding one would put a GCS call in front of
    every probe tick, for an input only a pin change moves, in a probe that
    exists to report while other things are down. Whoever wants it anyway
    arrives here, and the docs that say "manual" change with it.
    """
    units = sorted(_DEPLOY.glob("*.service"))
    assert units, "the glob found no units - the guard would pass by finding nothing"
    # A drop-in is where a step gets added to a unit without editing it, and
    # deploy/ already ships them.
    dropins = sorted(_DEPLOY.glob("*.service.d/*.conf"))
    running = [
        str(u.relative_to(_DEPLOY)) for u in units + dropins if "sync_wheelhouse" in u.read_text()
    ]
    assert not running, f"units running the wheelhouse sync: {running}"


def test_the_unit_loads_environment_only_from_etc_broker() -> None:
    """The dev tree's ``.env`` carries the cohort's GitHub tokens and an
    Anthropic key, and the probe reads none of them (CannObserv/broker#37).

    Loading it handed all of them to a process that runs every ten minutes. The
    grant rule in ``deploy/redis-acl.conf`` - a grant nothing issues names its
    caller - applies to credentials too, and this file's caller is an operator
    at a shell, never this unit.
    """
    files = [f.lstrip("-") for f in _directive(REPO_SERVICE.read_text(), "EnvironmentFile")]
    assert files, "the probe needs BROKER_REDIS_URL from somewhere"
    stray = [f for f in files if not f.startswith("/etc/broker/")]
    assert not stray, f"environment files outside /etc/broker/: {stray}"


def test_the_unit_unsets_the_wheelhouse_key() -> None:
    """``GOOGLE_APPLICATION_CREDENTIALS`` is the operator's, not the probe's
    (CannObserv/broker#37).

    It shares ``/etc/broker/.env`` with ``BROKER_REDIS_URL`` so the manual sync
    is one ``source`` away. The unit has no use for it: ``uv run`` resolves
    ``co-core`` from ``./.wheelhouse``, a local directory, and the probe imports
    no storage SDK. Unset rather than split into a second file, so the operator's
    command stays what AGENTS.md says it is.
    """
    unset = " ".join(_directive(REPO_SERVICE.read_text(), "UnsetEnvironment")).split()
    assert WHEELHOUSE_KEY in unset
    assert WHEELHOUSE_KEY not in _variables_the_probe_reads(), (
        "the probe now reads the key it unsets - it would start without it"
    )


def test_every_variable_the_probe_inherits_is_one_it_reads() -> None:
    """The credential form of the ACL's grant rule, checked on the live file.

    ``/etc/broker/.env`` is edited on the node, not in this repo, so a variable
    added there reaches the probe with nothing in review to notice. Each name it
    assigns is either read by ``src/broker/bus_health.py`` or unset by the unit.
    Names only: no value leaves the file, pass or fail.
    """
    read_installed(SHARED_ENV)
    assigned = _names_assigned_in(SHARED_ENV)
    unset = set(" ".join(_directive(REPO_SERVICE.read_text(), "UnsetEnvironment")).split())
    unused = sorted(assigned - unset - _variables_the_probe_reads())
    assert not unused, (
        f"{SHARED_ENV} hands the probe variables it never reads: {unused}. "
        "Read it in src/broker/bus_health.py, or add it to the unit's "
        "UnsetEnvironment= and say whose it is"
    )


def test_the_probe_password_is_an_encrypted_credential() -> None:
    """``brokeradmin``'s password reaches the probe decrypted, and only the probe.

    Until CannObserv/broker#52 it was the password segment of
    ``BROKER_REDIS_URL`` in ``/etc/broker/.env``, ``0640 root:exedev`` - the one
    Redis credential on this node readable without sudo, at any time. Now it is a
    ``systemd-creds`` credential encrypted to this host, which systemd decrypts
    into the unit's ``$CREDENTIALS_DIRECTORY`` for the life of the tick. The ID
    here is the name ``src/broker/bus_health.py`` reads, and the path is
    systemd's own credential store.
    """
    loads = _directive(REPO_SERVICE.read_text(), "LoadCredentialEncrypted")
    assert loads == [f"{BROKER_CREDENTIAL}:{CREDSTORE / BROKER_CREDENTIAL}"], loads
    assert not _directive(REPO_SERVICE.read_text(), "LoadCredential"), (
        "a plaintext LoadCredential= would put the password back on disk"
    )
    assert not _directive(REPO_SERVICE.read_text(), "SetCredential"), (
        "SetCredential= writes the password into the unit"
    )


def test_the_shared_env_carries_no_redis_password() -> None:
    """The other half: the URL names the user, never the password.

    redis-py's ``from_url`` lets a password in the URL override the one the
    probe passes from its credential, so one put back in ``/etc/broker/.env``
    would silently win - and be readable by the account again. The verdict
    alone leaves the file.
    """
    read_installed(SHARED_ENV)
    carries = _url_carries_a_password(SHARED_ENV, "BROKER_REDIS_URL")
    assert carries is not None, (
        f"{SHARED_ENV} assigns no BROKER_REDIS_URL: the probe starts, logs "
        '"nothing to probe" and checks nothing. Restore '
        "BROKER_REDIS_URL=redis://brokeradmin@<host>:6379/0 (deploy/README.md)."
    )
    assert not carries, (
        f"BROKER_REDIS_URL in {SHARED_ENV} carries a password. It overrides the unit's "
        f"encrypted credential and is readable without sudo: make it "
        "redis://brokeradmin@<host>:6379/0 (CannObserv/broker#52)."
    )


@pytest.mark.parametrize(
    ("url", "carries"),
    [
        ("redis://brokeradmin@localhost:6379/0", False),
        ("redis://brokeradmin:minted@localhost:6379/0", True),
        ("'redis://brokeradmin:minted@localhost:6379/0'", True),
        ("redis://:minted@localhost:6379/0", True),
    ],
    ids=["user-only", "user-and-password", "quoted", "password-only"],
)
def test_a_url_password_is_detected_in_any_spelling(tmp_path, url, carries) -> None:
    env = tmp_path / ".env"
    env.write_text(f"OTHER=x\nBROKER_REDIS_URL={url}\n")
    assert _url_carries_a_password(env, "BROKER_REDIS_URL") is carries


@pytest.mark.parametrize(
    ("lines", "carries"),
    [
        (["export BROKER_REDIS_URL=redis://brokeradmin:minted@localhost:6379/0"], True),
        (["  BROKER_REDIS_URL = redis://brokeradmin:minted@localhost:6379/0"], True),
        (
            [
                "BROKER_REDIS_URL=redis://brokeradmin@localhost:6379/0",
                "#BROKER_REDIS_URL=redis://brokeradmin:minted@localhost:6379/0",
            ],
            True,
        ),
        (["# BROKER_REDIS_URL=redis://brokeradmin@localhost:6379/0 (the old form)"], None),
    ],
    ids=["export", "spaced", "commented-rollback", "commented-without-password"],
)
def test_a_url_password_is_detected_in_any_assignment_form(tmp_path, lines, carries) -> None:
    """A commented copy kept "as the rollback" is the shape #49 found a leaked
    secret in; `export` is honoured by the `set -a; .` sourcing this repo uses;
    and systemd's parser accepts whitespace around `=`. A comment carrying no
    password is not an assignment, so it neither passes nor fails the file."""
    env = tmp_path / ".env"
    env.write_text("\n".join(["OTHER=x", *lines]) + "\n")
    assert _url_carries_a_password(env, "BROKER_REDIS_URL") is carries


# --- the node signal the installed checks above skip on (broker#81) ---


@pytest.mark.parametrize("present", [True, False], ids=["on-node", "off-node"])
@pytest.mark.parametrize(
    ("constant", "test"),
    [
        ("INSTALLED_SERVICE", test_installed_service_matches_repo),
        ("INSTALLED_TIMER", test_installed_timer_matches_repo),
        ("SHARED_ENV", test_every_variable_the_probe_inherits_is_one_it_reads),
        ("SHARED_ENV", test_the_shared_env_carries_no_redis_password),
    ],
    ids=["service", "timer", "env-names", "env-password"],
)
def test_an_absent_file_fails_on_the_node_and_skips_off_it(
    monkeypatch, tmp_path, constant: str, test, present: bool
) -> None:
    """Each check reads one file the node installs. Keyed on that file, deleting
    it would skip the one check that guards it, on the one host it is for."""
    monkeypatch.setattr(sys.modules[__name__], constant, tmp_path / "absent")
    pretend_node(monkeypatch, tmp_path, present=present)
    expected = pytest.fail.Exception if present else pytest.skip.Exception
    assert isinstance(outcome_of(test), expected)


def test_a_shared_env_without_the_url_fails_on_the_node(monkeypatch, tmp_path) -> None:
    """A separate finding from an absent file: the probe starts, logs
    "nothing to probe" and checks nothing, every tick."""
    env = tmp_path / ".env"
    env.write_text("OTHER=x\n# BROKER_REDIS_URL=redis://brokeradmin@localhost:6379/0\n")
    monkeypatch.setattr(sys.modules[__name__], "SHARED_ENV", env)
    pretend_node(monkeypatch, tmp_path, present=True)
    outcome = outcome_of(test_the_shared_env_carries_no_redis_password)
    assert isinstance(outcome, AssertionError)
    assert "assigns no BROKER_REDIS_URL" in str(outcome)
