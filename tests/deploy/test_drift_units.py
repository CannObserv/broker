"""The drift check's units must exist, carry their guards, and stay in sync (#110).

Same shape as ``test_bus_health_units.py`` and ``test_backup_units.py``: content
asserted on the repo copy everywhere, byte-parity against
``/etc/systemd/system/`` on the node, where a missing unit fails rather than
skips (CannObserv/broker#81).
"""

from __future__ import annotations

import re
import shlex
import sys
from pathlib import Path

import pytest

from src.broker import checkin, drift
from tests.deploy.conftest import (
    LIVE,
    outcome_of,
    pretend_node,
    read_installed,
    require_broker_node,
)

_REPO = Path(__file__).resolve().parents[2]
REPO_SERVICE = _REPO / "deploy" / "broker-drift.service"
REPO_TIMER = _REPO / "deploy" / "broker-drift.timer"
README = _REPO / "deploy" / "README.md"
INSTALLED_SERVICE = Path("/etc/systemd/system/broker-drift.service")
INSTALLED_TIMER = Path("/etc/systemd/system/broker-drift.timer")
#: co-broker-drift, tenant co-broker (CannObserv/status#51). Not a secret.
MONITOR = "01M5CVMEFPKRGP2Y48F7QTPP7D"
KEY_FILE = "/etc/broker/status-checkin.key"


def _directive(text: str, key: str) -> list[str]:
    return [ln.split("=", 1)[1] for ln in text.splitlines() if ln.startswith(f"{key}=")]


def test_service_is_a_oneshot_drift_check_from_live() -> None:
    text = REPO_SERVICE.read_text()
    assert _directive(text, "Type") == ["oneshot"]
    assert _directive(text, "WorkingDirectory") == [str(LIVE)]
    (exec_start,) = _directive(text, "ExecStart")
    assert shlex.split(exec_start) == [f"{LIVE}/.venv/bin/python", "-m", "src.broker.drift"], (
        "the release's venv, run from live, so read_revision() reads live's REVISION (#105 CR 2)"
    )


def test_service_writes_no_bytecode() -> None:
    assert "PYTHONDONTWRITEBYTECODE=1" in _directive(REPO_SERVICE.read_text(), "Environment")


def test_service_runs_as_a_dynamic_user() -> None:
    """D6: no account of its own to hold anything, and none of exedev's."""
    text = REPO_SERVICE.read_text()
    assert _directive(text, "DynamicUser") == ["yes"]
    assert _directive(text, "User") == []
    assert _directive(text, "Group") == []


def test_service_names_the_monitor_and_no_host() -> None:
    """The id is not a secret and lives in the unit; the host is checkin's constant."""
    environment = _directive(REPO_SERVICE.read_text(), "Environment")
    assert f"{drift.MONITOR_ID_ENV}={MONITOR}" in environment
    assert not [e for e in environment if "status:" in e or "http" in e]
    assert drift._MONITOR_ID.fullmatch(MONITOR)


def test_service_reads_the_key_as_a_credential_with_an_empty_fallback() -> None:
    """``LoadCredential=`` from a root-only file of its own, never an environment
    variable. ``SetCredential=`` is the fallback for a missing file: the check
    then runs, logs that it has no key, and exits 1, rather than failing to start."""
    text = REPO_SERVICE.read_text()
    assert _directive(text, "LoadCredential") == [f"{checkin.CREDENTIAL_NAME}:{KEY_FILE}"]
    assert _directive(text, "SetCredential") == [f"{checkin.CREDENTIAL_NAME}:\\n"]
    assert "STATUS_API_KEY" not in text


def test_service_holds_no_redis_credential_and_no_env_file() -> None:
    """D6, B8: the drift check holds no Redis credential. /etc/broker/.env carries
    the probe's Redis user and the operator's wheelhouse key; status.env the probe's
    copy of the co-status key. None of them is this unit's."""
    text = REPO_SERVICE.read_text()
    assert _directive(text, "EnvironmentFile") == []
    assert _directive(text, "LoadCredentialEncrypted") == []
    assert "brokeradmin" not in text and "acladmin" not in text
    assert "BROKER_REDIS_URL" not in text


def test_service_sees_no_home_and_hides_nothing_its_credential_needs() -> None:
    """The dev tree's .env, under /home, holds the cohort's GitHub tokens. Nothing
    may hide /etc/broker (CR 2): the credential's source is there, and whether
    systemd reads it before the mount namespace is not documented. A dynamic user
    reads nothing there anyway (test_etc_broker_holds_nothing_a_dynamic_user_reads)."""
    text = REPO_SERVICE.read_text()
    key_dir = str(Path(KEY_FILE).parent)
    for key in ("TemporaryFileSystem", "InaccessiblePaths", "BindPaths", "BindReadOnlyPaths"):
        assert not [v for v in _directive(text, key) if key_dir in v], key
    assert _directive(text, "ProtectHome") == ["yes"]
    assert _directive(text, "ProtectSystem") == ["strict"]
    assert _directive(text, "PrivateTmp") == ["yes"]
    assert _directive(text, "NoNewPrivileges") == ["yes"]


def test_etc_broker_holds_nothing_a_dynamic_user_reads() -> None:
    """What stands in for a hidden /etc/broker: every file there is root's, and none
    is readable by a user outside root's and exedev's groups. On the node only."""
    require_broker_node()
    readable = sorted(
        p.name for p in Path("/etc/broker").iterdir() if p.is_file() and p.stat().st_mode & 0o004
    )
    assert readable == [], f"world-readable under /etc/broker: {readable}"


def test_service_bounds_its_own_runtime_and_never_restarts() -> None:
    """No GitHub call starts past 60 s, then one check-in: 90 s is the hard stop,
    and the next hour is the retry."""
    text = REPO_SERVICE.read_text()
    assert _directive(text, "TimeoutStartSec") == ["90"]
    assert drift.CHECK_TIMEOUT_SECONDS + checkin.TIMEOUT_SECONDS < 90
    assert _directive(text, "Restart") == []


def test_service_is_not_bound_to_the_broker() -> None:
    """A Redis outage is not the drift check's business, nor the reverse."""
    text = REPO_SERVICE.read_text()
    for key in ("After", "Wants", "Requires", "BindsTo", "PartOf"):
        assert not [v for v in _directive(text, key) if "redis" in v or "broker-" in v], key


def test_timer_is_hourly_and_starts_after_boot() -> None:
    """co-broker-drift expects a check-in every 3600 s, with a grace of 5400 s
    (CannObserv/status#51): one missed run is absorbed, two alarm."""
    text = REPO_TIMER.read_text()
    assert _directive(text, "Unit") == ["broker-drift.service"]
    assert _directive(text, "OnBootSec") == ["5min"]
    assert _directive(text, "OnUnitActiveSec") == ["1h"]
    assert _directive(text, "WantedBy") == ["timers.target"]


def test_the_readme_runbook_names_the_units_monitor() -> None:
    """The transient --test-alert copies the unit's Environment=, and the README
    quotes the id beside it for reading co-status back: one id, everywhere."""
    text = README.read_text()
    ids = set(re.findall(r"\b01[0-9A-HJKMNP-TV-Z]{24}\b", text.split("## The drift check")[1]))
    assert ids == {MONITOR}


def test_installed_service_matches_repo() -> None:
    installed = read_installed(INSTALLED_SERVICE)
    assert installed == REPO_SERVICE.read_text(), (
        f"{INSTALLED_SERVICE} has drifted from {REPO_SERVICE}: deploy the release that"
        " carries it (deploy/README.md, Deploying)"
    )


def test_installed_timer_matches_repo() -> None:
    installed = read_installed(INSTALLED_TIMER)
    assert installed == REPO_TIMER.read_text(), (
        f"{INSTALLED_TIMER} has drifted from {REPO_TIMER}: deploy the release that"
        " carries it (deploy/README.md, Deploying)"
    )


# --- the node signal the installed checks above skip on (broker#81) ---


@pytest.mark.parametrize("present", [True, False], ids=["on-node", "off-node"])
@pytest.mark.parametrize(
    ("constant", "test"),
    [
        ("INSTALLED_SERVICE", test_installed_service_matches_repo),
        ("INSTALLED_TIMER", test_installed_timer_matches_repo),
    ],
    ids=["service", "timer"],
)
def test_an_absent_unit_fails_on_the_node_and_skips_off_it(
    monkeypatch, tmp_path, constant: str, test, present: bool
) -> None:
    """Keyed on the node, never the unit: removing the unit cannot skip its own check."""
    monkeypatch.setattr(sys.modules[__name__], constant, tmp_path / "absent")
    pretend_node(monkeypatch, tmp_path, present=present)
    expected = pytest.fail.Exception if present else pytest.skip.Exception
    assert isinstance(outcome_of(test), expected)
