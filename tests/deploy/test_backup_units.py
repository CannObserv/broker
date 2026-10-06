"""The backup timer units must exist, carry their guards, and stay in sync
(CannObserv/broker#4).

Same shape as ``test_bus_health_units.py``: content asserted on the repo copy
everywhere, byte-parity against ``/etc/systemd/system/`` on the node, where a
missing unit fails rather than skips (CannObserv/broker#81).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from tests.deploy.conftest import outcome_of, pretend_node, read_installed

_DEPLOY = Path(__file__).resolve().parents[2] / "deploy"
REPO_SERVICE = _DEPLOY / "broker-backup.service"
REPO_TIMER = _DEPLOY / "broker-backup.timer"
REPO_PROBE_SERVICE = _DEPLOY / "broker-bus-health.service"
INSTALLED_SERVICE = Path("/etc/systemd/system/broker-backup.service")
INSTALLED_TIMER = Path("/etc/systemd/system/broker-backup.timer")


def _directive(text: str, key: str) -> list[str]:
    return [ln.split("=", 1)[1] for ln in text.splitlines() if ln.startswith(f"{key}=")]


def test_service_is_a_oneshot_running_the_backup_module() -> None:
    text = REPO_SERVICE.read_text()
    assert "Type=oneshot" in text
    assert "src.broker.backup" in text


def test_service_holds_no_redis_credential() -> None:
    """The job reads a file. It has no reason to hold a Redis URL, and the file
    that carries one (/etc/broker/.env) also carries the operator's read-only
    wheelhouse key - two things this unit must not inherit.

    It ships the ACL digests since CannObserv/broker#72, and this still holds:
    a digest is not a credential, because ``AUTH`` takes the plaintext. That
    rests on each plaintext being high-entropy (40 alphanumerics from urandom,
    docs/ACL-CUTOVER.md) - an unsalted SHA-256 of a guessable one would be a
    credential by another name. The job refuses a passwords file carrying any
    plaintext line (``project_digests``), and the node users' lines never ship.
    """
    text = REPO_SERVICE.read_text()
    assert "EnvironmentFile=/etc/broker/.env" not in text
    assert "EnvironmentFile=-/etc/broker/.env" not in text
    assert "Environment=BROKER_REDIS_URL" not in text


def test_service_ships_the_acl_digests_from_the_passwords_file() -> None:
    """Named on the command line rather than left to the default, so the path
    the unit reads is the one this test and docs/RECOVERY.md say it reads."""
    (exec_start,) = _directive(REPO_SERVICE.read_text(), "ExecStart")
    assert "--acl-passwords /etc/redis/broker-acl-passwords" in exec_start


def test_service_sees_only_its_own_key_under_etc_broker() -> None:
    """Root with ``CAP_DAC_READ_SEARCH`` reads anything, and since broker#72 it
    reads /etc/redis on purpose. /etc/broker holds other identities' keys - the
    probe's env, the wheelhouse reader, the co-status check-in - so it is an
    allowlist, not a blocklist: an empty read-only tmpfs over the directory with
    the writer key bound back in, and any file added later is hidden by default.
    backup.env is read by systemd before the namespace exists, so it needs no bind.
    """
    text = REPO_SERVICE.read_text()
    assert _directive(text, "TemporaryFileSystem") == ["/etc/broker:ro"]
    assert _directive(text, "BindReadOnlyPaths") == ["/etc/broker/co-broker-backup.json"]


def test_service_cannot_read_the_node_credentials() -> None:
    """The node users' encrypted credentials. ``-``: absent is no failure,
    only unreadable is the point."""
    assert _directive(REPO_SERVICE.read_text(), "InaccessiblePaths") == [
        "-/etc/credstore.encrypted"
    ]


def test_service_config_is_required_not_optional() -> None:
    """No leading ``-``: a backup unit without its bucket and writer key must
    fail loudly. The probe's check-in file is optional because absent is its
    supported state; a backup with nowhere to write has no such state."""
    assert "EnvironmentFile=/etc/broker/backup.env" in REPO_SERVICE.read_text()


def test_service_can_read_redis_and_write_nothing_of_redis() -> None:
    """root, confined. ``/var/lib/redis`` is 0750 redis:redis, so the reader is
    root or in the redis group - and the group holds WRITE on dump.rdb. The
    sandbox is what makes root the safer choice: the filesystem is read-only
    except the state directory and a private /tmp, and the only capability
    left is the one that reads."""
    text = REPO_SERVICE.read_text()
    assert _directive(text, "User") == ["root"]
    assert _directive(text, "ProtectSystem") == ["strict"]
    assert _directive(text, "PrivateTmp") == ["yes"]
    assert _directive(text, "NoNewPrivileges") == ["yes"]
    assert _directive(text, "CapabilityBoundingSet") == ["CAP_DAC_READ_SEARCH"]
    assert _directive(text, "ReadWritePaths") == []  # StateDirectory is the only writable path
    assert _directive(text, "StateDirectory") == ["broker-backup"]


def test_service_runs_the_venv_interpreter_not_uv() -> None:
    """``uv run`` wants a writable cache and may sync the environment, both of
    which the sandbox refuses. Dependency sync is a deploy step."""
    (exec_start,) = _directive(REPO_SERVICE.read_text(), "ExecStart")
    assert exec_start.startswith("/home/exedev/broker/.venv/bin/python ")


def test_service_bounds_its_own_runtime() -> None:
    (line,) = _directive(REPO_SERVICE.read_text(), "TimeoutStartSec")
    assert 0 < int(line.rstrip("s")) <= 600


def test_service_is_not_bound_to_the_broker() -> None:
    """The artifact is a file. A stopped broker still has a dump.rdb worth
    shipping, and binding to its unit would skip exactly that backup."""
    text = REPO_SERVICE.read_text()
    assert "Requires=redis-server.service" not in text
    assert "BindsTo=redis-server.service" not in text


def test_timer_is_hourly_and_catches_up_after_downtime() -> None:
    text = REPO_TIMER.read_text()
    assert _directive(text, "OnCalendar") == ["hourly"]
    assert _directive(text, "Persistent") == ["true"]


def test_the_probe_is_told_where_the_backup_state_lives() -> None:
    """The freshness finding is the probe's, so its unit names the path.
    Read-only by construction: it is under the backup unit's StateDirectory,
    which the probe's ``User=`` cannot write."""
    assert "--backup-state-file /var/lib/broker-backup/state.json" in REPO_PROBE_SERVICE.read_text()


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
    """Keyed on the unit itself, removing the backup would skip its own parity check."""
    monkeypatch.setattr(sys.modules[__name__], constant, tmp_path / "absent")
    pretend_node(monkeypatch, tmp_path, present=present)
    expected = pytest.fail.Exception if present else pytest.skip.Exception
    assert isinstance(outcome_of(test), expected)
