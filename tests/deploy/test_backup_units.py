"""The backup timer units must exist, carry their guards, and stay in sync
(CannObserv/broker#4).

Same shape as ``test_bus_health_units.py``: content asserted on the repo copy
everywhere, byte-parity against ``/etc/systemd/system/`` on the node, where a
missing unit fails rather than skips (CannObserv/broker#81). Also the
interpreter docs/RECOVERY.md runs ``restore`` with as root, the unit's twin by
hand (#108). And, on the node, the static user the unit runs as (#109).
"""

from __future__ import annotations

import grp
import pwd
import re
import shlex
import sys
from pathlib import Path

import pytest

from src.broker.backup import DEFAULT_ACL_PASSWORDS, DEFAULT_RDB
from tests.deploy.conftest import (
    LIVE,
    outcome_of,
    pretend_node,
    read_installed,
    require_broker_node,
)

_DEPLOY = Path(__file__).resolve().parents[2] / "deploy"
REPO_SERVICE = _DEPLOY / "broker-backup.service"
REPO_TIMER = _DEPLOY / "broker-backup.timer"
REPO_PROBE_SERVICE = _DEPLOY / "broker-bus-health.service"
README = _DEPLOY / "README.md"
RECOVERY = _DEPLOY.parent / "docs" / "RECOVERY.md"
INSTALLED_SERVICE = Path("/etc/systemd/system/broker-backup.service")
INSTALLED_TIMER = Path("/etc/systemd/system/broker-backup.timer")
#: Where ``systemctl edit`` writes, which the parity check above never reads.
DROPIN_DIR = Path("/etc/systemd/system/broker-backup.service.d")
#: The static system user (#109): no home, no login, in no supplementary group.
USER = "broker-backup"
#: How deploy/README.md's Install makes it, and what a missing user's failure says.
USERADD = f"sudo useradd --system --no-create-home --shell /usr/sbin/nologin {USER}"
WRITER_KEY = "/etc/broker/co-broker-backup.json"
#: The one line root runs: coreutils, never Python. ``-p`` keeps the mtime
#: ``snapshot_time`` falls back to.
COPY = (
    f"/usr/bin/install -p -m 0400 -o {USER} -g {USER} {DEFAULT_RDB} ${{RUNTIME_DIRECTORY}}/dump.rdb"
)


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
    assert _directive(text, "EnvironmentFile") == []
    assert _directive(text, "LoadCredentialEncrypted") == []
    assert "brokeradmin" not in text and "acladmin" not in text
    assert "BROKER_REDIS_URL" not in text


def test_service_loads_both_files_as_credentials_with_empty_fallbacks() -> None:
    """The user reads neither source: the manager hands each over at start
    (#109). ``SetCredential=`` makes a missing source no start failure, so the
    run starts, fails in Python and is recorded - the probe's next tick says so,
    not the three-hour staleness rule. An empty passwords file still ships the
    snapshot first (#72); an empty key fails before the snapshot is taken."""
    text = REPO_SERVICE.read_text()
    assert sorted(_directive(text, "LoadCredential")) == [
        f"acl-passwords:{DEFAULT_ACL_PASSWORDS}",
        f"gcs-writer:{WRITER_KEY}",
    ]
    assert sorted(_directive(text, "SetCredential")) == ["acl-passwords:\\n", "gcs-writer:\\n"]


def test_service_ships_the_acl_digests_from_the_passwords_credential() -> None:
    """Named on the command line rather than left to the default, so the path
    the unit reads is the one this test and docs/RECOVERY.md say it reads.
    ``%d``, not ``${CREDENTIALS_DIRECTORY}``: the manager resolves it, and
    ``systemctl show`` prints the value."""
    (exec_start,) = _directive(REPO_SERVICE.read_text(), "ExecStart")
    assert "--acl-passwords %d/acl-passwords" in exec_start


def test_service_sees_nothing_under_etc_broker() -> None:
    """/etc/broker holds other identities' keys - the probe's env, the wheelhouse
    reader, the co-status check-in - so it is hidden whole: an empty read-only
    tmpfs, nothing bound back in. The writer key arrives as a credential, and
    whether the manager reads it before the namespace hides its source is
    #110's open question: #109's rehearsal on the node decides whether this
    stays. Defence in depth for a user no file there is readable by
    (test_drift_units' world-readable check)."""
    text = REPO_SERVICE.read_text()
    assert _directive(text, "TemporaryFileSystem") == ["/etc/broker:ro"]
    assert _directive(text, "BindReadOnlyPaths") == []
    assert _directive(text, "BindPaths") == []


def test_service_cannot_read_the_node_credentials() -> None:
    """The node users' encrypted credentials. ``-``: absent is no failure,
    only unreadable is the point."""
    assert _directive(REPO_SERVICE.read_text(), "InaccessiblePaths") == [
        "-/etc/credstore.encrypted"
    ]


def test_service_names_its_bucket_and_key_and_reads_no_backup_env() -> None:
    """Not /etc/broker/backup.env (#109): whichever backup unit is installed
    reads that file, so pointing it at this unit's credential would break the
    #108 unit a switch back restores, and every ``restore`` in docs/RECOVERY.md,
    which reads it as root. And an ``EnvironmentFile=`` overrides
    ``Environment=``. The bucket is not a secret; it is the one Install writes."""
    text = REPO_SERVICE.read_text()
    (bucket,) = re.findall(r"printf 'BROKER_BACKUP_BUCKET=([a-z0-9-]+)\\n", README.read_text())
    environment = _directive(text, "Environment")
    assert f"BROKER_BACKUP_BUCKET={bucket}" in environment
    assert "GOOGLE_APPLICATION_CREDENTIALS=%d/gcs-writer" in environment
    assert not [e for e in environment if e.startswith("BROKER_BACKUP_PREFIX=")], (
        "the hostname is the default: a pinned prefix would put a second node in prod's history"
    )


def test_service_runs_as_its_own_user_with_no_capability() -> None:
    """Not root, not the ``redis`` group - it holds WRITE on dump.rdb and on
    /etc/redis - and not ``DynamicUser=``, which moves ``StateDirectory=`` under
    0700 /var/lib/private where the probe cannot read state.json (#22 B1)."""
    text = REPO_SERVICE.read_text()
    assert _directive(text, "User") == [USER]
    assert _directive(text, "Group") == [USER]
    assert _directive(text, "SupplementaryGroups") == []
    assert _directive(text, "DynamicUser") == []
    assert _directive(text, "CapabilityBoundingSet") == [""]
    assert _directive(text, "AmbientCapabilities") == []
    assert _directive(text, "ProtectSystem") == ["strict"]
    assert _directive(text, "PrivateTmp") == ["yes"]
    assert _directive(text, "NoNewPrivileges") == ["yes"]
    assert _directive(text, "ReadWritePaths") == []  # StateDirectory is the only writable path
    assert _directive(text, "StateDirectory") == ["broker-backup"]
    # The default 0755: the probe and deploy.sh's verify_backup read state.json
    # as exedev, so a tighter mode is a finding every tick and a switch back.
    assert _directive(text, "StateDirectoryMode") == []


def test_the_copy_is_the_only_privileged_line() -> None:
    """``+`` runs one command as root outside the sandbox: ``install``, from the
    one file the user cannot read into a directory only it can. ``-``: a failed
    copy is no start failure, so Python finds no file and records it. ``!`` and
    ``!!`` lift privilege too (ambient capabilities), so they count."""
    text = REPO_SERVICE.read_text()
    execs = [
        (key, value)
        for ln in text.splitlines()
        if ln.startswith("Exec")
        for key, value in [ln.split("=", 1)]
    ]
    privileged = [(key, value) for key, value in execs if set("+!") & set(value.split("/", 1)[0])]
    assert privileged == [("ExecStartPre", f"+-{COPY}")], privileged
    assert _directive(text, "RuntimeDirectory") == ["broker-backup"]
    assert _directive(text, "RuntimeDirectoryMode") == ["0700"]
    assert _directive(text, "RuntimeDirectoryPreserve") == []  # gone at stop, RAM freed


def test_no_line_of_the_unit_names_the_dev_tree() -> None:
    """Root runs only root-owned code (#22 goal 5, #108): a release, never the
    checkout any ``exedev`` session can edit.

    Comments included: one that still says the dev tree is a runbook pointing
    the next reader back at it."""
    assert "/home/exedev" not in REPO_SERVICE.read_text()


def test_service_cannot_see_home() -> None:
    """Nothing under /home is read once the unit runs a release, so none of it
    is visible: the checkout's ``.env`` and its GitHub tokens included. ``yes``
    hides /root too, which the backup never needed: it authenticates through
    ``GOOGLE_APPLICATION_CREDENTIALS``."""
    assert _directive(REPO_SERVICE.read_text(), "ProtectHome") == ["yes"]


def test_the_backup_runs_from_live() -> None:
    """``live`` is resolved at each start, and ``scripts/deploy.sh``'s
    ``expected_of`` reads this line for the build the forced run must report."""
    assert _directive(REPO_SERVICE.read_text(), "WorkingDirectory") == [str(LIVE)]


def test_the_backup_runs_the_release_venv_never_uv() -> None:
    """``uv run`` wants a writable cache and may sync the environment, both of
    which the sandbox refuses. The release's venv is frozen at build, and the
    arguments are the ones the backup took before the switch. ``-m`` from
    ``WorkingDirectory=`` is what reads the release's ``REVISION`` (#105 CR 2)."""
    (exec_start,) = _directive(REPO_SERVICE.read_text(), "ExecStart")
    argv = exec_start.split()
    assert argv[:3] == [f"{LIVE}/.venv/bin/python", "-m", "src.broker.backup"], argv
    assert argv[3:] == [
        "--rdb",
        "${RUNTIME_DIRECTORY}/dump.rdb",
        "--acl-passwords",
        "%d/acl-passwords",
        "--state-file",
        "${STATE_DIRECTORY}/state.json",
    ], argv
    assert shlex.split(COPY)[-1] == argv[4], "the copy is what the run reads"


def test_the_backup_writes_no_bytecode() -> None:
    """The release is ``555`` and compiled at build (#22 Q6)."""
    assert "PYTHONDONTWRITEBYTECODE=1" in _directive(REPO_SERVICE.read_text(), "Environment")


def _restore_runs(text: str) -> list[str]:
    return [ln.strip() for ln in text.splitlines() if "-m src.broker.restore" in ln]


def test_recovery_runs_restore_as_root_isolated() -> None:
    """Every ``restore`` in the runbook runs as root, so as the release's import
    probe does (#106 CR 9): ``-I`` drops the caller's PYTHONPATH and user site,
    ``-B`` keeps root's bytecode out of whatever tree it imports from."""
    runs = _restore_runs(RECOVERY.read_text())
    assert len(runs) == 5, runs
    assert all(" -I -B -m src.broker.restore" in run for run in runs), runs


def test_recovery_verifies_a_backup_with_the_release() -> None:
    """On a running node, by hand, root runs the release, as the unit does
    (#108). Only a rebuild's steps 2-3 run the checkout: no release exists until
    step 6's deploy, and that deploy's forced backup must follow the data."""
    heading = "## Verifying a backup by hand"
    text = RECOVERY.read_text()
    assert heading in text, f"{RECOVERY.name} has no {heading!r} section"
    rebuild_text, by_hand = text.split(heading, 1)
    runs = _restore_runs(by_hand)
    assert len(runs) == 2, runs
    assert all(run.startswith(f"{LIVE}/.venv/bin/python ") for run in runs), runs
    rebuild = _restore_runs(rebuild_text)
    assert all(run.startswith("/home/exedev/broker/.venv/bin/python ") for run in rebuild), rebuild


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


def test_the_user_is_static_and_in_no_supplementary_group() -> None:
    """``id broker-backup`` lists no ``redis``, so the user cannot read
    /var/lib/redis (0750 redis:redis) except through the unit's copy. On the
    node only; ``useradd`` comes before the deploy that installs the unit."""
    require_broker_node()
    try:
        user = pwd.getpwnam(USER)
    except KeyError:
        pytest.fail(f"no {USER} user: {USERADD}")
    assert grp.getgrgid(user.pw_gid).gr_name == USER
    assert [g.gr_name for g in grp.getgrall() if USER in g.gr_mem] == []
    assert user.pw_shell == "/usr/sbin/nologin"
    assert not Path(user.pw_dir).exists()


def test_install_makes_the_user_before_the_deploy() -> None:
    """A ``User=`` that does not exist fails the deploy's forced run, which then
    switches back: the user comes first, spelled as the failure above says."""
    lines = [ln.strip() for ln in README.read_text().splitlines()]
    assert USERADD in lines
    deploys = [i for i, ln in enumerate(lines) if "scripts/deploy.sh" in ln]
    assert deploys and lines.index(USERADD) < deploys[0]


def _dropin_lines_beyond_the_prefix(directory: Path) -> list[str]:
    """Each drop-in line that is not ``[Service]``, a comment, a blank or the
    prefix. An absent directory has none."""
    if not directory.is_dir():
        return []
    return [
        f"{conf.name}: {ln.strip()}"
        for conf in sorted(directory.glob("*.conf"))
        for ln in conf.read_text().splitlines()
        if ln.strip()
        and not ln.lstrip().startswith(("#", ";"))
        and ln.strip() != "[Service]"
        and not re.fullmatch(r"Environment=BROKER_BACKUP_PREFIX=[A-Za-z0-9._-]+", ln.strip())
    ]


def test_a_drop_in_sets_the_prefix_and_nothing_else() -> None:
    """docs/RECOVERY.md puts ``BROKER_BACKUP_PREFIX`` in a drop-in on a node not
    named co-broker (#109). The parity check reads only the unit file, so a
    drop-in could put back ``User=root``, a capability or an ``EnvironmentFile=``
    that overrides the bucket or key, and every pin above would still pass."""
    require_broker_node()
    assert _dropin_lines_beyond_the_prefix(DROPIN_DIR) == []


def test_the_drop_in_check_names_anything_but_the_prefix(tmp_path) -> None:
    assert _dropin_lines_beyond_the_prefix(tmp_path / "absent") == []
    (tmp_path / "override.conf").write_text(
        "### Editing /etc/systemd/system/broker-backup.service.d/override.conf\n"
        "[Service]\nEnvironment=BROKER_BACKUP_PREFIX=co-broker\n\n"
    )
    assert _dropin_lines_beyond_the_prefix(tmp_path) == []
    (tmp_path / "zz.conf").write_text("[Service]\nUser=root\n")
    assert _dropin_lines_beyond_the_prefix(tmp_path) == ["zz.conf: User=root"]


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
