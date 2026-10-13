"""The lock audit reports and never fails (#86).

``scripts/audit_lock.sh`` runs ``uv audit --frozen`` against ``uv.lock`` and
``.github/workflows/audit.yml`` runs it on every push, every PR and once a week.
It is the update mechanism patching-hosts' record cites (run.md section 7).
Dependabot and Renovate are out: neither updater can sync the private
wheelhouse ``co-core`` resolves from (watcher's ``dependabot.yml`` names the
same blocker).

**Report-only is the contract.** A gate that failed on a finding would turn
``main`` red on someone else's disclosure date, and every later check-in would
read that red (#79). So every exit is 0. A finding becomes a warning
annotation plus the job summary, and an audit that could not run gets its own
title, so it never reads as clean.

The script runs here against a fake ``uv`` on ``PATH``, so nothing reaches OSV.
"""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "audit_lock.sh"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "audit.yml"


def _run(tmp_path: Path, uv_exit: int, uv_output: str = "", *, actions: bool = True):
    """Run the script with a fake ``uv`` that records its argv, prints
    ``uv_output`` and exits ``uv_exit``."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    argv_file = tmp_path / "argv"
    (tmp_path / "output").write_text(uv_output)
    fake = bin_dir / "uv"
    fake.write_text(
        "#!/usr/bin/env bash\n"
        f'printf "%s\\n" "$@" > "{argv_file}"\n'
        f'cat "{tmp_path / "output"}"\n'
        f"exit {uv_exit}\n"
    )
    fake.chmod(0o755)
    summary = tmp_path / "summary.md"
    env = {"PATH": f"{bin_dir}:{os.environ['PATH']}", "HOME": str(tmp_path)}
    if actions:
        env |= {"GITHUB_ACTIONS": "true", "GITHUB_STEP_SUMMARY": str(summary)}
    result = subprocess.run(
        ["bash", str(SCRIPT)], env=env, capture_output=True, text=True, timeout=30
    )
    argv = argv_file.read_text().split() if argv_file.exists() else []
    return result, argv, summary.read_text() if summary.exists() else ""


@pytest.mark.parametrize("uv_exit", [0, 1, 2, 127])
def test_every_exit_is_zero(tmp_path, uv_exit):
    """Whatever uv says, the step stays green: report-only (#79)."""
    result, _, _ = _run(tmp_path, uv_exit)
    assert result.returncode == 0, result.stderr


def test_audits_the_lock_without_relocking(tmp_path):
    """``--frozen`` reads ``uv.lock`` as committed. A relock would need the
    private wheelhouse, which this job has no credential to fetch."""
    _, argv, _ = _run(tmp_path, 0)
    assert argv[:2] == ["audit", "--frozen"]


def test_clean_lock_raises_no_warning(tmp_path):
    result, _, summary = _run(tmp_path, 0, "Found no known vulnerabilities\n")
    assert "::warning" not in result.stdout
    assert "Found no known vulnerabilities" in summary


def test_finding_is_a_warning_and_lands_in_the_summary(tmp_path):
    out = "- PYSEC-2026-4175: urllib3: HTTPS proxy TLS configuration\n"
    result, _, summary = _run(tmp_path, 1, out)
    assert "::warning title=uv.lock advisories::" in result.stdout
    assert "PYSEC-2026-4175" in summary


@pytest.mark.parametrize("uv_exit", [2, 127])
def test_an_audit_that_did_not_run_never_reads_as_clean(tmp_path, uv_exit):
    """uv exits 1 for findings and 2 for an error: an outage at OSV, or a
    changed flag on an experimental command. A missing uv exits 127."""
    result, _, summary = _run(tmp_path, uv_exit)
    assert "::warning title=uv audit did not run::" in result.stdout
    assert f"exit {uv_exit}" in summary


def test_uv_output_cannot_issue_workflow_commands(tmp_path):
    """Advisory text comes from OSV. Echoed bare, a line starting ``::`` would be
    read by the runner as a workflow command, so it is echoed between
    ``stop-commands`` and the resume token."""
    result, _, _ = _run(tmp_path, 1, "::error::forged\n")
    lines = result.stdout.splitlines()
    stop = next(i for i, line in enumerate(lines) if line.startswith("::stop-commands::"))
    token = lines[stop].removeprefix("::stop-commands::")
    assert len(token) >= 32
    resume = lines.index(f"::{token}::")
    assert stop < lines.index("::error::forged") < resume


def test_runs_off_actions_without_a_summary(tmp_path):
    """Run by hand, it prints the audit and touches no summary file."""
    result, _, summary = _run(tmp_path, 1, "a finding\n", actions=False)
    assert result.returncode == 0
    assert "a finding" in result.stdout
    assert "stop-commands" not in result.stdout
    assert summary == ""


def test_workflow_runs_the_script_on_a_schedule_with_no_secret():
    """Disclosures arrive without commits, so the job runs weekly as well as on
    push and PR. It holds no secret and no OIDC token: ``--frozen`` needs no
    wheelhouse, so it needs no WIF."""
    text = WORKFLOW.read_text()
    on_block = re.search(r"^on:\n((?:[ #].*\n|\n)+)", text, re.MULTILINE)
    assert on_block, "no top-level on: block"
    triggers = set(re.findall(r"^  (\w+):", on_block.group(1), re.MULTILINE))
    assert {"push", "pull_request", "schedule"} <= triggers
    assert re.search(r"^\s+run: bash scripts/audit_lock\.sh$", text, re.MULTILINE)
    assert "secrets." not in text
    assert "id-token" not in text
    assert "continue-on-error" not in text, "report-only is the script's exit, not a mask"
