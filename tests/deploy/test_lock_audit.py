"""The lock audit reports and never fails (#86).

``scripts/audit_lock.sh`` runs ``uv audit --frozen`` against ``uv.lock`` and
``.github/workflows/audit.yml`` runs it on every push, every PR and once a week.
It is the update mechanism patching-hosts' record cites (run.md section 7).
Dependabot's ``uv`` ecosystem and Renovate are out: neither updater can sync
the private wheelhouse ``co-core`` resolves from. Dependabot moves the action
pins alone (``.github/dependabot.yml``, #90).

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


# uv < 0.11.19 has no ``audit-command`` name (astral-sh/uv#19645): its parser
# rejects the value before the command runs.
UNKNOWN_FLAG = (
    "error: invalid value 'audit-command' for '--preview-features <PREVIEW_FEATURES>': "
    "Unknown feature flag\n"
)


def _run(
    tmp_path: Path,
    uv_exit: int,
    uv_output: str = "",
    *,
    actions: bool = True,
    knows_flag: bool = True,
):
    """Run the script with a fake ``uv`` that records each call's argv, prints
    ``uv_output`` and exits ``uv_exit``. With ``knows_flag=False`` it answers
    ``--preview-features`` as uv < 0.11.19 does, exit 2, and runs nothing."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    argv_file = tmp_path / "argv"
    (tmp_path / "output").write_text(uv_output)
    (tmp_path / "unknown").write_text(UNKNOWN_FLAG)
    fake = bin_dir / "uv"
    reject = (
        ""
        if knows_flag
        else (
            'if [[ " $* " == *" --preview-features "* ]]; then\n'
            f'\tcat "{tmp_path / "unknown"}" >&2\n'
            "\texit 2\n"
            "fi\n"
        )
    )
    fake.write_text(
        "#!/usr/bin/env bash\n"
        f'echo "$*" >> "{argv_file}"\n'
        f"{reject}"
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
    calls = (
        [line.split() for line in argv_file.read_text().splitlines()] if argv_file.exists() else []
    )
    return result, calls, summary.read_text() if summary.exists() else ""


@pytest.mark.parametrize("uv_exit", [0, 1, 2, 127])
def test_every_exit_is_zero(tmp_path, uv_exit):
    """Whatever uv says, the step stays green: report-only (#79)."""
    result, _, _ = _run(tmp_path, uv_exit)
    assert result.returncode == 0, result.stderr


def test_audits_the_lock_without_relocking(tmp_path):
    """``--frozen`` reads ``uv.lock`` as committed. A relock would need the
    private wheelhouse, which this job has no credential to fetch. ``uv audit``
    is a preview command, so the flag goes first and a uv that knows it runs
    once."""
    _, calls, _ = _run(tmp_path, 0)
    assert calls == [["audit", "--frozen", "--preview-features", "audit-command"]]


@pytest.mark.parametrize(
    ("uv_exit", "uv_output", "warning"),
    [
        (0, "Found no known vulnerabilities\n", None),
        (1, "- PYSEC-2026-4175: urllib3\n", "::warning title=uv.lock advisories::"),
    ],
)
def test_a_uv_older_than_the_flag_still_audits(tmp_path, uv_exit, uv_output, warning):
    """uv < 0.11.19 rejects ``audit-command`` with exit 2 and "Unknown feature
    flag" (#103), so the script retries without the flag: the command is
    there, only the name is not. From 0.11.20 an unknown name only warns, so
    no later uv reaches this path. The rejected attempt never reaches the
    report."""
    result, calls, summary = _run(tmp_path, uv_exit, uv_output, knows_flag=False)
    assert calls == [
        ["audit", "--frozen", "--preview-features", "audit-command"],
        ["audit", "--frozen"],
    ]
    assert "did not run" not in result.stdout
    if warning:
        assert warning in result.stdout
    else:
        assert "::warning" not in result.stdout
    assert f"exit {uv_exit}" in summary
    assert uv_output.strip() in summary
    assert "Unknown feature flag" not in summary


def test_any_other_exit_2_is_not_retried(tmp_path):
    """Only the unknown-flag rejection retries. An outage at OSV also exits 2,
    and a second call would only repeat it."""
    result, calls, _ = _run(tmp_path, 2, "error: OSV unreachable\n")
    assert len(calls) == 1
    assert "::warning title=uv audit did not run::" in result.stdout


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
