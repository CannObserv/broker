"""The deploy_command knob names ``scripts/deploy.sh``, waiting on it (#123).

``.skills/deploy_command`` is what the vendored ``shipping-work`` reads to ship
through the project's own deploy rather than "migrate, then restart", which
here would restart nothing: every unit runs ``/srv/broker/live``
(gregoryfoster/skills#345). Its reader, ``run-deploy-command.sh``, appends the
pushed SHA as the last argument, which ``scripts/deploy.sh [<ref>]`` takes as
its ref.

CI cannot run the reader's ``--check``: it needs HEAD to equal ``@{u}``, which a
detached checkout lacks, and it reads the primary checkout's knob, not a
worktree's. So the line is parsed here by the reader's own grammar, and
``--check`` runs only where HEAD is what was pushed:

- **One command line, no shell.** Words are split on whitespace and run as
  argv, so a pipe, a redirect or ``&&`` would reach ``deploy.sh`` as an argument.
- **``setsid -w``.** The deploy gets its own session and the wait keeps its
  exit code. A plain ``setsid`` returns once the deploy has started, so 0
  would mean "started", and the skill would close an issue on it.
- **The script by path.** A first word without ``/`` is looked up on PATH; the
  script after ``setsid -w`` resolves from the primary checkout's root.
- **No flag.** ``--skip-ci`` and ``--build-only`` would put every ship past CI
  or nowhere.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from tests.deploy.conftest import on_broker_node

REPO_ROOT = Path(__file__).resolve().parents[2]
KNOB = REPO_ROOT / ".skills" / "deploy_command"
RUNNER = REPO_ROOT / "skills" / "shipping-work" / "scripts" / "run-deploy-command.sh"

SHELL_SYNTAX = set("|&;<>()$`*?[]{}~'\"\\")


def _command_lines() -> list[list[str]]:
    """The reader's grammar: blank lines and whole-line ``#`` comments drop."""
    lines = []
    for raw in KNOB.read_text().splitlines():
        words = raw.split()
        if words and not words[0].startswith("#"):
            lines.append(words)
    return lines


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(REPO_ROOT), *args], capture_output=True, text=True
    ).stdout.strip()


def test_the_knob_is_one_command_line() -> None:
    assert len(_command_lines()) == 1


def test_the_knob_waits_on_scripts_deploy_sh() -> None:
    assert _command_lines() == [["setsid", "-w", "scripts/deploy.sh"]]


def test_the_knob_holds_no_shell_syntax() -> None:
    for word in _command_lines()[0]:
        assert not SHELL_SYNTAX & set(word), f"shell syntax in {word!r}: argv, not a shell"


def test_the_named_script_is_executable() -> None:
    script = REPO_ROOT / _command_lines()[0][-1]
    assert script.is_file()
    assert os.access(script, os.X_OK)


def test_the_reader_checks_the_knob_where_head_was_pushed() -> None:
    if not RUNNER.is_file():
        if on_broker_node():
            pytest.fail(f"{RUNNER} missing on the node: run bash .skills/doctor.sh")
        pytest.skip("shipping-work submodule not initialised")
    primary = _git("worktree", "list", "--porcelain").splitlines()[0].removeprefix("worktree ")
    if Path(primary).resolve() != REPO_ROOT:
        pytest.skip("a worktree: the reader reads the primary checkout's knob")
    head = _git("rev-parse", "HEAD")
    if not head or head != _git("rev-parse", "--verify", "-q", "@{u}"):
        pytest.skip("HEAD is not what was pushed: the reader refuses (exit 2)")
    result = subprocess.run(
        ["bash", str(RUNNER), "--check"], cwd=REPO_ROOT, capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == f"setsid -w scripts/deploy.sh {head}"
