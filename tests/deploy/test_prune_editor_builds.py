"""The by-hand editor-build prune keeps what runs and what is newer (CannObserv/broker#126).

``scripts/prune_editor_builds.sh`` is the interim step until patching-hosts
prunes editor builds itself (option 1 of #126). Its rules are the issue's:

- never remove a build a running process uses, matched by its commit hash in
  ``ps``, and checked again just before each removal;
- keep anything newer than the running build: a server build by its version,
  a CLI binary by its mtime;
- record what it removed and how much that freed, ``df /`` before and after;
- a dry run unless ``--apply``, so it is never run blind mid-session.

The tree is a fake ``~/.vscode-server`` and ``ps`` a stub, so nothing here
reads or touches the editor this session runs in.
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "prune_editor_builds.sh"

RUNNING = "a" * 40  # 1.140.0, what the editor runs
NEWEST = "b" * 40  # 1.143.1, staged by the agent host
OLDER = "c" * 40  # 1.139.0
BETWEEN = "d" * 40  # 1.141.0, newer than the running build
NO_VERSION = "e" * 40  # no package.json
OLD_CLI = "f" * 40
NEW_CLI = "9" * 40

VERSIONS = {RUNNING: "1.140.0", NEWEST: "1.143.1", OLDER: "1.139.0", BETWEEN: "1.141.0"}


def _ps_line(h: str) -> str:
    return (
        f"/home/x/.vscode-server/cli/servers/Stable-{h}/server/node "
        f"/home/x/.vscode-server/cli/servers/Stable-{h}/server/out/server-main.js\n"
        f"/home/x/.vscode-server/code-{h} command-shell --cli-data-dir /home/x/.vscode-server/cli\n"
    )


@pytest.fixture
def home(tmp_path: Path) -> Path:
    home = tmp_path / "home"
    vsc = home / ".vscode-server"
    servers = vsc / "cli" / "servers"
    for h in (RUNNING, NEWEST, OLDER, BETWEEN, NO_VERSION):
        server = servers / f"Stable-{h}" / "server"
        server.mkdir(parents=True)
        (server / "node").write_bytes(b"n" * 8192)
        if h in VERSIONS:
            (server / "package.json").write_text(
                json.dumps({"name": "code-server", "version": VERSIONS[h]}, indent=2)
            )
    (servers / f"Stable-{OLDER}.staging-1234").mkdir()
    (servers / ".locks").mkdir()
    (servers / ".locks" / f"Stable-{OLDER}").touch()
    (servers / "lru.json").write_text(json.dumps([f"Stable-{NEWEST}"]))
    for h, mtime in ((OLD_CLI, 1_000), (RUNNING, 2_000), (NEW_CLI, 3_000)):
        cli = vsc / f"code-{h}"
        cli.write_bytes(b"c" * 4096)
        os.utime(cli, (mtime, mtime))
        (vsc / f".cli.{h}.log").write_text("log\n")
    return home


def _run(
    tmp_path: Path, home: Path, *args: str, ps: list[str] | None = None
) -> subprocess.CompletedProcess[str]:
    """Run under a ``ps`` stub that prints ``ps[i]`` on its i-th call (the last
    one thereafter), and an ``id`` that says the caller is not root."""
    stub = tmp_path / "stub-bin"
    stub.mkdir(exist_ok=True)
    calls = [_ps_line(RUNNING)] if ps is None else ps
    for i, out in enumerate(calls):
        (stub / f"ps.{i}").write_text(out)
    (stub / "ps").write_text(
        "#!/bin/sh\n"
        f'd="{stub}"; n=$(cat "$d/count" 2>/dev/null || echo 0)\n'
        f'echo $((n + 1)) > "$d/count"; i=$n; [ "$i" -ge {len(calls)} ] && i={len(calls) - 1}\n'
        'cat "$d/ps.$i"\n'
    )
    (stub / "ps").chmod(0o755)
    env = {**os.environ, "HOME": str(home), "PATH": f"{stub}:{os.environ['PATH']}"}
    return subprocess.run(
        ["bash", str(SCRIPT), *args], env=env, capture_output=True, text=True, check=False
    )


def _servers(home: Path) -> set[str]:
    return {p.name for p in (home / ".vscode-server" / "cli" / "servers").iterdir()}


def _clis(home: Path) -> set[str]:
    return {
        p.name
        for p in (home / ".vscode-server").iterdir()
        if "code-" in p.name or ".cli." in p.name
    }


def _record(home: Path) -> str:
    (record,) = (home / ".local" / "state" / "editor-prune").glob("*.log")
    return record.read_text()


def test_a_dry_run_removes_nothing_and_says_what_it_would(tmp_path, home) -> None:
    before = (_servers(home), _clis(home))
    result = _run(tmp_path, home)
    assert result.returncode == 0, result.stderr
    assert (_servers(home), _clis(home)) == before
    assert "would remove" in result.stdout
    assert f"Stable-{OLDER}" in result.stdout
    assert "--apply" in result.stdout


def test_apply_keeps_what_runs_and_what_is_newer(tmp_path, home) -> None:
    result = _run(tmp_path, home, "--apply")
    assert result.returncode == 0, result.stderr
    assert _servers(home) == {
        f"Stable-{RUNNING}",
        f"Stable-{NEWEST}",
        f"Stable-{BETWEEN}",
        f"Stable-{NO_VERSION}",
        # Not a build: the editor's own staging, lock and LRU state stay as found.
        f"Stable-{OLDER}.staging-1234",
        ".locks",
        "lru.json",
    }
    assert _clis(home) == {
        f"code-{RUNNING}",
        f".cli.{RUNNING}.log",
        f"code-{NEW_CLI}",
        f".cli.{NEW_CLI}.log",
    }


def test_apply_records_what_it_removed_and_what_that_freed(tmp_path, home) -> None:
    result = _run(tmp_path, home, "--apply")
    record = _record(home)
    assert record in result.stdout
    assert f"removed {home}/.vscode-server/cli/servers/Stable-{OLDER} " in record
    assert f"removed {home}/.vscode-server/code-{OLD_CLI} " in record
    assert f"kept {home}/.vscode-server/cli/servers/Stable-{RUNNING}: running" in record
    assert "freed " in record
    assert record.count("df /:") == 2


def test_a_build_started_since_the_plan_is_left(tmp_path, home) -> None:
    """The plan's ``ps`` shows only the running build; every later call also shows
    the older one, as if a client had connected with it in between."""
    later = _ps_line(RUNNING) + _ps_line(OLDER)
    result = _run(tmp_path, home, "--apply", ps=[_ps_line(RUNNING), later])
    assert result.returncode == 0, result.stderr
    assert f"Stable-{OLDER}" in _servers(home)
    assert f"left {home}/.vscode-server/cli/servers/Stable-{OLDER}: running now" in _record(home)


def test_with_nothing_running_it_keeps_the_newest(tmp_path, home) -> None:
    result = _run(tmp_path, home, "--apply", ps=["bash\n"])
    assert result.returncode == 0, result.stderr
    assert {s for s in _servers(home) if len(s) == len("Stable-") + 40} == {
        f"Stable-{NEWEST}",
        f"Stable-{NO_VERSION}",
    }
    assert {c for c in _clis(home) if c.startswith("code-")} == {f"code-{NEW_CLI}"}


def test_no_editor_tree_is_nothing_to_do(tmp_path) -> None:
    home = tmp_path / "empty-home"
    home.mkdir()
    result = _run(tmp_path, home, "--apply")
    assert result.returncode == 0, result.stderr
    assert "nothing to prune" in result.stdout


def test_an_unknown_argument_is_refused(tmp_path, home) -> None:
    before = _servers(home)
    result = _run(tmp_path, home, "--aply")
    assert result.returncode == 2
    assert _servers(home) == before
