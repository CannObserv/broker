"""SocratiCode guards that hold before broker joins the shared index
(CannObserv/broker#17).

Landed ahead of the config itself: none of these needs ``.socraticode.json`` to
exist, and one of them exists to stop the rest of #17 landing in the wrong order.
The config's own tests - ``projectId``, ``linkedProjects``, the client ``env``
block - arrive with the config.

The namespace guards mirror notifier's ``tests/deploy/test_socraticode_config.py``.
The key and project-id guards are broker's own, because what they pin is sharper
here: this repo is public, and this checkout's path is the one the stale broker
index was built from.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG = REPO_ROOT / ".socraticode.json"
INDEX_IGNORE = REPO_ROOT / ".socraticodeignore"
SETTINGS = REPO_ROOT / ".claude" / "settings.json"
SETTINGS_LOCAL = REPO_ROOT / ".claude" / "settings.local.json"

#: Everywhere a variable can reach the MCP server's environment on this host: the
#: two env files a shell here sources, and the three settings scopes Claude Code
#: merges into a session.
ENV_SOURCES = {
    "etc-broker-env": Path("/etc/broker/.env"),
    "repo-env": REPO_ROOT / ".env",
    "project-settings": SETTINGS,
    "local-settings": SETTINGS_LOCAL,
    "user-settings": Path.home() / ".claude" / "settings.json",
}

#: Set, and the cohort's collections split with every health check green: the
#: prefix is prepended to the instance-global ``socraticode_metadata`` as well.
COLLECTION_PREFIX = "QDRANT_COLLECTION_PREFIX"

#: "true", and the project id gains a branch suffix - a fresh collection set per
#: branch, indexed from empty.
BRANCH_AWARE = "SOCRATICODE_BRANCH_AWARE"

#: Outranks ``.socraticode.json``. notifier#63 sets it for one removal and says it
#: must never be persisted; persisted here, every operation from this host
#: addresses whatever it names.
PROJECT_ID_OVERRIDE = "SOCRATICODE_PROJECT_ID"

#: #17 trap 3: the address is ``QDRANT_URL``. External mode refuses a host with
#: no URL, and the fallback built from one defaults to port 16333, not 6333.
QDRANT_HOST = "QDRANT_HOST"

#: gregoryfoster/skills content: ``skills/`` and ``.claude/skills/`` are symlinks
#: into ``skills-vendor/``.
VENDORED = {"skills-vendor/", "skills/", ".claude/skills/"}

#: using-git-worktrees' fallback root (no ``.skills/worktree_root`` here) and the
#: harness's. Both sit inside this tree.
NESTED_CHECKOUTS = {".worktrees/", ".claude/worktrees/"}

_ASSIGNMENT = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=(.*)$")


def _read_if_present(path: Path) -> str | None:
    """Only ``FileNotFoundError`` means absent - a ``PermissionError`` propagates
    rather than silently passing."""
    try:
        return path.read_text()
    except FileNotFoundError:
        return None


def _variables(path: Path, text: str) -> dict[str, str]:
    """What the file would put in the server's environment.

    A settings file by its ``env`` block, the only part Claude Code exports; an
    env file by its assignments, so a comment naming a variable is not a finding.
    """
    if path.suffix == ".json":
        return {k: str(v) for k, v in json.loads(text).get("env", {}).items()}
    found = {}
    for line in text.splitlines():
        if m := _ASSIGNMENT.match(line):
            found[m[1]] = m[2].strip().strip("'\"")
    return found


def _index_ignore_entries() -> set[str]:
    lines = (ln.strip() for ln in INDEX_IGNORE.read_text().splitlines())
    return {ln for ln in lines if ln and not ln.startswith("#")}


@pytest.mark.parametrize("path", list(ENV_SOURCES.values()), ids=list(ENV_SOURCES))
@pytest.mark.parametrize(
    "variable", [COLLECTION_PREFIX, BRANCH_AWARE, PROJECT_ID_OVERRIDE, QDRANT_HOST]
)
def test_guarded_variables_are_set_nowhere(variable: str, path: Path) -> None:
    """VM-local where the file is VM-local; skips loudly rather than passing vacuously."""
    text = _read_if_present(path)
    if text is None:
        pytest.skip(f"{path} not present on this machine")
    assert variable not in _variables(path, text), f"{path} sets {variable}"


def test_the_shared_store_is_never_addressed_without_a_project_id() -> None:
    """The store's address and this repo's name ship together, or not at all.

    Without ``.socraticode.json`` the project id is ``sha256(abs_path)[:12]``, and
    exe.dev VMs check a repo out at the same path. This checkout's is
    ``d4eab3ecb321`` - the id notifier's own ``/home/exedev/broker`` clone indexed
    broker under while ``co-index`` was stood up (notifier#63). A session here
    that can reach the store with no config file writes into that set: two hosts,
    one collection, the concurrent write D11 forbids, and nothing reports it. The
    ids #17 says differ differ only once the config exists.
    """
    addressing = []
    for label, path in ENV_SOURCES.items():
        text = _read_if_present(path)
        if text is None:
            continue
        env = _variables(path, text)
        if env.get("QDRANT_MODE") == "external" or "QDRANT_URL" in env:
            addressing.append(label)
    if not addressing:
        return
    assert CONFIG.exists(), f"{addressing} address the shared store, but {CONFIG.name} is missing"
    assert json.loads(CONFIG.read_text()).get("projectId"), f"{CONFIG.name} names no projectId"


def test_local_settings_are_ignored_by_the_tracked_gitignore() -> None:
    """The file that will hold the cohort's one Qdrant key, in a public repo.

    Qdrant on ``co-index`` has a single global api key: every VM holds the same
    one, so a leak here is a rotation everywhere, with no overlap window.
    notifier's ``install_qdrant_key.sh`` writes this file and does not check that
    it is ignored.

    Asserted against ``.gitignore`` by the source ``check-ignore -v`` names: a
    global excludesfile or ``.git/info/exclude`` passes on the one machine that
    has it and protects no other clone. A tracked file is never reported as
    ignored, so a committed key fails here too.
    """
    if not shutil.which("git"):
        pytest.skip("git not installed")
    target = str(SETTINGS_LOCAL.relative_to(REPO_ROOT))
    result = subprocess.run(
        ["git", "check-ignore", "-v", target],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"{target} is not ignored, or is tracked: {result.stderr}"
    source = result.stdout.split(":", 1)[0]
    assert source == ".gitignore", f"{target} is ignored by {source}, not the tracked .gitignore"


def test_the_committed_settings_hold_no_qdrant_key() -> None:
    """``settings.json`` travels with every clone; the key lives in the local file."""
    assert "QDRANT_API_KEY" not in _variables(SETTINGS, SETTINGS.read_text())


def test_the_index_excludes_the_vendored_skills() -> None:
    """Vendored skill prose outnumbers broker's own files and would outrank them.

    At adoption (2026-09-15), 360 files under ``skills-vendor/`` against 57 of
    broker's own. Every sibling's ``includeLinked`` search reads broker's
    collection too, so unexcluded it would serve gregoryfoster/skills as broker
    across the cohort.
    """
    assert VENDORED <= _index_ignore_entries()


def test_the_index_excludes_nested_checkouts() -> None:
    """An index of the main checkout must not also take each worktree's copy."""
    assert NESTED_CHECKOUTS <= _index_ignore_entries()


def test_every_excluded_skill_is_vendored() -> None:
    """``skills/`` is excluded whole only because nothing in it is broker's.

    A first-party skill added there would drop out of the index without a word.
    Narrow the exclusion to ``skills-vendor/`` and ``.claude/skills/`` first, as
    notifier's ``.socraticodeignore`` does for its own overrides.
    """
    entries = sorted((REPO_ROOT / "skills").iterdir())
    assert entries, "skills/ is empty"
    for entry in entries:
        assert entry.is_symlink(), f"skills/{entry.name} is not a symlink - a first-party skill?"
        target = os.readlink(entry)
        assert target.startswith("../skills-vendor/"), f"skills/{entry.name} -> {target}"
