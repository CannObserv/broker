"""SocratiCode's config, and the guards around it (CannObserv/broker#17).

The guards landed first, ahead of the config: none of them needs
``.socraticode.json`` to exist, and one exists to stop the rest of #17 landing in
the wrong order. The config's own tests - ``projectId``, ``linkedProjects``, the
client ``env`` block, the manifest, the two hooks - arrived with it.

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
from urllib.parse import urlparse

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG = REPO_ROOT / ".socraticode.json"
INDEX_IGNORE = REPO_ROOT / ".socraticodeignore"
SETTINGS = REPO_ROOT / ".claude" / "settings.json"
SETTINGS_LOCAL = REPO_ROOT / ".claude" / "settings.local.json"

#: Where a file on this host puts a variable in the MCP server's environment: the
#: two env files a shell here sources, and the three settings scopes Claude Code
#: merges into a session. That server is the socraticode@socraticode plugin, which
#: inherits the session's environment. Not read: an ``env`` block on a standalone
#: server entry in ``.mcp.json`` or ``~/.claude.json`` - init-socraticode's
#: duplicate-config trap, which is removed on sight rather than guarded.
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
#: into ``skills-vendor/``, and ``.skills/`` holds managing-skills' installed
#: ``doctor.sh`` copy and curating-context's telemetry.
VENDORED = {"skills-vendor/", "skills/", ".claude/skills/", ".skills/"}

#: The two of those holding one link per skill, and where every link must point.
SKILL_LINK_DIRS = {"skills": "../skills-vendor/", ".claude/skills": "../../skills/"}

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

    And the name must be broker's. A verbatim copy of notifier's config - #17's
    reference implementation - or the hash id itself satisfies "a config exists"
    and writes into another set all the same. A literal, not ``REPO_ROOT.name``:
    inside a worktree that is the worktree's directory.
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
    project_id = json.loads(CONFIG.read_text()).get("projectId")
    assert project_id == "broker", f"{CONFIG.name} names {project_id!r}, not broker"


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

    Measured 2026-09-15, before adoption: 360 files under ``skills-vendor/``
    against 57 of broker's own. Every sibling's ``includeLinked`` search reads
    broker's collection too, so unexcluded it would serve gregoryfoster/skills as
    broker across the cohort.
    """
    assert VENDORED <= _index_ignore_entries()


def test_the_index_excludes_nested_checkouts() -> None:
    """An index of the main checkout must not also take each worktree's copy."""
    assert NESTED_CHECKOUTS <= _index_ignore_entries()


@pytest.mark.parametrize(
    ("directory", "prefix"), list(SKILL_LINK_DIRS.items()), ids=list(SKILL_LINK_DIRS)
)
def test_every_excluded_skill_is_vendored(directory: str, prefix: str) -> None:
    """Both skill directories are excluded whole only because nothing in them is broker's.

    ``.claude/skills/`` is where Claude Code looks for a project's own skills, so a
    first-party one is likelier there than in ``skills/``. Either way it would drop
    out of the index without a word: replace that directory's entry in
    ``.socraticodeignore`` with narrower ones before adding it.
    """
    entries = sorted((REPO_ROOT / directory).iterdir())
    assert entries, f"{directory}/ is empty"
    for entry in entries:
        name = f"{directory}/{entry.name}"
        assert entry.is_symlink(), f"{name} is not a symlink - a first-party skill?"
        target = os.readlink(entry)
        assert target.startswith(prefix), f"{name} -> {target}"


# ── The config itself ────────────────────────────────────────────────────────

MANIFEST = REPO_ROOT / ".socraticodecontextartifacts.json"

#: Upstream's own validator: config.js assertValidProjectId rejects anything else.
PROJECT_ID_CHARS = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")

#: The store on co-index, as every other client of it spells the same values. A
#: collection holds vectors from one model at one dimension, so a client that
#: differs does not fail - it poisons the collection every sibling searches.
CLIENT_ENV = {
    "QDRANT_MODE": "external",
    "QDRANT_URL": "https://index.taild0fb76.ts.net:6333",
    "OLLAMA_MODE": "external",
    "OLLAMA_URL": "http://index:11434",
    "EMBEDDING_MODEL": "nomic-embed-text",
    "EMBEDDING_DIMENSIONS": "768",
}

#: Each SessionStart hook, and the dedupe marker its entry carries. Distinct per
#: hook so one hook's strip cannot evict the other's entry from the array.
HOOKS = {
    "socraticode-reminder.sh": "socraticode-prefetch",
    "socraticode-health.sh": "socraticode-health",
}


@pytest.fixture(scope="module")
def config() -> dict:
    return json.loads(CONFIG.read_text())


@pytest.fixture(scope="module")
def client_env() -> dict[str, str]:
    return _variables(SETTINGS, SETTINGS.read_text())


def test_config_exists_and_parses() -> None:
    """A malformed file is ignored by upstream, not reported.

    ``loadSocratiCodeConfig`` catches every parse error and returns null, so a
    typo here degrades to the path-hash id with no message - the same silence
    class as the linked-project skip.
    """
    assert CONFIG.exists(), f"{CONFIG.name} is missing"
    json.loads(CONFIG.read_text())


def test_project_id_is_this_repo(config: dict) -> None:
    """``broker``, so the collections read as ``codebase_broker`` in a shared store."""
    assert config["projectId"] == "broker"


def test_project_id_is_qdrant_safe(config: dict) -> None:
    """Upstream throws on anything outside [a-zA-Z0-9_-] rather than sanitizing."""
    assert set(config["projectId"]) <= PROJECT_ID_CHARS


def test_linked_projects_are_relative_siblings(config: dict) -> None:
    """An absolute entry names one host's layout; relative works on every clone."""
    linked = config["linkedProjects"]
    assert linked, "linkedProjects is empty"
    for entry in linked:
        assert not Path(entry).is_absolute(), f"{entry} is absolute"
        assert entry.startswith("../"), f"{entry} does not name a sibling"


def test_linked_projects_name_the_cohort(config: dict) -> None:
    assert set(config["linkedProjects"]) == {
        "../archiver",
        "../notifier",
        "../replicator",
        "../watcher",
    }


def test_linked_projects_exclude_this_repo(config: dict) -> None:
    """Upstream drops a self-link, but a self-link in the file is still a mistake."""
    assert "../broker" not in config["linkedProjects"]


def test_the_client_env_is_the_cohort_store(client_env: dict[str, str]) -> None:
    """Every value the other clients of co-index use, including the embedder's.

    ``EMBEDDING_MODEL`` and ``EMBEDDING_DIMENSIONS`` are not local preferences:
    they describe the vectors already in the shared collections.
    """
    assert {k: client_env.get(k) for k in CLIENT_ENV} == CLIENT_ENV


def test_qdrant_is_addressed_by_a_full_https_url(client_env: dict[str, str]) -> None:
    """#17 traps 3 and 4, which present as network faults rather than config errors.

    The full MagicDNS name because that is what the certificate names; https
    because upstream refuses to send the key over plain http to anything but
    loopback; the port spelled out because a URL built from ``QDRANT_HOST``
    instead defaults to 16333, not Qdrant's 6333.
    """
    url = urlparse(client_env["QDRANT_URL"])
    assert url.scheme == "https", "a key is refused over plain http"
    assert url.port == 6333
    assert url.hostname is not None and url.hostname.endswith(".ts.net")
    assert url.hostname.count(".") >= 2, f"{url.hostname} is not the full MagicDNS name"
    assert url.path in ("", "/"), "upstream's client drops a path"


def test_the_manifest_is_an_object_whose_paths_resolve() -> None:
    """A rejected manifest is silent: ``codebase_status`` omits the artifact line
    and the repo indexes 'successfully' with no context search at all.

    A bare top-level array is refused outright; a path that does not resolve is
    skipped one at a time, so ``artifacts N/N`` never reaches parity.
    """
    manifest = json.loads(MANIFEST.read_text())
    assert isinstance(manifest, dict), "a top-level array is rejected outright"
    artifacts = manifest["artifacts"]
    assert artifacts, "no artifacts configured"
    names = [a["name"] for a in artifacts]
    assert len(names) == len({n.lower() for n in names}), f"duplicate names in {names}"
    for artifact in artifacts:
        assert set(artifact) == {"name", "path", "description"}, artifact
        path = artifact["path"]
        assert not any(c in path for c in "*?["), f"{path} is a glob - the server stat()s it"
        assert (REPO_ROOT / path).exists(), f"{path} does not resolve"


@pytest.mark.parametrize("hook", list(HOOKS), ids=list(HOOKS))
def test_each_hook_is_a_symlink_into_the_vendored_tree(hook: str) -> None:
    """A copy freezes at install day while reading as a healthy install.

    The health hook is the worst candidate for one: it is silent when clean, so a
    stale copy that has stopped detecting something looks exactly like a working
    one. Shape, not resolution - both dangle wherever the submodule is not
    checked out, which includes CI and every fresh worktree.
    """
    path = REPO_ROOT / ".claude" / "hooks" / hook
    assert path.is_symlink(), f"{hook} is not a symlink"
    target = os.readlink(path)
    assert not Path(target).is_absolute(), f"{hook} -> {target} is absolute"
    assert "skills-vendor/" in target, f"{hook} -> {target} leaves the vendored tree"


@pytest.mark.parametrize(("hook", "marker"), list(HOOKS.items()), ids=list(HOOKS))
def test_each_hook_is_registered_exactly_once(hook: str, marker: str) -> None:
    """Registered, or the hook is a file that never runs; once, or it runs twice."""
    entries = [
        h
        for group in json.loads(SETTINGS.read_text())["hooks"]["SessionStart"]
        for h in group["hooks"]
        if h["command"].endswith(f"# {marker}")
    ]
    assert len(entries) == 1, f"{marker}: {len(entries)} SessionStart entries"
    assert hook in entries[0]["command"], f"{marker} does not run {hook}"
