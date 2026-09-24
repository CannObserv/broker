"""The vendored skills are linked in both discovery paths and listed, and no
Mayfly channel URL is committed beside them (#63).

managing-skills wires one skill as three artifacts: ``skills/<name>`` for
agentskills.io, ``.claude/skills/<name>`` for Claude Code, and a row in
``docs/SKILLS.md``'s *Inventory*. The daily refresh hook bumps the submodule
pointer and creates none of them, so a new skill is a manual step - and a
half-done one is silent: a link in one path only is discoverable by one agent,
and an unlisted one is a skill nobody reading the doc knows is there.

Names and link shapes only. Whether a link resolves is ``.skills/doctor.sh``'s
job, and it needs the submodule initialised, which this module does not.

``using-mayfly-chat`` brings a rule of its own: a channel URL is read, write and
delete access to the channel, with no owner and no revocation, so it never
reaches a durable store. Upstream guards its own tree with
``tests/structural/test_no_channel_urls.py``; the last test here is that guard
for this one, with the same pattern.
"""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SKILLS = REPO_ROOT / "skills"
CLAUDE_SKILLS = REPO_ROOT / ".claude" / "skills"
SKILLS_DOC = REPO_ROOT / "docs" / "SKILLS.md"

#: The skill's leak check (``references/security.md``): the 22-character id and
#: the ``#`` with a 43-character key. That matches a live URL on any host, not the
#: keyless view URL, and - built from character classes - not its own text.
CHANNEL_URL = re.compile(r"/c/[A-Za-z0-9_-]{22}#[A-Za-z0-9_-]{43}")

_ROW = re.compile(r"^\|\s*`([a-z0-9-]+)`\s*\|")


def _links(directory: Path) -> set[str]:
    return {entry.name for entry in directory.iterdir() if entry.is_symlink()}


def _inventory() -> set[str]:
    """Skill names in the first column of the table under ``## Inventory``."""
    _, _, section = SKILLS_DOC.read_text().partition("\n## Inventory\n")
    assert section, f"{SKILLS_DOC.name} has no '## Inventory' section"
    section = section.split("\n## ", 1)[0]
    return {m.group(1) for line in section.splitlines() if (m := _ROW.match(line))}


def test_both_discovery_paths_link_the_same_skills() -> None:
    assert _links(SKILLS), "skills/ holds no links"
    assert _links(CLAUDE_SKILLS) == _links(SKILLS)


def test_each_claude_link_goes_through_skills() -> None:
    """``../../skills/<name>``, so an override in ``skills/`` shadows both paths."""
    for name in _links(CLAUDE_SKILLS):
        assert os.readlink(CLAUDE_SKILLS / name) == f"../../skills/{name}"


def test_each_skill_link_names_its_vendored_skill() -> None:
    for name in _links(SKILLS):
        target = os.readlink(SKILLS / name)
        assert re.fullmatch(rf"\.\./skills-vendor/[^/]+/skills/{re.escape(name)}", target), (
            f"skills/{name} -> {target}"
        )


def test_the_inventory_lists_exactly_the_linked_skills() -> None:
    assert _inventory() == _links(SKILLS)


def test_using_mayfly_chat_is_vendored() -> None:
    """#63: the cohort's agent-to-agent channel skill, gregoryfoster/skills#302."""
    assert "using-mayfly-chat" in _links(SKILLS)


def _committable() -> list[Path]:
    """Tracked files plus untracked, unignored ones: what a ``git add -A`` takes.

    The submodule is a gitlink here, not files; its own tree is upstream's to guard.
    """
    out = subprocess.run(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        cwd=REPO_ROOT,
        capture_output=True,
        check=True,
    ).stdout
    paths = (REPO_ROOT / name for name in out.decode().split("\0") if name)
    return [p for p in paths if p.is_file() and not p.is_symlink()]


def test_the_channel_url_pattern_is_live() -> None:
    """A positive control, built at runtime so this file never holds a live URL:
    a pattern edited into matching nothing would otherwise pass the scan below."""
    live = "https://example.test/c/" + "A" * 22 + "#" + "b" * 43
    assert CHANNEL_URL.search(live)
    assert not CHANNEL_URL.search("https://example.test/c/" + "A" * 22)
    assert not CHANNEL_URL.search("https://example.test/c/<ID>#<key>")


def test_no_channel_url_is_committable() -> None:
    leaks = [
        str(path.relative_to(REPO_ROOT))
        for path in _committable()
        if CHANNEL_URL.search(path.read_bytes().decode(errors="replace"))
    ]
    assert not leaks, f"Mayfly channel URL in {leaks} - delete the channel, then the text"
