"""ruff's reach into the docs (CannObserv/broker#38).

From 0.16, ruff's default ``include`` takes ``*.md``, and ``ruff format`` rewrites
the ``python`` / ``py`` fences inside a doc. #38 accepted that rather than
excluding the docs, as gregoryfoster/skills' own ``pyproject.toml`` does. The
likeliest way to lose the decision is to copy that ``extend-exclude = ["*.md"]``
across from the vendored tree beside this one, so the first test fails if a
Python fence in a doc stops being formatted. The second pins the escape the
comment in ``pyproject.toml`` names for a verbatim quotation of another repo's
source: a fence under any other tag is left as written.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

#: A doc path under this repo's config. It need not exist: the doc is read from stdin.
PROBE_DOC = REPO_ROOT / "docs" / "ruff-scope-probe.md"

#: A fence body the formatter would rewrite: it normalises the spacing.
UNFORMATTED = 'x = {  "a":1 }\n'


def _format_check(markdown: str) -> subprocess.CompletedProcess[str]:
    """``ruff format --check`` a doc, read from stdin, under this repo's config.

    ``--force-exclude`` is load-bearing. Without it ruff formats a
    ``--stdin-filename`` whatever ``include`` and ``exclude`` say, so a doc the
    config excludes fails the check exactly as an included one does, and the
    first test would pass with the docs excluded.
    """
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "ruff",
            "format",
            "--check",
            "--force-exclude",
            "--stdin-filename",
            str(PROBE_DOC),
            "-",
        ],
        input=markdown,
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        check=False,
    )


def test_a_python_fence_in_a_doc_is_formatted() -> None:
    """Exit 1 is ``--check``'s "would reformat". An excluded doc exits 0, and
    ruff before 0.16 exits 2 (Markdown formatting was preview-only)."""
    result = _format_check(f"# Probe\n\n```python\n{UNFORMATTED}```\n")
    assert result.returncode == 1, result.stdout + result.stderr


def test_a_fence_under_another_tag_is_left_as_written() -> None:
    result = _format_check(f"# Probe\n\n```text\n{UNFORMATTED}```\n")
    assert result.returncode == 0, result.stdout + result.stderr
