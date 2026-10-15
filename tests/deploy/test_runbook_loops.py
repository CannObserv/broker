"""Every stream loop in the two runbooks reaches every grouped stream (#98).

``docs/RECOVERY.md``'s ``XLEN`` and ``XINFO GROUPS`` loops spelled their
streams, and predated three that now carry groups: ``content.persist``
(#64), ``content.process`` and ``content.derived`` (#62). A restore verified
as written never checked three of the nine groups. ``docs/RESTART-WINDOW.md``
had been updated for each (#87), and nothing tied the two together.

Each doc now names the grouped streams once, in ``GROUPED``, and every stream
loop in it iterates over that variable. The set it must hold is read off
``docs/STREAMS.md``'s *Consumer group* column, which ``test_redis_acl.py``
already pins to the probe's groups, so a new grouped stream turns this red
until both runbooks name it.

The list is stated, not derived with ``--scan``: after a restore, a stream that
did not come back would drop out of a scanned list and pass silently. For the
same reason a loop in a different code block from the definition must use
``${GROUPED:?...}``, so a shell that never ran the definition fails loudly
rather than looping over nothing.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from tests.canonical import CANONICAL_STREAMS
from tests.deploy.test_redis_acl import documented_group_consumers

DOCS = Path(__file__).resolve().parents[2] / "docs"

#: Each runbook, and how many stream loops it holds: a positive control, so a
#: reformat the parser no longer sees cannot pass by finding no loop at all.
RUNBOOKS = {"RECOVERY.md": 3, "RESTART-WINDOW.md": 2}

VAR = "GROUPED"
_DEFINITION = re.compile(rf'^{VAR}="([^"]*)"', re.MULTILINE)
_LOOP = re.compile(r"^\s*for \w+ in ([^;\n]*); do\n(.*?)^\s*done\b", re.MULTILINE | re.DOTALL)
_REFERENCE = re.compile(rf"\$(?:{VAR}\b|\{{{VAR}(:\?[^}}]*)?\}})")
_FENCE = re.compile(r"^```", re.MULTILINE)


def _text(name: str) -> str:
    """The doc with shell line continuations joined, so a loop's list is one line."""
    return (DOCS / name).read_text().replace("\\\n", " ")


def _block(text: str, offset: int) -> int:
    """Which fenced code block ``offset`` falls in, counted by fences before it."""
    return len(_FENCE.findall(text, 0, offset)) // 2


def _stream_loops(text: str) -> list[re.Match[str]]:
    return [
        m
        for m in _LOOP.finditer(text)
        if _REFERENCE.search(m.group(1)) or set(m.group(1).split()) & set(CANONICAL_STREAMS)
    ]


def test_the_inventory_names_the_grouped_streams() -> None:
    """Nine groups on eight streams as of #98; fewer means the table moved."""
    assert len(documented_group_consumers()) >= 8


@pytest.mark.parametrize("name", sorted(RUNBOOKS))
def test_the_runbook_names_every_grouped_stream_once(name: str) -> None:
    definitions = _DEFINITION.findall(_text(name))
    assert len(definitions) == 1, f"{name} defines {VAR} {len(definitions)} times, not once"
    assert set(definitions[0].split()) == set(documented_group_consumers()), (
        f"{name}'s {VAR} is not docs/STREAMS.md's grouped streams"
    )


@pytest.mark.parametrize("name", sorted(RUNBOOKS))
def test_every_stream_loop_iterates_over_the_grouped_streams(name: str) -> None:
    text = _text(name)
    definition = _DEFINITION.search(text)
    assert definition, f"{name} never defines {VAR}"
    defined_in = _block(text, definition.start())
    loops = _stream_loops(text)
    assert len(loops) == RUNBOOKS[name], f"{name} has {len(loops)} stream loops"
    grouped = set(documented_group_consumers())
    for loop in loops:
        words, ref = loop.group(1), _REFERENCE.search(loop.group(1))
        assert ref, f"{name}: `for ... in {words}` skips {VAR}"
        spelled = sorted(grouped & set(words.split()))
        assert not spelled, f"{name}: {spelled} spelled beside {VAR} - a second list to drift"
        if _block(text, loop.start()) != defined_in:
            assert ref.group(1), f"{name}: a loop outside {VAR}'s block needs ${{{VAR}:?...}}"
