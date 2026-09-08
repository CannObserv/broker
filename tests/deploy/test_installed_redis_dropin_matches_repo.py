"""The installed Redis drop-in must match the repo copy, and must set a cap.

Carried over from CannObserv/archiver (archiver#128) under archiver#193 D6,
**repointed at this host**. That repointing is the whole reason the test had to
move: it asserts a path under ``/etc/systemd/system/`` on the machine running
the tests, so on archiver's host it silently began measuring a machine that no
longer runs a broker. The install filename moved with it - ``broker.conf``, not
``archiver.conf`` - so a node still carrying the old name is a node that was
never cut over.

The drift here is sharper than a stale flag. ``maxmemory-policy noeviction``
without an explicit ``maxmemory`` is *inert*: with the default ``maxmemory 0``
there is no ceiling to refuse writes at, so the bounded "error and let the
producer retry" degradation the drop-in documents never engages and an untrimmed
stream grows until the kernel OOM-kills ``redis-server``. A drop-in that says
``noeviction`` while the broker runs uncapped reads as protection and provides
none.

Assertions, deliberately split:

- The parser itself is table-driven - it must read the ``ExecStart`` line and
  nothing else, and must judge the *value* rather than its spelling.
- The repo file declares a non-zero cap - pure, runs in CI, locks the invariant.
- The installed file matches the repo - skips when absent, so CI and dev clones
  pass; it asserts only on a host that actually runs the broker.

**Not yet reconciled with the node, and deliberately left that way (broker#1
Phase 5).** This file arrived from archiver, where it was the *only* mechanism
tuning the broker. On the new node it is not the mechanism at all: broker#1
Phase 2 appended ``appendonly yes`` / ``appendfsync everysec`` /
``maxmemory 512mb`` / ``maxmemory-policy noeviction`` directly to
``/etc/redis/redis.conf`` (verified live), and gave the drop-in slot to tailnet
ordering instead. So the settings this file describes ARE in force - just not
from here.

That leaves the repo tracking a file nothing installs, which is exactly the
drift a parity test exists to catch, so it is recorded rather than papered
over. broker#1 Phase 1 step 1 posed the choice ("keep layering on Debian's
package unit or own the whole thing on a dedicated host") and Phase 2 answered
it in practice without the repo following; Phase 5 is where the two get
reconciled. Until then this test points at an install path nothing writes,
so it **skips** - honest, rather than red against a drop-in that is not this
one.

**Scope limit.** All of this compares *files*. It cannot see a broker whose
running config was changed by ``CONFIG SET`` - which is how the cap is applied
without a restart. Each participant's ``check_redis_floor.sh`` reads the live
value at its own ``ExecStartPre`` (warn-only), and the bus-health probe here
reports ``maxmemory 0`` as a finding every tick.
"""

import re
from pathlib import Path

import pytest

REPO_DROPIN = Path(__file__).resolve().parents[2] / "deploy" / "redis-server.dropin.conf"
# NOT ``broker.conf``. That filename is already taken on the broker node by a
# DIFFERENT drop-in - broker#1 Phase 2's tailnet ordering (``After=tailscaled``
# plus the ``/proc/net/fib_trie`` wait that R1 exists for). Installing this file
# there would delete that ordering and re-open the boot race observo#473 cost
# two weeks. See the module docstring's "Not yet reconciled" note.
INSTALLED_DROPIN = Path("/etc/systemd/system/redis-server.service.d/broker-tuning.conf")

# Match only on an ExecStart= line that actually launches redis-server. The file
# is mostly prose, and an earlier version of this guard searched the whole text -
# which a *comment* mentioning `--maxmemory 512mb` would have satisfied while
# ExecStart carried no cap at all .
_EXECSTART_LINE = re.compile(r"^ExecStart=\S*redis-server\b.*$", re.MULTILINE)
_MAXMEMORY_ARG = re.compile(r"--maxmemory\s+(\S+)")

# Redis size suffixes. `k`/`m`/`g` are decimal, `kb`/`mb`/`gb` binary; a bare
# number is bytes. Only the magnitude matters here - the guard asks "is it zero?",
# not "is it exactly N" - but parsing to a number is what catches `0mb`, which
# disables the cap just as surely as `0` and which a literal `!= "0"` test admits
# .
_SIZE_UNITS = {
    "": 1,
    "b": 1,
    "k": 1_000,
    "kb": 1_024,
    "m": 1_000**2,
    "mb": 1_024**2,
    "g": 1_000**3,
    "gb": 1_024**3,
}
_SIZE = re.compile(r"^(\d+)([a-z]*)$")


def _execstart_maxmemory_bytes(text: str) -> int | None:
    """Return the ``--maxmemory`` value from the ExecStart line, in bytes.

    ``None`` when no redis-server ExecStart line carries the flag at all, or when
    its value is unparseable. Comments are never consulted.
    """
    for line in _EXECSTART_LINE.findall(text):
        match = _MAXMEMORY_ARG.search(line)
        if match is None:
            continue
        size = _SIZE.match(match.group(1).lower())
        if size is None:
            return None
        digits, unit = size.groups()
        if unit not in _SIZE_UNITS:
            return None
        return int(digits) * _SIZE_UNITS[unit]
    return None


def _read_if_installed(path: Path) -> str | None:
    """Return the drop-in's text, or None when it is genuinely not installed.

    Only ``FileNotFoundError`` means "not installed" - a ``PermissionError``
    propagates rather than becoming a silent pass, matching
    ``test_installed_unit_matches_repo``.
    """
    try:
        return path.read_text()
    except FileNotFoundError:
        return None


@pytest.mark.parametrize(
    ("execstart", "expected"),
    [
        # The shapes that must pass.
        ("ExecStart=/usr/bin/redis-server /etc/redis/redis.conf --maxmemory 512mb", 512 * 1024**2),
        ("ExecStart=/usr/bin/redis-server /etc/redis/redis.conf --maxmemory 536870912", 536870912),
        ("ExecStart=/usr/bin/redis-server --maxmemory-policy noeviction --maxmemory 1gb", 1024**3),
        # The shapes that must not.
        ("ExecStart=/usr/bin/redis-server --maxmemory 0", 0),
        ("ExecStart=/usr/bin/redis-server --maxmemory 0mb", 0),  # a spelled zero is still a zero
        ("ExecStart=/usr/bin/redis-server --maxmemory 0kb", 0),
        ("ExecStart=/usr/bin/redis-server --maxmemory-policy noeviction", None),
        # A comment must never satisfy the guard .
        (
            "# example: --maxmemory 512mb\n"
            "ExecStart=/usr/bin/redis-server --maxmemory-policy noeviction",
            None,
        ),
        # Nor may the policy flag be mistaken for the cap.
        ("ExecStart=/usr/bin/redis-server --maxmemory-policy noeviction\n", None),
    ],
)
def test_execstart_maxmemory_bytes_parses_only_the_execstart_line(
    execstart: str, expected: int | None
) -> None:
    """The guard's parser: ExecStart only, and value-aware rather than literal.

    Table-driven because both holes this closes were spelling problems, not logic
    problems - `0mb` reads as a cap and disables one; a `--maxmemory` in prose
    reads as a cap and is not one.
    """
    assert _execstart_maxmemory_bytes(execstart) == expected


def test_repo_dropin_sets_an_explicit_nonzero_maxmemory() -> None:
    """`noeviction` is only meaningful with a ceiling to refuse writes at.

    Paired with ``OutOfMemoryError`` being transient in
    ``CannObserv/archiver:src/core/changes/publisher.py``: the cap turns memory
    pressure into a retryable ``OOM command not allowed`` instead of a broker
    kill, and the classification keeps that from dead-lettering valid events.
    Neither half stands alone - and since archiver#193 D6 they are in two
    repositories with no test spanning them, so each names the other.
    """
    text = REPO_DROPIN.read_text()
    assert "--maxmemory-policy noeviction" in text
    cap = _execstart_maxmemory_bytes(text)
    assert cap is not None and cap > 0, (
        "deploy/redis-server.dropin.conf must set an explicit non-zero "
        "--maxmemory on its redis-server ExecStart line; noeviction without a "
        "cap never refuses a write, so the broker is OOM-killed instead of "
        f"erroring (CannObserv/archiver#128). Parsed: {cap!r}"
    )


def test_installed_dropin_matches_repo() -> None:
    installed = _read_if_installed(INSTALLED_DROPIN)
    if installed is None:
        pytest.skip(f"{INSTALLED_DROPIN} not present - not a host running the broker")
    assert installed == REPO_DROPIN.read_text(), (
        f"{INSTALLED_DROPIN} has drifted from {REPO_DROPIN}.\n"
        "Reinstall with:\n"
        f"  sudo cp {REPO_DROPIN} {INSTALLED_DROPIN}\n"
        "  sudo systemctl daemon-reload\n"
        "\n"
        "Most drift here is comment-only - this file is mostly prose - and needs\n"
        "no restart. Restart redis-server ONLY if the ExecStart line itself\n"
        "changed, and prefer applying a changed value live instead:\n"
        "  redis-cli CONFIG SET maxmemory <value from ExecStart>  # suffix ok\n"
        "The unit supplies it from the next restart onward either way.\n"
        "Verify: redis-cli CONFIG GET maxmemory  # must not be 0"
    )
