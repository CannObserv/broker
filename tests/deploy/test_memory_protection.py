"""The node's defences against dev tooling's memory pressure (broker#21).

On 2026-09-16 the then-2 GB node ran out of memory under dev tooling. There was
no OOM kill: the kernel failed *atomic* allocations in ``kswapd0``,
``tailscaled`` and ``ksoftirqd``, so the bus's network path degraded while every
process stayed alive, and the probe went silent for most of an hour. The VM is
8 GB now; these are the defences that do not depend on size.

Tracked in ``deploy/``, installed as:

- ``sysctl.d/60-broker-memory.conf`` -> ``/etc/sysctl.d/``
- ``system.slice.d/broker-memory.conf`` -> ``/etc/systemd/system/system.slice.d/``
- ``redis-server.service.d/memory.conf`` -> ``/etc/systemd/system/redis-server.service.d/``
- ``tailscaled.service.d/memory.conf`` -> ``/etc/systemd/system/tailscaled.service.d/``
- ``earlyoom.default`` -> ``/etc/default/earlyoom``

Split like the other deploy tests: **pure** assertions on the tracked copies
run everywhere; **installed parity** and **live** assertions skip where the node
is not this one.
"""

from __future__ import annotations

import re
import shlex
import shutil
import subprocess
from pathlib import Path

import pytest

from tests.deploy.conftest import DEPLOY
from tests.deploy.test_installed_redis_config_matches_repo import (
    REPO_REDIS_CONF,
    parse_directives,
    parse_size,
)

SYSCTL = DEPLOY / "sysctl.d" / "60-broker-memory.conf"
SYSTEM_SLICE = DEPLOY / "system.slice.d" / "broker-memory.conf"
REDIS_MEMORY = DEPLOY / "redis-server.service.d" / "memory.conf"
TAILSCALED_MEMORY = DEPLOY / "tailscaled.service.d" / "memory.conf"
EARLYOOM = DEPLOY / "earlyoom.default"

INSTALLED = {
    SYSCTL: Path("/etc/sysctl.d/60-broker-memory.conf"),
    SYSTEM_SLICE: Path("/etc/systemd/system/system.slice.d/broker-memory.conf"),
    REDIS_MEMORY: Path("/etc/systemd/system/redis-server.service.d/memory.conf"),
    TAILSCALED_MEMORY: Path("/etc/systemd/system/tailscaled.service.d/memory.conf"),
    EARLYOOM: Path("/etc/default/earlyoom"),
}

#: The cgroup each MemoryLow drop-in protects, and where the kernel exposes it.
CGROUPS = {
    SYSTEM_SLICE: Path("/sys/fs/cgroup/system.slice/memory.low"),
    REDIS_MEMORY: Path("/sys/fs/cgroup/system.slice/redis-server.service/memory.low"),
    TAILSCALED_MEMORY: Path("/sys/fs/cgroup/system.slice/tailscaled.service/memory.low"),
}

#: The two drop-ins that also lower their unit's OOM score, and the unit each is for.
OOM_UNITS = {REDIS_MEMORY: "redis-server", TAILSCALED_MEMORY: "tailscaled"}

#: The effective score earlyoom 1.7 gives a ``--prefer`` match whose own
#: ``oom_score`` is 0 - which, under exe.dev, is every one on this node.
PREFER_FLOOR = 300

#: ``comm`` of what carries the bus, and of how anyone reaches the node at all -
#: ``sshd`` and ``exe-init`` are exe.dev's own. Ranked last by earlyoom, not exempt.
PROTECTED = [
    "redis-server",
    "tailscaled",
    "systemd",
    "systemd-journal",
    "systemd-logind",
    "dbus-daemon",
    "exe-init",
    "sshd",
]

#: ``comm`` of what took the node down: a plain ``node`` is ``node``, npx titles
#: itself ``npm exec <pkg>`` (15 chars), and VSCode Server's node processes
#: rename their main thread ``MainThread``.
DEV_TOOLING = ["node", "npm exec socrat", "npx", "MainThread"]

_UNITS = {"K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}


def _read_if_installed(path: Path) -> str | None:
    """Only ``FileNotFoundError`` means "not installed" - a ``PermissionError``
    propagates rather than silently passing."""
    try:
        return path.read_text()
    except FileNotFoundError:
        return None


def sysctl_settings() -> dict[str, str]:
    """``key = value`` lines of the tracked sysctl drop-in, comments skipped."""
    return {
        k.strip(): v.strip()
        for k, _, v in (ln.partition("=") for ln in SYSCTL.read_text().splitlines())
        if k.strip() and not k.strip().startswith(("#", ";"))
    }


def memory_low(path: Path) -> int:
    """Bytes from the drop-in's single ``MemoryLow=`` (systemd's base-1024 suffixes)."""
    values = [
        ln.split("=", 1)[1].strip()
        for ln in path.read_text().splitlines()
        if ln.startswith("MemoryLow=")
    ]
    assert len(values) == 1, f"{path.name}: expected exactly one MemoryLow=, got {values}"
    match = re.fullmatch(r"(\d+)([KMGT]?)", values[0])
    assert match, f"{path.name}: MemoryLow={values[0]!r} is not a plain size"
    return int(match.group(1)) * _UNITS.get(match.group(2), 1)


def oom_score_adjust(path: Path) -> int:
    values = [
        ln.split("=", 1)[1].strip()
        for ln in path.read_text().splitlines()
        if ln.startswith("OOMScoreAdjust=")
    ]
    assert len(values) == 1, f"{path.name}: expected exactly one OOMScoreAdjust=, got {values}"
    return int(values[0])


def earlyoom_args() -> list[str]:
    """``EARLYOOM_ARGS`` as systemd will pass it: split on whitespace, quotes literal."""
    lines = [ln for ln in EARLYOOM.read_text().splitlines() if ln.startswith("EARLYOOM_ARGS=")]
    assert len(lines) == 1, "expected exactly one EARLYOOM_ARGS= line"
    (value,) = shlex.split(lines[0].split("=", 1)[1])
    return value.split()


def flag(args: list[str], name: str) -> str:
    assert name in args, f"EARLYOOM_ARGS has no {name}"
    return args[args.index(name) + 1]


# --- pure: runs in CI and in a dev clone ---


def test_min_free_kbytes_reserves_memory_for_atomic_allocations() -> None:
    """The failure that happened was atomic allocations - softirq context, which
    cannot reclaim - finding no free page. ``vm.min_free_kbytes`` is the pool they
    draw on. The kernel derived 5,663 kB at 2 GB and 11,399 kB at 8 GB; 32 MiB is
    the floor this repo holds it to.
    """
    assert int(sysctl_settings()["vm.min_free_kbytes"]) >= 32 * 1024


def test_overcommit_is_enabled_as_redis_asks() -> None:
    """Redis warns at every start on anything but 1, and here the warning is inert -
    so the setting that silences it costs nothing this node can measure (broker#26).

    ``checkOvercommit`` (redis 7.0, ``src/syscheck.c``) warns for every value
    except 1, describing the risk as the kernel refusing a save's fork "if we
    don't have enough free memory to satisfy double the current memory usage".
    **On this kernel mode 0 does not consider free memory.** Measured on the node
    (6.12.93) with private anonymous mappings that were never touched: 7.29 GiB -
    more than ``MemAvailable`` (~6.3 GiB) - was granted, and 8.75 GiB, ``MemTotal``
    + 1 GiB, was refused. So mode 0 refuses only a single request larger than RAM
    plus swap, there is no swap, and a fork commits at most redis's own private
    memory, which ``maxmemory`` bounds. The two modes differ for nothing the cap
    allows.

    What the change does cost: an allocation larger than the whole machine now
    fails at first touch - an earlyoom or kernel OOM kill, in the order the tests
    above pin - instead of up front with ``ENOMEM``. That is the trade, taken
    because a WARNING at every start, which step 1d of the restart runbook walks
    a reader straight into, is worse than a setting whose two modes are
    indistinguishable at any size ``maxmemory`` permits.

    ``jemalloc#1328``, which the warning cites, is about mode **2**:
    ``os_overcommits_proc`` (``src/pages.c``) treats 0 and 1 alike as overcommit
    enabled.

    **Re-measure after a kernel change.** "Mode 0 ignores free memory" is this
    kernel's behaviour, not a guarantee.
    """
    assert sysctl_settings()["vm.overcommit_memory"] == "1"


def test_redis_protection_covers_the_cap_and_a_forks_copy_on_write() -> None:
    """Read from the tracked cap, so raising ``maxmemory`` without this fails here.

    Twice the cap: a full dataset plus a background save or AOF rewrite dirtying
    all of it under copy-on-write, the worst case ``docs/RESTART-WINDOW.md``
    already sized ``maxmemory`` against.
    """
    cap = parse_size(parse_directives(REPO_REDIS_CONF.read_text())["maxmemory"])
    assert memory_low(REDIS_MEMORY) >= 2 * cap


def test_system_slice_protects_at_least_what_its_children_claim() -> None:
    """This node's cgroup2 is mounted **without** ``memory_recursiveprot``, so a
    child's effective protection is capped by its parent's - and
    ``system.slice``'s default is 0. Without this drop-in the two below would read
    correctly in ``systemctl show`` and protect nothing.
    """
    assert memory_low(SYSTEM_SLICE) >= memory_low(REDIS_MEMORY) + memory_low(TAILSCALED_MEMORY)


@pytest.mark.parametrize("tracked", list(OOM_UNITS), ids=lambda p: OOM_UNITS[p])
def test_the_bus_scores_below_what_exe_dev_starts(tracked: Path) -> None:
    """``--avoid`` alone left the bus ranked **above** dev tooling.

    exe.dev's ``exe-init`` and ``sshd`` run at ``oom_score_adj`` -1000, and every
    session process inherits it - VSCode Server, its ``MainThread`` children,
    ``claude`` - so their ``oom_score`` reads 0. earlyoom 1.7 floors a
    ``--prefer`` match at 300 and gives an unmatched one 0, while a unit at the
    default adj 0 reads ~667 here, which ``--avoid`` only brings to ~367. Its dry
    run on 2026-09-16 would have killed tailscaled and redis-server before any of
    them.

    -900, not -1000. The kernel's own OOM killer can never pick dev tooling at
    -1000; with the bus at -1000 too, nothing big would be left to kill, and
    ``kernel.panic = 0`` hangs the node on that panic. At -900 the kernel's last
    resort is a kill ``Restart=`` recovers in 100 ms. Debian gives the system
    ``dbus-daemon`` the same.
    """
    assert -1000 < oom_score_adjust(tracked) <= -900


def test_earlyoom_args_survive_systemd_word_splitting() -> None:
    """The unit runs ``earlyoom $EARLYOOM_ARGS``: systemd splits that on whitespace
    and does not interpret quotes, so a quoted or spaced regex arrives mangled."""
    for arg in earlyoom_args():
        assert not set(arg) & {"'", '"'}, f"{arg!r} carries a quote systemd would pass literally"


@pytest.mark.parametrize("name", PROTECTED)
def test_earlyoom_avoid_list_covers_the_bus(name: str) -> None:
    """A ranking, not an exclusion: earlyoom 1.7 has no ``--ignore``, and ``--avoid``
    only subtracts 300 from ``oom_score``. The regex is half of it;
    ``test_the_bus_scores_below_what_exe_dev_starts`` is the other half.
    """
    args = earlyoom_args()
    assert re.search(flag(args, "--avoid"), name), f"--avoid does not cover {name}"
    assert not re.search(flag(args, "--prefer"), name), f"--prefer matches {name}"


@pytest.mark.parametrize("name", DEV_TOOLING)
def test_earlyoom_prefers_dev_tooling(name: str) -> None:
    args = earlyoom_args()
    assert re.search(flag(args, "--prefer"), name), f"--prefer misses {name}"
    assert not re.search(flag(args, "--avoid"), name), f"--avoid protects {name}"


def test_earlyoom_acts_while_memory_is_still_available() -> None:
    """``-m`` is the available-memory percentage at which it sends SIGTERM. At 8 GB,
    10% is ~790 MiB - well before the ~400-500 MiB the 2 GB node failed at."""
    assert 5 <= int(flag(earlyoom_args(), "-m").split(",")[0]) <= 15


# --- installed parity and live values: this node only ---


@pytest.mark.parametrize("tracked", list(INSTALLED), ids=lambda p: p.name)
def test_installed_copy_matches_tracked(tracked: Path) -> None:
    installed = _read_if_installed(INSTALLED[tracked])
    if installed is None:
        pytest.skip(f"{INSTALLED[tracked]} not installed on this host")
    assert installed == tracked.read_text()


@pytest.mark.parametrize("key", sorted(sysctl_settings()))
def test_live_sysctl_is_the_tracked_value(key: str) -> None:
    """Every key the drop-in sets, not the one that happened to be first.

    A line in ``/etc/sysctl.d`` is not a value in the kernel until ``sysctl -p``
    runs, and the install step that writes the file is a separate line from the
    one that applies it - so a key added to the tracked copy and never applied
    is exactly the drift this reads back. Derived from the file rather than
    named, so the next key gets the check without an edit here.
    """
    if _read_if_installed(INSTALLED[SYSCTL]) is None:
        pytest.skip("sysctl drop-in not installed on this host")
    live = Path("/proc/sys", *key.split(".")).read_text().strip()
    assert live == sysctl_settings()[key]


@pytest.mark.parametrize("tracked", list(CGROUPS), ids=lambda p: p.parent.name)
def test_live_memory_low_is_the_tracked_value(tracked: Path) -> None:
    """A drop-in on disk is not a value in the kernel until systemd applies it."""
    if _read_if_installed(INSTALLED[tracked]) is None:
        pytest.skip(f"{INSTALLED[tracked]} not installed on this host")
    assert int(CGROUPS[tracked].read_text()) == memory_low(tracked)


@pytest.mark.parametrize("tracked", list(OOM_UNITS), ids=lambda p: OOM_UNITS[p])
def test_live_bus_ranks_below_dev_tooling(tracked: Path) -> None:
    """``OOMScoreAdjust=`` applies at exec: a running process keeps its old score
    until ``choom`` or a restart, so read the kernel's. Under ``PREFER_FLOOR``,
    ``--avoid`` takes it below zero - beneath every ``--prefer`` match and beneath
    the dev tooling no regex names, which scores 0."""
    if _read_if_installed(INSTALLED[tracked]) is None:
        pytest.skip(f"{INSTALLED[tracked]} not installed on this host")
    unit = OOM_UNITS[tracked]
    main_pid = subprocess.run(
        ["systemctl", "show", "-p", "MainPID", "--value", unit], capture_output=True, text=True
    ).stdout.strip()
    assert main_pid not in ("", "0"), f"{unit} is not running"
    proc = Path("/proc") / main_pid
    assert int((proc / "oom_score_adj").read_text()) == oom_score_adjust(tracked)
    assert int((proc / "oom_score").read_text()) < PREFER_FLOOR


@pytest.mark.parametrize(("verb", "expected"), [("is-active", "active"), ("is-enabled", "enabled")])
def test_earlyoom_is_running_and_survives_a_boot(verb: str, expected: str) -> None:
    """Enabled as well as active: exe.dev restarts are hard stops, and a unit that
    is only running protects nothing after the next one."""
    if _read_if_installed(INSTALLED[EARLYOOM]) is None:
        pytest.skip("earlyoom not configured on this host")
    if not shutil.which("systemctl"):
        pytest.skip("no systemctl")
    state = subprocess.run(["systemctl", verb, "earlyoom"], capture_output=True, text=True)
    assert state.stdout.strip() == expected
