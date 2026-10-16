"""Which engine the throwaway servers run (CannObserv/broker#102, #85's P2).

Valkey ships ``valkey-server`` and no ``redis-server``, its compat package
included, so a lookup of ``redis-server`` alone would skip every throwaway-server
test on the node after the swap. During the window's soak both packages are
installed, and only the unit that is running says which engine the node is:
the tests judge that one, never the first binary found.

Every case here runs against stand-ins on a ``PATH`` of the test's own - a
``systemctl`` and the two server binaries as shell scripts - so it needs neither
engine installed. The one that does is the last: ``acl_server`` names the engine
it started, which is how #101's control says which engine it judged.
"""

from pathlib import Path

import pytest

from tests.deploy import conftest
from tests.deploy.conftest import ENGINE_OVERRIDE, acl_server, engine_binary, render_tracked_acl


def _script(path: Path, body: str) -> Path:
    path.write_text(f"#!/bin/sh\n{body}\n")
    path.chmod(0o755)
    return path


def _host(monkeypatch, tmp_path: Path, *, installed: tuple[str, ...], active: str | None) -> Path:
    """A ``PATH`` holding ``installed`` and a ``systemctl`` whose ``active``
    unit runs that binary from it; every other unit is inactive."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name in installed:
        _script(bin_dir / name, f"echo '{name} v=0.0.0'")
    running = (
        f'"{active}.service") echo ActiveState=active; '
        f"echo 'ExecStart={{ path={bin_dir / active} ; argv[]={bin_dir / active} x ; }}' ;;"
        if active
        else ""
    )
    _script(
        bin_dir / "systemctl",
        f'for a; do unit="$a"; done\ncase "$unit" in\n  {running}\n'
        "  *) echo ActiveState=inactive; echo ExecStart= ;;\nesac",
    )
    monkeypatch.setenv("PATH", str(bin_dir))
    monkeypatch.delenv(ENGINE_OVERRIDE, raising=False)
    return bin_dir


@pytest.mark.parametrize("active", ["valkey-server", "redis-server"])
def test_with_both_installed_the_active_unit_decides(monkeypatch, tmp_path, active) -> None:
    """The soak: both packages installed, one running. ``redis-server`` first
    would judge Redis while the node runs Valkey."""
    bin_dir = _host(
        monkeypatch, tmp_path, installed=("redis-server", "valkey-server"), active=active
    )
    assert engine_binary() == str(bin_dir / active)


@pytest.mark.parametrize(
    ("installed", "expected"),
    [
        (("redis-server", "valkey-server"), "redis-server"),
        (("valkey-server",), "valkey-server"),
        (("redis-server",), "redis-server"),
    ],
)
def test_with_no_unit_running_redis_server_then_valkey_server(
    monkeypatch, tmp_path, installed, expected
) -> None:
    """Off the node, or on it between one engine's stop and the other's start:
    no unit says, so ``valkey-server`` only where ``redis-server`` is absent."""
    bin_dir = _host(monkeypatch, tmp_path, installed=installed, active=None)
    assert engine_binary() == str(bin_dir / expected)


def test_with_no_engine_installed_there_is_none(monkeypatch, tmp_path) -> None:
    _host(monkeypatch, tmp_path, installed=(), active=None)
    assert engine_binary() is None


def test_an_explicit_engine_wins_over_the_running_unit(monkeypatch, tmp_path) -> None:
    """For a rehearsal against an extracted binary, never installed on the node,
    as #85 ran Valkey 7.2.13."""
    _host(monkeypatch, tmp_path, installed=("redis-server",), active="redis-server")
    extracted = _script(tmp_path / "valkey-server", "echo 'Server v=7.2.13'")
    monkeypatch.setenv(ENGINE_OVERRIDE, str(extracted))
    assert engine_binary() == str(extracted)


def test_engine_name_is_the_binary_and_its_version(tmp_path) -> None:
    """Valkey's ``--version`` reads ``Server v=7.2.13``, naming no engine, so the
    binary's own name carries it."""
    valkey = _script(tmp_path / "valkey-server", "echo 'Server v=7.2.13 sha=00000000:0'")
    redis = _script(tmp_path / "redis-server", "echo 'Redis server v=7.0.15 sha=00000000:0'")
    assert conftest.engine_name(str(valkey)) == "valkey-server 7.2.13"
    assert conftest.engine_name(str(redis)) == "redis-server 7.0.15"


def test_acl_server_names_the_engine_it_started(tmp_path) -> None:
    binary = engine_binary()
    if binary is None:
        pytest.skip("no redis-server or valkey-server installed")
    with acl_server(render_tracked_acl(tmp_path), tmp_path) as connect:
        assert connect.engine == conftest.engine_name(binary)
        assert connect("acladmin").info("server")["process_id"]
