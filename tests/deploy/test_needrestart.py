"""needrestart lists restarts and never performs them (CannObserv/broker#65).

apt's ``DPkg::Post-Invoke`` hook (``/etc/apt/apt.conf.d/99needrestart``) runs
``needrestart -m u`` after every dpkg run. Ubuntu's patch turns ``-m u`` into
**automatic** restarts when ``$nrconf{restart}`` is unset - the stock state -
so a ``libc6`` security update would restart ``redis-server`` and drop every
participant off the bus, outside any window. ``NEEDRESTART_MODE=l`` in the
environment overrides that only if it survives every process between the
operator and the hook; this drop-in makes the answer not depend on it.

Tracked in ``deploy/``, installed as:

- ``needrestart.conf.d/broker.conf`` -> ``/etc/needrestart/conf.d/``

Split like the other deploy tests: **pure** assertions on the tracked copy run
everywhere; **installed parity** and **live** assertions skip where the node is
not this one - decided by ``on_broker_node``, not by needrestart being installed.
GitHub's ``ubuntu-latest`` ships needrestart without this drop-in, and the live
test failed on it for 27 pushes in a row (broker#79). On the node, a missing
drop-in fails both rather than skipping them.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from tests.deploy.conftest import DEPLOY, pretend_node, read_installed, require_broker_node

DROPIN = DEPLOY / "needrestart.conf.d" / "broker.conf"
INSTALLED = Path("/etc/needrestart/conf.d/broker.conf")
MAIN_CONF = Path("/etc/needrestart/needrestart.conf")

# needrestart's config is Perl, eval'd into `%nrconf`. Evaluating it the same
# way is the only honest parse: a syntax error makes needrestart die, and the
# apt hook swallows that with `|| true`.
_EVAL = (
    "our %nrconf; our $LOGPREF = q(); "
    "eval do { local(@ARGV, $/) = $ARGV[0]; <> }; die $@ if $@; "
    "print defined $nrconf{restart} ? $nrconf{restart} : q(undef);"
)


def _restart_mode(conf: Path) -> str:
    perl = shutil.which("perl")
    if perl is None:
        pytest.skip("perl not available on this host")
    return subprocess.run(
        [perl, "-e", _EVAL, str(conf)],
        capture_output=True,
        text=True,
        check=True,
    ).stdout


def test_dropin_sets_list_only_restart_mode() -> None:
    assert _restart_mode(DROPIN) == "l"


def test_dropin_sets_nothing_else() -> None:
    """One key, so the drop-in cannot quietly change needrestart's other
    behaviour, and ``$nrconf{ui}`` stays unset - setting it would force
    interactive mode instead."""
    lines = [
        ln.strip()
        for ln in DROPIN.read_text().splitlines()
        if ln.strip() and not ln.strip().startswith("#")
    ]
    assert lines == ["$nrconf{restart} = 'l';"]


def test_installed_copy_matches_tracked() -> None:
    assert read_installed(INSTALLED) == DROPIN.read_text()


def test_live_config_chain_resolves_to_list_only() -> None:
    """The main config globs ``conf.d/*.conf`` in sort order, so a later file
    could override this one. Evaluate the chain needrestart itself reads."""
    require_broker_node()
    assert MAIN_CONF.exists(), f"{MAIN_CONF} absent - needrestart not installed on the node"
    assert _restart_mode(MAIN_CONF) == "l"


# --- the node signal the two tests above skip on (broker#79) ---


@pytest.fixture
def stock_needrestart(tmp_path, monkeypatch) -> Path:
    """A needrestart main config with no drop-in behind it: GitHub's
    ``ubuntu-latest``, which ships needrestart and resolves to stock ``undef``."""
    main = tmp_path / "needrestart.conf"
    main.write_text("$nrconf{verbosity} = 1;\n")
    monkeypatch.setattr(sys.modules[__name__], "MAIN_CONF", main)
    monkeypatch.setattr(sys.modules[__name__], "INSTALLED", tmp_path / "conf.d" / "broker.conf")
    return main


def test_live_chain_skips_off_the_node_even_with_needrestart_installed(
    stock_needrestart, monkeypatch, tmp_path
) -> None:
    pretend_node(monkeypatch, tmp_path, present=False)
    with pytest.raises(pytest.skip.Exception):
        test_live_config_chain_resolves_to_list_only()


def test_live_chain_fails_on_the_node_without_the_dropin(
    stock_needrestart, monkeypatch, tmp_path
) -> None:
    """The hazard the obvious fix had: keyed on the drop-in, its deletion from the
    node would skip the one test that guards it."""
    if shutil.which("perl") is None:
        pytest.skip("perl not available on this host")
    pretend_node(monkeypatch, tmp_path, present=True)
    with pytest.raises(AssertionError, match="undef"):
        test_live_config_chain_resolves_to_list_only()


def test_installed_copy_fails_on_the_node_when_absent(
    stock_needrestart, monkeypatch, tmp_path
) -> None:
    pretend_node(monkeypatch, tmp_path, present=True)
    with pytest.raises(pytest.fail.Exception):
        test_installed_copy_matches_tracked()


def test_installed_copy_skips_off_the_node(stock_needrestart, monkeypatch, tmp_path) -> None:
    pretend_node(monkeypatch, tmp_path, present=False)
    with pytest.raises(pytest.skip.Exception):
        test_installed_copy_matches_tracked()
