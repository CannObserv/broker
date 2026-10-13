"""Tailscale's apt origin is scoped to the packages it was added for (#67).

The knob follows ``pkgs.tailscale.com`` in patching-hosts' maintenance lane. At
apt's default priority, 500, which ties Ubuntu's, a package that site served
under an Ubuntu name at a higher version would replace Ubuntu's, so the skill's
policy.md scopes a followed origin by pins: ``Package: *`` below 500 for the
site, and a package pin above 500 for each package it was added for. Without
them the probe reports ``unscoped:pkgs.tailscale.com`` and ``unpinned:<pkg>``.

Tracked in ``deploy/``, installed as:

- ``apt/preferences.d/tailscale.pref`` -> ``/etc/apt/preferences.d/``
"""

from __future__ import annotations

from pathlib import Path

from tests.deploy.conftest import DEPLOY, read_installed

PINS = DEPLOY / "apt" / "preferences.d" / "tailscale.pref"
INSTALLED = Path("/etc/apt/preferences.d/tailscale.pref")
SITE = "pkgs.tailscale.com"
ADDED_FOR = {"tailscale", "tailscale-archive-keyring"}


def _stanzas() -> list[dict[str, str]]:
    """apt_preferences(5): blank-line separated stanzas of ``Field: value``."""
    stanzas, current = [], {}
    for line in PINS.read_text().splitlines():
        if line.startswith("#"):
            continue
        if not line.strip():
            if current:
                stanzas.append(current)
            current = {}
            continue
        field, _, value = line.partition(":")
        current[field.strip()] = value.strip()
    if current:
        stanzas.append(current)
    return stanzas


def test_every_stanza_pins_the_tailscale_site() -> None:
    stanzas = _stanzas()
    assert stanzas
    for stanza in stanzas:
        assert set(stanza) == {"Package", "Pin", "Pin-Priority"}, stanza
        assert stanza["Pin"] == f"origin {SITE}", stanza


def test_the_site_is_below_ubuntu_for_everything_else() -> None:
    catch_all = [s for s in _stanzas() if s["Package"] == "*"]
    assert len(catch_all) == 1
    assert int(catch_all[0]["Pin-Priority"]) < 500


def test_each_package_it_was_added_for_is_above_500() -> None:
    named = {
        package: int(s["Pin-Priority"])
        for s in _stanzas()
        if s["Package"] != "*"
        for package in s["Package"].split()
    }
    assert set(named) == ADDED_FOR
    assert all(priority > 500 for priority in named.values()), named


def test_installed_copy_matches_tracked() -> None:
    assert read_installed(INSTALLED) == PINS.read_text()
