"""ci.yml and audit.yml pin their actions and their runner on purpose (#88).

**Actions.** ``astral-sh/setup-uv`` publishes no floating major tag since v8,
so ``@v10`` would not resolve. Both setup-uv and ``google-github-actions/auth``
pin a commit SHA with a ``# vX.Y.Z`` comment, the form CannObserv/watcher
uses, and the two workflows pin the same setup-uv.

**The cache.** setup-uv v9 flipped ``prune-cache`` to false. Unpruned, the uv
cache that ci.yml saves holds co-core unpacked from the private wheelhouse,
and broker is public: a fork's pull request can restore a cache saved on
``main``. ``uv cache prune --ci`` drops every pre-built wheel, co-core's
included, so ci.yml sets it back. audit.yml has nothing to cache, since
``uv audit --frozen`` downloads no package, so it neither restores nor saves.

**The runner.** ``ubuntu-24.04``, not ``ubuntu-latest``: the image matches the
node's userland (bash, perl, coreutils), which the shell under test runs on.
``ubuntu-latest`` moves to 26.04 on GitHub's schedule, between 2026-10-19 and
2026-11-19 per actions/runner-images#14748. When the node moves, CI moves with
it, and on the node the last test here fails until it does.
"""

from __future__ import annotations

import re
from pathlib import Path

from tests.deploy.conftest import outcome_of, pretend_node, require_broker_node

WORKFLOWS = Path(__file__).resolve().parents[2] / ".github" / "workflows"
CI = WORKFLOWS / "ci.yml"
AUDIT = WORKFLOWS / "audit.yml"
OS_RELEASE = Path("/etc/os-release")

PINNED = ("astral-sh/setup-uv", "google-github-actions/auth")
SHA_WITH_TAG = re.compile(r"@[0-9a-f]{40} # v\d+\.\d+\.\d+$")


def _uses(path: Path, action: str) -> list[str]:
    """Every ``uses: <action>@...`` line in ``path``, from the ``@`` on."""
    return re.findall(
        rf"^[ \t]+(?:- )?uses: {re.escape(action)}(@.*)$", path.read_text(), re.MULTILINE
    )


def _step_with(path: Path, action: str) -> dict[str, str]:
    """The ``with:`` keys of the one step using ``action`` in ``path``."""
    text = path.read_text()
    step = re.search(rf"^([ \t]+)uses: {re.escape(action)}@.*\n((?:\1.*\n)*)", text, re.MULTILINE)
    assert step, f"{path.name}: no step uses {action}"
    return dict(re.findall(r"^[ \t]+([\w-]+): (\S+)$", step.group(2), re.MULTILINE))


def _runners(path: Path) -> set[str]:
    return set(re.findall(r"^[ \t]+runs-on: (\S+)$", path.read_text(), re.MULTILINE))


def test_setup_uv_and_auth_pin_a_commit_with_its_tag():
    for path in (CI, AUDIT):
        for action in PINNED:
            for ref in _uses(path, action):
                assert SHA_WITH_TAG.match(ref), f"{path.name}: {action}{ref}"
    assert _uses(CI, "google-github-actions/auth"), "ci.yml lost its WIF step"


def test_ci_and_audit_pin_the_same_setup_uv():
    refs = set(_uses(CI, "astral-sh/setup-uv")) | set(_uses(AUDIT, "astral-sh/setup-uv"))
    assert len(refs) == 1, refs


def test_ci_prunes_the_cache_it_saves():
    """co-core's private source stays out of a public repo's Actions cache."""
    with_ = _step_with(CI, "astral-sh/setup-uv")
    assert with_.get("enable-cache") == "true"
    assert with_.get("prune-cache") == "true"


def test_audit_touches_no_cache():
    assert _step_with(AUDIT, "astral-sh/setup-uv").get("enable-cache") == "false"


def test_ci_and_audit_run_on_one_pinned_image():
    runners = _runners(CI) | _runners(AUDIT)
    assert len(runners) == 1, runners
    assert not runners.pop().endswith("-latest")


def _os_version_id() -> str:
    found = re.search(r'^VERSION_ID="?([^"\n]+)"?$', OS_RELEASE.read_text(), re.MULTILINE)
    assert found, f"{OS_RELEASE} names no VERSION_ID"
    return found[1]


def test_runner_image_matches_the_node():
    require_broker_node()
    assert _runners(CI) == {f"ubuntu-{_os_version_id()}"}


def test_runner_image_check_fails_on_a_node_that_moved(monkeypatch, tmp_path):
    os_release = tmp_path / "os-release"
    os_release.write_text('NAME="Ubuntu"\nVERSION_ID="26.04"\n')
    monkeypatch.setattr(f"{__name__}.OS_RELEASE", os_release)
    pretend_node(monkeypatch, tmp_path, present=True)
    assert isinstance(outcome_of(test_runner_image_matches_the_node), AssertionError)
