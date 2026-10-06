"""A deploy conftest's fixtures reach every deploy module, whatever the argument order
(CannObserv/broker#73).

pytest 9.1.0 and 9.1.1 bind a directory's conftest fixtures to the first
``Directory`` node collected for it, and match a test's fixtures by node identity.
A command line that puts a ``tests/`` module between two ``tests/deploy/`` modules
re-collects ``tests`` and builds a second ``Dir deploy``, so every test under it
fails with ``fixture '...' not found``. That is pytest-dev/pytest#14635 (bisected
to pytest 46478fad), fixed by pytest-dev/pytest#14645 and backported to 9.1.x
unreleased as of 2026-10-06. ``pyproject.toml`` excludes the two releases; this
pins the behaviour, so it is the check that a later pytest really carries the fix.

The tree mirrors this repo's: a package with a conftest, and a subpackage whose
conftest holds the fixture. Its top-level package is not called ``tests`` because
the inner run is in-process, and this repo's ``tests`` is already imported. It
runs without pytest-asyncio: the toy has no coroutine, and the plugin's unset-scope
warning would be an error under this repo's ``error::DeprecationWarning``.
"""

from __future__ import annotations

from itertools import permutations

import pytest

DEPLOY_A = "suite/deploy/test_a.py"
TOP = "suite/test_top.py"
DEPLOY_B = "suite/deploy/test_b.py"


@pytest.fixture
def tree(pytester: pytest.Pytester) -> pytest.Pytester:
    pytester.makeini("[pytest]\n")
    pytester.makepyfile(
        **{
            "suite/__init__": "",
            "suite/conftest": "",
            "suite/deploy/__init__": "",
            "suite/deploy/conftest": (
                "import pytest\n\n\n@pytest.fixture\ndef deploy_only():\n    return 1\n"
            ),
            DEPLOY_A.removesuffix(".py"): "def test_a(deploy_only):\n    assert deploy_only\n",
            TOP.removesuffix(".py"): "def test_top():\n    pass\n",
            DEPLOY_B.removesuffix(".py"): "def test_b(deploy_only):\n    assert deploy_only\n",
        }
    )
    return pytester


@pytest.mark.parametrize(
    "order",
    list(permutations((DEPLOY_A, TOP, DEPLOY_B))),
    ids=lambda order: "-".join(p.rsplit("/", 1)[-1].removesuffix(".py") for p in order),
)
def test_deploy_fixtures_survive_any_argument_order(
    tree: pytest.Pytester, order: tuple[str, ...]
) -> None:
    """The issue's reproduction is ``test_a-test_top-test_b``. On pytest 9.1.1 it and
    ``test_b-test_top-test_a``, the two orders that interleave, each give 2 passed
    and 1 error. The other four already pass there; they are kept because the claim
    is any order, and a fix that broke a contiguous order would otherwise go unseen.
    """
    result = tree.runpytest("-p", "no:cacheprovider", "-p", "no:asyncio", *order)
    result.assert_outcomes(passed=3)
