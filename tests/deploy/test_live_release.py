"""``/srv/broker/live`` is a root-owned release of a commit on ``origin/main``.

The node half of #22 Q12. ``scripts/deploy.sh`` makes this true when it
switches, and ``tests/deploy/test_deploy.py`` proves it does against stubs; this
reads the node afterwards, so a link repointed by hand, a release handed back to
``exedev``, or a build from a commit that never reached ``main`` fails here.
Off the node it skips by ``on_broker_node``, never by the link's own absence: on
the node a missing ``live`` fails (CannObserv/broker#81).
"""

import os
import re
import subprocess
from collections.abc import Callable
from pathlib import Path

import pytest

from tests.deploy.conftest import outcome_of, pretend_node, require_broker_node

_ROOT = Path(__file__).resolve().parents[2]
LIVE = Path("/srv/broker/live")
SHA12 = re.compile(r"[0-9a-f]{12}")


def _on_origin_main(sha12: str) -> bool:
    """Whether ``sha12`` is ``origin/main`` or behind it, as this checkout last fetched."""
    result = subprocess.run(
        ["git", "-C", str(_ROOT), "merge-base", "--is-ancestor", sha12, "origin/main"],
        capture_output=True,
        check=False,
    )
    return result.returncode == 0


def _held_by_root(directory: Path, owner: tuple[int, int]) -> str | None:
    """deploy.sh's ``roots_alone`` for one directory holding ``live``, or ``None``."""
    if not directory.is_dir():
        return f"{directory} is not a directory"
    if (directory.stat().st_uid, directory.stat().st_gid) != owner:
        return f"{directory} is not root's, so exedev can repoint live"
    if directory.stat().st_mode & 0o022:
        return f"{directory} is writable by more than root, so exedev can repoint live"
    return None


def _problems(
    live: Path,
    *,
    owner: tuple[int, int] = (0, 0),
    on_main: Callable[[str], bool] = _on_origin_main,
) -> list[str]:
    """What is wrong with the release ``live`` names; nothing when it is sound.

    The directories holding ``live`` first: a link is only as safe as they are.
    ``owner`` and ``on_main`` are seams for the pins below, which cannot make a
    root-owned file or vouch for a commit on any host's ``origin/main``.
    """
    if not live.is_symlink():
        return [f"{live} is not a link: scripts/deploy.sh makes it (deploy/README.md, Deploying)"]
    held = (_held_by_root(d, owner) for d in (live.parent, live.parent / "releases"))
    problems = [problem for problem in held if problem]
    if live.lstat().st_uid != owner[0]:
        problems.append(f"{live} is not root's link")
    release = live.resolve()
    if release.parent != (live.parent / "releases").resolve():
        return [*problems, f"{live} -> {release}, which is not under {live.parent}/releases"]
    if not release.is_dir():
        return [*problems, f"{live} -> {release}, and there is no release there: deploy one"]
    if (release.stat().st_uid, release.stat().st_gid) != owner:
        problems.append(f"{release} is not root's, so exedev can change what the probe runs")
    if release.stat().st_mode & 0o222:
        problems.append(f"{release} is writable: the build leaves it 555")
    revision = release / "REVISION"
    if not revision.is_file():
        return [*problems, f"{release} has no REVISION: an interrupted build"]
    if revision.stat().st_uid != owner[0] or revision.stat().st_mode & 0o777 != 0o444:
        problems.append(f"{revision} is not root's 444")
    build = revision.read_text().strip()
    if not SHA12.fullmatch(build) or build != release.name:
        problems.append(f"{revision} says {build!r}, not the release's name {release.name}")
    elif not on_main(build):
        problems.append(f"{build} is not on origin/main (git fetch origin, then look again)")
    return problems


def test_live_is_a_root_owned_release_of_a_main_commit() -> None:
    require_broker_node()
    problems = _problems(LIVE)
    assert not problems, "\n".join(problems)


# --- the pins: the node signal, and each problem the check names ---


@pytest.mark.parametrize("present", [True, False], ids=["on-node", "off-node"])
def test_an_absent_live_fails_on_the_node_and_skips_off_it(monkeypatch, tmp_path, present) -> None:
    monkeypatch.setattr(f"{__name__}.LIVE", tmp_path / "absent")
    pretend_node(monkeypatch, tmp_path, present=present)
    expected = AssertionError if present else pytest.skip.Exception
    assert isinstance(outcome_of(test_live_is_a_root_owned_release_of_a_main_commit), expected)


def _release(tmp_path: Path, build: str = "0123456789ab", *, revision: str | None = None) -> Path:
    """``tmp_path/live -> releases/<build>``, built as the deploy leaves it."""
    release = tmp_path / "releases" / build
    release.mkdir(parents=True)
    (release / "REVISION").write_text(f"{build if revision is None else revision}\n")
    (release / "REVISION").chmod(0o444)
    release.chmod(0o555)
    (tmp_path / "live").symlink_to(f"releases/{build}")
    return tmp_path / "live"


MINE = (os.getuid(), os.getgid())


def _on_main(sha12: str) -> bool:
    return True


def test_a_release_as_the_deploy_leaves_it_has_no_problem(tmp_path) -> None:
    assert _problems(_release(tmp_path), owner=MINE, on_main=_on_main) == []


def test_a_release_not_roots_is_named(tmp_path) -> None:
    problems = "\n".join(_problems(_release(tmp_path), on_main=_on_main))
    assert "is not root's, so exedev can change what the probe runs" in problems
    assert "not root's 444" in problems


@pytest.mark.parametrize("held", ["root", "releases"])
def test_a_directory_more_than_root_can_write_is_named(tmp_path, held) -> None:
    """deploy.sh's ``roots_alone``: a link is only as safe as its directory.
    Writable by ``exedev``, ``live`` can be repointed at any release without
    sudo, an older sound one included, and every other check would pass."""
    live = _release(tmp_path)
    (tmp_path if held == "root" else tmp_path / "releases").chmod(0o777)
    (problem,) = _problems(live, owner=MINE, on_main=_on_main)
    assert "writable by more than root" in problem


def test_directories_and_link_not_roots_are_named(tmp_path) -> None:
    problems = _problems(_release(tmp_path), on_main=_on_main)
    assert sum("so exedev can repoint live" in p for p in problems) == 2, problems
    assert any("not root's link" in p for p in problems), problems


def test_a_writable_release_is_named(tmp_path) -> None:
    live = _release(tmp_path)
    live.resolve().chmod(0o755)
    (problem,) = _problems(live, owner=MINE, on_main=_on_main)
    assert "writable" in problem


def test_a_release_without_revision_is_an_interrupted_build(tmp_path) -> None:
    live = _release(tmp_path)
    release = live.resolve()
    release.chmod(0o755)
    (release / "REVISION").unlink()
    release.chmod(0o555)
    (problem,) = _problems(live, owner=MINE, on_main=_on_main)
    assert "interrupted build" in problem


def test_a_revision_that_is_not_the_release_name_is_named(tmp_path) -> None:
    live = _release(tmp_path, revision="ba9876543210")
    (problem,) = _problems(live, owner=MINE, on_main=_on_main)
    assert "not the release's name" in problem


def test_a_build_off_main_is_named(tmp_path) -> None:
    (problem,) = _problems(_release(tmp_path), owner=MINE, on_main=lambda _: False)
    assert "not on origin/main" in problem


def test_a_link_outside_releases_is_named(tmp_path) -> None:
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    (tmp_path / "releases").mkdir()
    (tmp_path / "live").symlink_to(elsewhere)
    (problem,) = _problems(tmp_path / "live", owner=MINE, on_main=_on_main)
    assert "not under" in problem


def test_main_ancestry_is_asked_of_git(tmp_path) -> None:
    """The real seam, against this checkout: a sha12 git has never seen is not
    on ``origin/main``, whatever else is true of it."""
    assert not _on_origin_main("000000000000")


def test_a_link_to_a_removed_release_is_named(tmp_path) -> None:
    (tmp_path / "releases").mkdir()
    (tmp_path / "live").symlink_to("releases/0123456789ab")
    (problem,) = _problems(tmp_path / "live", owner=MINE, on_main=_on_main)
    assert "no release there" in problem
