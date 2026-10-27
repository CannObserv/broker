"""pytest keeps a failed test's ``tmp_path`` and nothing else (CannObserv/broker#126).

Under pytest's default retention (``all``, three sessions) a node run of this
suite leaves about 200 MB under ``/tmp/pytest-of-<user>``, so about 600 MB sat
on a 20 GB root that is also Redis's AOF headroom. ``tmp_path_retention_policy
= "failed"`` keeps only failed tests' directories: the evidence a failure needs,
and nothing a passing run does.

The deploy tests leave read-only (``555``) release directories. The per-test
cleanup under ``failed`` is ``shutil.rmtree(..., ignore_errors=True)``, which
cannot enter them, so a small residue stays; the numbered-directory rotation
removes it with ``rm_rf``, which makes them writable first. Both halves are
pinned here, on the policy ``pyproject.toml`` actually sets, in a subprocess
rooted at its own temp root so the rotation is the real one (pytester's own
``--basetemp`` would switch both off).
"""

from __future__ import annotations

import os
import subprocess
import sys
import tomllib
from collections.abc import Iterator
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
OPTIONS = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())["tool"]["pytest"]["ini_options"]

#: A deploy-shaped test: a release directory left ``555``, beside a plain file.
INNER = """
from pathlib import Path


def test_passes(tmp_path):
    release = tmp_path / "releases" / "0123456789ab"
    release.mkdir(parents=True)
    (release / "payload").write_bytes(b"x" * 4096)
    release.chmod(0o555)
    (tmp_path / "plain").write_bytes(b"y" * 65536)


def test_fails(tmp_path):
    (tmp_path / "evidence").write_text("kept")
    assert False
"""


def _ini() -> str:
    keys = ("tmp_path_retention_policy", "tmp_path_retention_count")
    return "[pytest]\n" + "".join(f"{k} = {OPTIONS[k]}\n" for k in keys if k in OPTIONS)


def _run(work: Path, root: Path, *select: str, rc: int = 0) -> None:
    """One inner session. ``rc`` is its expected exit: a run that broke for another
    reason (a collection error) would otherwise fail later, as a file-list diff."""
    env = {**os.environ, "PYTEST_DEBUG_TEMPROOT": str(root)}
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-p", "no:asyncio"]
        + list(select),
        cwd=work,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == rc, result.stdout + result.stderr


@pytest.fixture
def work(tmp_path: Path) -> Iterator[tuple[Path, Path]]:
    work = tmp_path / "work"
    work.mkdir()
    (work / "pytest.ini").write_text(_ini())
    (work / "test_inner.py").write_text(INNER)
    root = tmp_path / "root"
    root.mkdir()
    yield work, root
    # The inner runs' read-only residue sits under this test's own tmp_path.
    for d, _, _ in os.walk(root):
        os.chmod(d, 0o700)


def _sessions(root: Path) -> list[Path]:
    (base,) = root.glob("pytest-of-*")
    return sorted(base.glob("pytest-[0-9]*"), key=lambda p: int(p.name.split("-")[1]))


def _files(path: Path) -> list[str]:
    return sorted(str(p.relative_to(path)) for p in path.rglob("*") if p.is_file())


def test_the_policy_is_failed() -> None:
    assert OPTIONS.get("tmp_path_retention_policy") == "failed"


def test_the_count_is_one() -> None:
    """#127: one session's tree, so the disk line is the baseline plus one run's."""
    assert OPTIONS.get("tmp_path_retention_count") == 1


def test_a_passing_test_keeps_nothing_but_its_read_only_release(work) -> None:
    work_dir, root = work
    _run(work_dir, root, "-k", "passes")
    (session,) = _sessions(root)
    assert _files(session) == ["test_passes0/releases/0123456789ab/payload"]


def test_a_failing_test_keeps_its_evidence(work) -> None:
    work_dir, root = work
    _run(work_dir, root, rc=1)
    (session,) = _sessions(root)
    assert "test_fails0/evidence" in _files(session)
    assert "test_passes0/plain" not in _files(session)


def test_the_rotation_removes_the_read_only_residue(work) -> None:
    work_dir, root = work
    count = int(OPTIONS.get("tmp_path_retention_count", 3))
    for _ in range(count + 1):
        _run(work_dir, root, "-k", "passes")
    sessions = _sessions(root)
    assert len(sessions) == count
    assert sessions[0].name == "pytest-1", "pytest-0, read-only release and all, is gone"
