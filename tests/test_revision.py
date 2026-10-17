"""The build id the probe and the backup report (broker#105, #22 D5).

A release carries a ``REVISION`` file at its root; a checkout carries none and
reports ``dev``, so this ships before the first release exists.
"""

from __future__ import annotations

from pathlib import Path

from src.broker import revision

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_absent_reads_as_dev(tmp_path) -> None:
    assert revision.read_revision(tmp_path / "REVISION") == "dev"


def test_present_reads_as_its_stripped_contents(tmp_path) -> None:
    path = tmp_path / "REVISION"
    path.write_text("0123456789ab\n")
    assert revision.read_revision(path) == "0123456789ab"


def test_an_unreadable_file_is_unknown_never_dev_and_never_raises(tmp_path) -> None:
    """The probe reads it before its summary line and check-in, the backup
    before its state record: a raise would cost either its report. `dev` would
    pass a broken release off as a checkout."""
    path = tmp_path / "REVISION"
    path.mkdir()  # IsADirectoryError, an OSError, as a permission denial is
    assert revision.read_revision(path) == "unknown"


def test_undecodable_contents_are_unknown(tmp_path) -> None:
    path = tmp_path / "REVISION"
    path.write_bytes(b"\xff\xfe\x00")
    assert revision.read_revision(path) == "unknown"


def test_an_empty_file_is_unknown(tmp_path) -> None:
    path = tmp_path / "REVISION"
    path.write_text("\n")
    assert revision.read_revision(path) == "unknown"


def test_the_file_is_looked_for_at_the_project_root() -> None:
    """Beside ``pyproject.toml``, where a release root puts it - not inside the
    package, which a deploy does not write into."""
    assert revision.REVISION_PATH == REPO_ROOT / "REVISION"
    assert (REPO_ROOT / "pyproject.toml").is_file()


def test_the_default_is_read_at_call_time(tmp_path, monkeypatch) -> None:
    """Callers pass nothing; the path is resolved when called, so the tests
    can point it elsewhere without a release on disk."""
    path = tmp_path / "REVISION"
    path.write_text("feedfacecafe\n")
    monkeypatch.setattr(revision, "REVISION_PATH", path)
    assert revision.read_revision() == "feedfacecafe"
