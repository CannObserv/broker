"""The build id: which code a probe tick or a backup run was (broker#105).

A release carries a ``REVISION`` file at its root, written by the deploy; a
checkout carries none and reads as ``dev``. Read by the code rather than asked
of git, because a release is not a checkout (#22 D5, status R10).
"""

from pathlib import Path

REVISION_PATH = Path(__file__).resolve().parents[2] / "REVISION"
DEV = "dev"


def read_revision(path: Path | None = None) -> str:
    """The ``REVISION`` file's stripped contents, or ``dev`` when it is absent.

    Resolved at call time so the tests can point it elsewhere. Only absence is
    ``dev``: an unreadable file is a broken release and raises, rather than
    reporting itself as a checkout.
    """
    try:
        return (path or REVISION_PATH).read_text().strip()
    except FileNotFoundError:
        return DEV
