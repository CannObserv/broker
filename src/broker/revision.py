"""The build id: which code a probe tick or a backup run was (broker#105).

A release carries a ``REVISION`` file at its root, written by the deploy; a
checkout carries none and reads as ``dev``. Read by the code rather than asked
of git, because a release is not a checkout (#22 D5, status R10).
"""

from pathlib import Path

REVISION_PATH = Path(__file__).resolve().parents[2] / "REVISION"
DEV = "dev"
UNKNOWN = "unknown"


def read_revision(path: Path | None = None) -> str:
    """The ``REVISION`` file's stripped contents; ``dev`` when it is absent.

    Resolved at call time so the tests can point it elsewhere. Never raises: the
    probe reads it before its summary line and check-in, the backup before its
    state record, and a build id must not cost either its report. A file that
    is there but unreadable or empty is a broken release, so it reads as
    ``unknown`` - never ``dev``, which would pass it off as a checkout.
    """
    try:
        return (path or REVISION_PATH).read_text().strip() or UNKNOWN
    except FileNotFoundError:
        return DEV
    except (OSError, ValueError):
        return UNKNOWN
