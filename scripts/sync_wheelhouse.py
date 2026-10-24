"""Mirror the private cannobserv package index into the local wheelhouse.

Downloads every object under ``gs://co-gcs-pypi/wheels/`` into ``./.wheelhouse``
(repo root), skipping any file already present with a matching size - except a
file ``uv.lock`` names, which a release installs: that one is held to the
sha256 pyproject.toml's ``[tool.broker.wheelhouse-sha256]`` pins (#125),
fetched again when the copy here misses it, and refused when the download
misses it too. A size, or GCS's own checksum, comes over the same TLS as the
bytes, so neither tells a spoofed bucket from the real one. ``uv``
then resolves ``co-core`` / ``co-core-aio`` from that directory via the
``[tool.uv] find-links`` entry in ``pyproject.toml``. Lifted from
CannObserv/archiver (archiver#72, #75).

Runs standalone, *before* ``uv sync`` - it must not import the project (whose
deps are what the wheelhouse provides). Its own deps are uv.lock's
``wheelhouse`` group, which ``--only-group`` installs without the project, so an
empty wheelhouse is no obstacle (CannObserv/broker#113):

    uv run --frozen --only-group wheelhouse python scripts/sync_wheelhouse.py

Authentication is Application Default Credentials. On the VM/deploy that is the
service-account key at ``GOOGLE_APPLICATION_CREDENTIALS`` (set in
``/etc/broker/.env``); in CI it is the ADC file written by
``google-github-actions/auth`` (keyless Workload Identity Federation). Either
way the identity needs only ``roles/storage.objectViewer`` on the bucket.

Exit codes: ``0`` success (including a no-op re-run) · ``1`` failure (auth,
network, a missing bucket, a locked file with no pinned digest, or a download
that is not the pinned file).

**No broker unit runs this, and none ever has** (CannObserv/broker#37).
``scripts/deploy.sh`` runs it before every release build (#106), and copies in
only the wheels ``uv.lock`` names; a release's venv is resolved then, so no unit
resolves anything at start. CI runs it on every job. In a dev checkout it is run
by hand, before any ``uv sync`` or ``uv run`` that follows a ``co-core`` pin
change; skip it there and that command fails with a ``uv`` resolution error.
Archiver's service runs it as a non-fatal ``ExecStartPre``; that was this
docstring's claim until broker#37, carried over with the script. Here it would
put a GCS call in front of every probe tick, for an input only a pin change moves.
"""

from __future__ import annotations

import hashlib
import os
import sys
import tempfile
import tomllib
from pathlib import Path

from google.cloud import storage

BUCKET = os.environ.get("BROKER_WHEELHOUSE_BUCKET", "co-gcs-pypi")
PREFIX = os.environ.get("BROKER_WHEELHOUSE_PREFIX", "wheels/")
PROJECT = Path(__file__).resolve().parent.parent
# pyproject.toml's table of digests, under [tool.broker]: one per locked file.
TABLE = "wheelhouse-sha256"
# How uv.lock names the wheelhouse as a source; deploy.sh's locked_wheels() takes
# both (#106 CR 12).
WHEELHOUSE_SOURCES = (".wheelhouse", "./.wheelhouse")


def locked_files(lock: dict) -> list[str]:
    """The wheelhouse files a parsed uv.lock names: the sdist and wheel paths of
    each package whose source is the wheelhouse, as deploy.sh copies them."""
    names = []
    for package in lock.get("package", []):
        if package.get("source", {}).get("registry") not in WHEELHOUSE_SOURCES:
            continue
        for dist in [package.get("sdist", {}), *package.get("wheels", [])]:
            if "path" in dist:
                names.append(dist["path"])
    return names


def pins() -> dict[str, str]:
    """``{file: sha256}`` for each file the checkout's uv.lock names; raises for
    one pyproject.toml pins no digest for."""
    lock = tomllib.loads((PROJECT / "uv.lock").read_text())
    pyproject = tomllib.loads((PROJECT / "pyproject.toml").read_text())
    table = pyproject.get("tool", {}).get("broker", {}).get(TABLE, {})
    names = locked_files(lock)
    missing = [name for name in names if name not in table]
    if missing:
        raise ValueError(
            f"uv.lock names {', '.join(missing)}, which pyproject.toml's"
            f" [tool.broker.{TABLE}] pins no digest for: pin each from"
            " `sha256sum .wheelhouse/<file>`"
        )
    return {name: table[name] for name in names}


def sha256(path: str | Path) -> str:
    with open(path, "rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def sync() -> int:
    """Mirror ``gs://{BUCKET}/{PREFIX}`` into the checkout's wheelhouse; return an exit code."""
    dest = PROJECT / ".wheelhouse"
    dest.mkdir(parents=True, exist_ok=True)
    downloaded = skipped = 0
    try:
        pinned = pins()
        unlisted = set(pinned)
        client = storage.Client()
        for blob in client.list_blobs(BUCKET, prefix=PREFIX):
            name = blob.name.removeprefix(PREFIX)
            if not name:  # the prefix "directory" placeholder object, if any
                continue
            target = dest / name
            digest = pinned.get(name)
            unlisted.discard(name)
            # A locked file is skipped only when it is the pinned one. Any other
            # is skipped when a same-size file is present: published artifacts
            # are server-side immutable (cannobserv#215), nothing installs it,
            # and it meets its pin when a lock names it.
            if target.exists() and (
                sha256(target) == digest if digest else target.stat().st_size == blob.size
            ):
                skipped += 1
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            # Download to a sibling temp file then atomically rename, so an
            # interrupted run never leaves a partial wheel in place (which a
            # concurrent reader, or a same-size coincidence, could mistake for
            # a complete one). os.replace is atomic within the same directory.
            fd, tmp = tempfile.mkstemp(dir=target.parent, prefix=f".{target.name}.", suffix=".part")
            os.close(fd)
            try:
                blob.download_to_filename(tmp)
                # Refused before it replaces anything: a planted copy stays as
                # found, for whoever reads why, and deploy.sh refuses it too.
                if digest and (got := sha256(tmp)) != digest:
                    raise ValueError(
                        f"{name} downloads with sha256 {got}, not the {digest}"
                        f" pyproject.toml's [tool.broker.{TABLE}] pins"
                    )
                os.replace(tmp, target)
            finally:
                Path(tmp).unlink(missing_ok=True)
            downloaded += 1
        # A locked file the loop never reached was held to nothing: a copy here
        # under that name would pass to uv sync unchecked.
        if unlisted:
            raise ValueError(
                f"uv.lock names {', '.join(sorted(unlisted))}, which the bucket does not list"
            )
    except Exception as exc:  # broad by design: auth/network/bucket failures degrade identically
        print(f"error: could not sync gs://{BUCKET}/{PREFIX}: {exc}", file=sys.stderr)
        return 1

    print(f"wheelhouse in sync: {downloaded} downloaded, {skipped} already present -> {dest}")
    return 0


if __name__ == "__main__":
    sys.exit(sync())
