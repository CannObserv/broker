"""Mirror the private cannobserv package index into the local wheelhouse.

Downloads every object under ``gs://co-gcs-pypi/wheels/`` into ``./.wheelhouse``
(repo root), skipping any file already present with a matching size. ``uv``
then resolves ``co-core`` / ``co-core-aio`` from that directory via the
``[tool.uv] find-links`` entry in ``pyproject.toml``. Lifted from
CannObserv/archiver (archiver#72, #75).

Runs standalone, *before* ``uv sync`` - it must not import the project (whose
deps are what the wheelhouse provides), so invoke it in an isolated env:

    uv run --no-project --with 'google-cloud-storage>=2,<4' python scripts/sync_wheelhouse.py

Authentication is Application Default Credentials. On the VM/deploy that is the
service-account key at ``GOOGLE_APPLICATION_CREDENTIALS`` (set in
``/etc/broker/.env``); in CI it is the ADC file written by
``google-github-actions/auth`` (keyless Workload Identity Federation). Either
way the identity needs only ``roles/storage.objectViewer`` on the bucket.

Exit codes: ``0`` success (including a no-op re-run) · ``1`` failure (auth,
network, or a missing bucket). The unit runs this as a non-fatal
``ExecStartPre`` (``-`` prefix): a transient failure is surfaced to the journal,
and if the wheelhouse is already populated the service still starts - only a
genuinely missing wheel surfaces later as a hard ``uv`` resolution error.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

from google.cloud import storage

BUCKET = os.environ.get("BROKER_WHEELHOUSE_BUCKET", "co-gcs-pypi")
PREFIX = os.environ.get("BROKER_WHEELHOUSE_PREFIX", "wheels/")
DEST = Path(__file__).resolve().parent.parent / ".wheelhouse"


def sync() -> int:
    """Mirror ``gs://{BUCKET}/{PREFIX}`` into ``DEST``; return an exit code."""
    DEST.mkdir(parents=True, exist_ok=True)
    downloaded = skipped = 0
    try:
        client = storage.Client()
        for blob in client.list_blobs(BUCKET, prefix=PREFIX):
            name = blob.name.removeprefix(PREFIX)
            if not name:  # the prefix "directory" placeholder object, if any
                continue
            target = DEST / name
            # Skip when a same-size file is already present. Published artifacts
            # are server-side immutable (cannobserv#215), so name + size is
            # sufficient; no need to fetch and compare the crc32c.
            if target.exists() and target.stat().st_size == blob.size:
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
                os.replace(tmp, target)
            finally:
                Path(tmp).unlink(missing_ok=True)
            downloaded += 1
    except Exception as exc:  # broad by design: auth/network/bucket failures degrade identically
        print(f"error: could not sync gs://{BUCKET}/{PREFIX}: {exc}", file=sys.stderr)
        return 1

    print(f"wheelhouse in sync: {downloaded} downloaded, {skipped} already present -> {DEST}")
    return 0


if __name__ == "__main__":
    sys.exit(sync())
