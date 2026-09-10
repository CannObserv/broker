"""Fakes for the object-store client, shared by the backup and restore tests.

**They owe their fidelity to the SDK, not to the tests** - the lesson
CannObserv/replicator recorded on its own copy (replicator#7 CR #1, #2): a fake
that raised where the real client returned ``False`` let a preflight look tested
while unable to see the one misconfiguration it existed for. So every method
here does what ``google-cloud-storage`` does, and where the real behaviour is
surprising it is commented rather than smoothed over.

Three calls this repo makes, and nothing it does not:

- ``Blob.upload_from_filename(..., if_generation_match=0)`` - a create, never a
  put; ``PreconditionFailed`` when the object already exists.
- ``Client.list_blobs(bucket, prefix=, max_results=)`` - lazy; the request
  happens on iteration, and a missing bucket raises ``NotFound`` there.
- ``Blob.download_to_filename`` - ``NotFound`` for an absent object.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

from google.api_core.exceptions import NotFound, PreconditionFailed


class FakeBlob:
    """Just enough ``google.cloud.storage.Blob`` to answer this repo's calls."""

    def __init__(self, bucket: FakeBucket, name: str) -> None:
        self._bucket = bucket
        self.name = name
        self.metadata: dict[str, str] | None = None
        self.content_type: str | None = None
        self.size: int | None = None

    def upload_from_filename(
        self,
        filename: str,
        content_type: str | None = None,
        if_generation_match: int | None = None,
        timeout: float | None = None,
    ) -> None:
        if if_generation_match == 0 and self.name in self._bucket.objects:
            # The precondition is evaluated against the object's generation
            # before anything is written, which is what lets an identity holding
            # no ``storage.objects.delete`` still receive a 412 here rather than
            # a 403 - the property CannObserv/replicator's T4 rests on.
            raise PreconditionFailed("object already exists")
        self._bucket.objects[self.name] = Path(filename).read_bytes()
        self._bucket.content_types[self.name] = content_type
        # Metadata rides the upload as object metadata, not a second call.
        self._bucket.metadata[self.name] = dict(self.metadata or {})
        self._bucket.preconditions.append(if_generation_match)
        self._bucket.timeouts.append(timeout)

    def download_to_filename(self, filename: str, timeout: float | None = None) -> None:
        if self.name not in self._bucket.objects:
            raise NotFound("no such object")
        Path(filename).write_bytes(self._bucket.objects[self.name])


class FakeBucket:
    def __init__(self, name: str) -> None:
        self.name = name
        self.objects: dict[str, bytes] = {}
        self.content_types: dict[str, str | None] = {}
        self.metadata: dict[str, dict[str, str]] = {}
        self.preconditions: list[int | None] = []
        self.timeouts: list[float | None] = []

    def blob(self, name: str) -> FakeBlob:
        return FakeBlob(self, name)


class FakeClient:
    """A client over one bucket, with the listing the preflight probes through."""

    def __init__(self, bucket: FakeBucket | None = None, *, missing: bool = False) -> None:
        self._bucket = bucket if bucket is not None else FakeBucket("a-backup-bucket")
        # "this bucket is not there" - the state a misspelled
        # BROKER_BACKUP_BUCKET puts the job in, and the one the listing
        # preflight exists to report.
        self._missing = missing
        self.listings: list[dict] = []

    def bucket(self, name: str) -> FakeBucket:
        assert name == self._bucket.name, f"unexpected bucket {name!r}"
        return self._bucket

    def list_blobs(
        self,
        bucket_or_name: str | FakeBucket,
        max_results: int | None = None,
        prefix: str | None = None,
        timeout: float | None = None,
    ) -> Iterator[FakeBlob]:
        """Lazy, like the real one - the request happens when it is iterated."""
        name = bucket_or_name if isinstance(bucket_or_name, str) else bucket_or_name.name
        assert name == self._bucket.name, f"unexpected bucket {name!r}"
        self.listings.append({"max_results": max_results, "prefix": prefix, "timeout": timeout})

        def _iter() -> Iterator[FakeBlob]:
            if self._missing:
                raise NotFound(f"bucket {name} not found")
            names = sorted(k for k in self._bucket.objects if not prefix or k.startswith(prefix))
            if max_results is not None:
                names = names[:max_results]
            for key in names:
                # A listing returns full object resources, metadata included -
                # which is what lets `restore --list` describe snapshots from a
                # node that has no gcloud and cannot read bucket metadata.
                blob = self._bucket.blob(key)
                blob.metadata = dict(self._bucket.metadata.get(key, {})) or None
                blob.size = len(self._bucket.objects[key])
                yield blob

        return _iter()
