"""scripts/sync_wheelhouse.py and the digests it holds a locked file to (#125).

A file ``uv.lock`` names from the wheelhouse is what a release installs, and
uv records no digest for one: a find-links or flat-index path source locks by
file name alone, and a hand-added ``hash`` goes at the next ``uv lock``. So
pyproject.toml's ``[tool.broker.wheelhouse-sha256]`` carries one per file, and
the sync holds a locked file to it: a same-size file with other bytes is
fetched again, and a download that does not match is refused, not installed.

The real pin is checked here too: every wheelhouse file the lock names has a
digest, in the one-line form ``scripts/deploy.sh`` reads, and no digest
outlives its file. A pin change and its digest are one reviewed diff.
"""

import hashlib
import importlib.util
import re
import tomllib
from pathlib import Path

import pytest

from tests.gcs_fakes import FakeBlob, FakeBucket, FakeClient

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "sync_wheelhouse.py"
TABLE = "wheelhouse-sha256"

_spec = importlib.util.spec_from_file_location("sync_wheelhouse", SCRIPT)
sync_wheelhouse = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sync_wheelhouse)

WHEEL = "co_core-9.9.0-py3-none-any.whl"
SDIST = "co_core-9.9.0.tar.gz"
OTHER = "other-1.0-py3-none-any.whl"
GOOD = {WHEEL: b"the wheel", SDIST: b"the sdist", OTHER: b"other"}


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def lock(*, source: str = ".wheelhouse") -> str:
    return f"""version = 1

[[package]]
name = "co-core"
version = "9.9.0"
source = {{ registry = "{source}" }}
sdist = {{ path = "{SDIST}" }}
wheels = [
    {{ path = "{WHEEL}" }},
]

[[package]]
name = "pydantic"
version = "2.12.0"
source = {{ registry = "https://pypi.org/simple" }}
sdist = {{ url = "https://files.example/pydantic-2.12.0.tar.gz", hash = "sha256:00" }}
"""


def pyproject(digests: dict[str, str]) -> str:
    rows = "".join(f'"{name}" = "{value}"\n' for name, value in digests.items())
    return f'[project]\nname = "t"\n\n[tool.broker.{TABLE}]\n{rows}'


class Node:
    """A checkout's root, its wheelhouse, and the bucket the sync mirrors."""

    def __init__(self, tmp: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self.project = tmp
        self.wheelhouse = tmp / ".wheelhouse"
        self.bucket = FakeBucket("co-gcs-pypi")
        for name, data in GOOD.items():
            self.bucket.objects[f"wheels/{name}"] = data
        self.downloads: list[str] = []
        (tmp / "uv.lock").write_text(lock())
        self.pin({WHEEL: sha(GOOD[WHEEL]), SDIST: sha(GOOD[SDIST])})
        client = FakeClient(self.bucket)
        real = FakeBlob.download_to_filename
        node = self

        def download(blob, filename, timeout=None):
            node.downloads.append(blob.name.removeprefix("wheels/"))
            real(blob, filename, timeout=timeout)

        monkeypatch.setattr(FakeBlob, "download_to_filename", download)
        monkeypatch.setattr(sync_wheelhouse.storage, "Client", lambda: client)
        monkeypatch.setattr(sync_wheelhouse, "PROJECT", tmp)

    def pin(self, digests: dict[str, str]) -> None:
        (self.project / "pyproject.toml").write_text(pyproject(digests))

    def plant(self, name: str, data: bytes) -> None:
        self.wheelhouse.mkdir(exist_ok=True)
        (self.wheelhouse / name).write_bytes(data)

    def sync(self, capsys) -> tuple[int, str]:
        rc = sync_wheelhouse.sync()
        return rc, capsys.readouterr().err


@pytest.fixture
def node(tmp_path, monkeypatch):
    return Node(tmp_path, monkeypatch)


class TestALockedFile:
    def test_a_same_size_wheel_with_other_bytes_is_fetched_again(self, node, capsys):
        """The swap a size-only skip kept forever: same name, same size."""
        planted = b"the WHEEL"
        assert len(planted) == len(GOOD[WHEEL])
        node.plant(WHEEL, planted)
        rc, err = node.sync(capsys)
        assert rc == 0, err
        assert (node.wheelhouse / WHEEL).read_bytes() == GOOD[WHEEL]
        assert WHEEL in node.downloads

    def test_a_download_that_is_not_the_pinned_file_is_refused(self, node, capsys):
        """What a spoofed channel serves: its own wheel, and a checksum to match."""
        node.bucket.objects[f"wheels/{WHEEL}"] = b"the WHEEL"
        rc, err = node.sync(capsys)
        assert rc == 1
        assert WHEEL in err
        assert sha(b"the WHEEL") in err and sha(GOOD[WHEEL]) in err
        assert not (node.wheelhouse / WHEEL).exists()
        assert not list(node.wheelhouse.glob("*.part"))

    def test_a_planted_file_is_left_as_found_when_the_refetch_misses_too(self, node, capsys):
        """Evidence for whoever reads why; deploy.sh refuses it either way."""
        node.plant(WHEEL, b"the wheeL")
        node.bucket.objects[f"wheels/{WHEEL}"] = b"the WHEEL"
        rc, err = node.sync(capsys)
        assert rc == 1
        assert WHEEL in err
        assert (node.wheelhouse / WHEEL).read_bytes() == b"the wheeL"

    def test_a_locked_file_the_bucket_does_not_list_fails_the_sync(self, node, capsys):
        """CR 1: the loop visits what the listing names, so a copy planted under a
        name the bucket lacks was never held to its pin."""
        del node.bucket.objects[f"wheels/{WHEEL}"]
        node.plant(WHEEL, b"the WHEEL")
        rc, err = node.sync(capsys)
        assert rc == 1
        assert WHEEL in err and "does not list" in err

    def test_a_file_that_matches_its_pin_is_not_fetched_again(self, node, capsys):
        node.plant(WHEEL, GOOD[WHEEL])
        node.plant(SDIST, GOOD[SDIST])
        rc, err = node.sync(capsys)
        assert rc == 0, err
        assert WHEEL not in node.downloads and SDIST not in node.downloads

    def test_the_sdist_is_held_to_its_pin_as_well(self, node, capsys):
        """deploy.sh copies it into the release too."""
        node.plant(SDIST, b"the SDIST")
        rc, err = node.sync(capsys)
        assert rc == 0, err
        assert (node.wheelhouse / SDIST).read_bytes() == GOOD[SDIST]

    def test_a_locked_file_with_no_digest_fails_the_sync(self, node, capsys):
        node.pin({WHEEL: sha(GOOD[WHEEL])})
        rc, err = node.sync(capsys)
        assert rc == 1
        assert SDIST in err and TABLE in err
        assert "sha256sum .wheelhouse/" in err, "CR 3: how to make the pin"

    @pytest.mark.parametrize("value", ["", "abc123", "A" * 64])
    def test_a_pin_that_is_not_a_sha256_fails_the_sync(self, node, capsys, value):
        """CR 5: a blank pin was falsy, so the file fell back to the size skip."""
        node.pin({WHEEL: value, SDIST: sha(GOOD[SDIST])})
        node.plant(WHEEL, b"the WHEEL")
        rc, err = node.sync(capsys)
        assert rc == 1
        assert WHEEL in err and "not a sha256" in err
        assert not node.downloads

    def test_a_lock_that_spells_the_wheelhouse_as_pyproject_does_is_still_read(self, node, capsys):
        """deploy.sh's locked_wheels() takes ./.wheelhouse too (#106 CR 12)."""
        (node.project / "uv.lock").write_text(lock(source="./.wheelhouse"))
        node.plant(WHEEL, b"the WHEEL")
        rc, err = node.sync(capsys)
        assert rc == 0, err
        assert (node.wheelhouse / WHEEL).read_bytes() == GOOD[WHEEL]


class TestAFileTheLockDoesNotName:
    def test_a_same_size_file_is_still_skipped(self, node, capsys):
        """Nothing installs it: the bucket holds every co-core ever published,
        and only the locked ones reach a release. It meets its pin, if it ever
        gets one, when a lock names it."""
        node.plant(OTHER, b"OTHER")
        rc, err = node.sync(capsys)
        assert rc == 0, err
        assert OTHER not in node.downloads
        assert (node.wheelhouse / OTHER).read_bytes() == b"OTHER"


def real_locked_files() -> set[str]:
    data = tomllib.loads((REPO_ROOT / "uv.lock").read_text())
    return set(sync_wheelhouse.locked_files(data))


def real_pins() -> dict[str, str]:
    data = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())
    return data["tool"]["broker"][TABLE]


class TestTheRealPin:
    def test_the_lock_names_co_core_from_the_wheelhouse(self):
        assert any(name.startswith("co_core-") for name in real_locked_files())

    def test_every_locked_wheelhouse_file_has_a_digest_and_no_digest_outlives_one(self):
        assert set(real_pins()) == real_locked_files()

    def test_each_digest_is_a_sha256(self):
        for name, value in real_pins().items():
            assert re.fullmatch(r"[0-9a-f]{64}", value), name

    def test_the_table_is_in_the_form_deploy_sh_reads(self):
        """deploy.sh reads the table with awk, a line at a time: one quoted name,
        one quoted digest. tomllib takes more, so the form is held here."""
        text = (REPO_ROOT / "pyproject.toml").read_text()
        section = re.search(rf"^\[tool\.broker\.{TABLE}\]\n(.*?)(?=^\[|\Z)", text, re.M | re.S)
        assert section, f"[tool.broker.{TABLE}] is not a table header of its own"
        rows = {}
        for line in section.group(1).splitlines():
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            match = re.fullmatch(r'"([^"/]+)" = "([0-9a-f]{64})"', line)
            assert match, f"not a line deploy.sh reads: {line!r}"
            rows[match.group(1)] = match.group(2)
        assert rows == real_pins()

    def test_the_synced_files_are_the_pinned_ones(self):
        """Where the wheelhouse is synced: the node, and CI after its sync step."""
        wheelhouse = REPO_ROOT / ".wheelhouse"
        present = {n: wheelhouse / n for n in real_pins() if (wheelhouse / n).is_file()}
        if not present:
            pytest.skip("no synced wheelhouse here")
        for name, path in present.items():
            assert sha(path.read_bytes()) == real_pins()[name], name
