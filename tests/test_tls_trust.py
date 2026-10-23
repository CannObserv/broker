"""The GCS clients' TLS trust is certifi's bundle; it must not trust emSign ECC G3 (#124).

The backup, the restore and the wheelhouse sync reach GCS through ``google-auth`` /
``google-cloud-storage`` over ``requests``, which verify against ``certifi.where()``,
not the system store, so exe.dev's system-store remediation of the eMudhra
compromise doesn't reach them (https://exe.dev/security/emudhra-compromise).
certifi 2026.10.19 drops ``emSign ECC Root CA - G3``, the revoked intermediates'
root, and keeps three emSign roots it judged unaffected (CannObserv/provisioner#77's
correction): so the test is "no G3", never "no emSign". ``[tool.uv]
constraint-dependencies`` holds the floor through a re-lock. Modelled on
CannObserv/processor#66.
"""

import ssl
import tomllib
from pathlib import Path

import certifi

ROOT = Path(__file__).resolve().parent.parent
FLOOR = (2026, 10, 19)
DISTRUSTED = "emSign ECC Root CA - G3"


def _dated(version: str) -> tuple[int, ...]:
    # The lock normalises 2026.07.22 to 2026.7.22; compare parts, not strings.
    return tuple(int(part) for part in version.split("."))


def _bundle_common_names() -> set[str]:
    # The certificates themselves, not cacert.pem's comment lines.
    certs = ssl.create_default_context(cafile=certifi.where()).get_ca_certs()
    return {dict(rdn[0] for rdn in c["subject"]).get("commonName", "") for c in certs}


def test_dated_compares_parts() -> None:
    assert _dated("2026.7.22") == _dated("2026.07.22") < FLOOR <= _dated("2026.10.19")


def test_the_floor_is_a_constraint_a_relock_keeps() -> None:
    uv = tomllib.loads((ROOT / "pyproject.toml").read_text())["tool"]["uv"]
    assert f"certifi>={'.'.join(map(str, FLOOR))}" in uv.get("constraint-dependencies", [])


def test_lock_resolves_certifi_at_the_floor() -> None:
    lock = tomllib.loads((ROOT / "uv.lock").read_text())
    (locked,) = [p["version"] for p in lock["package"] if p["name"] == "certifi"]
    assert _dated(locked) >= FLOOR, locked


def test_installed_certifi_is_at_the_floor() -> None:
    assert _dated(certifi.__version__) >= FLOOR, certifi.__version__


def test_the_bundle_does_not_trust_emsign_ecc_g3() -> None:
    names = _bundle_common_names()
    # Control: the matcher sees eMudhra's roots, and one 2026.10.19 keeps.
    assert "emSign Root CA - G1" in names
    assert DISTRUSTED not in names
