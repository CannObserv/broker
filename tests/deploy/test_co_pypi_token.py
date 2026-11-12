"""The co-pypi wrapper (#144): one key, an allowlisted environment, and a token
that never reaches output.

Ported from CannObserv/replicator#183's tests with processor#121's default key
path. The node's ``/etc/broker/.env`` sets ``GOOGLE_APPLICATION_CREDENTIALS``
shell-wide for whoever sources it, and ``~/broker/.env`` carries org-wide
``GH_TOKEN_*`` PATs, so neither may reach the wrapped ``uv``: not merely "not be
read". No network: the token exchange is faked.
"""

import importlib.util
import json
import os
import re
import stat
import subprocess
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

REPO = Path(__file__).resolve().parents[2]
WRAPPER = REPO / "scripts" / "co-pypi.sh"
MODULE = REPO / "scripts" / "co_pypi_token.py"
TOKEN = "ya29.SECRET-TOKEN-VALUE"

_spec = importlib.util.spec_from_file_location("co_pypi_token", MODULE)
cp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cp)

READER = cp.DEFAULT_KEY
# What a shell that sourced both env files holds, abridged.
LOADED = {
    "PATH": "/usr/local/bin:/usr/bin:/bin",
    "HOME": "/home/exedev",
    "LANG": "C.UTF-8",
    "LC_ALL": "C.UTF-8",
    "XDG_CACHE_HOME": "/home/exedev/.cache",
    "UV_CACHE_DIR": "/home/exedev/.cache/uv",
    "UV_PYTHON_DOWNLOADS": "never",
    "SSL_CERT_FILE": "/etc/ssl/certs/ca-certificates.crt",
    "https_proxy": "http://proxy:3128",
    "VIRTUAL_ENV": "/home/exedev/broker/.venv",
    "GOOGLE_APPLICATION_CREDENTIALS": "/etc/broker/co-pypi-reader.json",
    "BROKER_REDIS_URL": "redis://brokeradmin@localhost:6379/0",
    "GH_TOKEN": "ghp_broker",
    "GH_TOKEN_ARCHIVER": "ghp_archiver",
    "REDISCLI_AUTH": "a-redis-password",
}


class Exec:
    """Records the exec instead of replacing the process."""

    def __init__(self, error: OSError | None = None):
        self.calls: list[tuple[str, list[str], dict[str, str]]] = []
        self.error = error

    def __call__(self, file, args, env):
        self.calls.append((file, args, env))
        if self.error:
            raise self.error


def _mint_from(seen: list[str]):
    def mint(key_path):
        seen.append(key_path)
        return TOKEN

    return mint


# --- the environment -------------------------------------------------------


def test_the_child_environment_is_the_allowlist():
    assert cp.child_environment(LOADED) == {
        k: LOADED[k]
        for k in (
            "PATH",
            "HOME",
            "LANG",
            "LC_ALL",
            "XDG_CACHE_HOME",
            "UV_CACHE_DIR",
            "UV_PYTHON_DOWNLOADS",
            "SSL_CERT_FILE",
            "https_proxy",
        )
    }


def test_no_credential_reaches_uv():
    execvpe = Exec()
    cp.main(["uv", "sync", "--locked"], LOADED, mint=lambda _: TOKEN, execvpe=execvpe)
    (_, _, env) = execvpe.calls[0]
    assert "GOOGLE_APPLICATION_CREDENTIALS" not in env
    assert not [k for k in env if k.startswith("GH_TOKEN")]
    assert "REDISCLI_AUTH" not in env and "BROKER_REDIS_URL" not in env
    assert "VIRTUAL_ENV" not in env, "a wrapped `uv sync` in a release would warn about it"
    assert cp.KEY_VAR not in env, "the key's path is the mint's input, not uv's"


def test_uv_gets_the_pair_and_its_arguments_unchanged():
    execvpe = Exec()
    assert cp.main(["uv", "lock"], LOADED, mint=lambda _: TOKEN, execvpe=execvpe) is None
    (file, args, env) = execvpe.calls[0]
    assert (file, args) == ("uv", ["uv", "lock"])
    assert env[cp.USERNAME_VAR] == "oauth2accesstoken"
    assert env[cp.PASSWORD_VAR] == TOKEN
    assert TOKEN not in " ".join(args)


# --- the key ---------------------------------------------------------------


def test_the_nodes_reader_key_by_default():
    seen: list[str] = []
    cp.main(["uv", "lock"], LOADED, mint=_mint_from(seen), execvpe=Exec())
    assert seen == ["/etc/broker/co-pypi-reader.json"]


def test_the_key_variable_overrides_the_default():
    seen: list[str] = []
    environ = {**LOADED, cp.KEY_VAR: "/elsewhere/reader.json"}
    cp.main(["uv", "lock"], environ, mint=_mint_from(seen), execvpe=Exec())
    assert seen == ["/elsewhere/reader.json"]


def test_a_blank_key_variable_is_the_default():
    seen: list[str] = []
    cp.main(["uv", "lock"], {**LOADED, cp.KEY_VAR: "  "}, mint=_mint_from(seen), execvpe=Exec())
    assert seen == [READER]


def test_never_the_shell_wide_adc_variable():
    seen: list[str] = []
    environ = {**LOADED, "GOOGLE_APPLICATION_CREDENTIALS": "/etc/broker/someone-else.json"}
    cp.main(["uv", "lock"], environ, mint=_mint_from(seen), execvpe=Exec())
    assert seen == [READER]


def _service_account_key(path: Path, email: str) -> str:
    """A throwaway service-account key file, real enough for google-auth to load."""
    pem = (
        rsa.generate_private_key(public_exponent=65537, key_size=2048)
        .private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        .decode()
    )
    key = {
        "type": "service_account",
        "project_id": "test",
        "private_key_id": "0",
        "private_key": pem,
        "client_email": email,
        "client_id": "0",
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    path.write_text(json.dumps(key))
    return str(path)


def test_the_mint_loads_the_named_key_and_never_runs_adc(monkeypatch, tmp_path):
    """The real loader runs, and only the token exchange is faked. Another key
    sits where ADC would look: the token is the named key's, and
    google.auth.default never runs."""
    reader = _service_account_key(tmp_path / "reader.json", "co-pypi-reader@test.iam")
    other = _service_account_key(tmp_path / "other.json", "other@test.iam")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", other)
    minted_for: list[tuple[str, tuple]] = []

    def refresh(self, request):
        minted_for.append((self.service_account_email, tuple(self.scopes)))
        self.token = TOKEN

    def adc(*a, **k):
        raise AssertionError("google.auth.default() must not run")

    monkeypatch.setattr(cp.service_account.Credentials, "refresh", refresh)
    monkeypatch.setattr("google.auth.default", adc)
    assert cp.mint_token(reader) == TOKEN
    assert minted_for == [("co-pypi-reader@test.iam", tuple(cp.SCOPES))]


def test_a_key_that_is_not_a_key_fails_the_mint(tmp_path, capsys):
    bad = tmp_path / "not-a-key.json"
    bad.write_text("{}")
    execvpe = Exec()
    assert cp.main(["uv", "lock"], {**LOADED, cp.KEY_VAR: str(bad)}, execvpe=execvpe) == 1
    assert execvpe.calls == []
    assert str(bad) in capsys.readouterr().err


# --- refusals and failures -------------------------------------------------


@pytest.mark.parametrize("argv", [[], ["pip", "install"], ["bash", "-c", "env"], ["/bin/uvx"]])
def test_anything_but_uv_is_refused_before_a_mint(argv, capsys):
    seen: list[str] = []
    assert cp.main(argv, LOADED, mint=_mint_from(seen), execvpe=Exec()) == 2
    assert seen == []
    assert "usage" in capsys.readouterr().err


def test_a_uv_by_path_is_still_uv():
    execvpe = Exec()
    cp.main(["/usr/local/bin/uv", "sync"], LOADED, mint=lambda _: TOKEN, execvpe=execvpe)
    assert execvpe.calls[0][0] == "/usr/local/bin/uv"


def test_a_failed_mint_runs_nothing_and_prints_no_token(capsys):
    def mint(_):
        raise RuntimeError("invalid_grant: account not found")

    execvpe = Exec()
    assert cp.main(["uv", "lock"], LOADED, mint=mint, execvpe=execvpe) == 1
    out = capsys.readouterr()
    assert execvpe.calls == []
    assert "invalid_grant" in out.err and READER in out.err
    assert TOKEN not in out.out + out.err


def test_a_failed_exec_names_the_command_and_prints_no_token(capsys):
    execvpe = Exec(FileNotFoundError(2, "No such file or directory"))
    assert cp.main(["uv", "lock"], LOADED, mint=lambda _: TOKEN, execvpe=execvpe) == 127
    out = capsys.readouterr()
    assert "uv" in out.err
    assert TOKEN not in out.out + out.err


# --- the shell wrapper ------------------------------------------------------


@pytest.fixture
def fake_uv(tmp_path):
    """A `uv` first on PATH that records its argv and environment."""
    bindir = tmp_path / "bin"
    bindir.mkdir()
    record = tmp_path / "record"
    uv = bindir / "uv"
    uv.write_text(f'#!/bin/bash\nprintf "%s\\n" "$@" > {record}.argv\nenv > {record}.env\n')
    uv.chmod(uv.stat().st_mode | stat.S_IXUSR)
    return bindir, record


def test_the_shell_wrapper_runs_the_locked_group_from_the_allowlist(fake_uv):
    bindir, record = fake_uv
    environ = {**LOADED, cp.KEY_VAR: READER, "PATH": f"{bindir}:{os.environ['PATH']}"}
    subprocess.run(["bash", str(WRAPPER), "uv", "sync", "--locked"], env=environ, check=True)
    env = dict(
        line.split("=", 1) for line in Path(f"{record}.env").read_text().splitlines() if "=" in line
    )
    argv = Path(f"{record}.argv").read_text().splitlines()
    assert "GOOGLE_APPLICATION_CREDENTIALS" not in env
    assert not [k for k in env if k.startswith("GH_TOKEN")]
    assert "REDISCLI_AUTH" not in env and "VIRTUAL_ENV" not in env
    assert env[cp.KEY_VAR] == READER, "the mint's one input"
    # This checkout's lock, its one group, never resolved at run time (#113).
    assert argv[:6] == ["run", "--project", str(REPO), "--frozen", "--only-group", "co-pypi"]
    assert argv[6:] == ["python", str(MODULE), "uv", "sync", "--locked"]


def test_the_shell_wrapper_allowlist_matches_the_python_one():
    """Two layers, one list: a variable added to one and not the other would
    reach uv through the gap."""
    text = WRAPPER.read_text()

    def array(name: str) -> set[str]:
        m = re.search(rf"^{name}=\(([^)]*)\)", text, flags=re.MULTILINE)
        assert m, f"{name}=(...) not found in {WRAPPER.name}: the derivation broke"
        return set(m.group(1).split())

    # The shell layer also carries the key variable: it is the mint's input.
    assert array("ALLOWED") == set(cp.ALLOWED) | {cp.KEY_VAR}
    assert array("PREFIXES") == set(cp.ALLOWED_PREFIXES)


def test_the_wrapper_is_executable():
    assert os.access(WRAPPER, os.X_OK)
