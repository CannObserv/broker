"""``cred``/``rcli`` load from one copy that cannot run anything else (#132).

The only canonical copy of the two read-only helpers sat inside
``docs/NODE-CREDENTIALS.md``'s rotation, directly ahead of its first write, and
every recorded recipe extracted it by pattern from ``cred() {`` - after the
block's ``set -euo pipefail`` and ``u=``. On 2026-10-30T00:26:14Z (#128) an
improvised range ran past ``rcli()`` and ``eval`` ran the rotation in the
caller's shell with ``$u`` empty: ACL user ``""`` created and saved, two
``.new`` files written as root, one ``acladmin`` AUTH denial.

Two changes, each pinned here:

- **The guard.** The rotation refuses any ``u`` but the two node users, after
  the helpers and before ``C=``: a check beside ``u=`` is skipped by the same
  extraction. The rehearsal replays the over-wide range under a ``sudo`` and a
  ``redis-cli`` that only record their argv, so no Redis and no root is touched.
  It stops at the closing fence: past it bash would parse prose.
- **The loading path.** *Loading the helpers*, the doc's last section, holds
  ``cred`` and ``rcli`` between two sentinel lines; the recipe there starts on
  the first, quits on the second, and an overrun past it reaches the end of the
  file. The runbooks keep their own copies, so each stays copy-paste whole on a
  rebuilt node, and each is held byte-identical to the canonical one.
"""

from __future__ import annotations

import hashlib
import os
import re
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
NODE_CREDENTIALS = REPO_ROOT / "docs" / "NODE-CREDENTIALS.md"

BEGIN = "# broker-helpers: begin"
END = "# broker-helpers: end"
GUARD = 'case "${u:-}" in acladmin|brokeradmin)'

#: Every file carrying the helpers, and how many copies: a new copy turns
#: ``test_every_copy_is_known`` red until it is held to the canonical one.
#: RESTART-WINDOW's sits in a ``> `` blockquote.
COPIES = {
    "docs/NODE-CREDENTIALS.md": 2,  # the canonical block, and the rotation's own
    "docs/RECOVERY.md": 1,
    "docs/RESTART-WINDOW.md": 1,
    "deploy/README.md": 1,
}

#: What a stub records for a command that changes something.
_WRITE = re.compile(
    r"ACL SETUSER|ACL SAVE|systemd-creds encrypt|^sudo (?:-n )?(?:install|cp|mv|chmod|rm|tee)\b",
    re.MULTILINE,
)
#: The same, on the rotation's own source lines.
_WRITE_SOURCE = re.compile(
    r"ACL SETUSER|ACL SAVE|systemd-creds encrypt|\bsudo (?:-n )?(?:install|cp|mv|chmod|rm|tee)\b"
)
_HELPER_START = re.compile(r"^(?:> )?cred\(\) \{", re.MULTILINE)
_HELPER_LAST = '"$@"; }'
_RECIPE = re.compile(r"""^h="\$\((sed -n '[^']*' docs/NODE-CREDENTIALS\.md)\)"$""", re.MULTILINE)

#: The value the stubs' ``systemd-creds decrypt`` returns for a credential no
#: stub encrypted. Not a password anywhere; asserted off every recorded argv.
STUB_CREDENTIAL = "stub-credential-not-a-password"

_SUDO = r"""#!/usr/bin/env bash
# Records argv; does what root would to a scratch state dir, never to /etc.
printf 'sudo %s\n' "$*" >> "$STUB_LOG"
[ "$1" = -n ] && shift
key() { printf '%s' "$1" | tr / _; }
case "$1" in
  systemd-creds)
    if [ "$2" = encrypt ]; then cat > "$STUB_STATE/$(key "${*: -1}")"
    else f="$STUB_STATE/$(key "${*: -2:1}")"
         if [ -f "$f" ]; then cat "$f"; else printf %s "$STUB_CREDENTIAL"; fi
    fi ;;
  cat) printf '__ACLADMIN_PW_SHA256__=%064d\n__BROKERADMIN_PW_SHA256__=%064d\n' 0 0 ;;
  install) cat > "$STUB_STATE/$(key "${*: -1}")" ;;
  grep) shift; f="${*: -1}"; set -- "${@:1:$#-1}" "$STUB_STATE/$(key "$f")"; exec grep "$@" ;;
esac
exit 0
"""

_REDIS_CLI = r"""#!/usr/bin/env bash
# Records argv; keeps one user's password digests, enough for the rotation's checks.
printf 'redis-cli %s\n' "$*" >> "$STUB_LOG"
h="$STUB_STATE/hashes"
case "$*" in
  *" ACL SETUSER "*)
    r="${*: -1}"
    case "$r" in
      "#"*) printf '%s\n' "${r#\#}" >> "$h" ;;
      "!"*) grep -vx "${r#!}" "$h" > "$h.tmp"; mv "$h.tmp" "$h" ;;
    esac
    echo OK ;;
  *" ACL GETUSER "*) cat "$h" ;;
  *" ACL SAVE") echo OK ;;
esac
"""


def _text() -> str:
    return NODE_CREDENTIALS.read_text()


def _fenced_blocks(text: str) -> list[str]:
    return re.findall(r"^```bash\n(.*?)^```$", text, re.MULTILINE | re.DOTALL)


def _rotation() -> list[str]:
    (block,) = [b for b in _fenced_blocks(_text()) if re.search(r"^u=acladmin\b", b, re.M)]
    return block.splitlines()


def _helper_spans(text: str) -> list[str]:
    """Each copy of the helpers, ``cred() {`` through ``rcli()``'s last line."""
    spans = []
    for m in _HELPER_START.finditer(text):
        end = text.index(_HELPER_LAST, m.start()) + len(_HELPER_LAST)
        span = text[m.start() : end]
        spans.append(re.sub(r"^> ?", "", span, flags=re.MULTILINE))
    return spans


def _canonical() -> str:
    lines = _text().splitlines(keepends=True)
    start = lines.index(BEGIN + "\n")
    return "".join(lines[start : lines.index(END + "\n", start) + 1])


def _recipe() -> str:
    """The sed command the doc's recipe evals, as the doc states it."""
    found = _RECIPE.findall(_text())
    assert len(found) == 1, f"NODE-CREDENTIALS.md states the recipe {len(found)} times"
    return found[0]


@pytest.fixture
def stubs(tmp_path: Path) -> tuple[dict[str, str], Path]:
    """A ``sudo`` and a ``redis-cli`` first on ``PATH``; the environment and argv log."""
    bin_dir, state = tmp_path / "bin", tmp_path / "state"
    bin_dir.mkdir()
    state.mkdir()
    for name, body in (("sudo", _SUDO), ("redis-cli", _REDIS_CLI)):
        (bin_dir / name).write_text(body)
        (bin_dir / name).chmod(0o755)
    old = hashlib.sha256(STUB_CREDENTIAL.encode()).hexdigest()
    (state / "hashes").write_text(old + "\n")
    log = tmp_path / "argv"
    log.touch()
    env = {k: v for k, v in os.environ.items() if k != "u"} | {
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "STUB_LOG": str(log),
        "STUB_STATE": str(state),
        "STUB_CREDENTIAL": STUB_CREDENTIAL,
    }
    return env, log


def _bash(script: str, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", "-c", script],
        cwd=REPO_ROOT,
        env=env,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=60,
    )


class TestTheRehearsal:
    """The #128 failure, replayed: the rotation from ``cred() {`` on, ``eval``'d."""

    @staticmethod
    def _over_wide_range() -> str:
        lines = _rotation()
        start = next(i for i, line in enumerate(lines) if line.startswith("cred() {"))
        return "\n".join(lines[start:]) + "\n"

    @pytest.mark.parametrize("u", [None, "", "acladmn"], ids=["unset", "empty", "typo"])
    def test_an_extraction_past_rcli_stops_before_any_write(self, stubs, tmp_path, u) -> None:
        env, log = stubs
        if u is not None:
            env["u"] = u
        fragment = tmp_path / "range"
        fragment.write_text(self._over_wide_range())
        ran = _bash(f'eval "$(cat {fragment})"', env)
        writes = _WRITE.findall(log.read_text())
        assert not writes, f"u={u!r}: the range reached {writes}\n{log.read_text()}"
        assert ran.returncode != 0, "the guard exits, ending the caller's shell"
        assert "acladmin or brokeradmin" in ran.stderr

    def test_with_u_set_it_still_adds_before_it_retires(self, stubs, tmp_path) -> None:
        """The positive control: run as documented, ``bash rotate.sh``, the guard
        lets ``acladmin`` through and every write lands in the original order."""
        env, log = stubs
        script = tmp_path / "rotate.sh"
        script.write_text("\n".join(_rotation()) + "\n")
        ran = subprocess.run(
            ["bash", str(script)], env=env, stdin=subprocess.DEVNULL,
            capture_output=True, text=True, timeout=60,
        )  # fmt: skip
        assert ran.returncode == 0, ran.stderr
        recorded = log.read_text()
        writes = [line for line in recorded.splitlines() if _WRITE.search(line)]
        steps = [
            r"ACL SETUSER acladmin #[0-9a-f]{64}$",
            r"ACL SAVE$",
            r"systemd-creds encrypt --name=broker-acladmin - \S+/broker-acladmin\.new$",
            r"^sudo chmod 0400 ",
            r"^sudo cp -p ",
            r"^sudo mv \S+/broker-acladmin\.new ",
            r"^sudo install .* /etc/redis/broker-acl-passwords\.new$",
            r"^sudo mv /etc/redis/broker-acl-passwords\.new /etc/redis/broker-acl-passwords$",
            r"ACL SETUSER acladmin ![0-9a-f]{64}$",
            r"ACL SAVE$",
            r"^sudo rm \S+/broker-acladmin\.old$",
        ]
        assert len(writes) == len(steps), "\n".join(writes)
        for line, step in zip(writes, steps, strict=True):
            assert re.search(step, line), f"{line!r} is not {step!r}"
        assert STUB_CREDENTIAL not in recorded, "a credential reached argv"


class TestTheGuard:
    def test_the_guard_sits_after_the_helpers_and_before_the_first_write(self) -> None:
        lines = _rotation()
        guards = [i for i, line in enumerate(lines) if GUARD in line]
        assert len(guards) == 1, f"the rotation has {len(guards)} guards"
        guard = guards[0]
        assert "exit 1" in lines[guard]
        assert "redis-cli --user" not in lines[guard], "it would end an rcli extraction early"
        helpers = [i for i, line in enumerate(lines) if re.match(r"\w+\(\) \{", line)]
        assert helpers and guard > max(helpers), "an extraction from cred() { skips it"
        assert not any("redis-cli --user" in line for line in lines[max(helpers) + 1 : guard])
        code = [(i, line) for i, line in enumerate(lines) if not line.lstrip().startswith("#")]
        first_c = next(i for i, line in code if line.startswith("C="))
        first_write = next(i for i, line in code if _WRITE_SOURCE.search(line))
        assert guard < first_c < first_write


class TestTheLoadingPath:
    def test_the_canonical_block_is_the_last_thing_in_the_file(self) -> None:
        """An overrun past the end sentinel reaches nothing but the closing fence."""
        lines = _text().splitlines()
        assert lines.count(BEGIN) == 1 and lines.count(END) == 1, "one block, one recipe"
        assert lines[lines.index(END) + 1 :] == ["```"]

    def test_the_recipe_extracts_exactly_the_canonical_block(self, stubs) -> None:
        env, _ = stubs
        extracted = _bash(_recipe(), env)
        assert extracted.returncode == 0, extracted.stderr
        assert extracted.stdout == _canonical()

    def test_the_recipe_runs_nothing_and_defines_cred_and_rcli(self, stubs) -> None:
        env, log = stubs
        loaded = _bash(f'h="$({_recipe()})"; eval "$h"; declare -F', env)
        assert loaded.returncode == 0, loaded.stderr
        assert log.read_text() == "", "loading ran a command"
        assert sorted(re.findall(r"^declare -f (\S+)$", loaded.stdout, re.M)) == [
            "cred",
            "rcli",
        ]

    def test_the_loaded_rcli_authenticates_off_argv(self, stubs) -> None:
        env, log = stubs
        ran = _bash(f'h="$({_recipe()})"; eval "$h"; rcli acladmin PING', env)
        assert ran.returncode == 0, ran.stderr
        assert log.read_text().splitlines() == [
            "sudo -n systemd-creds decrypt --name=broker-acladmin "
            "/etc/credstore.encrypted/broker-acladmin -",
            "redis-cli --user acladmin -h localhost -p 6379 PING",
        ]


class TestTheCopies:
    def test_every_copy_is_known(self) -> None:
        found = {}
        for path in [*(REPO_ROOT / "docs").glob("*.md"), *(REPO_ROOT / "deploy").glob("*.md")]:
            if n := len(_HELPER_START.findall(path.read_text())):
                found[str(path.relative_to(REPO_ROOT))] = n
        if n := len(_HELPER_START.findall((REPO_ROOT / "README.md").read_text())):
            found["README.md"] = n
        assert found == COPIES

    @pytest.mark.parametrize("name", sorted(COPIES))
    def test_each_copy_matches_the_canonical_one(self, name: str) -> None:
        (canonical,) = _helper_spans(_canonical())
        for span in _helper_spans((REPO_ROOT / name).read_text()):
            assert span == canonical, f"{name}'s helpers drifted from *Loading the helpers*"
