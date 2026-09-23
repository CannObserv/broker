"""``deploy/render-acl.sh`` emits a digest for every user, whichever spelling it is given.

The node's passwords file holds each credential either as the plaintext
(``__X_PW__=<value>``, hashed at render) or as the sha256 digest alone
(``__X_PW_SHA256__=<64-hex>``, passed through). The second exists because a
hash-only handoff leaves this node holding no plaintext for a service at all -
``archiver`` since CannObserv/archiver#251 - and until CannObserv/broker#49 the
render could not emit such a user, so the dry run in ``docs/ACL-CUTOVER.md``
step 2 and a rebuild's re-render both failed.

Digests for *every* user, not only the ones given as digests, because the
rendered file is then plaintext-free by construction, it matches the
``#<sha256>`` form ``ACL SAVE`` writes, and moving a user from one spelling to
the other is a passwords-file edit rather than a tracked-file one. It is also
what lets ``test_live_acl_matches_tracked_acl.py`` render the node's real file
and read the result back without a secret crossing into pytest.

Every refusal here is exercised for what it *prints* as well as its exit code:
the passwords file is the one input to this script that must never reach a
terminal, a log or a CI transcript.
"""

import hashlib
import re
import subprocess

import pytest

from tests.deploy.conftest import (
    ACL_FILE,
    DIGEST_PLACEHOLDERS,
    PASSWORD,
    RENDER_SCRIPT,
    parse_users,
)

PLACEHOLDERS = sorted(set(re.findall(r"__[A-Z]+_PW__", ACL_FILE.read_text())))

# What an empty value hashes to - a valid digest `ACL SETUSER` accepts, which
# is why it is refused by value (docs/ACL-CUTOVER.md, "Rotating __DEFAULT_PW__").
EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _plaintext(placeholder: str) -> str:
    """A distinct value per placeholder, so a digest landing on the wrong user shows."""
    return f"plain-{placeholder.strip('_').lower()}-0123456789"


def _digest_key(placeholder: str) -> str:
    return placeholder.removesuffix("__") + "_SHA256__"


def _render(tmp_path, lines: list[str]) -> subprocess.CompletedProcess:
    passwords = tmp_path / "passwords"
    passwords.write_text("".join(f"{line}\n" for line in lines))
    return subprocess.run(
        [str(RENDER_SCRIPT), str(passwords)], capture_output=True, text=True, check=False
    )


def _all_plaintext(**overrides: str | None) -> list[str]:
    """One plaintext line per placeholder; an override replaces a placeholder's
    line, and ``None`` drops it."""
    lines = []
    for p in PLACEHOLDERS:
        line = overrides.get(p, f"{p}={_plaintext(p)}")
        if line is not None:
            lines.append(line)
    return lines


def _user_of(placeholder: str) -> str:
    return next(
        name
        for name, rules in parse_users(ACL_FILE.read_text()).items()
        if f">{placeholder}" in rules
    )


def test_every_user_renders_as_the_digest_of_its_own_plaintext(tmp_path) -> None:
    result = _render(tmp_path, _all_plaintext())
    assert result.returncode == 0, result.stderr
    users = parse_users(result.stdout)
    for p in PLACEHOLDERS:
        assert f"#{_sha256(_plaintext(p))}" in users[_user_of(p)], p


def test_the_render_carries_no_plaintext(tmp_path) -> None:
    result = _render(tmp_path, _all_plaintext())
    assert result.returncode == 0, result.stderr
    for p in PLACEHOLDERS:
        assert _plaintext(p) not in result.stdout, p
    for name, rules in parse_users(result.stdout).items():
        assert not [r for r in rules if r.startswith(">")], name


def test_a_digest_line_passes_through(tmp_path) -> None:
    p = PLACEHOLDERS[0]
    digest = _sha256("held-elsewhere")
    result = _render(tmp_path, _all_plaintext(**{p: f"{_digest_key(p)}={digest}"}))
    assert result.returncode == 0, result.stderr
    assert f"#{digest}" in parse_users(result.stdout)[_user_of(p)]


def test_a_placeholder_with_neither_spelling_is_refused_and_named(tmp_path) -> None:
    """The #49 shape: the plaintext line commented out, no digest line."""
    p = PLACEHOLDERS[0]
    result = _render(tmp_path, _all_plaintext(**{p: f"#{p}=commented-out"}))
    assert result.returncode != 0
    assert p in result.stderr
    assert not result.stdout


def test_both_spellings_for_one_placeholder_is_refused(tmp_path) -> None:
    """Two credentials for one user is a rotation left half-done, and which one
    the render chose would be an accident of line order."""
    p = PLACEHOLDERS[0]
    lines = [*_all_plaintext(), f"{_digest_key(p)}={_sha256('other')}"]
    result = _render(tmp_path, lines)
    assert result.returncode != 0
    assert p in result.stderr


def test_a_placeholder_given_twice_is_refused(tmp_path) -> None:
    p = PLACEHOLDERS[0]
    result = _render(tmp_path, [*_all_plaintext(), f"{p}=second-value-0123456789"])
    assert result.returncode != 0
    assert p in result.stderr


@pytest.mark.parametrize(
    "digest",
    [
        "a" * 63,
        "a" * 65,
        "A" * 64,  # redis accepts lowercase hex only
        "g" * 64,
        "",
        EMPTY_SHA256,
    ],
    ids=["short", "long", "uppercase", "not-hex", "empty", "empty-string-digest"],
)
def test_a_digest_redis_would_refuse_or_that_hashes_nothing_is_refused(tmp_path, digest) -> None:
    p = PLACEHOLDERS[0]
    result = _render(tmp_path, _all_plaintext(**{p: f"{_digest_key(p)}={digest}"}))
    assert result.returncode != 0
    assert _digest_key(p) in result.stderr


def test_an_empty_plaintext_is_refused(tmp_path) -> None:
    """It hashes to ``EMPTY_SHA256``: a user whose password is nothing."""
    p = PLACEHOLDERS[0]
    result = _render(tmp_path, _all_plaintext(**{p: f"{p}="}))
    assert result.returncode != 0
    assert p in result.stderr


@pytest.mark.parametrize(
    "suffix", [" ", "\r", "\t"], ids=["trailing-space", "carriage-return", "tab"]
)
def test_a_value_carrying_whitespace_is_refused_not_hashed(tmp_path, suffix) -> None:
    """A CRLF file or a pasted trailing space hashes to a credential no service
    holds. Against a live broker the node test catches it; on a rebuild there is
    no live broker, and every such user is locked out with nothing saying why."""
    p, q = PLACEHOLDERS[0], PLACEHOLDERS[1]
    lines = _all_plaintext(
        **{
            p: f"{p}={_plaintext(p)}{suffix}",
            q: f"{_digest_key(q)}={_sha256('x')}{suffix}",
        }
    )
    result = _render(tmp_path, lines)
    assert result.returncode != 0
    assert p in result.stderr
    assert _digest_key(q) in result.stderr
    assert _plaintext(p) not in result.stderr


def test_an_unrecognised_line_is_refused_without_echoing_it(tmp_path) -> None:
    """A bare value on a line of its own - a password pasted without its key -
    is the likeliest malformed line, and printing it would publish it."""
    stray = "stray-secret-0123456789"
    result = _render(tmp_path, [*_all_plaintext(), stray])
    assert result.returncode != 0
    assert "line" in result.stderr
    assert stray not in result.stderr + result.stdout


def test_no_refusal_echoes_a_value(tmp_path) -> None:
    p, q = PLACEHOLDERS[0], PLACEHOLDERS[1]
    lines = _all_plaintext(**{p: f"{p}=secret-one-0123456789"})
    lines += [f"{p}=secret-two-0123456789", f"{_digest_key(q)}={'Z' * 64}"]
    result = _render(tmp_path, lines)
    assert result.returncode != 0
    for value in ("secret-one-0123456789", "secret-two-0123456789", _plaintext(q), "Z" * 64):
        assert value not in result.stderr, value


def test_the_last_line_needs_no_trailing_newline(tmp_path) -> None:
    """``read`` returns false on a final unterminated line, and the loop used to
    drop it - reporting a placeholder missing that is plainly in the file."""
    passwords = tmp_path / "passwords"
    passwords.write_text("\n".join(_all_plaintext()))
    result = subprocess.run(
        [str(RENDER_SCRIPT), str(passwords)], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr


def test_a_user_rendered_by_digest_authenticates_with_its_plaintext(tracked_acl_broker) -> None:
    """The throwaway fixture renders ``archiver`` by digest, as the node holds it,
    so this is the end-to-end half: redis loads ``#<sha256>`` from an aclfile and
    ``AUTH`` with the plaintext is admitted. Every other test that connects as
    ``archiver`` to that server leans on the same path."""
    assert DIGEST_PLACEHOLDERS == ("__ARCHIVER_PW__",)
    assert tracked_acl_broker("archiver").ping()
    assert _sha256(PASSWORD) in tracked_acl_broker("acladmin").acl_getuser("archiver")["passwords"]
