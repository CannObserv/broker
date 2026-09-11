"""What the deploy tests share: the tracked ACL file, and two servers.

``parse_users`` and ``ACL_FILE`` are here rather than in a test module because
two test modules read the tracked file and neither should have to import the
other to do it - importing ``test_redis_acl`` for a parser dragged co-core into
the import graph of a module that has no use for it.

Then the two fixtures, and the distinction between them is the whole reason
they are here:

``tracked_acl_broker``
    A throwaway ``redis-server`` loading the ACL file **this repo tracks**, on a
    free loopback port with no persistence. It never reads
    ``BROKER_REDIS_URL``, so it cannot reach ``co-broker``.

``live_client``
    The **running broker**, as the probe's own ``brokeradmin`` credential.
    Skips unless ``BROKER_REDIS_URL`` is set and the node answers.

Each lived in the module that first needed it until
``test_live_acl_matches_tracked_acl.py`` needed both *in one test* - which is
also why the throwaway one is no longer called ``live_acl_broker``. Standing
beside a fixture that really is the live broker, that name said the opposite of
what it is, and the test that compares the two is exactly where a reader must
not have to guess which is which.
"""

import os
import re
import shutil
import socket
import subprocess
import time
from pathlib import Path

import pytest
import redis as redis_pkg

DEPLOY = Path(__file__).resolve().parents[2] / "deploy"
ACL_FILE = DEPLOY / "redis-acl.conf"
RENDER_SCRIPT = DEPLOY / "render-acl.sh"

# Throwaway, for the spawned server below. Never a real credential.
PASSWORD = "throwaway-password"


def parse_users(text: str) -> dict[str, list[str]]:
    """`user <name> <rule> <rule> ...`, one per line.

    Deliberately strict about the one-line rule: neither redis.conf nor an
    aclfile supports backslash continuation, and the version of this file
    drafted in the issue thread used it.
    """
    users: dict[str, list[str]] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        assert not line.endswith("\\"), f"aclfile has no line continuation: {line!r}"
        assert line.startswith("user "), f"not a user line: {line!r}"
        _, name, *rules = line.split()
        users[name] = rules
    return users


def split_rules(rules: list[str]) -> tuple[list[str], list[list[str]]]:
    """One user's tokens, separated into root rules and selector rule groups.

    A **selector** is Redis 7's answer to "this command on that pattern only":
    `(+xdel ~a.dlq ~b.dlq)`, written inside the user line, granting its commands
    on its own key patterns and nothing else. The root permission set never
    learns the command.

    It has to be parsed rather than split, and the failure if it is not is
    silent in the worst direction: `line.split()` scatters a selector across
    tokens with the brackets still attached, so `~b.dlq)` reads as a **root** key
    pattern - one with a trailing bracket, naming a stream that does not exist -
    while the genuinely root patterns and the selector's become
    indistinguishable. Every assertion in this directory about what a user can
    name rests on this separation.
    """
    root: list[str] = []
    selectors: list[list[str]] = []
    current: list[str] | None = None
    for token in rules:
        if current is None:
            if not token.startswith("("):
                root.append(token)
                continue
            current, token = [], token[1:]
        if token.endswith(")"):
            current.append(token[:-1])
            selectors.append([t for t in current if t])
            current = None
        elif token:
            current.append(token)
    assert current is None, f"unclosed selector in {rules}"
    return root, selectors


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _stop(proc: subprocess.Popen) -> None:
    """Reaped on every exit path, and with a ``kill`` behind the ``terminate``.

    A ``terminate`` that is never waited on leaves a zombie, and a ``wait`` with
    no fallback turns a server that ignores SIGTERM into a ``TimeoutExpired``
    raised from teardown - with the server still holding its port.
    """
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)


@pytest.fixture(scope="module")
def tracked_acl_broker(tmp_path_factory):
    """A throwaway redis-server running the tracked ACL file.

    This is the assertion that could otherwise only be made during the restart
    window. `aclfile` is an immutable config, so it is enabled by restarting an
    instance three services depend on - and redis **aborts startup** on an ACL
    error, refusing the whole file rather than the offending line. A syntax
    error found there is found with the broker down.

    It has already earned this twice. The first run caught that an aclfile
    permits no comments; the second that `+client|setinfo` does not exist before
    Redis 7.2, so the pre-emptive grant broker#2 recommended would have taken
    every user down with it.

    Binds loopback on a free port with no persistence and never reads
    BROKER_REDIS_URL, so it cannot reach `co-broker`.

    **Module-scoped deliberately, not by oversight.** Sharing it across the two
    modules that use it would save one spawn, and cost the isolation that
    `test_retiring_default_is_reversible_live_as_acladmin` needs - that test
    disables and re-enables `default` on this server. It restores it in a
    `finally`, but a fixture every module in the directory leans on is the wrong
    place to rely on that.
    """
    binary = shutil.which("redis-server")
    if not binary:
        pytest.skip("redis-server not installed")

    tmp_path = tmp_path_factory.mktemp("acl")
    passwords = tmp_path / "passwords"
    placeholders = sorted(set(re.findall(r"__[A-Z]+_PW__", ACL_FILE.read_text())))
    passwords.write_text("".join(f"{m}={PASSWORD}\n" for m in placeholders))
    acl = tmp_path / "users.acl"
    # Rendered through the same script the install uses, so what is tested is
    # what is installed - including the comment strip, which is not cosmetic.
    acl.write_text(
        subprocess.run(
            [str(RENDER_SCRIPT), str(passwords)], capture_output=True, text=True, check=True
        ).stdout
    )

    port = _free_port()
    # Logged to a file rather than a pipe nobody reads: this fixture outlives the
    # whole module, and a `stdout=PIPE` whose 64KB buffer fills blocks the server
    # on its next log line. The diagnostic is what the pipe was for, so it is
    # read back from the file on the refusal path.
    log = tmp_path / "redis-server.log"
    with log.open("w") as stream:
        proc = subprocess.Popen(
            [
                binary,
                "--port",
                str(port),
                "--bind",
                "127.0.0.1",
                "--save",
                "",
                "--appendonly",
                "no",
                "--aclfile",
                str(acl),
            ],
            stdout=stream,
            stderr=subprocess.STDOUT,
            text=True,
        )
    deadline = time.time() + 10
    while time.time() < deadline:
        if proc.poll() is not None:
            pytest.fail(f"redis-server refused the ACL file:\n{log.read_text()}")
        with socket.socket() as s:
            s.settimeout(0.2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                break
        time.sleep(0.1)
    else:
        _stop(proc)
        pytest.fail(f"redis-server did not start:\n{log.read_text()}")

    def connect(user: str | None = None):
        kwargs = {} if user is None else {"username": user, "password": PASSWORD}
        return redis_pkg.Redis(
            host="127.0.0.1",
            port=port,
            socket_connect_timeout=2,
            socket_timeout=2,
            decode_responses=True,
            **kwargs,
        )

    yield connect
    _stop(proc)


@pytest.fixture(scope="module")
def live_client():
    url = os.environ.get("BROKER_REDIS_URL")
    if not url:
        pytest.skip("BROKER_REDIS_URL not set - not a host with broker credentials")

    # No ``importorskip``: this module imports redis at the top, so a clone
    # without it never reaches here - and redis is a hard dependency of the
    # project, not an extra.
    client = redis_pkg.Redis.from_url(
        url, socket_connect_timeout=2, socket_timeout=2, decode_responses=True
    )
    try:
        try:
            client.ping()
        except redis_pkg.exceptions.RedisError as e:
            pytest.skip(f"broker not answering: {e!r}")
        yield client
    finally:
        # Closed on the skip path too: ``ping`` failing still leaves whatever
        # connection the pool created behind it.
        client.close()
