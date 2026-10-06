"""What the deploy tests share: the tracked ACL file, two servers, and the node signal.

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
    The **running broker**, as ``acladmin`` - the operator, whose password is
    decrypted from the node's credential store through ``sudo -n`` for the life
    of the module. Until CannObserv/broker#52 this was the probe's
    ``brokeradmin``, which then had to carry every grant these tests read with.
    ``BROKER_REDIS_URL`` supplies the address alone. Skips off the node; and on
    it without the URL (the env not sourced), without passwordless sudo, or when
    the broker does not answer. A credential absent, undecryptable or refused
    on the node fails.

Each lived in the module that first needed it until
``test_live_acl_matches_tracked_acl.py`` needed both *in one test* - which is
also why the throwaway one is no longer called ``live_acl_broker``. Standing
beside a fixture that really is the live broker, that name said the opposite of
what it is, and the test that compares the two is exactly where a reader must
not have to guess which is which.

``on_broker_node`` is the one answer to "is this the node?" for a test whose
subject is an installed file (CannObserv/broker#79), ``read_installed`` the read
that uses it, and ``sudo_installed`` the same for a file only root reads
(CannObserv/broker#81).

Both throwaway servers - ``tracked_acl_broker``'s, and the one that loads the
node's *saved* ACL to compare it with the live one (CannObserv/broker#54) - are
spawned by ``acl_server``, which is why it is a context manager here rather than
the body of one fixture.
"""

import hashlib
import os
import re
import shutil
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path

import pytest
import redis as redis_pkg
from redis.connection import parse_url

DEPLOY = Path(__file__).resolve().parents[2] / "deploy"
ACL_FILE = DEPLOY / "redis-acl.conf"
RENDER_SCRIPT = DEPLOY / "render-acl.sh"

# Throwaway, for the spawned server below. Never a real credential.
PASSWORD = "throwaway-password"

# Rendered from a `__X_PW_SHA256__` digest line rather than a plaintext one, as
# the node holds `archiver` since CannObserv/archiver#251's hash-only handoff -
# so every test below that connects as `archiver` also exercises the digest path
# end to end (CannObserv/broker#49).
DIGEST_PLACEHOLDERS = ("__ARCHIVER_PW__",)

# The participants. Each holds its own ACL user of the same name and a row in
# the participants table; both facts are asserted from this one tuple.
# `processor` was declared ahead of its consumer (CannObserv/broker#75, which
# re-homed #62's `observo` user), so CannObserv/processor#1 shipped against a
# grant rather than a NOPERM; it has consumed since 2026-10-02. The live tests
# that read `CLIENT LIST` assert only on the participants that are connected,
# so a declared one that is not yet is no finding.
SERVICE_USERS = ("archiver", "watcher", "replicator", "processor")


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


@contextmanager
def acl_server(acl: Path, workdir: Path):
    """A throwaway ``redis-server`` loading ``acl``, yielding a ``connect(user)``.

    Binds loopback on a free port with no persistence and never reads
    BROKER_REDIS_URL, so it cannot reach ``co-broker``. ``connect`` authenticates
    with ``PASSWORD``, so ``acl`` must give whichever user is named that one.

    Shared by the two things loaded this way: the tracked file, rendered with
    throwaway credentials, and the node's *saved* file, which carries the real
    digests (CannObserv/broker#54).
    """
    binary = shutil.which("redis-server")
    if not binary:
        pytest.skip("redis-server not installed")

    port = _free_port()
    # Logged to a file rather than a pipe nobody reads: a module-scoped caller
    # keeps this alive for the whole module, and a `stdout=PIPE` whose 64KB
    # buffer fills blocks the server on its next log line. The diagnostic is
    # what the pipe was for, so it is read back from the file on the refusal path.
    log = workdir / "redis-server.log"
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
                # A test that runs SAVE or BGREWRITEAOF - the operator's window
                # commands (CannObserv/broker#52) - writes here rather than into
                # whatever directory pytest was started from.
                "--dir",
                str(workdir),
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

    # The port, for the one caller that cannot use the closure: the probe's
    # collectors take an *async* client, and running them against this server is
    # what proves a new check needs no grant the tracked ACL does not already
    # give ``brokeradmin`` (CannObserv/broker#20). Published as an attribute
    # rather than by widening the fixture's return, so every existing caller
    # keeps the shape it has.
    connect.port = port

    try:
        yield connect
    finally:
        _stop(proc)


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

    **Module-scoped deliberately, not by oversight.** Sharing it across the two
    modules that use it would save one spawn, and cost the isolation that
    `_enabled` in `test_redis_acl.py` needs - it switches `default` and `citest`
    on and off again on this server. It restores them in a
    `finally`, but a fixture every module in the directory leans on is the wrong
    place to rely on that.
    """
    tmp_path = tmp_path_factory.mktemp("acl")
    passwords = tmp_path / "passwords"
    placeholders = sorted(set(re.findall(r"__[A-Z]+_PW__", ACL_FILE.read_text())))
    digest = hashlib.sha256(PASSWORD.encode()).hexdigest()
    passwords.write_text(
        "".join(
            f"{m.removesuffix('__')}_SHA256__={digest}\n"
            if m in DIGEST_PLACEHOLDERS
            else f"{m}={PASSWORD}\n"
            for m in placeholders
        )
    )
    acl = tmp_path / "users.acl"
    # Rendered through the same script the install uses, so what is tested is
    # what is installed - including the comment strip, which is not cosmetic.
    acl.write_text(
        subprocess.run(
            [str(RENDER_SCRIPT), str(passwords)], capture_output=True, text=True, check=True
        ).stdout
    )

    with acl_server(acl, tmp_path) as connect:
        yield connect


#: What only the broker node carries, readable without sudo or an env file. **Any**
#: one present means "this is the node", so the absence of a single installed
#: drop-in is a finding there rather than a skip. Keying a test on the file it
#: guards - or on the package that file configures - is what made
#: ``test_needrestart.py`` fail on every CI runner shipping needrestart, and would
#: have made the drop-in's deletion skip its own guard on the node (broker#79).
NODE_MARKERS = (
    Path("/etc/broker"),
    Path("/etc/systemd/system/broker-bus-health.service"),
    Path("/etc/systemd/system/broker-backup.service"),
    Path("/etc/systemd/system/redis-server.service.d/broker.conf"),
)


def on_broker_node() -> bool:
    return any(marker.exists() for marker in NODE_MARKERS)


def require_broker_node() -> None:
    """Skip unless ``on_broker_node``: the one place a test leaves the node."""
    if not on_broker_node():
        pytest.skip("not the broker node - no NODE_MARKERS present")


def pretend_node(monkeypatch, tmp_path: Path, *, present: bool) -> None:
    """Point ``NODE_MARKERS`` at one marker under ``tmp_path``, present or not,
    so a pin can show both sides of ``require_broker_node`` on any host."""
    marker = tmp_path / "node-marker"
    if present:
        marker.touch()
    monkeypatch.setattr(sys.modules[__name__], "NODE_MARKERS", (marker,))


def outcome_of(test, *args) -> BaseException | None:
    """The skip, fail or assertion ``test(*args)`` ends in, or ``None`` if it returns.

    For the pins around ``require_broker_node``. Written as
    ``pytest.raises(pytest.fail.Exception)``, a pin whose test skips does not
    fail: the skip escapes the ``raises`` and skips the pin, so "fails on the
    node" regressing to "skips on the node" - the hazard the pins exist for -
    reads as one more skip (CannObserv/broker#81).
    """
    try:
        test(*args)
    except (pytest.skip.Exception, pytest.fail.Exception, AssertionError) as outcome:
        return outcome
    return None


def read_installed(path: Path) -> str:
    """``path``'s contents on the broker node; a skip anywhere else.

    Off the node is decided by ``on_broker_node``, never by ``path`` itself, and
    on the node an absent ``path`` fails: it is a deploy step not taken, or one
    undone.
    """
    require_broker_node()
    try:
        return path.read_text()
    except FileNotFoundError:
        pytest.fail(f"{path} is not installed on the broker node")


#: The operator's password, a ``systemd-creds`` credential encrypted to the node
#: (CannObserv/broker#52). Its name inside the envelope is the file's own.
OPERATOR_CREDENTIAL = Path("/etc/credstore.encrypted/broker-acladmin")


def node_credential(path: Path) -> str | None:
    """A credential from the node's store, decrypted through ``sudo -n``, or ``None``.

    ``None`` off the node - no passwordless sudo, or no such credential - so the
    caller skips. The value goes from ``systemd-creds``' stdout into memory and
    nowhere else: no argv, no file, and a helper frame of its own so a failing
    test's ``pytest -l`` locals never hold it. A trailing newline is stripped,
    as the probe strips it (``src/broker/bus_health.py``'s ``_unit_credential``),
    so both read an ``echo``-minted credential as the same password.
    """
    result = subprocess.run(
        ["sudo", "-n", "systemd-creds", "decrypt", f"--name={path.name}", str(path), "-"],
        capture_output=True,
        text=True,
        check=False,
    )
    value = result.stdout.rstrip("\r\n") if result.returncode == 0 else ""
    return value or None


def _sudo_status(*argv: str) -> int:
    return subprocess.run(["sudo", "-n", *argv], capture_output=True, check=False).returncode


def sudo_installed(path: Path) -> Path:
    """``path``, a file only root reads, present on the broker node; a skip elsewhere.

    ``read_installed`` for what needs ``sudo -n`` to see, in the same order: off
    the node is decided by ``on_broker_node`` before any ``sudo`` runs - working
    sudo is no node signal, GitHub's runners have it - and on the node an absent
    ``path`` fails (CannObserv/broker#81). No passwordless sudo on the node is
    still a skip: it is the account the suite runs as, not a file not installed.
    """
    require_broker_node()
    if _sudo_status("true"):
        pytest.skip("no passwordless sudo - cannot read root's files")
    if _sudo_status("test", "-f", str(path)):
        pytest.fail(f"{path} is not installed on the broker node")
    return path


def _operator_client(url: str) -> redis_pkg.Redis | None:
    """``acladmin`` at the broker ``url`` names, or ``None`` off the node.

    Only the address is taken from ``url``: it names ``brokeradmin``, and
    redis-py lets a URL's own fields override keyword arguments, so passing it
    through would authenticate as the probe with the operator's password.
    """
    password = node_credential(OPERATOR_CREDENTIAL)
    if password is None:
        return None
    address = parse_url(url)
    return redis_pkg.Redis(
        host=address.get("host", "localhost"),
        port=address.get("port", 6379),
        db=address.get("db", 0),
        username="acladmin",
        password=password,
        socket_connect_timeout=2,
        socket_timeout=2,
        decode_responses=True,
    )


@pytest.fixture(scope="module")
def live_client():
    url = os.environ.get("BROKER_REDIS_URL")
    if not url:
        pytest.skip("BROKER_REDIS_URL not set - not a host with broker credentials")

    # No ``importorskip``: this module imports redis at the top, so a clone
    # without it never reaches here - and redis is a hard dependency of the
    # project, not an extra.
    #
    # On the node, three skips stay: the URL unset (the env not sourced), no
    # passwordless sudo, and the broker not answering. None of them is the
    # credential's state. The credential itself never skips there: one that
    # will not decrypt or that the broker refuses is the half-done rotation
    # test_each_node_credential_authenticates_its_user exists for, and a skip
    # would hide it behind the very fixture that test depends on. An ABSENT one
    # fails too (CannObserv/broker#81): the rotation never leaves the path
    # empty, and the mint runs before the first render.
    sudo_installed(OPERATOR_CREDENTIAL)
    client = _operator_client(url)
    if client is None:
        pytest.fail(f"{OPERATOR_CREDENTIAL} exists but does not decrypt through sudo -n")
    try:
        try:
            client.ping()
        except redis_pkg.exceptions.AuthenticationError:
            pytest.fail(
                f"the broker refuses acladmin's password from {OPERATOR_CREDENTIAL} - a "
                "rotation left half-done? docs/NODE-CREDENTIALS.md"
            )
        except redis_pkg.exceptions.RedisError as e:
            pytest.skip(f"broker not answering: {e!r}")
        yield client
    finally:
        # Closed on the skip path too: ``ping`` failing still leaves whatever
        # connection the pool created behind it.
        client.close()
