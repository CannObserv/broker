"""Fixtures for the deploy tests that need a *server* rather than a file.

Two of them, and the distinction between them is the whole reason they are
here:

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


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


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
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    deadline = time.time() + 10
    while time.time() < deadline:
        if proc.poll() is not None:
            pytest.fail(f"redis-server refused the ACL file:\n{proc.stdout.read()}")
        with socket.socket() as s:
            s.settimeout(0.2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                break
        time.sleep(0.1)
    else:
        proc.terminate()
        pytest.fail("redis-server did not start")

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
    proc.terminate()
    proc.wait(timeout=10)


@pytest.fixture(scope="module")
def live_client():
    url = os.environ.get("BROKER_REDIS_URL")
    if not url:
        pytest.skip("BROKER_REDIS_URL not set - not a host with broker credentials")

    redis = pytest.importorskip("redis")
    client = redis.Redis.from_url(
        url, socket_connect_timeout=2, socket_timeout=2, decode_responses=True
    )
    try:
        try:
            client.ping()
        except redis.exceptions.RedisError as e:
            pytest.skip(f"broker not answering: {e!r}")
        yield client
    finally:
        # Closed on the skip path too: ``ping`` failing still leaves whatever
        # connection the pool created behind it.
        client.close()
