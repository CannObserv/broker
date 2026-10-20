"""Guards that must hold for every test in this tree, not one module's worth.

Here rather than beside the tests that provoked them because each hazard is
process-scoped: what makes the probe's co-status check-in inert is the absence
of two environment variables, and any module that drives ``run_once`` inherits
whatever shell started pytest. The drift check's guard is the same rule for a
second destination (#110), and the local HTTP stubs its tests answer from.
"""

import socket
import threading

import pytest

from src.broker import checkin, drift
from tests.http_stub import HttpStub

#: Nothing listens on the discard port: a call that slips past a stub fails at
#: once, and never reaches co-status or GitHub.
NOWHERE = "http://127.0.0.1:9"


@pytest.fixture(autouse=True)
def no_checkin_env(monkeypatch):
    """Never let the suite post to the live monitor.

    ``post_checkin`` is a no-op only while ``STATUS_MONITOR_ID`` and
    ``STATUS_API_KEY`` are both unset, and several tests drive ``run_once``
    end to end without stubbing ``_http_post``. ``deploy/README.md`` tells an
    operator to source ``/etc/broker/status.env`` for the curl check - in that
    same shell, ``uv run pytest`` would POST fabricated findings, a synthetic DLQ
    depth and pending count, to the production monitor and raise a real alert
    from a test run.

    Autouse and at the root, so a module added later cannot forget it.
    ``checkin_env`` in ``test_bus_health.py`` sets both back for the handful of
    tests that are about the check-in itself.
    """
    monkeypatch.delenv("STATUS_MONITOR_ID", raising=False)
    monkeypatch.delenv("STATUS_API_KEY", raising=False)


@pytest.fixture(autouse=True)
def no_drift_destination(monkeypatch):
    """Never let the suite check in to ``co-broker-drift``, or ask GitHub (#110).

    co-status's check-in route has no dry run: a test ``ok`` resets the monitor's
    timer, and a test ``alert`` pages (CannObserv/status#51). The monitor id
    lives only in the unit and the key only in a root-only credential, so a dev
    shell holds neither; this makes the destinations themselves unreachable too.
    A test that means to reach a stub points these at it.
    """
    monkeypatch.delenv(drift.MONITOR_ID_ENV, raising=False)
    monkeypatch.delenv("CREDENTIALS_DIRECTORY", raising=False)
    monkeypatch.setattr(checkin, "STATUS_CHECKIN_BASE", f"{NOWHERE}/api/v1/monitors")
    monkeypatch.setattr(drift, "GITHUB_API", NOWHERE)


@pytest.fixture
def http_stub():
    """A fresh :class:`HttpStub`, shut down after the test."""
    stub = HttpStub()
    yield stub
    stub.close()


@pytest.fixture
def http_stub_2():
    """A second :class:`HttpStub`, for a test that needs GitHub and co-status both."""
    stub = HttpStub()
    yield stub
    stub.close()


@pytest.fixture
def garbled_http():
    """A server that answers anything but HTTP: ``http.client`` raises
    ``BadStatusLine``, an ``HTTPException`` and not an ``OSError`` (#110 CR 1)."""
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen()

    def answer() -> None:
        while True:
            try:
                conn, _ = server.accept()
            except OSError:
                return
            with conn:
                conn.recv(65536)
                conn.sendall(b"NOT HTTP\r\n\r\n")

    threading.Thread(target=answer, daemon=True).start()
    yield f"http://127.0.0.1:{server.getsockname()[1]}"
    server.close()
