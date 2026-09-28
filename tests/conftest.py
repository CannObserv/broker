"""Guards that must hold for every test in this tree, not one module's worth.

Only one so far, and it is here rather than beside the tests that provoked it
because the hazard is process-scoped: what makes the co-status check-in inert is
the absence of two environment variables, and any module that drives
``run_once`` inherits whatever shell started pytest.
"""

import pytest


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
