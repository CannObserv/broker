"""``src.broker.checkin``: one check-in to a co-status monitor, for the drift check (#110).

Against a local HTTP stub, never co-status: ``tests/conftest.py`` points the
base at nowhere for every test, and these point it at the stub.
"""

import json
from pathlib import Path

import pytest

from src.broker import bus_health, checkin
from src.broker.checkin import CREDENTIAL_NAME, CheckinFailed, post_checkin, read_key

MONITOR = "01M46EXP45TVCVAMQXK043N1Q7"
KEY = "sk-test-0123456789abcdef"
CHECKIN = f"/api/v1/monitors/{MONITOR}/checkin"


@pytest.fixture
def status(http_stub, monkeypatch):
    monkeypatch.setattr(checkin, "STATUS_CHECKIN_BASE", f"{http_stub.url}/api/v1/monitors")
    http_stub.route("POST", CHECKIN, 202, {"monitor_id": MONITOR, "state": "ok"})
    return http_stub


class TestTheBase:
    def test_it_is_production_and_the_probes_own(self):
        """A constant, never configuration (bus_health's reasoning): ``status:9001``
        is status_dev, and a dead-man's monitor posted elsewhere is inverted, not
        degraded. One spelling for both units; the guard in conftest patches
        ``checkin``'s, so the probe's import is the unpatched binding."""
        assert bus_health.STATUS_CHECKIN_BASE == "http://status:9000/api/v1/monitors"
        sources = [Path(m.__file__).read_text() for m in (checkin, bus_health)]
        assert [s.count('"http://status:9000') for s in sources] == [1, 0]


class TestReadKey:
    def test_outside_a_unit_there_is_none(self):
        assert read_key(None) == ""

    def test_absent_is_none(self, tmp_path):
        assert read_key(tmp_path) == ""

    def test_the_fallback_newline_is_none(self, tmp_path):
        """``SetCredential=status-checkin-key:\\n``: the unit's fallback for a missing file."""
        (tmp_path / CREDENTIAL_NAME).write_text("\n")
        assert read_key(tmp_path) == ""

    def test_a_trailing_newline_is_stripped(self, tmp_path):
        """CannObserv/status#51 rule 5: the copy from status.env ends in one."""
        (tmp_path / CREDENTIAL_NAME).write_text(f"{KEY}\n")
        assert read_key(tmp_path) == KEY


class TestPostCheckin:
    def test_one_post_answered_202(self, status):
        code = post_checkin(MONITOR, KEY, "ok", {"kind": "ok"})
        assert code == 202
        (request,) = status.requests
        assert (request["method"], request["path"]) == ("POST", CHECKIN)
        assert request["headers"]["X-API-Key"] == KEY
        assert request["headers"]["Content-Type"] == "application/json"
        assert json.loads(request["body"]) == {"status": "ok", "variables": {"kind": "ok"}}

    def test_metadata_goes_only_when_given(self, status):
        post_checkin(MONITOR, KEY, "alert", {"kind": "lag"}, metadata={"fault": "lag"})
        assert json.loads(status.requests[0]["body"])["metadata"] == {"fault": "lag"}

    def test_the_key_is_in_the_header_alone(self, status):
        post_checkin(MONITOR, KEY, "ok", {"kind": "ok"})
        assert KEY.encode() not in status.requests[0]["body"]
        assert KEY not in status.requests[0]["path"]

    @pytest.mark.parametrize(
        ("code", "body", "reason"),
        [
            (401, {"detail": "invalid API key"}, "401 invalid API key"),
            # A failed template preview: nothing recorded (CannObserv/status#51 rule 2).
            (422, {"detail": {"section": "title"}}, "422 {'section': 'title'}"),
            (500, b"oops", "500"),
            (200, {}, "200"),
        ],
    )
    def test_anything_but_202_fails_once(self, status, code, body, reason):
        status.route("POST", CHECKIN, code, body)
        with pytest.raises(CheckinFailed) as caught:
            post_checkin(MONITOR, KEY, "ok", {"kind": "ok"})
        assert str(caught.value) == reason
        assert len(status.requests) == 1, "one attempt, never a loop"

    def test_unreachable_fails_naming_no_key(self):
        with pytest.raises(CheckinFailed) as caught:
            post_checkin(MONITOR, KEY, "ok", {"kind": "ok"})
        assert str(caught.value).startswith("URLError")
        assert KEY not in str(caught.value)

    @pytest.mark.parametrize("bad", ["", f"{KEY}\n{KEY}", "sk test", "clé"])
    def test_a_key_that_is_not_one_printable_token_is_refused_unsent(self, status, bad):
        """http.client's refusal of a bad header quotes the value; this never sends one."""
        with pytest.raises(CheckinFailed) as caught:
            post_checkin(MONITOR, bad, "ok", {"kind": "ok"})
        assert CREDENTIAL_NAME in str(caught.value)
        if bad:
            assert bad not in str(caught.value)
        assert status.requests == []
