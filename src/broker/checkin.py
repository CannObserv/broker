"""One check-in to a co-status monitor, for the drift check (#110).

Ported from CannObserv/processor's ``src/processor/checkin.py`` (at ``540975e``),
on ``urllib`` rather than ``requests``, as the probe's own check-in is. co-status's
contract: ``POST /api/v1/monitors/{id}/checkin`` with ``{"status": "ok"|"alert",
"variables": {...}}``, plus ``metadata`` when given, and an ``X-API-Key``,
answered 202. An ``alert`` renders the monitor's templates against ``variables``
first, and a failed preview is a 422 with nothing recorded (CannObserv/status#51).

**One attempt, never a loop:** the next run is the retry, and the monitor's grace
absorbs one missed check-in. A failure raises :class:`CheckinFailed`, whose
message names co-status's answer and never the key.

**The key is a systemd credential** (``LoadCredential=``), read from
``$CREDENTIALS_DIRECTORY``: in no process environment, never logged.

The probe's own check-in (``bus_health.post_checkin``) keeps its environment
variables and its never-fails contract; it shares only the base, from here.
"""

from __future__ import annotations

import json
import re
import urllib.error
import urllib.request
from pathlib import Path
from typing import Literal

# --- the destination: a constant, never configuration ---
#
# THE BASE URL IS A CONSTANT, NOT CONFIGURATION, and that is deliberate.
# `status:9001` is status_dev against its own database. Since a monitor here
# alarms on the *absence* of check-ins, a wrong destination would not degrade it
# but invert it: check-ins land somewhere nothing alarms, the production monitor
# receives nothing, and it reports a perfectly healthy broker as dead. The
# retired `notifier:9000` is the same trap one host over. The operator
# therefore supplies a monitor id and never a host or a port. Same move as
# `databases 1` against the db15 vector - make the wrong destination unnameable
# rather than merely discouraged. Both the probe and the drift check post here.
STATUS_CHECKIN_BASE = "http://status:9000/api/v1/monitors"

#: The ``LoadCredential=`` name ``broker-drift.service`` gives the key.
CREDENTIAL_NAME = "status-checkin-key"
TIMEOUT_SECONDS = 10.0
#: One token of printable ASCII: what co-status mints. Anything else is a bad
#: paste, refused before http.client sees it, since its refusal quotes the value.
_KEY = re.compile(r"[\x21-\x7e]+")


class CheckinFailed(Exception):
    """co-status did not take the check-in: its answer, or why there was none."""


def read_key(directory: Path | None) -> str:
    """The key credential under *directory*, stripped; ``""`` when there is none.

    Absent, empty and the unit's lone-newline ``SetCredential=`` fallback all read
    as ``""``; outside a unit there is no directory at all.
    """
    if directory is None:
        return ""
    try:
        return (directory / CREDENTIAL_NAME).read_text().strip()
    except FileNotFoundError:
        return ""


def post_checkin(
    monitor_id: str,
    key: str,
    status: Literal["ok", "alert"],
    variables: dict[str, str],
    *,
    metadata: dict[str, str] | None = None,
    timeout: float = TIMEOUT_SECONDS,
) -> int:
    """Check in once; return co-status's answer code (202), or raise :class:`CheckinFailed`."""
    if not _KEY.fullmatch(key):
        raise CheckinFailed(
            f"the {CREDENTIAL_NAME} credential is not one token of printable ASCII; "
            "reinstall it (deploy/README.md, The drift check)"
        )
    body: dict[str, object] = {"status": status, "variables": variables}
    if metadata is not None:
        body["metadata"] = metadata
    request = urllib.request.Request(
        f"{STATUS_CHECKIN_BASE}/{monitor_id}/checkin",
        data=json.dumps(body).encode(),
        headers={"X-API-Key": key, "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            code, raw = int(response.status), response.read()
    except urllib.error.HTTPError as exc:
        code, raw = int(exc.code), exc.read()
    except (OSError, ValueError) as exc:
        # By type and urllib's own text, which names the reason, never a header.
        raise CheckinFailed(f"{type(exc).__name__}: {exc}") from None
    if code != 202:
        raise CheckinFailed(f"{code} {_detail(raw)}".strip())
    return code


def _detail(raw: bytes) -> str:
    try:
        detail = json.loads(raw).get("detail", "")
    except (ValueError, AttributeError):
        return ""
    return detail if isinstance(detail, str) else str(detail)
