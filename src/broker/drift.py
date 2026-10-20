"""Does live lag origin/main in code that runs? The drift check (#110, epic #22 D6).

Since #107 a merge changes nothing the probe runs until ``scripts/deploy.sh``,
and nothing else reports a merge that was never deployed. ``python -m
src.broker.drift``, hourly from ``broker-drift.timer``, asks GitHub how far the
live release's ``REVISION`` is behind ``main``, and checks in to co-status's
``co-broker-drift`` monitor (:mod:`src.broker.checkin`; CannObserv/status#51):

- **ok** while live is ``main``, behind only in paths that never run
  (:func:`counts`), or behind in code for no longer than :data:`GRACE`;
- **alert** once code has waited longer than that since the push that brought
  it, naming ``main``'s CI result (deploy.sh's gate refuses a red one); and at
  once when live is not on ``main``, or is unstamped. Every alert carries
  ``metadata.fault``, its kind, so a change of fault reports at once;
- **nothing** when GitHub cannot say. A long outage goes silent, and the
  monitor's grace turns that into ``missing``: never a false ok.

Ported from CannObserv/processor's ``src/processor/drift.py`` (at ``540975e``),
itself from CannObserv/status's ``src/core/drift.py`` (status#12), their CR
numbers kept. Where it differs: ``urllib`` rather than ``requests`` (this repo's
dependency list is three entries long on purpose); only ``deploy/broker-*`` of
``deploy/`` runs; ``dev`` and ``unknown`` are both unstamped; an alert carries
its fault; and co-status's base is the probe's constant, never a setting.

The clock starts at the push, never the commit: a CI run's ``created_at`` is
when its push landed, and a commit can be days older than that. Unauthenticated,
like the deploy gate: the repo is public, and GitHub allows an address 60
requests an hour, shared with ``scripts/deploy.sh``. A run costs 1 in sync or
behind in docs, 2 behind in code, and at most 2 + :data:`WALK_LIMIT` past the
grace (status CR 15).

It holds no Redis credential and imports no Redis client: ``ACL LOG`` cannot
gain an entry from it (D6, B8).
"""

from __future__ import annotations

import argparse
import http.client
import json
import logging
import os
import re
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

from src.broker.checkin import CREDENTIAL_NAME, CheckinFailed, post_checkin, read_key
from src.broker.logging import configure_logging, get_logger
from src.broker.revision import DEV, UNKNOWN, read_revision

# Literal rather than __name__: the timer runs this module via ``python -m``,
# where __name__ is "__main__" - a useless journald filter key.
logger = get_logger("src.broker.drift")

#: How long code may sit on ``main`` undeployed. Deploys are by hand; CI takes
#: about two minutes.
GRACE = timedelta(hours=8)
GITHUB_API = "https://api.github.com/repos/CannObserv/broker"
#: CI's push runs on main, newest first: when each push landed (deploy.sh's gate asks the same).
RUNS_PATH = "actions/workflows/ci.yml/runs?event=push&branch=main&per_page=100"
#: At most this many extra compares to find the push that brought code. Past
#: it, the first push left unchecked starts the clock: the earliest the code
#: can have come, so an alert sooner, never later (status CR 1).
WALK_LIMIT = 8
#: No call to GitHub starts past this. Each call is bounded per socket operation
#: by ``REQUEST_TIMEOUT_SECONDS`` (urllib's timeout is per read, not per request),
#: so one answer trickling in can run past it. The hard stop is the unit's
#: ``TimeoutStartSec=90``; a kill there sends no check-in: silence, never a false ok.
CHECK_TIMEOUT_SECONDS = 60.0
REQUEST_TIMEOUT_SECONDS = 10.0
#: GitHub lists at most this many files in a compare; a list this long may be
#: cut short, and what was cut could be anything.
COMPARE_FILES_LIMIT = 300
USER_AGENT = "co-broker-drift (CannObserv/broker#110)"

#: What a release runs (the design's D6): the package, broker's own units, the
#: deploy, the dependencies. Of ``deploy/``, only ``broker-*``: the Redis config,
#: the ACL and the drop-ins are installed by hand and never run from live (D9).
#: Everything else (docs, tests, CI, skills, the other scripts) never runs.
RUNTIME_PREFIXES = ("src/", "deploy/broker-")
RUNTIME_FILES = frozenset({"scripts/deploy.sh", "pyproject.toml", "uv.lock"})

#: Where ``deploy/broker-drift.service`` names ``co-broker-drift``'s id.
MONITOR_ID_ENV = "BROKER_DRIFT_MONITOR_ID"
#: A ULID, as co-status mints: a path segment, so nothing else is let through.
_MONITOR_ID = re.compile(r"[0-9A-HJKMNP-TV-Z]{26}")
#: What a REVISION holds when it names a commit: deploy.sh writes 12 hex digits.
_BUILD = re.compile(r"[0-9a-f]{7,40}")

#: ``silent`` sends no check-in; ``ok`` sends ``ok``; every other kind an ``alert``.
Kind = Literal["ok", "lag", "off_main", "unstamped", "test", "silent"]
Get = Callable[[str], Mapping]


@dataclass(frozen=True)
class Verdict:
    """What to tell ``co-broker-drift``, and the variables its templates render."""

    kind: Kind
    body: str
    live: str
    main: str = "unknown"

    @property
    def status(self) -> Literal["ok", "alert"] | None:
        """The check-in's ``status``; ``None``: send none."""
        if self.kind == "silent":
            return None
        return "ok" if self.kind == "ok" else "alert"

    def variables(self) -> dict[str, str]:
        """Every variable on every check-in: the templates render under StrictUndefined."""
        return {"kind": self.kind, "live": self.live, "main": self.main, "body": self.body}

    def metadata(self) -> dict[str, str] | None:
        """``fault``, the one value co-status compares, on an alert; none on an ok."""
        return {"fault": self.kind} if self.status == "alert" else None


@dataclass(frozen=True)
class Push:
    """A push to ``main``: its newest commit, and when it landed."""

    sha: str
    at: datetime


def counts(path: str) -> bool:
    """Whether a change to *path* changes what a release runs."""
    return path in RUNTIME_FILES or path.startswith(RUNTIME_PREFIXES)


def diff_counts(compare: Mapping) -> bool:
    """Whether a GitHub compare touches a path that :func:`counts`, or may.

    A rename counts by either name: a file moved out of ``src/`` left the release
    (status#26). GitHub omits ``previous_filename`` for anything else.
    """
    files = compare.get("files")
    if files is None or len(files) >= COMPARE_FILES_LIMIT:
        return True
    if len(compare["commits"]) < compare["total_commits"]:
        return True
    return any(counts(f["filename"]) or counts(f.get("previous_filename") or "") for f in files)


def unstamped(live: str) -> str | None:
    """Why *live* names no commit, or ``None`` when it does."""
    if live == DEV:
        return "dev: its release has no REVISION"
    if live == UNKNOWN:
        return "unknown: its REVISION is unreadable or empty"
    if not _BUILD.fullmatch(live):
        return f"{live[:40]!r}: its REVISION is not a commit id"
    return None


def first_look(live: str, compare: Mapping) -> Verdict | None:
    """The verdict from ``compare/<live>...main`` alone, or ``None`` if it needs the clock.

    An unstamped release sends ``live: unknown``, as co-status's contract spells
    it (CannObserv/status#51); the body says which.
    """
    why = unstamped(live)
    if why is not None:
        return Verdict("unstamped", f"live is unstamped ({why})", "unknown")
    status = compare["status"]
    if status == "identical":
        return Verdict("ok", f"live {live} is main", live, live)
    main = _main(compare)
    if status != "ahead":
        return Verdict("off_main", f"live {live} is not on main (GitHub: {status})", live, main)
    if not diff_counts(compare):
        return Verdict("ok", f"{_ahead(live, compare)}, none that runs", live, main)
    return None


def pushes(compare: Mapping, runs: Mapping) -> list[Push]:
    """The pushes to ``main`` since live, oldest first, from CI's push runs.

    GitHub runs CI once per push, on its newest commit. A ``main`` with no run
    (a workflow's ``GITHUB_TOKEN`` push, or ``[skip ci]``) falls back to its
    commit date, the only clock left, but no earlier than any push under it: it
    landed after them, and the walk needs ``main``'s push last (status CR 10).

    ``main`` is the compare's last commit, up to the 250 commits a compare lists;
    past that the diff already counts (:func:`diff_counts`), but the tip named,
    its date and its CI may be an older commit's (status CR 6).
    """
    undeployed = {c["sha"] for c in compare["commits"]}
    landed: dict[str, datetime] = {}
    for run in runs["workflow_runs"]:
        if run["event"] != "push" or run["head_branch"] != "main":
            continue
        if run["head_sha"] not in undeployed:
            continue
        at = _parse(run["created_at"])
        landed[run["head_sha"]] = min(at, landed.get(run["head_sha"], at))
    tip = compare["commits"][-1]
    if tip["sha"] not in landed:
        committed = _parse(tip["commit"]["committer"]["date"])
        landed[tip["sha"]] = max([committed, *landed.values()])
    return sorted((Push(s, at) for s, at in landed.items()), key=lambda p: p.at)


def tip_ci(runs: Mapping, tip: str) -> str:
    """``main``'s CI result: its newest push run's conclusion, else its status."""
    mine = [
        r
        for r in runs["workflow_runs"]
        if r["head_sha"] == tip and r["event"] == "push" and r["head_branch"] == "main"
    ]
    if not mine:
        return "no run"
    newest = max(mine, key=lambda r: r["created_at"])
    if newest["status"] != "completed":
        return newest["status"]
    return newest["conclusion"] or "nothing"


def lagging(live: str, compare: Mapping, since: Push, *, ci: str, now: datetime) -> Verdict:
    """The verdict for live behind in code, the clock started at the push *since*.

    Within the grace nothing is walked, so *since* is the oldest push since live,
    which may be docs only: the body names the clock, never "the code since"
    (status CR 4).
    """
    age = now - since.at
    body = (
        f"{_ahead(live, compare)}, the clock started at the push of {_stamp(since.at)} "
        f"({age / timedelta(hours=1):.1f} h ago; grace {GRACE / timedelta(hours=1):.0f} h)"
    )
    if age <= GRACE:
        return Verdict("ok", body, live, _main(compare))
    return Verdict("lag", f"{body}. main's CI: {ci} - {_remedy(ci)}", live, _main(compare))


def _remedy(ci: str) -> str:
    if ci == "success":
        return "scripts/deploy.sh"
    if ci == "no run":
        # A workflow's push never gets one, and deploy.sh refuses it at once (#106).
        return "scripts/deploy.sh <sha12>, the newest commit with a green run"
    return "deploy.sh refuses it until CI passes"


class GitHubSilent(Exception):
    """GitHub gave no answer this check can use."""


class GitHubNotFound(GitHubSilent):
    """GitHub answered 404."""


def assess(live: str, *, now: datetime, get: Get) -> Verdict:
    """Ask GitHub, through *get*, about *live*, the live release's build id. Never raises
    for anything GitHub does."""
    try:
        return _assess(get, live, now)
    except GitHubSilent as exc:
        return Verdict("silent", f"GitHub did not answer: {exc}", live)
    except (KeyError, TypeError, IndexError, ValueError, AttributeError):
        # GitHub's shape changed, or this module has a bug: the traceback says which
        # (status CR 3).
        logger.warning("drift check: GitHub's answer is not the JSON expected", exc_info=True)
        return Verdict("silent", "GitHub's answer is not the JSON expected", live)


def _assess(get: Get, live: str, now: datetime) -> Verdict:
    if unstamped(live) is not None:
        return first_look(live, {})
    try:
        compare = get(f"compare/{live}...main")
    except GitHubNotFound as exc:
        # A base GitHub does not know is live off main, not GitHub silent (status CR 2).
        return Verdict(
            "off_main",
            f"GitHub does not know live {live} ({exc}): main rewritten since the deploy, "
            "or the repo is no longer public",
            live,
        )
    verdict = first_look(live, compare)
    if verdict is not None:
        return verdict
    runs = get(RUNS_PATH)
    found = pushes(compare, runs)
    since = found[0]
    if now - since.at > GRACE:
        since = _first_counting(get, live, found, now)
    return lagging(live, compare, since, ci=tip_ci(runs, compare["commits"][-1]["sha"]), now=now)


def _first_counting(get: Get, live: str, found: list[Push], now: datetime) -> Push:
    """The oldest push whose diff from *live* counts; ``main``'s (the last) is known to.

    Past :data:`WALK_LIMIT`, the first push not asked about: every one before it
    is known not to count, so the code came with it at the earliest. A push
    inside the grace ends the walk too: it, or a newer one, brought the code,
    and the verdict is ok either way (status CR 14).
    """
    for push in found[:-1][:WALK_LIMIT]:
        if now - push.at <= GRACE:
            return push
        if diff_counts(get(f"compare/{live}...{push.sha}")):
            return push
    return found[min(WALK_LIMIT, len(found) - 1)]


def github(api: str | None = None) -> Get:
    """A ``get`` for :func:`assess`: GitHub's REST API, no call started past one deadline.

    Raises :class:`GitHubSilent` for anything but a JSON object: a refusal (with
    GitHub's message), an error page, a timeout, an empty or wrong-shaped 200.
    """
    base = (api or GITHUB_API).rstrip("/")
    deadline = time.monotonic() + CHECK_TIMEOUT_SECONDS

    def get(path: str) -> Mapping:
        left = deadline - time.monotonic()
        if left <= 0:
            raise GitHubSilent(f"Timeout: no GitHub call starts past {CHECK_TIMEOUT_SECONDS:.0f} s")
        timeout = min(REQUEST_TIMEOUT_SECONDS, left)
        request = urllib.request.Request(
            f"{base}/{path}",
            headers={"Accept": "application/vnd.github+json", "User-Agent": USER_AGENT},
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                code, raw = int(response.status), response.read()
        except urllib.error.HTTPError as exc:
            error = GitHubNotFound if exc.code == 404 else GitHubSilent
            raise error(f"{exc.code} {_message(exc)}".strip()) from None
        except (OSError, http.client.HTTPException, ValueError) as exc:
            raise GitHubSilent(_failure(exc, timeout)) from None
        try:
            answer = json.loads(raw)
        except ValueError:
            answer = None
        if not isinstance(answer, Mapping):
            raise GitHubSilent(f"{code}, not a JSON object")
        return answer

    return get


def _message(exc: urllib.error.HTTPError) -> str:
    """GitHub's ``message`` from an error answer, or nothing."""
    try:
        message = json.loads(exc.read()).get("message", "")
    except (OSError, ValueError, AttributeError):
        return ""
    return message if isinstance(message, str) else ""


def _failure(exc: BaseException, timeout: float) -> str:
    """No answer, by type and urllib's text; a timeout names the one it ran out of."""
    text = f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__
    reason = getattr(exc, "reason", exc)
    if isinstance(exc, TimeoutError) or isinstance(reason, TimeoutError):
        text += f" (timeout {timeout:.3g} s)"
    return text


def deliberate_alert(live: str) -> Verdict:
    """The deliberate alert ``--test-alert`` sends: proves the channels."""
    return Verdict(
        "test",
        f"a test alert from co-broker's drift check, sent by hand; live {live}",
        "unknown" if unstamped(live) else live,
    )


def _main(compare: Mapping) -> str:
    return compare["commits"][-1]["sha"][:12]


def _ahead(live: str, compare: Mapping) -> str:
    n = compare["total_commits"]
    return f"live {live}, main {_main(compare)}: {n} commit{'' if n == 1 else 's'} ahead"


def _parse(stamp: str) -> datetime:
    return datetime.fromisoformat(stamp.replace("Z", "+00:00"))


def _stamp(at: datetime) -> str:
    return at.strftime("%Y-%m-%dT%H:%M:%SZ")


# --- the command: one check, one check-in, one record -----------------------


def main(argv: list[str] | None = None) -> int:
    """One drift check, one check-in, one ``drift check`` record.

    Exit 0 when co-status took the check-in, ok or alert; 1 when there was none
    to send (GitHub silent) or co-status did not take it, a 422 included; 2 when
    the monitor id is not one. Never a retry: the next hour is.
    """
    parser = argparse.ArgumentParser(
        description="broker drift check: does live lag origin/main? (CannObserv/broker#110)"
    )
    parser.add_argument(
        "--test-alert", action="store_true", help="send one test alert, asking GitHub nothing"
    )
    args = parser.parse_args(argv)

    configure_logging()
    monitor_id = os.environ.get(MONITOR_ID_ENV, "")
    if monitor_id and not _MONITOR_ID.fullmatch(monitor_id):
        # Not echoed: whatever it is, it is not an id, and the unit file says what it is.
        logger.error("invalid settings", extra={"errors": [f"{MONITOR_ID_ENV} is not a ULID"]})
        return 2

    live = read_revision()
    if args.test_alert:
        verdict = deliberate_alert(live)
    else:
        verdict = assess(live, now=datetime.now(UTC), get=github())
    fields = {"build": live, "main": verdict.main, "kind": verdict.kind, "body": verdict.body}
    if verdict.status is None:
        logger.warning("drift check", extra=fields | {"checkin": None})
        return 1
    directory = os.environ.get("CREDENTIALS_DIRECTORY")
    key = read_key(Path(directory) if directory else None)
    missing = [
        name
        for name, value in (
            (MONITOR_ID_ENV, monitor_id),
            (f"the {CREDENTIAL_NAME} credential", key),
        )
        if not value
    ]
    if missing:
        logger.error(
            "drift check", extra=fields | {"checkin": f"not sent: missing {', '.join(missing)}"}
        )
        return 1
    try:
        code = post_checkin(
            monitor_id, key, verdict.status, verdict.variables(), metadata=verdict.metadata()
        )
    except CheckinFailed as exc:
        logger.error("drift check", extra=fields | {"checkin": f"failed: {exc}"})
        return 1
    level = logging.INFO if verdict.status == "ok" else logging.WARNING
    logger.log(level, "drift check", extra=fields | {"checkin": code})
    return 0


if __name__ == "__main__":
    sys.exit(main())
