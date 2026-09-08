"""Broker-side bus health probe.

Moved here from CannObserv/archiver (`src/core/bus_health.py`, archiver#130)
by CannObserv/archiver#193 D6. The reason for the move is the reason this file
reads the way it does: **every check below measures the broker's host**, and
archiver stopped being that host. Its disk check is about AOF headroom, its
memory check about the `noeviction` cap this repo's redis.conf sets, and its
`XLEN` thresholds about retention mechanisms that live on this node. Run from
a client, all three silently measured the wrong machine.

WARN-only by contract. The probe observes a broker that every producer on it
is designed to tolerate the loss of, so nothing here blocks or restarts
anything; a finding is a journald line.

Why a standalone timer rather than a check inside some service's publisher
loop: anything riding a producer's loop stops reporting exactly when that
producer is down, which is the state an operator most needs told about. A
consumer can likewise be wedged on a PEL entry while its publisher drains
normally. Broker observability stays independent of participant liveness.

The checks, per tick:

- ``used_memory`` vs ``maxmemory`` fraction - warns *before* the ``noeviction``
  cap starts refusing ``XADD`` instance-wide, so the alert precedes the
  producers' retry-WARNING flood rather than accompanying it.
- ``XLEN`` per stream, each threshold derived as that stream's own retention
  cap + margin, so a breach means the retention mechanism broke rather than
  that traffic grew. Three different caps apply here - see the constants below.
- last-entry age via ``XINFO STREAM`` for the permanently-groupless streams,
  which are invisible to any ``XPENDING``-based check.
- ``XPENDING`` on every consumer group on this node, warning only on two
  consecutive non-zero ticks - a healthy steady state is pending 0, and one
  tick of in-flight delivery is normal.
- ``XLEN > 0`` on every ``*.dlq`` key - resting state is depth 0, and every
  entry is operator-actionable. The entries are dumped to local storage on
  first sight and the finding names the service that owes it triage; see
  "The DLQ split" below.
- disk usage on ``/`` - the AOF self-bounds, but the headroom is thinner than
  the memory headroom and nothing else alerts on it.

What deliberately did **not** come with the move: the ``changes_outbox`` probe
and the dashboard's group-lag collector. Both query archiver's database or
serve archiver's UI, and both stay in that repo (archiver#193 D6). This process
holds no database credential at all.

Outbox monitoring is therefore archiver's; see `docs/STREAMS.md` for the
per-stream division of who watches what.

**The DLQ split (CannObserv/broker#1 Phase 5).** "Drainer" used to name one
role and it was two jobs. Detecting a non-resting queue, preserving its entries
and naming an addressee is mechanical, keyed on the ``*.dlq`` suffix, and needs
no idea what a payload means - so it is the broker's, and it is here. Reading
the payloads to tell residue from a real permanent failure, and the ``XTRIM``
that follows, needs a model of the messages this repo deliberately does not
have; that half belongs to the stream's own consumer, per ``DLQ_DRAINERS``.

Evidence capture is the load-bearing half. ``docs/STREAMS.md`` orders the drain
audit, back up, trim, verify - reversing it destroys what the trim needed
justifying with - and the back-up is the step an operator under time pressure
skips. Doing it on the tick that first sees the depth means the evidence exists
before anyone can reach for ``XTRIM``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from co_core.pure.adapters.bus.streams import (
    CONTENT_ARTIFACTS,
    CONTENT_BLOBS,
    CONTENT_FETCH,
    CONTENT_FETCH_POLICY,
    CONTENT_REPLICATE,
    CONTENT_REVISIONS,
    INFO_CHANGES,
    INFO_REGISTRY,
    INFO_WATCH_STATUS,
    dlq_name,
    group_name,
    stream_kind,
)
from redis.asyncio import Redis
from redis.exceptions import RedisError, ResponseError

from src.broker.logging import configure_logging, get_logger

# Literal rather than __name__: the timer runs this module via ``python -m``,
# where __name__ is "__main__" - a useless journald filter key.
logger = get_logger("src.broker.bus_health")

# Warn while writes still succeed: past this fraction of maxmemory the next
# stop is the noeviction cap, where XADD fails instance-wide for every
# producer and each one starts its retry flood.
MEMORY_WARN_FRACTION = 0.75

# Root-filesystem headroom. On this node ``/`` is where ``/var/lib/redis``
# lives, so this is the AOF's headroom and nothing else alerts on it. The
# fraction matches the state observed when archiver#130 was un-deferred (91%
# used); the absolute floor catches the same condition on a small disk where a
# healthy-looking fraction hides <2 GiB.
DISK_WARN_USED_FRACTION = 0.90
DISK_WARN_MIN_FREE_BYTES = 2 * 1024**3
DISK_PATH = "/"

# Well inside systemd's default TimeoutStartSec (90s) even if every probe in
# the tick hits its ceiling, so a hung broker is reported rather than fatal.
SOCKET_CONNECT_TIMEOUT_SECONDS = 5.0
SOCKET_TIMEOUT_SECONDS = 10.0

# Both LWW streams republish their full set on `*/5 * * * *`
# (CannObserv/watcher#264, #265), so 3x the period of silence means the
# producer is down, not slow.
LWW_WARN_LAST_ENTRY_AGE_SECONDS = 900.0
# info.registry guarantees >=1 entry/hour on a non-empty corpus via archiver's
# periodic snapshot; 2x that interval of silence means the producer is down. An
# empty stream skips the age check entirely - the corpus-size guard
# (CannObserv/archiver#147).
REGISTRY_WARN_LAST_ENTRY_AGE_SECONDS = 7200.0

# Every length threshold is its stream's retention cap plus this margin, so a
# warning means the retention mechanism itself broke rather than that traffic
# grew.
WARN_LENGTH_MARGIN = 0.10


def with_margin(cap: int) -> int:
    """The WARN threshold for a stream capped at ``cap`` entries."""
    return int(cap * (1 + WARN_LENGTH_MARGIN))


# --- the three retention caps, and the one seam the repo split created ---
#
# In archiver these were *imported* from the modules that own them. Across a
# repo boundary they cannot be, so each is mirrored here with its owner named.
# That is a real cost of CannObserv/archiver#193 D6 and it is recorded rather
# than hidden: a cap raised in its home repo and not here turns this probe's
# WARN into a false alarm (never a missed one - a stale-low threshold fires
# early, it does not go quiet). See docs/STREAMS.md, "Mirrored constants".
#
# Three different caps apply on this broker, and they are not interchangeable:
# - fact streams archiver's outbox publishes ride its operator-side periodic
#   XTRIM (ARCHIVER_REDIS_STREAM_MAXLEN);
# - info.registry is excluded from that loop and capped on every publish
#   instead, because its retention floor is a consumer boot contract;
# - the LWW streams are capped by their producer, Watcher.
FACT_PRODUCER_MAXLEN = 100_000
"""Mirrors ``DEFAULT_STREAM_MAXLEN`` in archiver's ``src/core/changes/publisher.py``."""

REGISTRY_PRODUCER_MAXLEN = 50_000
"""Mirrors ``DEFAULT_REGISTRY_STREAM_MAXLEN`` in archiver's
``src/core/changes/registry_snapshot.py``."""

LWW_PRODUCER_MAXLEN = 50_000
"""Mirrors Watcher's producer-side ``BusPublish.maxlen`` (CannObserv/watcher#265)."""

FACT_WARN_LENGTH = with_margin(FACT_PRODUCER_MAXLEN)
REGISTRY_WARN_LENGTH = with_margin(REGISTRY_PRODUCER_MAXLEN)
LWW_WARN_LENGTH = with_margin(LWW_PRODUCER_MAXLEN)

# Group names are *derived*, not mirrored - which is the whole point of
# cannobserv#384's `<service>.<stream-suffix>` convention. An operator or an
# alert rule can compute a group name from a stream name with no lookup table,
# so this repo needs no agreement with archiver about the literal string.
REVISIONS_GROUP = group_name(CONTENT_REVISIONS, "archiver")
ARTIFACTS_GROUP = group_name(CONTENT_ARTIFACTS, "archiver")
BLOBS_GROUP = group_name(CONTENT_BLOBS, "watcher")
FETCH_GROUP = group_name(CONTENT_FETCH, "replicator")
REPLICATE_GROUP = group_name(CONTENT_REPLICATE, "replicator")


# --- who owes each DLQ its triage (CannObserv/broker#1 Phase 5) ---
#
# The *key* is derived through co-core's ``dlq_name``, the same helper the
# writers use, so this cannot name a queue nothing writes. The *value* is an
# assignment and cannot be derived from anything - it is recorded in
# docs/STREAMS.md, "Who drains a DLQ", and mirrored here so the finding an
# operator actually reads carries the addressee.
#
# The rule behind the values: the drainer is the stream's consumer, because it
# is the service whose ``dead_letter()`` put the entry there and therefore the
# only one that can read it. That also costs nothing under D3 - each service
# already holds ``~<its own topic>.dlq`` in the draft ACL, where Archiver's old
# cluster-wide role would have needed instance-wide SCAN plus a grant on every
# other service's queues.
DLQ_DRAINERS: dict[str, str] = {
    dlq_name(CONTENT_REVISIONS): "archiver",
    dlq_name(CONTENT_ARTIFACTS): "archiver",
    dlq_name(CONTENT_FETCH): "replicator",
    dlq_name(CONTENT_REPLICATE): "replicator",
    dlq_name(CONTENT_BLOBS): "watcher",
    # Prospective: info.changes has no consumer group yet (CannObserv/archiver#155),
    # so nothing writes this queue. Recorded now because the day it appears is
    # the day nobody remembers who owns it.
    dlq_name(INFO_CHANGES): "replicator",
}

# Deliberately not a KeyError. A `*.dlq` key nobody claims is the "DLQ with
# nobody named" failure itself, and the broker is the only party that can even
# see one - the per-service ACL users cannot SCAN the instance. So it is
# reported as unassigned rather than skipped or fatal.
DLQ_UNASSIGNED = "no drainer assigned, broker is backstop"

# Lives inside systemd's StateDirectory, derived from --state-file rather than
# taking a second flag, so it cannot be pointed somewhere the unit's User= does
# not own.
DLQ_EVIDENCE_DIRNAME = "dlq-evidence"


@dataclass(frozen=True)
class Finding:
    """One WARN-worthy observation; ``check`` names the probe, ``subject`` the
    stream/group/resource it fired on."""

    check: str
    subject: str
    message: str


@dataclass(frozen=True)
class StreamCheck:
    """Per-stream expectations, mirroring the ``docs/STREAMS.md`` inventory."""

    topic: str
    warn_length: int | None = None
    warn_last_entry_age_seconds: float | None = None
    pending_group: str | None = None
    # Carved out of archiver's drain loop's trim set: capping a command stream
    # would delete commands the consumer group has not delivered and orphan the
    # PEL entries naming them. Growth is therefore expected, and a breach is a
    # volume milestone rather than a broken cap.
    never_trimmed: bool = False

    def __post_init__(self) -> None:
        """Refuse a ``pending_group`` on a config/state stream.

        A group on a config/state stream accumulates a PEL nothing drains:
        every worker needs every message, so no reader acks on behalf of the
        others. co-core has always stated the rule; since >=0.13.1 the taxonomy
        is machine-readable via ``stream_kind``, so the rule can be enforced
        here rather than resting on whoever edits ``STREAM_CHECKS`` next
        knowing it (cannobserv#384).

        Deliberately a hard ``ValueError`` at import time rather than a probe
        finding: this is a statement about the stream's *kind*, which cannot
        become true at runtime, so there is nothing an operator could act on
        and no reason to let the process start.

        A topic ``stream_kind`` cannot classify - a synthetic name in a test, a
        derived ``<topic>.dlq`` - is left alone rather than rejected. The guard
        exists to catch a *known* config/state stream being given a group, and
        it has no opinion about a name outside the taxonomy.

        ``stream_kind`` signals "not canonical" by raising ``ValueError``, and
        co-core publishes no public set of canonical topics to test membership
        against (``_STREAM_KINDS`` is private), so the check has to run through
        the exception. That makes this guard **fail open** if co-core ever
        raises ``ValueError`` here for a reason other than an unknown topic;
        ``test_every_canonical_stream_constant_is_classifiable`` turns that into
        a caught test failure rather than a silently disabled guard.
        """
        if self.pending_group is None:
            return
        try:
            kind = stream_kind(self.topic)
        except ValueError:
            return
        if kind == "config_state":
            raise ValueError(
                f"{self.topic} is a config_state stream and must not carry a "
                f"consumer group (got {self.pending_group!r})"
            )


STREAM_CHECKS: tuple[StreamCheck, ...] = (
    StreamCheck(INFO_CHANGES, warn_length=FACT_WARN_LENGTH),
    StreamCheck(
        INFO_REGISTRY,
        warn_length=REGISTRY_WARN_LENGTH,
        warn_last_entry_age_seconds=REGISTRY_WARN_LAST_ENTRY_AGE_SECONDS,
    ),
    StreamCheck(CONTENT_FETCH, warn_length=FACT_WARN_LENGTH, pending_group=FETCH_GROUP),
    StreamCheck(
        CONTENT_REVISIONS,
        warn_length=FACT_WARN_LENGTH,
        pending_group=REVISIONS_GROUP,
    ),
    StreamCheck(
        CONTENT_ARTIFACTS,
        warn_length=FACT_WARN_LENGTH,
        pending_group=ARTIFACTS_GROUP,
    ),
    StreamCheck(
        CONTENT_REPLICATE,
        warn_length=FACT_WARN_LENGTH,
        never_trimmed=True,
        pending_group=REPLICATE_GROUP,
    ),
    StreamCheck(
        CONTENT_FETCH_POLICY,
        warn_length=LWW_WARN_LENGTH,
        warn_last_entry_age_seconds=LWW_WARN_LAST_ENTRY_AGE_SECONDS,
    ),
    StreamCheck(
        INFO_WATCH_STATUS,
        warn_length=LWW_WARN_LENGTH,
        warn_last_entry_age_seconds=LWW_WARN_LAST_ENTRY_AGE_SECONDS,
    ),
    # content.blobs: a group row, and deliberately nothing else.
    #
    # The old "never content.blobs" rule was Archiver's *role* boundary, and a
    # neutral node has no role to be out of bounds of - so the group is probed
    # like every other. What survives the move is the part that was never about
    # roles: this repo owns no retention cap for this stream, so it states no
    # opinion on its length or its age. Neither the fact cap nor the LWW cap
    # governs it, and inventing one here would be a threshold with no owner.
    StreamCheck(CONTENT_BLOBS, pending_group=BLOBS_GROUP),
)


# --- pure evaluators ---


def evaluate_memory(*, used_memory: int, maxmemory: int) -> list[Finding]:
    """Warn on headroom pressure, and on ``maxmemory 0`` - which makes
    ``noeviction`` inert and re-opens the whole-broker OOM-kill tail."""
    if maxmemory == 0:
        return [
            Finding(
                check="memory",
                subject="redis",
                message="maxmemory is 0 - noeviction has no ceiling to enforce; "
                "see deploy/README.md (CannObserv/archiver#128)",
            )
        ]
    fraction = used_memory / maxmemory
    if fraction >= MEMORY_WARN_FRACTION:
        return [
            Finding(
                check="memory",
                subject="redis",
                message=f"used_memory {used_memory} is {fraction:.0%} of "
                f"maxmemory {maxmemory} (warn at {MEMORY_WARN_FRACTION:.0%}); "
                "at 100% XADD fails instance-wide for every producer",
            )
        ]
    return []


def evaluate_disk(*, total: int, free: int) -> list[Finding]:
    used_fraction = (total - free) / total
    if used_fraction >= DISK_WARN_USED_FRACTION or free < DISK_WARN_MIN_FREE_BYTES:
        return [
            Finding(
                check="disk",
                subject=DISK_PATH,
                message=f"{used_fraction:.0%} used, {free / 1024**3:.1f} GiB free "
                f"(warn at {DISK_WARN_USED_FRACTION:.0%} used or "
                f"<{DISK_WARN_MIN_FREE_BYTES / 1024**3:.0f} GiB free)",
            )
        ]
    return []


def evaluate_stream(
    check: StreamCheck, *, length: int, last_entry_ms: int | None, now_ms: int
) -> list[Finding]:
    findings: list[Finding] = []
    if check.warn_length is not None and length > check.warn_length:
        diagnosis = (
            "this stream is never trimmed by design (capping it would orphan "
            "undelivered commands), so this is a volume milestone - size the "
            "broker for it rather than looking for a broken cap"
            if check.never_trimmed
            else "the retention cap for this stream is not being applied"
        )
        findings.append(
            Finding(
                check="stream-length",
                subject=check.topic,
                message=f"XLEN {length} exceeds {check.warn_length} - {diagnosis}",
            )
        )
    if check.warn_last_entry_age_seconds is not None and length > 0 and last_entry_ms is not None:
        age = (now_ms - last_entry_ms) / 1000.0
        if age > check.warn_last_entry_age_seconds:
            findings.append(
                Finding(
                    check="stream-age",
                    subject=check.topic,
                    message=f"last entry is {age:.0f}s old "
                    f"(warn over {check.warn_last_entry_age_seconds:.0f}s) - "
                    "the producer's periodic republish has stopped",
                )
            )
    return findings


def evaluate_pending(check: StreamCheck, *, pending_now: int, pending_prev: int) -> list[Finding]:
    """Two-tick rule: one tick of non-zero pending is in-flight delivery;
    non-zero across two consecutive ticks means the consumer is wedged or its
    database is down."""
    if pending_now > 0 and pending_prev > 0:
        return [
            Finding(
                check="pending",
                subject=f"{check.topic}/{check.pending_group}",
                message=f"XPENDING {pending_now} for two consecutive ticks "
                f"(was {pending_prev}) - consumer wedged or DB down; messages "
                "are accruing unconsumed while the stream keeps accepting them",
            )
        ]
    return []


# --- collectors ---


def _decode(value: str | bytes) -> str:
    return value.decode(errors="replace") if isinstance(value, bytes) else str(value)


def _entry_ms(entry_id: str | bytes) -> int:
    return int(_decode(entry_id).split("-", 1)[0])


def _id_sort_key(entry_id: str | bytes) -> tuple[int, int]:
    """Order a stream id the way Redis does, as ``(ms, seq)``.

    Never compare these as strings: ``"9-0" > "10-0"`` lexicographically while
    ``9 < 10``. CannObserv/broker#1 recorded that against the watcher#285 group
    rename and it bites the same way here - a string comparison reads a 9-0
    capture as ahead of the queue and then never captures 10-0, which is wrong
    in exactly the direction that loses evidence.
    """
    ms, _, seq = _decode(entry_id).partition("-")
    return int(ms), int(seq or 0)


def _evidence_high_water(topic_dir: Path) -> tuple[int, int] | None:
    """The newest id already captured for this queue, read back from the dump
    filenames rather than from the state file.

    Keeping the high-water mark in the same directory as the evidence means the
    two cannot disagree: deleting a dump after triage correctly re-arms capture
    for those ids, and a state file restored without its dumps cannot claim a
    backup that is not there.
    """
    ids = []
    for path in topic_dir.glob("*.json"):
        try:
            ids.append(_id_sort_key(path.stem))
        except ValueError:
            continue  # not one of ours; a stray file must not disarm capture
    return max(ids, default=None)


async def _capture_dlq_evidence(client: Redis, topic: str, evidence_dir: Path) -> str:
    """Dump the not-yet-captured entries of a non-resting DLQ, and describe what
    happened in a clause the finding can carry.

    Incremental by id: a queue that fills one entry at a time would otherwise
    re-dump its whole contents on every tick that saw growth. Best-effort by
    design - the Redis read is left outside the guard so a genuinely unreachable
    broker still reports as one, while a filesystem failure degrades to a loud
    clause instead of taking the tick's only depth signal with it.
    """
    entries = await client.xrange(topic)
    topic_dir = evidence_dir / topic
    try:
        topic_dir.mkdir(parents=True, exist_ok=True)
        high_water = _evidence_high_water(topic_dir)
        fresh = [e for e in entries if high_water is None or _id_sort_key(e[0]) > high_water]
        if not fresh:
            return f"evidence already captured under {topic_dir}"
        path = topic_dir / f"{_decode(fresh[-1][0])}.json"
        path.write_text(
            json.dumps(
                [
                    {
                        "id": _decode(entry_id),
                        "fields": {_decode(k): _decode(v) for k, v in fields.items()},
                    }
                    for entry_id, fields in fresh
                ],
                indent=2,
            )
        )
        return f"{len(fresh)} entries captured at {path}"
    except OSError as e:
        return f"evidence capture FAILED ({e!r}) - audit before any XTRIM"


async def _collect_stream(
    client: Redis, check: StreamCheck, previous_pending: dict[str, int]
) -> tuple[list[Finding], dict[str, int]]:
    findings: list[Finding] = []
    pending: dict[str, int] = {}

    if not await client.exists(check.topic):
        # A stream nothing has written yet is dormancy, not a fault - the age
        # and length checks both need entries to exist before they mean much.
        return findings, pending

    info = await client.xinfo_stream(check.topic)
    length = int(info.get("length", 0))
    last_entry = info.get("last-entry")
    last_entry_ms = _entry_ms(last_entry[0]) if last_entry else None
    now_ms = int(time.time() * 1000)
    findings.extend(
        evaluate_stream(check, length=length, last_entry_ms=last_entry_ms, now_ms=now_ms)
    )

    if check.pending_group is not None:
        key = f"{check.topic}/{check.pending_group}"
        try:
            summary = await client.xpending(check.topic, check.pending_group)
        except (ResponseError, IndexError):
            # Real Redis raises NOGROUP (a ResponseError); fakeredis's reply
            # for a missing group instead crashes redis-py's parse_xpending
            # with IndexError. Both mean the same thing here.
            findings.append(
                Finding(
                    check="group-missing",
                    subject=check.topic,
                    message=f"consumer group {check.pending_group!r} does not "
                    "exist - the consumer never provisioned itself",
                )
            )
        else:
            pending_now = int(summary["pending"])
            pending[key] = pending_now
            findings.extend(
                evaluate_pending(
                    check,
                    pending_now=pending_now,
                    pending_prev=previous_pending.get(key, 0),
                )
            )
    return findings, pending


async def _collect_memory(client: Redis) -> list[Finding]:
    try:
        info = await client.info("memory")
    except ResponseError:
        # A server without INFO (fakeredis) is a probe limitation, not a broker
        # fault. Connection failures propagate to the caller's broker finding.
        return []
    return evaluate_memory(
        used_memory=int(info.get("used_memory", 0)),
        maxmemory=int(info.get("maxmemory", 0)),
    )


async def _collect_dlqs(client: Redis, *, evidence_dir: Path | None = None) -> list[Finding]:
    """Scan is filtered to stream keys, and each XLEN is guarded anyway: a stray
    non-stream ``*.dlq`` key must not raise WRONGTYPE out of this function,
    where it would be reported as "broker unreachable" and discard every other
    finding on the tick.

    ``evidence_dir`` of ``None`` reports without capturing, which is what a
    caller holding no writable state directory wants.
    """
    findings: list[Finding] = []
    async for key in client.scan_iter(match="*.dlq", _type="stream"):
        topic = _decode(key)
        try:
            depth = await client.xlen(topic)
        except ResponseError:
            continue
        if depth == 0:
            continue
        drainer = DLQ_DRAINERS.get(topic)
        owner = f"{drainer}'s to triage" if drainer else DLQ_UNASSIGNED
        parts = [f"depth {depth} - {owner}"]
        if evidence_dir is not None:
            parts.append(await _capture_dlq_evidence(client, topic, evidence_dir))
        parts.append('resting state is 0; see docs/STREAMS.md, "Who drains a DLQ"')
        findings.append(Finding(check="dlq", subject=topic, message="; ".join(parts)))
    return findings


async def collect_broker_findings(
    client: Redis, *, previous_pending: dict[str, int], evidence_dir: Path | None = None
) -> tuple[list[Finding], dict[str, int]]:
    """All Redis-side probes. An unreachable broker is itself the finding, and
    ``previous_pending`` passes through untouched so an outage does not reset
    the two-tick grace window."""
    try:
        findings = await _collect_memory(client)
        pending: dict[str, int] = {}
        for check in STREAM_CHECKS:
            stream_findings, stream_pending = await _collect_stream(client, check, previous_pending)
            findings.extend(stream_findings)
            pending.update(stream_pending)
        findings.extend(await _collect_dlqs(client, evidence_dir=evidence_dir))
    except (RedisError, OSError) as e:  # ConnectionError is an OSError subclass
        return (
            [
                Finding(
                    check="broker",
                    subject="redis",
                    message=f"broker unreachable or probe failed: {e!r}",
                )
            ],
            dict(previous_pending),
        )
    return findings, pending


# --- state file (two-tick pending memory across oneshot runs) ---


def load_state(path: Path) -> dict[str, int]:
    try:
        raw = json.loads(path.read_text())
    except (FileNotFoundError, ValueError):
        return {}
    if not isinstance(raw, dict):
        return {}
    return {str(k): int(v) for k, v in raw.items() if isinstance(v, int)}


def save_state(path: Path, pending: dict[str, int]) -> None:
    path.write_text(json.dumps(pending))


# --- orchestration ---


async def run_once(
    client: Redis,
    *,
    state_path: Path,
    disk_usage: Callable[[str], tuple[int, int, int]] = shutil.disk_usage,
    evidence_dir: Path | None = None,
) -> list[Finding]:
    """One probe tick: collect everything, WARN per finding, one summary line.

    ``disk_usage`` is injectable so tests do not inherit the host's real
    headroom. ``evidence_dir`` defaults beside the state file, so the timer
    needs only ``--state-file`` and both land inside systemd's StateDirectory.
    """
    previous_pending = load_state(state_path)
    findings, pending = await collect_broker_findings(
        client,
        previous_pending=previous_pending,
        evidence_dir=evidence_dir or state_path.parent / DLQ_EVIDENCE_DIRNAME,
    )

    total, _used, free = disk_usage(DISK_PATH)
    findings.extend(evaluate_disk(total=total, free=free))

    save_state(state_path, pending)

    for finding in findings:
        logger.warning(
            f"Bus health: {finding.message}",
            extra={"check": finding.check, "subject": finding.subject},
        )
    summary = logger.warning if findings else logger.info
    summary("Bus health summary", extra={"finding_count": len(findings)})
    return findings


def main(argv: list[str] | None = None) -> int:
    """Timer entrypoint. Always exits 0 once the probe ran - WARN-only means a
    finding is a journald line, never a failed unit. Only a probe crash (a bug
    here, not a broker state) surfaces as a non-zero exit."""
    parser = argparse.ArgumentParser(description="broker bus health probe")
    parser.add_argument("--state-file", type=Path, required=True)
    args = parser.parse_args(argv)

    configure_logging()

    redis_url = os.environ.get("BROKER_REDIS_URL")
    if not redis_url:
        # Unlike the participants, a probe with no URL is a misconfiguration
        # rather than dormancy: this unit exists only to watch a broker, and
        # this host *is* one. Reported loudly, but still exit 0 - a WARN-only
        # unit that starts failing on a config mistake trains an operator to
        # ignore it.
        logger.error("BROKER_REDIS_URL not set - nothing to probe")
        return 0

    async def _run() -> None:
        # Bounded sockets: a hung (rather than refusing) broker would otherwise
        # block until systemd's TimeoutStartSec kills the unit, turning the
        # WARN-only "broker unreachable" finding into a failed unit in exactly
        # the degraded state this probe exists to report. The timeouts surface
        # as RedisTimeoutError, which the collector already renders as that
        # finding.
        client = Redis.from_url(
            redis_url,
            socket_connect_timeout=SOCKET_CONNECT_TIMEOUT_SECONDS,
            socket_timeout=SOCKET_TIMEOUT_SECONDS,
        )
        try:
            await run_once(client, state_path=args.state_file)
        finally:
            await client.aclose()

    asyncio.run(_run())
    return 0


if __name__ == "__main__":
    sys.exit(main())
