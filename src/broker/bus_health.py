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
- ``entries-added`` per ``*.dlq`` key, against the depth it accounts for - the
  half depth cannot see, because a queue filled and emptied inside one interval
  is empty at both observations (CannObserv/broker#13). Its key going missing
  between ticks is the same judgement and the same finding name.
- disk usage on ``/`` - the AOF self-bounds, but the headroom is thinner than
  the memory headroom and nothing else alerts on it.

What deliberately did **not** come with the move: the ``changes_outbox`` probe
and the dashboard's group-lag collector. Both query archiver's database or
serve archiver's UI, and both stay in that repo (archiver#193 D6). This process
holds no database credential at all.

Outbox monitoring is therefore archiver's; see `docs/BUS-HEALTH.md` for the
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
import socket
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
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

# The policy the cap is only safe under, mirrored from deploy/redis.conf.broker.
# It is checked every tick for the same reason `maxmemory 0` is: both are ways
# the protection silently becomes inert, both arrive as a live `CONFIG SET` that
# no file records, and a policy is the one an operator is most likely to reach
# for under memory pressure - "evict something" reads safer than "refuse
# writes" and is the opposite. See docs/BUS-HEALTH.md, "`noeviction` is
# load-bearing beyond refusing writes" (CannObserv/broker#9).
BROKER_EVICTION_POLICY = "noeviction"

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
# early, it does not go quiet). See docs/BUS-HEALTH.md, "Mirrored constants".
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
# only one that can read it. It is still far cheaper under D3 than Archiver's old
# cluster-wide role, which would have needed instance-wide SCAN plus a grant on
# every other service's queues.
#
# **It did not cost nothing, which is what this comment used to say.** Each
# service already held ``~<its own topic>.dlq``, so the claim looked right - but a
# key pattern is not a deletion grant, and no drainer held ``+xdel`` on anything.
# Every name in this table was a service that could fill its queue and not empty
# it, for five of the six queues below. Found by broker#12 when a frame parked in
# ``content.replicate.dlq``; closed by a selector per drainer in
# ``deploy/redis-acl.conf``, scoped so the grant cannot reach the stream the queue
# is a copy of. A row added here now needs the matching selector or
# ``test_the_drainer_can_delete_from_every_queue_it_drains_and_can_read`` goes
# red.
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

# Derived, never spelled - the same reason group names come from ``group_name()``.
DLQ_SUFFIX = dlq_name("")

# Lives inside systemd's StateDirectory, derived from --state-file rather than
# taking a second flag, so it cannot be pointed somewhere the unit's User= does
# not own.
DLQ_EVIDENCE_DIRNAME = "dlq-evidence"


def _dlq_owner(topic: str) -> str:
    """Who owes this queue triage, in the one phrasing every finding uses.

    The depth finding, the continuity findings and the vanished-key finding all
    name the addressee, and two spellings of ``DLQ_UNASSIGNED`` would be two
    strings an alert rule has to know about.
    """
    drainer = DLQ_DRAINERS.get(topic)
    return f"{drainer}'s to triage" if drainer else DLQ_UNASSIGNED


# --- the notifier check-in (CannObserv/broker#3) ---
#
# Findings were an audience of zero: a WARN line in journald on a node nobody is
# logged into. The check-in gives them a reader, and - the part that matters more
# - makes SILENCE detectable. A dead probe, a stopped timer, a wedged `uv run` or
# a dead node all produce zero findings and zero traffic, which is
# indistinguishable from a healthy broker. So a report goes every tick regardless
# of finding_count, and notifier alarms when one fails to arrive.
#
# THE BASE URL IS A CONSTANT, NOT CONFIGURATION, and that is deliberate.
# `notifier:9001` is notifier_dev running against DEV_DATABASE_URL, the tailnet
# policy currently admits it alongside :9000, and its /health is byte-identical
# to production's - same status, same build - so a wrong port cannot be caught by
# the obvious check. Since this monitor alarms on the *absence* of check-ins, a
# one-character typo would not degrade it but invert it: check-ins land in the
# dev database, the production monitor receives nothing, and it reports a
# perfectly healthy broker as dead. The operator therefore supplies a monitor id
# and never a host or a port. Same move as `databases 1` against the db15 vector
# - make the wrong destination unnameable rather than merely discouraged.
NOTIFIER_CHECKIN_BASE = "http://notifier:9000/api/v1/monitors"
NOTIFIER_TIMEOUT_SECONDS = 10.0


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


def _evaluate_eviction_policy(policy: str | None) -> list[Finding]:
    """Warn unless the instance is running the policy the cap assumes.

    The two wrong families fail differently, and the message says which,
    because the remedy differs and one of them is invisible:

    - ``volatile-*`` evicts **only** replicator's ``replicator:cmd:*`` dedupe
      keys, because they are the only volatile keys on this instance. Nothing
      reports an eviction, so the first symptom is a window of duplicate
      fetches at live origins.
    - ``allkeys-*`` evicts stream entries, which ``evaluate_stream_continuity``
      catches - but only after the loss, and only on the next tick.

    ``None`` is a server that does not report the field, a probe limitation
    rather than a fault, and is treated the way ``evaluate_persistence`` treats
    its missing fields.
    """
    if policy is None or policy == BROKER_EVICTION_POLICY:
        return []
    if policy.startswith("volatile-"):
        consequence = (
            "it evicts ONLY replicator's replicator:cmd:* dedupe keys - the only "
            "volatile keys here - and an eviction is reported to nobody, so the "
            "first symptom is duplicate fetches at live origins"
        )
    elif policy.startswith("allkeys-"):
        consequence = (
            "it evicts stream entries, which is data loss the continuity check "
            "can only report after the fact"
        )
    else:
        consequence = "the cap is only safe under a policy that refuses writes rather than evicting"
    return [
        Finding(
            check="eviction-policy",
            subject="redis",
            message=f"maxmemory-policy is {policy!r}, not {BROKER_EVICTION_POLICY!r} - "
            f"{consequence}; see docs/BUS-HEALTH.md, "
            '"noeviction is load-bearing beyond refusing writes"',
        )
    ]


def evaluate_memory(
    *, used_memory: int, maxmemory: int, policy: str | None = None
) -> list[Finding]:
    """Warn on headroom pressure, on ``maxmemory 0`` - which makes the policy
    inert and re-opens the whole-broker OOM-kill tail - and on a policy the cap
    is not safe under.

    All three are independent, so none of them returns early over another: a
    broker can be uncapped *and* set to evict, and hiding the second behind the
    first would report half a misconfiguration.
    """
    findings = _evaluate_eviction_policy(policy)
    if maxmemory == 0:
        findings.append(
            Finding(
                check="memory",
                subject="redis",
                message="maxmemory is 0 - noeviction has no ceiling to enforce; "
                "see deploy/README.md (CannObserv/archiver#128)",
            )
        )
        return findings  # the fraction below is undefined without a ceiling
    fraction = used_memory / maxmemory
    if fraction >= MEMORY_WARN_FRACTION:
        findings.append(
            Finding(
                check="memory",
                subject="redis",
                message=f"used_memory {used_memory} is {fraction:.0%} of "
                f"maxmemory {maxmemory} (warn at {MEMORY_WARN_FRACTION:.0%}); "
                "at 100% XADD fails instance-wide for every producer",
            )
        )
    return findings


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


# State-file keys for the continuity baseline. The `@` prefix namespaces them
# away from the pending counters, which are keyed `<topic>/<group>`; a consumer
# group name cannot start with `@` under cannobserv#384's convention, and
# nothing else writes this file.
CONTINUITY_ENTRIES_KEY = "@entries-added/{topic}"
CONTINUITY_LENGTH_KEY = "@length/{topic}"


def evaluate_stream_continuity(
    check: StreamCheck,
    *,
    entries_added: int,
    entries_added_prev: int | None,
    length: int,
    length_prev: int | None,
) -> list[Finding]:
    """Warn when a stream has lost entries rather than grown past a cap.

    **Every other check in this module is an upper bound**, so a broker that has
    been emptied looks healthier than one under load. CannObserv/broker#10, filed
    after a `databases 1` restart replayed a historical `FLUSHDB` against db0:
    the broker came up with 4% of its entries and this probe reported
    `finding_count: 0` twice, correctly by its own rules.

    Length alone cannot be the signal. Three streams shrink as normal operation
    - `info.changes` rides archiver's periodic `XTRIM`, `info.registry` is capped
    on every publish and can drop most of itself in one tick (archiver#141), and
    the LWW streams carry a producer-side `maxlen`. A percentage threshold would
    either miss the wipe or cry wolf on `info.registry` every snapshot.

    ``entries-added`` is the signal that separates them, because it is monotonic
    for the life of a stream *object*: a trim removes entries while it keeps
    climbing, and it can only fall if the stream was destroyed and recreated.
    `FLUSHDB`, `FLUSHALL`, a restore from a stale snapshot and a `DEL` followed
    by a fresh `XADD` all look identical from here, which is the point - the
    probe is not diagnosing the cause, it is refusing to call an empty broker
    healthy.

    ``never_trimmed`` streams get the cheaper rule as well: nothing legitimate
    shortens them, so any decrease is a fault.
    """
    if entries_added_prev is None or length_prev is None:
        # First tick after a deploy, a state-file loss, or a new stream. The
        # two-tick pending rule declines to alarm on one observation for the
        # same reason.
        return []

    findings: list[Finding] = []
    if entries_added < entries_added_prev:
        findings.append(
            Finding(
                check="stream-reset",
                subject=check.topic,
                message=f"entries-added went BACKWARDS, {entries_added_prev} -> "
                f"{entries_added} (length {length_prev} -> {length}) - that counter "
                "is monotonic for the life of a stream, so the stream was "
                "destroyed and recreated: a flush, a stale restore, or a DEL",
            )
        )
    elif check.never_trimmed and length < length_prev:
        findings.append(
            Finding(
                check="stream-shrank",
                subject=check.topic,
                message=f"length {length_prev} -> {length} on a stream that is "
                "never trimmed by design - nothing legitimate shortens it",
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


# --- the backup's freshness, and the persistence that feeds it (broker#4) ---
#
# src/broker/backup.py ships dump.rdb hourly and records each run in a state
# file under its own StateDirectory. The probe reads it - read-only, as another
# user - because a backup that fails is a unit nobody is watching, and a backup
# that "succeeds" while shipping the same stale file is not even that. The
# notifier check-in made the probe's own silence detectable; this does the same
# for the backup's.
#
# Three hours: two missed hourly ticks plus the timer's jitter, so one slow run
# does not flap.
BACKUP_WARN_MAX_AGE_SECONDS = 3 * 3600.0
# How long changes may sit unsaved before the `save` points are judged to have
# stopped. This broker's rules are `3600 1`, `300 100`, `60 10000`, so a single
# pending change is on disk within the hour; three hours of pending changes
# means a failing BGSAVE or a full disk, and every backup since has been of
# the same file. Read from INFO rather than from the backup's state file,
# because only the server can tell "nothing to save" from "not saving": an
# idle broker's snapshot is old and correct.
SAVE_OVERDUE_WARN_SECONDS = 3 * 3600.0


def _parse_iso(value: object) -> datetime | None:
    """A timestamp out of the state file, or ``None`` for anything that is not one."""
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _hours(seconds: float) -> str:
    return f"{seconds / 3600:.1f}h"


def evaluate_backup(state: dict | None, *, now: datetime, installed: bool = True) -> list[Finding]:
    """Findings about the last RDB backup, from the job's state file.

    ``installed`` is whether the backup unit's state *directory* exists -
    systemd creates it on the unit's first start, so its absence means no
    backup unit on this node, which is a deploy checklist item and not a
    finding every ten minutes. A directory with no readable state in it is
    the finding: installed, and never completed a run.

    One finding per situation, by precedence: never succeeded; a failure newer
    than the last success (the failure names the cause); a success too old
    (the timer is not completing). The case where the job succeeds every hour
    while shipping the same stale file is not judged from here - it is the
    server's `save` points having stopped, and ``evaluate_persistence`` reads
    that from the server, which can tell idle from broken.
    """
    if state is None and not installed:
        return []
    state = state or {}
    success_at = _parse_iso(state.get("last_success_at"))
    if success_at is None:
        return [
            Finding(
                check="backup",
                subject="rdb",
                message="no successful backup on record - the backup unit has never "
                "completed a run on this node (deploy/README.md, broker#4)",
            )
        ]
    failure_at = _parse_iso(state.get("last_failure_at"))
    if failure_at is not None and failure_at > success_at:
        return [
            Finding(
                check="backup",
                subject="rdb",
                message=f"last backup attempt failed: {state.get('last_error')} "
                f"(last success {_hours((now - success_at).total_seconds())} ago)",
            )
        ]
    success_age = (now - success_at).total_seconds()
    if success_age > BACKUP_WARN_MAX_AGE_SECONDS:
        return [
            Finding(
                check="backup",
                subject="rdb",
                message=f"last successful backup is {_hours(success_age)} old (warn over "
                f"{_hours(BACKUP_WARN_MAX_AGE_SECONDS)}) - the hourly timer is not "
                "completing; check `systemctl status broker-backup.timer` and the journal",
            )
        ]
    return []


_PERSISTENCE_STATUS_FIELDS = (
    "rdb_last_bgsave_status",
    "aof_last_write_status",
    "aof_last_bgrewrite_status",
)


def _as_int(value: object) -> int | None:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def evaluate_persistence(info: dict, *, now: datetime) -> list[Finding]:
    """Warn when the server reports its persistence failing, or quietly not happening.

    ``rdb_last_bgsave_status`` is the backup's blind spot seen from the other
    side: while it is ``err`` the file the job ships stops changing.
    ``aof_last_write_status`` is worse - an AOF the server cannot write is a
    broker that will start refusing writes. And changes that have waited longer
    than ``SAVE_OVERDUE_WARN_SECONDS`` with no save at all are the same stale
    file every hour without any status going ``err``. Missing fields are a
    server without the section (fakeredis), a probe limitation rather than a
    fault.
    """
    findings = [
        Finding(
            check="persistence",
            subject="redis",
            message=f"{field} is {info[field]!r} - see `INFO persistence` and the redis-server "
            "journal; while it stays that way the on-disk copy is not being refreshed",
        )
        for field in _PERSISTENCE_STATUS_FIELDS
        if field in info and str(info[field]) != "ok"
    ]
    changes = _as_int(info.get("rdb_changes_since_last_save"))
    last_save = _as_int(info.get("rdb_last_save_time"))
    if changes and last_save is not None:
        unsaved_for = now.timestamp() - last_save
        if unsaved_for > SAVE_OVERDUE_WARN_SECONDS:
            findings.append(
                Finding(
                    check="persistence",
                    subject="redis",
                    message=f"{changes} changes unsaved for {_hours(unsaved_for)} (warn over "
                    f"{_hours(SAVE_OVERDUE_WARN_SECONDS)}) - the save points are not firing, "
                    "so dump.rdb and every backup shipped since are the same stale file",
                )
            )
    return findings


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
        noun = "entry" if len(fresh) == 1 else "entries"
        return f"{len(fresh)} {noun} captured at {path}"
    except OSError as e:
        return f"evidence capture FAILED ({e!r}) - audit before any XTRIM"


async def _collect_stream(
    client: Redis, check: StreamCheck, previous_state: dict[str, int]
) -> tuple[list[Finding], dict[str, int]]:
    findings: list[Finding] = []
    pending: dict[str, int] = {}

    entries_key = CONTINUITY_ENTRIES_KEY.format(topic=check.topic)
    length_key = CONTINUITY_LENGTH_KEY.format(topic=check.topic)

    if not await client.exists(check.topic):
        # A stream nothing has written **yet** is dormancy, not a fault - the age
        # and length checks both need entries to exist before they mean much, and
        # on this cluster an untouched topic is ordinary: `content.replicate` sat
        # at zero entries for the whole of broker#1.
        #
        # **A stream that was written and is now absent is a different thing, and
        # it used to return from here in silence** (CannObserv/broker#13 review).
        # Nothing legitimate deletes a stream on this broker - a trim leaves the
        # key, and XDEL leaves the key - so absence after a baseline is a DEL, a
        # FLUSHDB, or a restore that came up empty. Returning early skipped the
        # continuity check, the group check and both baselines, so the tick
        # reported finding_count 0 AND erased the only record the stream had ever
        # existed. That is broker#10's failure mode exactly, and it was reachable
        # on the nine topics that carry the cluster's data while the same hole
        # was being closed for dead-letter queues.
        if previous_state.get(entries_key) is not None:
            findings.append(
                Finding(
                    # The name `evaluate_stream_continuity` already uses for
                    # "this stream's identity is gone". One condition, one name.
                    check="stream-reset",
                    subject=check.topic,
                    message=f"the stream KEY is GONE - its entries-added counter stood at "
                    f"{previous_state[entries_key]} (a LIFETIME total, not a depth) last "
                    "tick and the key does not exist now, so it was deleted, flushed or "
                    "lost with the database rather than trimmed: both XTRIM and XDEL "
                    "leave the key behind",
                )
            )
        # The baseline is deliberately not carried forward, so this fires once
        # rather than every tick until someone rewrites the stream.
        return findings, pending

    info = await client.xinfo_stream(check.topic)
    length = int(info.get("length", 0))
    last_entry = info.get("last-entry")
    last_entry_ms = _entry_ms(last_entry[0]) if last_entry else None
    now_ms = int(time.time() * 1000)
    findings.extend(
        evaluate_stream(check, length=length, last_entry_ms=last_entry_ms, now_ms=now_ms)
    )

    entries_added = int(info.get("entries-added", 0))
    findings.extend(
        evaluate_stream_continuity(
            check,
            entries_added=entries_added,
            entries_added_prev=previous_state.get(entries_key),
            length=length,
            length_prev=previous_state.get(length_key),
        )
    )
    pending[entries_key] = entries_added
    pending[length_key] = length

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
                    # Three causes, and the message must not pick one: after a
                    # wipe the group was lost with its stream, and "never
                    # provisioned" would send the reader to the wrong service.
                    message=f"consumer group {check.pending_group!r} does not exist on a "
                    "stream that does, so its lag cannot be read - the consumer never "
                    "created it, runs it under another name, or it was lost when the "
                    'stream was recreated; see docs/BUS-HEALTH.md, "The bus-health probe"',
                )
            )
        else:
            pending_now = int(summary["pending"])
            pending[key] = pending_now
            findings.extend(
                evaluate_pending(
                    check,
                    pending_now=pending_now,
                    pending_prev=previous_state.get(key, 0),
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
        # Rides the section the headroom check already pays for - no second
        # call, and no grant beyond the +info brokeradmin already holds.
        policy=info.get("maxmemory_policy"),
    )


async def _collect_persistence(client: Redis) -> list[Finding]:
    try:
        info = await client.info("persistence")
    except ResponseError:
        return []  # same limitation as _collect_memory: a server without INFO
    return evaluate_persistence(info, now=datetime.now(UTC))


async def _collect_dlqs(
    client: Redis,
    *,
    previous_state: dict[str, int],
    evidence_dir: Path | None = None,
) -> tuple[list[Finding], dict[str, int]]:
    """Depth, and what the depth cannot see.

    Scan is filtered to stream keys, and the read is guarded anyway: a stray
    non-stream ``*.dlq`` key must not raise WRONGTYPE out of this function,
    where it would be reported as "broker unreachable" and discard every other
    finding on the tick.

    ``evidence_dir`` of ``None`` reports without capturing, which is what a
    caller holding no writable state directory wants.

    **An empty queue is no longer skipped, and that is the point**
    (CannObserv/broker#13). Evidence capture happens on the tick that first sees
    a non-resting queue, so with a 10-minute timer an entry written and deleted
    inside one interval used to leave nothing anywhere - and depth cannot see it,
    because the queue is empty at both observations. ``entries-added`` can: it is
    monotonic, unaffected by XDEL and XTRIM, and already this repo's authority on
    trim-versus-wipe. It costs nothing: ``XINFO STREAM`` carries ``length`` too,
    so it *replaces* the ``XLEN`` this loop already ran, on a scan that was
    happening anyway - and reading both out of one reply is also what keeps the
    floor a floor, since two reads could straddle an ``XADD``.

    This **extends** ``evaluate_stream_continuity`` rather than paralleling it -
    same ``@entries-added/<topic>`` state key, same ``stream-reset`` finding when
    the counter falls. What could not be shared is the entry point: that check
    runs over ``STREAM_CHECKS``, a declared tuple, and a dead-letter queue is
    found by ``SCAN`` precisely so the probe notices one nobody declared. The
    genuinely new judgement here is ``dlq-unobserved``, which has no analogue for
    a fact stream - no other stream has a per-entry record on disk that a drain
    can outrun.

    ``entries-added`` cannot report its own stream's deletion, so the key simply
    going missing between ticks is judged separately and given the same name -
    see ``_evaluate_vanished_dlqs``.
    """
    findings: list[Finding] = []
    totals: dict[str, int] = {}
    seen: set[str] = set()
    async for key in client.scan_iter(match=f"*{DLQ_SUFFIX}", _type="stream"):
        topic = _decode(key)
        seen.add(topic)
        entries_key = CONTINUITY_ENTRIES_KEY.format(topic=topic)
        owner = _dlq_owner(topic)
        try:
            info = await client.xinfo_stream(topic)
        except ResponseError:
            # A stray non-stream key, or one ``DEL``eted between the ``SCAN``
            # and this read. ``seen`` already declines to call it vanished on
            # this tick, so dropping its baseline as well would make a deletion
            # that raced the scan unreportable for good - the exact silence
            # ``_evaluate_vanished_dlqs`` exists to close. Carry the baseline
            # forward untouched and let the next tick, which sees the key
            # either present or absent, be the one that judges.
            if entries_key in previous_state:
                totals[entries_key] = previous_state[entries_key]
            continue
        # Depth comes out of the SAME reply as ``entries-added``, not a second
        # ``XLEN``. One round trip, and - the part that matters - ONE atomic
        # observation: read apart, an ``XADD`` landing between them raises
        # ``added`` without appearing in ``depth``, and `dlq-unobserved` would
        # then claim payloads are gone for entries still sitting in the queue,
        # which the very next tick would capture. A floor that can overstate is
        # not a floor.
        depth = int(info.get("length", 0))

        if depth:
            parts = [f"depth {depth} - {owner}"]
            if evidence_dir is not None:
                parts.append(await _capture_dlq_evidence(client, topic, evidence_dir))
            parts.append('resting state is 0; see docs/STREAMS.md, "Who drains a DLQ"')
            findings.append(Finding(check="dlq", subject=topic, message="; ".join(parts)))

        try:
            added = int(info["entries-added"])
        except (KeyError, TypeError, ValueError):
            # No ``entries-added`` before Redis 7.0, and this repo's floor is a
            # client-side assertion rather than something the probe can rely on.
            # The continuity half degrades to silence; the depth finding and its
            # evidence capture above - the load-bearing half, per this module's
            # header - must not go quiet with it.
            continue
        totals[entries_key] = added
        findings.extend(
            _evaluate_dlq_continuity(
                topic,
                owner=owner,
                depth=depth,
                added=added,
                before=previous_state.get(entries_key),
            )
        )
    findings.extend(_evaluate_vanished_dlqs(previous_state, seen=seen))
    return findings, totals


def _evaluate_vanished_dlqs(previous: dict[str, int], *, seen: set[str]) -> list[Finding]:
    """A queue whose KEY is gone, which ``entries-added`` cannot report itself.

    Every legitimate disposal leaves the key behind - ``XDEL`` per entry since
    broker#12, and the ``XTRIM MAXLEN 0`` it replaced - so a ``*.dlq`` that had a
    baseline last tick and is absent from this tick's scan was ``DEL``eted,
    flushed, or lost with the database. Without this the worst case was the
    silent one: the counter-went-backwards finding needs the stream recreated
    *before the next tick* to have anything to compare, so a queue deleted and
    left deleted passed unremarked while its evidence dumps stayed on disk.

    Fires once. The baseline is not carried into this tick's state, so the next
    tick has nothing to compare and says nothing further.
    """
    prefix = CONTINUITY_ENTRIES_KEY.format(topic="")
    findings: list[Finding] = []
    for state_key, before in sorted(previous.items()):
        topic = state_key.removeprefix(prefix)
        if topic == state_key or not topic.endswith(DLQ_SUFFIX) or topic in seen:
            continue
        owner = _dlq_owner(topic)
        findings.append(
            Finding(
                # The same name the counter-went-backwards case uses, for the
                # same reason: one condition - this queue's identity is gone -
                # should not need two names in an alert rule.
                check="stream-reset",
                subject=topic,
                message=(
                    f"the dead-letter queue is NO LONGER A STREAM on this broker - its "
                    f"entries-added counter stood at {before} (a LIFETIME total, not a "
                    "depth) last tick and this tick's scan does not return it. Deleted, "
                    "flushed, lost with the database, or replaced by a non-stream key - "
                    "not drained, because XDEL and XTRIM both leave the stream behind. "
                    f"Any evidence dump on disk for it describes a queue that no longer "
                    f"exists ({owner})"
                ),
            )
        )
    return findings


def _evaluate_dlq_continuity(
    topic: str, *, owner: str, depth: int, added: int, before: int | None
) -> list[Finding]:
    """What ``entries-added`` says happened to this queue between two ticks.

    ``before`` of ``None`` is the first sighting - no baseline, so nothing is
    claimed. Inventing one would report every queue's entire history on the
    first tick after a deploy, which is the kind of noise that teaches people to
    ignore a check.
    """
    if before is None:
        return []
    if added < before:
        # Deliberately the SAME check name `evaluate_stream_continuity` uses for
        # the declared streams. It is the same condition with the same diagnosis
        # - the counter is monotonic for the life of a stream object, so it can
        # only fall if the object was destroyed - and an alert rule should not
        # need two names for it. Only the consequence differs, so only the
        # consequence is said here.
        return [
            Finding(
                check="stream-reset",
                subject=topic,
                message=(
                    f"entries-added went BACKWARDS, {before} -> {added}, on a dead-letter "
                    f"queue - the stream was destroyed and recreated, not drained, so any "
                    f"evidence dump on disk for it describes a queue that no longer exists "
                    f"({owner})"
                ),
            )
        ]
    # A lower bound, not a count. `depth` can include entries added before the
    # last tick - those were captured then - so subtracting it can only
    # understate what vanished unseen. Understating is the right direction for a
    # check that must not cry wolf, and "at least" is the honest word for it.
    unobserved = added - before - depth
    if unobserved <= 0:
        return []
    return [
        Finding(
            check="dlq-unobserved",
            subject=topic,
            message=(
                f"at least {unobserved} entries were added and removed between ticks with no "
                f"evidence captured - entries-added {before} -> {added} against depth {depth} "
                f"({owner}); capture only runs on a tick that sees a non-resting queue, so "
                f'these payloads are gone. See docs/STREAMS.md, "Who drains a DLQ"'
            ),
        )
    ]


def _checkin_url(monitor_id: str) -> str:
    return f"{NOTIFIER_CHECKIN_BASE}/{monitor_id}/checkin"


def _http_post(url: str, data: bytes, headers: dict[str, str], timeout: float) -> int:
    """One blocking POST, returning the status code. Seam for the tests, and the
    only place this module speaks HTTP.

    ``urllib`` rather than a client library: this repo's dependency list is three
    entries long on purpose, and one POST per ten minutes does not earn a fourth.
    """
    request = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return int(response.status)
    except urllib.error.HTTPError as e:
        return int(e.code)


async def post_checkin(findings: list[Finding]) -> None:
    """Report this tick to notifier, if this node has been wired to it.

    Never raises and never changes the unit's exit status. WARN-only is the
    probe's contract and the check-in does not get to break it: a monitoring unit
    that starts failing on its own transport trains an operator to ignore it,
    which is the failure CannObserv/broker#3 exists to prevent, arriving through
    the fix.
    """
    monitor_id = os.environ.get("NOTIFIER_MONITOR_ID")
    api_key = os.environ.get("NOTIFIER_API_KEY")

    if not monitor_id and not api_key:
        # Unset is the supported default. The probe predates the check-in and a
        # node that has not been wired to notifier is not misconfigured.
        return
    if not (monitor_id and api_key):
        logger.error(
            "notifier check-in half-configured - NOTIFIER_MONITOR_ID and "
            "NOTIFIER_API_KEY must both be set; not checking in",
            extra={"has_monitor_id": bool(monitor_id), "has_api_key": bool(api_key)},
        )
        return

    payload = {
        # Our judgement, not notifier's: it does not learn this repo's taxonomy.
        "status": "alert" if findings else "ok",
        "variables": {
            "source": socket.gethostname(),
            "finding_count": len(findings),
            "findings": [
                {"check": f.check, "subject": f.subject, "message": f.message} for f in findings
            ],
        },
    }
    try:
        status = await asyncio.to_thread(
            _http_post,
            _checkin_url(monitor_id),
            json.dumps(payload).encode(),
            {"X-API-Key": api_key, "Content-Type": "application/json"},
            NOTIFIER_TIMEOUT_SECONDS,
        )
    except (OSError, ValueError) as e:
        logger.warning(f"Bus health: notifier check-in failed: {e!r}", extra={"check": "checkin"})
        return
    if not 200 <= status < 300:
        logger.warning(
            f"Bus health: notifier check-in rejected with HTTP {status}",
            extra={"check": "checkin", "status": status},
        )


async def collect_broker_findings(
    client: Redis, *, previous_state: dict[str, int], evidence_dir: Path | None = None
) -> tuple[list[Finding], dict[str, int]]:
    """All Redis-side probes, and the memory the next tick needs.

    The returned mapping is the whole of what carries between ticks: per-group
    pending counts for the two-tick grace rule, and per-queue ``entries-added``
    for the DLQ continuity check. ``previous_state`` passes through **untouched**
    on an unreachable broker, so an outage resets neither - a grace window that
    restarted on every blip would never warn, and a dropped DLQ baseline would
    report the outage as a drain.
    """
    try:
        findings = await _collect_memory(client)
        findings.extend(await _collect_persistence(client))
        state: dict[str, int] = {}
        for check in STREAM_CHECKS:
            stream_findings, stream_pending = await _collect_stream(client, check, previous_state)
            findings.extend(stream_findings)
            state.update(stream_pending)
        dlq_findings, dlq_totals = await _collect_dlqs(
            client, previous_state=previous_state, evidence_dir=evidence_dir
        )
        findings.extend(dlq_findings)
        state.update(dlq_totals)
    except (RedisError, OSError) as e:  # ConnectionError is an OSError subclass
        return (
            [
                Finding(
                    check="broker",
                    subject="redis",
                    message=f"broker unreachable or probe failed: {e!r}",
                )
            ],
            dict(previous_state),
        )
    return findings, state


# --- state file (what one oneshot run tells the next) ---


def load_state(path: Path) -> dict[str, int]:
    try:
        raw = json.loads(path.read_text())
    except (FileNotFoundError, ValueError):
        return {}
    if not isinstance(raw, dict):
        return {}
    return {str(k): int(v) for k, v in raw.items() if isinstance(v, int)}


def save_state(path: Path, state: dict[str, int]) -> None:
    """Written through a temporary file and renamed, because a half-written file
    reads back as ``{}``: ``load_state`` cannot tell truncation from absence, and
    every baseline in here - the pending grace counts and the DLQ
    ``entries-added`` totals - is a comparison that silently declines to fire
    without it.

    The ``fsync`` is the half that makes the rename mean anything: rename is
    atomic against another *reader*, but against a power loss it can land while
    the bytes it points at have not, which is the truncated file again by
    another route. Flushed to disk first, so the only two states a crash can
    leave are last tick's file intact and this tick's file complete."""
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w") as f:
        f.write(json.dumps(state))
        f.flush()
        os.fsync(f.fileno())
    tmp.replace(path)


def read_backup_state(path: Path) -> dict | None:
    """The backup job's record, or ``None`` for missing or unreadable. Read
    here rather than through ``src.broker.backup`` so the probe's import graph
    stays free of the storage SDK it has no use for."""
    try:
        raw = json.loads(path.read_text())
    except (FileNotFoundError, PermissionError, ValueError):
        return None
    return raw if isinstance(raw, dict) else None


# --- orchestration ---


async def run_once(
    client: Redis,
    *,
    state_path: Path,
    disk_usage: Callable[[str], tuple[int, int, int]] = shutil.disk_usage,
    evidence_dir: Path | None = None,
    backup_state_path: Path | None = None,
) -> list[Finding]:
    """One probe tick: collect everything, WARN per finding, one summary line.

    ``disk_usage`` is injectable so tests do not inherit the host's real
    headroom. ``evidence_dir`` defaults beside the state file, so the timer
    needs only ``--state-file`` and both land inside systemd's StateDirectory.
    ``backup_state_path`` is the backup job's record; ``None`` skips the check,
    which is what dev and CI want and what only the deployed unit overrides.
    """
    previous_state = load_state(state_path)
    findings, state = await collect_broker_findings(
        client,
        previous_state=previous_state,
        evidence_dir=evidence_dir or state_path.parent / DLQ_EVIDENCE_DIRNAME,
    )

    total, _used, free = disk_usage(DISK_PATH)
    findings.extend(evaluate_disk(total=total, free=free))

    if backup_state_path is not None:
        findings.extend(
            evaluate_backup(
                read_backup_state(backup_state_path),
                now=datetime.now(UTC),
                installed=backup_state_path.parent.exists(),
            )
        )

    save_state(state_path, state)

    for finding in findings:
        logger.warning(
            f"Bus health: {finding.message}",
            extra={"check": finding.check, "subject": finding.subject},
        )
    summary = logger.warning if findings else logger.info
    summary("Bus health summary", extra={"finding_count": len(findings)})

    # Last, and after the journald lines: journald is the floor this repo never
    # gives up, so it must not be contingent on a network call succeeding.
    await post_checkin(findings)
    return findings


def main(argv: list[str] | None = None) -> int:
    """Timer entrypoint. Always exits 0 once the probe ran - WARN-only means a
    finding is a journald line, never a failed unit. Only a probe crash (a bug
    here, not a broker state) surfaces as a non-zero exit."""
    parser = argparse.ArgumentParser(description="broker bus health probe")
    parser.add_argument("--state-file", type=Path, required=True)
    parser.add_argument(
        "--backup-state-file",
        type=Path,
        default=None,
        help="the record broker-backup.service writes; omitted, the backup check is skipped",
    )
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
            await run_once(
                client,
                state_path=args.state_file,
                backup_state_path=args.backup_state_file,
            )
        finally:
            await client.aclose()

    asyncio.run(_run())
    return 0


if __name__ == "__main__":
    sys.exit(main())
