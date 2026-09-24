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
  Two of them are constants; the LWW one is ``max(mirrored default, 10 x the
  set watcher republishes)``, so its threshold is read off the stream each
  tick rather than mirrored whole (CannObserv/broker#44) - off its span, off
  what arrived since the last tick, and off a span remembered from before a
  gap entered the window (CannObserv/broker#45). A trimmed window too narrow
  for one republish a period is refused rather than read high
  (CannObserv/broker#51). The seven
  ``content.*`` streams have no cap and so no threshold: ``maxmemory`` is their
  only bound (CannObserv/broker#60; the processing pair joined them in #62).
- last-entry age via ``XINFO STREAM`` for the permanently-groupless streams,
  which are invisible to any pending-based check.
- the ``pending`` count of every consumer group on this node, warning only on
  two consecutive non-zero ticks - a healthy steady state is pending 0, and one
  tick of in-flight delivery is normal.
- the age of the oldest entry each group has **not been delivered**, by
  comparing its ``last-delivered-id`` with the stream's ``last-generated-id``.
  The pending count starts at delivery, so a consumer that has stopped calling
  ``XREADGROUP`` holds it at the healthy 0 forever - which is what reported a
  broker with a command stuck on it as having zero findings for hours on
  2026-09-16 (CannObserv/broker#20).

  Both of those, and the group's existence, come out of **one** ``XINFO
  GROUPS`` per grouped stream (CannObserv/broker#29): the reply carries
  ``pending`` beside ``last-delivered-id``, so the ``XPENDING`` that used to
  read the same group a round trip later bought nothing - and its absence from
  that list says more about a missing group than NOGROUP could, because it
  names the groups that do exist.
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
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from co_core.pure.adapters.bus.streams import (
    CONTENT_ARTIFACTS,
    CONTENT_BLOBS,
    CONTENT_DERIVED,
    CONTENT_FETCH,
    CONTENT_FETCH_POLICY,
    CONTENT_PROCESS,
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

# How many streams the headroom finding names, longest first. Since #60 the
# `content.*` streams - seven since #62 - have no length finding and
# `maxmemory` is their only bound, so without these names the warning arrives
# with headroom left and no culprit (CannObserv/broker#61). A naming aid, not a
# threshold.
MEMORY_NAMED_STREAMS = 3

# The policy the cap is only safe under, mirrored from deploy/redis.conf.broker.
# It is checked every tick for the same reason `maxmemory 0` is: both are ways
# the protection silently becomes inert, both arrive as a live `CONFIG SET` that
# no file records, and a policy is the one an operator is most likely to reach
# for under memory pressure - "evict something" reads safer than "refuse
# writes" and is the opposite. See docs/MEMORY-PROTECTION.md, "`noeviction` is
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

# The period both LWW streams republish their full set on, `*/5 * * * *`
# (CannObserv/watcher#264, #265; `info.watch-status` reads it from
# ``WATCHER_WATCH_STATUS_REPUBLISH_CRON`` and defaults to it). Spelled as a
# constant rather than folded into the age threshold below because
# ``read_set_size`` divides by it: the period is what turns a retained window
# into a count of republishes, and it was already load-bearing here -
# lengthened at home and not here, the age check below false-WARNs every tick.
# Shortened, or beaten by watcher's mutation-deferred republishes, the window
# narrows and the length check says so by name (CannObserv/broker#51): the
# rate is the one term here watcher#319's pins cannot reach.
LWW_REPUBLISH_PERIOD_SECONDS = 300.0

# 3x the period of silence means the producer is down, not slow.
LWW_WARN_LAST_ENTRY_AGE_SECONDS = 3 * LWW_REPUBLISH_PERIOD_SECONDS
# info.registry guarantees >=1 entry/hour on a non-empty corpus via archiver's
# periodic snapshot; 2x that interval of silence means the producer is down. An
# empty stream skips the age check entirely - the corpus-size guard
# (CannObserv/archiver#147).
REGISTRY_WARN_LAST_ENTRY_AGE_SECONDS = 7200.0

# How long the oldest entry a consumer group has NOT been delivered may sit
# there before the consumer is judged gone (CannObserv/broker#20).
#
# **Not a mirrored constant.** The caps below are copies of numbers owned in
# another repo; this one is owned here, because it is a property of the read
# loop as this node can observe it rather than a threshold any participant
# declares. Every live group on this broker is a blocking XREADGROUP, so delivery
# is immediate - `replicator.fetch` answered the 14:18:00Z command at 14:18:01Z -
# and five minutes is two orders of magnitude of slack over that, comfortably
# clear of normal batching.
#
# A consumer that ever moves to a schedule rather than a blocking read needs its
# own value on its row: that schedule's period plus margin, with the source
# named the way a mirrored constant names its owner. The two groups declared
# ahead of their consumers by CannObserv/broker#62 - `observo.process` and
# `watcher.derived` - take this value on the same assumption, a blocking read
# through co-core-aio's driver; observo#629 and watcher#325 are where a
# different loop would be stated.
#
# **Sized against the slowest handler on the node, not only the fastest**
# (CannObserv/broker#30). A blocking reader is not reading while it is inside a
# handler - replicator's loop reads `count=1`, handles, acks, then reads again,
# with no prefetch - so a queued entry ages for as long as the entries ahead of
# it take. The 14:18 bracket above is a `content.fetch` one. Replicator timed
# `content.replicate`'s whole handler against production-shaped GCS in
# CannObserv/replicator#96: ~0.25 s at both p50 and p95 on today's corpus, and
# 5.4 s for a blob at the 64 MiB `REPLICATOR_MAX_BLOB_BYTES` ceiling. Five
# minutes is ~55x that worst case, so the replicate row keeps the shared value
# on a measurement rather than by default.
#
# What no duration sizes is a consumer alive and not reading. Replicator names
# two: one stalled-provider attempt (a 30 s download and a 120 s create timeout,
# each with the SDK's retry deadline on top - inside five minutes, not by much),
# and recovery re-claiming its own failing entry every cycle so XREADGROUP
# never runs. The second is a state, not a duration, and to a positional check
# it looks like a gone consumer. Replicator's own case of it is fixed -
# CannObserv/replicator#98 takes a non-blocking look at the stream after every
# reclaim, so its head waits at most two handler durations - but any consumer
# can still take the shape. Both hold
# a delivered entry, which the 2026-09-16 consumer did not, so
# `evaluate_undelivered` words the finding by the group's pending count - read
# from the same reply as its position - instead of naming a stopped reader.
GROUP_WARN_UNDELIVERED_AGE_SECONDS = 300.0

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
# WARN into a false alarm (a stale-low threshold fires early, it does not go
# quiet). A cap *lowered* there and not here is the direction that misses: the
# threshold goes stale-high, and the range between the two is unreported -
# CannObserv/broker#40, a range wide enough to hold the backlog the cut was for.
# See docs/BUS-HEALTH.md, "Mirrored constants".
#
# Three different caps apply on this broker, and they are not interchangeable:
# - info.changes rides archiver's operator-side periodic XTRIM
#   (ARCHIVER_REDIS_STREAM_MAXLEN) - the only stream in its `trim_topics`
#   allowlist (CannObserv/archiver#239);
# - info.registry is excluded from that loop and capped on every publish
#   instead, because its retention floor is a consumer boot contract;
# - the LWW streams are capped by their producer, Watcher.
#
# The seven content.* streams are capped by nothing, so they carry no length
# threshold. Until CannObserv/broker#60 four of the then five borrowed the
# info.changes number, and a breach there would have said "the retention cap is
# not being applied" about a cap that does not exist; the processing pair
# joined with none of its own (CannObserv/broker#62).
CHANGES_PRODUCER_MAXLEN = 100_000
"""Mirrors ``DEFAULT_STREAM_MAXLEN`` in archiver's ``src/core/changes/publisher.py``.

Reaches ``info.changes`` only - it was ``FACT_PRODUCER_MAXLEN`` until the name
got it applied to four ``content.*`` streams it never trims
(CannObserv/broker#60)."""

REGISTRY_PRODUCER_MAXLEN = 50_000
"""Mirrors ``DEFAULT_REGISTRY_STREAM_MAXLEN`` in archiver's
``src/core/changes/registry_snapshot.py``."""

LWW_PRODUCER_MAXLEN = 500
"""Mirrors watcher's ``DEFAULT_FETCH_POLICY_STREAM_MAXLEN`` (``src/core/fetch_policy.py``)
and ``DEFAULT_WATCH_STATUS_STREAM_MAXLEN`` (``src/core/watch_status.py``), both cut
from 50k by CannObserv/watcher#292.

**The default, not the whole rule.** Watcher's ``resolve_stream_maxlen`` floors
each cap at ``LWW_RETAINED_FULL_SETS`` copies of the set being republished, so
the cap in force is ``max(500, 10 x set)`` and this number governs only while
the set is under 50 entries - it was 3 and 4 on 2026-09-22. Past that the floor
takes over, and a threshold left at 550 would have said "the retention cap is
not being applied" on every tick while it was being applied correctly, just
higher (CannObserv/broker#44). ``FullSetFloor`` is the other half; the set size
it needs is read off the stream rather than mirrored, because a set size is not
a constant anyone could mirror.
"""

LWW_RETAINED_FULL_SETS = 10
"""Mirrors ``RETAINED_FULL_SETS`` in watcher's ``src/core/bus.py`` (CannObserv/watcher#292).

The second mirrored number this stream's cap is made of, and it fails the same
way the first does: raised at home and not here the threshold goes stale-low and
warns early, lowered it goes stale-high. Unlike ``LWW_PRODUCER_MAXLEN`` it is a
multiplier rather than a bound, so it is also what decides *when* the mirrored
default stops governing at all.
"""

CHANGES_WARN_LENGTH = with_margin(CHANGES_PRODUCER_MAXLEN)
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
# The processing pair (CannObserv/broker#62, the cannobserv#486 contract): the
# command stream's one worker pool is Observo's, the fact stream's first group
# is Watcher's. Both are declared here ahead of their consumers - cannobserv
# v0.19.4 marks them *pending broker#62* under the #384 rule that a documented
# group exists on the broker - which is what lets the probe watch for them from
# the first entry either stream ever carries.
PROCESS_GROUP = group_name(CONTENT_PROCESS, "observo")
DERIVED_GROUP = group_name(CONTENT_DERIVED, "watcher")


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
    # The processing pair (CannObserv/broker#62): the driver's ``dead_letter``
    # parks an undecodable command here on Observo's behalf and an undecodable
    # fact on Watcher's, and each is the one party that can read its own.
    dlq_name(CONTENT_PROCESS): "observo",
    dlq_name(CONTENT_DERIVED): "watcher",
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
class FullSetFloor:
    """A producer that republishes its whole set on a timer and floors the
    stream's cap at ``retained_full_sets`` copies of it.

    The rule is watcher's ``resolve_stream_maxlen``: ``max(default, floor)``,
    where the caller passes ``len(set) * RETAINED_FULL_SETS`` as the floor
    (CannObserv/watcher#292). Both halves have to be here, because mirroring
    only the first is what CannObserv/broker#44 was - a cap that is correct
    today and wrong from about 54 entries per set, in the direction that says
    a working cap is broken.

    ``default_maxlen`` and ``retained_full_sets`` are mirrored constants. The third
    term - the set size - is **not mirrorable**: it is the size of watcher's
    corpus, it changes without anybody editing anything, and that is exactly
    the failure a mirror cannot be made to cover. It is read off the stream
    instead, by ``read_set_size``, which is what ``republish_period_seconds``
    is for.
    """

    #: Watcher's ``default``, and the name is its own: the cap in force is this
    #: or the floor, whichever is larger, and ``FlooredCap.maxlen`` is that.
    default_maxlen: int
    retained_full_sets: int
    republish_period_seconds: float

    def __post_init__(self) -> None:
        """Refuse the values that would make ``republished_set_size`` a crash or
        a no-op, at import time and for the same reason ``StreamCheck`` does:
        every one of them is reachable only by editing this file, which is
        exactly when a guard is worth having. A zero period is the sharp one -
        it divides.
        """
        if self.republish_period_seconds <= 0 or self.retained_full_sets <= 0:
            raise ValueError(
                "a full-set floor needs a positive republish period and multiplier, got "
                f"{self.republish_period_seconds}s x {self.retained_full_sets}"
            )


@dataclass(frozen=True)
class FlooredCap:
    """The cap a ``FullSetFloor`` puts in force, and the reading it came from.

    Every term the finding quotes, because an operator reading "XLEN 1402
    exceeds 1364" has to be able to tell that the 1364 came off this stream
    rather than out of a constant - the remedy differs, and the constant is the
    one they would go and check first.
    """

    set_size: int
    retained_full_sets: int
    #: The cap in force - ``set_size * retained_full_sets``, which by
    #: construction is above the default below.
    maxlen: int
    #: The mirrored default this cap overtook, for the finding to contrast with.
    default_maxlen: int
    #: Which reading the set size came from (CannObserv/broker#45). Named in the
    #: finding, because a remembered reading describes a window the span no
    #: longer does.
    source: SetSource


@dataclass(frozen=True)
class StreamCheck:
    """Per-stream expectations, mirroring the ``docs/STREAMS.md`` inventory."""

    topic: str
    warn_length: int | None = None
    warn_last_entry_age_seconds: float | None = None
    pending_group: str | None = None
    # How stale the oldest entry this group has not been delivered may be. The
    # contract is the consumer's read loop, not this stream's retention - which
    # is why `content.blobs` carries one while stating no opinion on its length
    # or its age (CannObserv/broker#20).
    warn_undelivered_age_seconds: float | None = None
    # In no trim path - absent from archiver's `trim_topics` allowlist
    # (CannObserv/archiver#239) and from every `+xtrim` selector in
    # deploy/redis-acl.conf (CannObserv/broker#14): capping a command stream
    # would delete commands the consumer group has not delivered and orphan the
    # PEL entries naming them. Growth is therefore expected, and such a row
    # carries no `warn_length`: there is no cap for one to mirror
    # (CannObserv/broker#60).
    never_trimmed: bool = False
    # The producer floors this stream's cap at N copies of the set it
    # republishes, so `warn_length` above is the threshold only while the set is
    # small enough for the mirrored default to win (CannObserv/broker#44).
    full_set_floor: FullSetFloor | None = None

    def __post_init__(self) -> None:
        """Refuse a ``pending_group`` on a config/state stream, an undelivered
        threshold on a row with no group at all, a ``warn_length`` on a
        never-trimmed row, and a ``full_set_floor`` that either sits on a
        never-trimmed row or disagrees with ``warn_length`` about the mirrored
        cap.

        A never-trimmed row with a ``warn_length`` is a threshold mirroring a
        cap that does not exist, which is CannObserv/broker#60: a breach could
        only mean traffic grew, and a length finding tells an operator the
        opposite. The same holds for every stream nothing trims, but only this
        flag says so on the row; the rest are pinned against
        ``docs/STREAMS.md``'s **No retention cap** by the deploy tests.

        The fourth keeps one number to one spelling. A row with a floor states
        the mirrored cap twice - once as the ``warn_length`` the length check
        compares against, once as the ``maxlen`` the floor has to beat before it
        governs - and two copies that can disagree is the shape of
        CannObserv/broker#44 in miniature, at a scale where nothing downstream
        would report the disagreement.

        The second is the cheaper guard and it is here for the same reason as
        the first. ``evaluate_undelivered`` builds its subject as
        ``<topic>/<group>``, so a threshold without a group would put the string
        ``t/None`` in front of whoever reads the alert. The collector cannot
        reach that state - it evaluates only where ``XPENDING`` found the group
        - but the evaluator is public, and an invariant asserted in one
        direction only is one half-held.

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
        if self.never_trimmed and self.warn_length is not None:
            raise ValueError(
                f"{self.topic} is never trimmed by design and carries a warn_length of "
                f"{self.warn_length} - a stream nothing caps has no cap for a threshold "
                "to mirror"
            )
        if self.full_set_floor is not None:
            if self.never_trimmed:
                raise ValueError(
                    f"{self.topic} is never trimmed by design and carries a full-set "
                    "floor - a stream nothing caps has no cap for a floor to raise"
                )
            expected = with_margin(self.full_set_floor.default_maxlen)
            if self.warn_length != expected:
                raise ValueError(
                    f"{self.topic} carries a full-set floor over default_maxlen "
                    f"{self.full_set_floor.default_maxlen} but a warn_length of "
                    f"{self.warn_length} - the row and the floor disagree about the "
                    f"mirrored cap (expected {expected})"
                )
        if self.pending_group is None:
            if self.warn_undelivered_age_seconds is not None:
                raise ValueError(
                    f"{self.topic} carries an undelivered threshold "
                    f"({self.warn_undelivered_age_seconds}) with no pending_group - there is "
                    "no group whose position it could be measured against"
                )
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


# The other half of the LWW cap, alongside LWW_PRODUCER_MAXLEN
# (CannObserv/broker#44). One object for both streams because watcher gives
# them one rule: the same default, the same floor multiplier, the same `*/5`.
LWW_FULL_SET_FLOOR = FullSetFloor(
    default_maxlen=LWW_PRODUCER_MAXLEN,
    retained_full_sets=LWW_RETAINED_FULL_SETS,
    republish_period_seconds=LWW_REPUBLISH_PERIOD_SECONDS,
)


STREAM_CHECKS: tuple[StreamCheck, ...] = (
    StreamCheck(INFO_CHANGES, warn_length=CHANGES_WARN_LENGTH),
    StreamCheck(
        INFO_REGISTRY,
        warn_length=REGISTRY_WARN_LENGTH,
        warn_last_entry_age_seconds=REGISTRY_WARN_LAST_ENTRY_AGE_SECONDS,
    ),
    # The four content.* rows below and content.blobs further down carry no
    # warn_length: nothing trims them. No producer passes a maxlen
    # (CannObserv/watcher#317, CannObserv/replicator#106) and archiver's trim
    # allowlist is info.changes alone, so maxmemory is their only bound and the
    # memory check is the finding that watches that bound (CannObserv/broker#60).
    # Retention is each producer's to decide - content.replicate's slice is
    # CannObserv/archiver#267 - and a cap one adopts brings a mirrored
    # threshold back with it, not before.
    StreamCheck(
        CONTENT_FETCH,
        pending_group=FETCH_GROUP,
        warn_undelivered_age_seconds=GROUP_WARN_UNDELIVERED_AGE_SECONDS,
    ),
    StreamCheck(
        CONTENT_REVISIONS,
        pending_group=REVISIONS_GROUP,
        warn_undelivered_age_seconds=GROUP_WARN_UNDELIVERED_AGE_SECONDS,
    ),
    StreamCheck(
        CONTENT_ARTIFACTS,
        pending_group=ARTIFACTS_GROUP,
        warn_undelivered_age_seconds=GROUP_WARN_UNDELIVERED_AGE_SECONDS,
    ),
    StreamCheck(
        CONTENT_REPLICATE,
        never_trimmed=True,
        pending_group=REPLICATE_GROUP,
        warn_undelivered_age_seconds=GROUP_WARN_UNDELIVERED_AGE_SECONDS,
    ),
    StreamCheck(
        CONTENT_FETCH_POLICY,
        warn_length=LWW_WARN_LENGTH,
        warn_last_entry_age_seconds=LWW_WARN_LAST_ENTRY_AGE_SECONDS,
        full_set_floor=LWW_FULL_SET_FLOOR,
    ),
    StreamCheck(
        INFO_WATCH_STATUS,
        warn_length=LWW_WARN_LENGTH,
        warn_last_entry_age_seconds=LWW_WARN_LAST_ENTRY_AGE_SECONDS,
        full_set_floor=LWW_FULL_SET_FLOOR,
    ),
    # content.blobs: its group's two contracts, and deliberately nothing else.
    #
    # The old "never content.blobs" rule was Archiver's *role* boundary, and a
    # neutral node has no role to be out of bounds of - so the group is probed
    # like every other. What survives the move is the part that was never about
    # roles: this repo owns no retention cap for this stream, so it states no
    # opinion on its length or its age. Neither the info.changes cap nor the
    # LWW cap governs it, and inventing one here would be a threshold with no
    # owner - the rule the four content.* rows above joined in
    # CannObserv/broker#60.
    #
    # The undelivered threshold is not a retention opinion and does not breach
    # that rule. It is a statement about `watcher.blobs`'s read loop, which this
    # node measures directly, and its owner is this repo - the same owner every
    # other group row's is.
    StreamCheck(
        CONTENT_BLOBS,
        pending_group=BLOBS_GROUP,
        warn_undelivered_age_seconds=GROUP_WARN_UNDELIVERED_AGE_SECONDS,
    ),
    # The processing pair (CannObserv/broker#62), each row in the posture of the
    # stream it is shaped like.
    #
    # content.process takes content.replicate's, not content.fetch's: a command
    # stream with one worker pool, in no trim path - no producer maxlen by
    # contract (the design of record: "no maxlen contract") and no +xtrim
    # selector on the instance naming it - so any decrease in its length is a
    # fault and there is no cap for a length threshold to mirror.
    # content.derived takes content.blobs's: one group per consuming service,
    # watcher.derived first, and no opinion on its length or its age, since a
    # cap there is Observo's call and would ride its publish.
    #
    # Both groups are declared ahead of their consumers. While nothing has
    # written a stream its row is dormant; once watcher writes content.process
    # and observo's group is not on it, the finding is group-missing every tick
    # - which for this stream IS "Observo is down", the state the design
    # surfaces on the watcher side as processing_timeout. The undelivered
    # threshold is the shared one on the shared assumption (see its comment).
    StreamCheck(
        CONTENT_PROCESS,
        never_trimmed=True,
        pending_group=PROCESS_GROUP,
        warn_undelivered_age_seconds=GROUP_WARN_UNDELIVERED_AGE_SECONDS,
    ),
    StreamCheck(
        CONTENT_DERIVED,
        pending_group=DERIVED_GROUP,
        warn_undelivered_age_seconds=GROUP_WARN_UNDELIVERED_AGE_SECONDS,
    ),
)


# --- stream ids, which both halves below read ---


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
    in exactly the direction that loses evidence. ``evaluate_undelivered``
    compares a group's position with the stream's last id through this for the
    same reason, and there the quiet direction is calling a group that is behind
    caught up.
    """
    ms, _, seq = _decode(entry_id).partition("-")
    return int(ms), int(seq or 0)


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
            f"{consequence}; see docs/MEMORY-PROTECTION.md, "
            '"noeviction is load-bearing beyond refusing writes"',
        )
    ]


def evaluate_memory(
    *,
    used_memory: int,
    maxmemory: int,
    policy: str | None = None,
    stream_lengths: Mapping[str, int] | None = None,
) -> list[Finding]:
    """Warn on headroom pressure, on ``maxmemory 0`` - which makes the policy
    inert and re-opens the whole-broker OOM-kill tail - and on a policy the cap
    is not safe under.

    All three are independent, so none of them returns early over another: a
    broker can be uncapped *and* set to evict, and hiding the second behind the
    first would report half a misconfiguration.

    ``stream_lengths`` only names the longest streams on a headroom finding
    already raised; a length never raises one (CannObserv/broker#61). Entries,
    not bytes: a ``content.blobs`` entry is far larger than an ``info.*`` one,
    so the list says where to look first, not which stream holds the memory.
    Only ``STREAM_CHECKS`` streams are in it: a ``*.dlq`` is found later in
    the tick, and any depth it has is already its own ``dlq`` finding.
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
        message = (
            f"used_memory {used_memory} is {fraction:.0%} of "
            f"maxmemory {maxmemory} (warn at {MEMORY_WARN_FRACTION:.0%}); "
            "at 100% XADD fails instance-wide for every producer"
        )
        longest = sorted(
            ((topic, length) for topic, length in (stream_lengths or {}).items() if length > 0),
            key=lambda item: (-item[1], item[0]),
        )[:MEMORY_NAMED_STREAMS]
        if longest:
            named = ", ".join(f"{topic} {length}" for topic, length in longest)
            message += (
                f"; longest checked streams: {named} "
                "(entries, not bytes; a DLQ's depth is its own finding)"
            )
        findings.append(Finding(check="memory", subject="redis", message=message))
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


def _periods_spanned(floor: FullSetFloor, *, first_entry_ms: int, last_entry_ms: int) -> int:
    """Whole republish periods between the oldest and newest retained entry."""
    return round((last_entry_ms - first_entry_ms) / 1000.0 / floor.republish_period_seconds)


def republished_set_size(
    floor: FullSetFloor, *, length: int, first_entry_ms: int, last_entry_ms: int
) -> int:
    """How many entries one republish of the full set puts on this stream, read
    off the retained window's span.

    The number the mirrored cap cannot carry (CannObserv/broker#44), read off
    **one** ``XINFO STREAM`` reply - the same reply the length and the
    last-entry age come out of. The one reading here that needs no history, so
    it is the one that speaks on the first tick after a deploy; ``read_set_size``
    adds the two that do.

    The arithmetic. A stream republished every ``republish_period_seconds``
    holds, between its oldest and newest retained entry, one republish per
    period that has elapsed. So ``length`` divided by that count is entries per
    republish - and it stays entries per republish whether or not the cap is
    being applied, which is what makes it safe to raise a threshold on: an
    untrimmed stream grows its span in step with its length.

    **Read high, not low, in two places.** The oldest retained entries are a
    *partial* set (``MAXLEN ~`` drops whole macro nodes, not whole republishes),
    so the window is one fragment followed by whole sets; dividing by the whole
    sets alone charges the fragment to them, and the ceiling rounds what is
    left. Both round the reading up, because up cannot invent a broken cap - it
    can only delay reporting a real one by a tick or two, against a failure
    whose whole shape is growth without bound.

    **A window narrower than one period is one republish** - on a stream not
    yet at its cap. A cap that trimmed *inside* a set would leave the stream
    below one full set, the partial-replay failure ``RETAINED_FULL_SETS`` exists
    to prevent, so such a window holds a complete republish and ``length`` is
    the set. On a *trimmed* stream the same window is the signature of
    republishing faster than the period, and ``read_set_size`` refuses it before
    this is reached (CannObserv/broker#51).

    What it assumes is a **uniform** window: every republish in it the same
    size, one per period. A gap, a set that changed size, and a producer
    republishing more often than its period each break that; ``read_set_size``
    is where each is answered, and docs/BUS-HEALTH.md, *A window that is not
    uniform*, has the replayed numbers.
    """
    whole_sets = _periods_spanned(floor, first_entry_ms=first_entry_ms, last_entry_ms=last_entry_ms)
    if whole_sets < 1:
        return length
    return -(-length // whole_sets)


#: Where a set reading came from, for the finding to name.
SetSource = Literal["span", "since-last-tick", "remembered"]


@dataclass(frozen=True)
class RememberedSet:
    """A span reading carried to later ticks, and the entry that anchors it
    (CannObserv/broker#45).

    The anchor is the newest entry of the window the reading was taken from.
    While that entry is still retained, the current window still contains the
    one the reading described - so a gap or a shrinking set that entered the
    window since has not yet trimmed out, and the older reading is the better
    one. Once the entry is trimmed, the distortion it was covering is gone with
    it. That is the staleness rule, and it needs no constant: it expires when
    the window says so, which after a gap is exactly when the gap leaves.

    The anchor has to be current for that to hold, so it moves on every tick
    whose window could not be hiding a gap or a step up - one no wider than
    ``retained_full_sets`` periods - and otherwise only for a reading at least as
    large. Moving it only for a larger one left it pinned to the fencepost's
    high reading while the window slid past it, so it had often expired before
    the tick it was kept for.
    """

    set_size: int
    anchor_ms: int

    def still_retained(self, *, first_entry_ms: int, last_entry_ms: int) -> bool:
        """Past the newest entry is a restore of an older snapshot; before the
        oldest is trimmed, or a recreated stream. Both mean the window it
        described is gone."""
        return first_entry_ms <= self.anchor_ms <= last_entry_ms


@dataclass(frozen=True)
class SetBaseline:
    """What one tick tells the next about a full-set stream.

    ``entries_added`` is the continuity baseline's own key, not a copy of it:
    one counter, one spelling in the state file.
    """

    entries_added: int | None = None
    last_entry_ms: int | None = None
    remembered: RememberedSet | None = None
    #: Consecutive ticks, this one included, on which a narrow window left no
    #: reading at all - the two-tick rule ``evaluate_pending`` also keeps.
    narrow_ticks: int = 0

    @classmethod
    def from_state(cls, topic: str, state: Mapping[str, int]) -> SetBaseline:
        size = state.get(SET_SIZE_KEY.format(topic=topic))
        anchor = state.get(SET_ANCHOR_KEY.format(topic=topic))
        return cls(
            entries_added=state.get(CONTINUITY_ENTRIES_KEY.format(topic=topic)),
            last_entry_ms=state.get(SET_LAST_ENTRY_KEY.format(topic=topic)),
            remembered=(
                RememberedSet(set_size=size, anchor_ms=anchor)
                if size is not None and anchor is not None
                else None
            ),
            narrow_ticks=state.get(SET_NARROW_TICKS_KEY.format(topic=topic), 0),
        )

    def to_state(self, topic: str) -> dict[str, int]:
        state: dict[str, int] = {}
        if self.entries_added is not None:
            state[CONTINUITY_ENTRIES_KEY.format(topic=topic)] = self.entries_added
        if self.last_entry_ms is not None:
            state[SET_LAST_ENTRY_KEY.format(topic=topic)] = self.last_entry_ms
        if self.remembered is not None:
            state[SET_SIZE_KEY.format(topic=topic)] = self.remembered.set_size
            state[SET_ANCHOR_KEY.format(topic=topic)] = self.remembered.anchor_ms
        if self.narrow_ticks:
            state[SET_NARROW_TICKS_KEY.format(topic=topic)] = self.narrow_ticks
        return state


@dataclass(frozen=True)
class SetReading:
    """One tick's answer to *how large is the set*, and what it carries forward.

    ``set_size`` is ``None`` when there is no reading to raise the threshold on,
    and the mirrored default governs. ``narrow_periods`` says why when the
    reason is the window itself (CannObserv/broker#51), so the finding can - and
    until ``withheld`` clears, on the second such tick in a row, there is no
    finding to say it in.
    """

    set_size: int | None = None
    source: SetSource | None = None
    narrow_periods: int | None = None
    carry: SetBaseline = SetBaseline()

    @property
    def withheld(self) -> bool:
        """A narrow window with no reading, on the first tick in a row: no
        length opinion at all. The straddle of a deep cut is one tick - a set
        that shrinks by more than a republish trims past every anchor in one
        ``XADD`` - and a faster republish is not. A broken cap costs nothing
        here: while the window is narrow its span is under ``retained_full_sets``
        periods, and it has to pass eleven before any threshold could report it.
        """
        return self.set_size is None and self.carry.narrow_ticks == 1


def set_size_since_last_tick(
    floor: FullSetFloor,
    *,
    entries_added: int | None,
    last_entry_ms: int,
    baseline: SetBaseline | None,
) -> int | None:
    """Entries per republish between the last tick's newest entry and this one's.

    The set as it is **now**, where the span reads the set averaged over the
    whole window - which is what a set that steps up needs (CannObserv/broker#45):
    the window then holds old sets and new, and the span reads between them for
    most of an hour. ``entries-added`` and the newest id come out of one reply
    on each tick, so the difference counts exactly the entries after the last
    tick's newest one, and the ids' own clock counts the periods between them -
    not the timer's, which is what #44's objection to tick-to-tick deltas was.

    ``None`` rather than a guess for what it cannot divide: no baseline, a
    counter that went backwards (a recreated stream, which ``stream-reset``
    reports), nothing added, or no whole period between the two newest entries.
    Like the span it counts republishes by the clock, so it reads a gap low
    across the one tick that straddles the producer's return - the remembered
    reading covers that tick - and it reads a faster republish high, so on a
    narrow window it stands only where the length vouches for it
    (``read_set_size``).
    """
    if (
        baseline is None
        or entries_added is None
        or baseline.entries_added is None
        or baseline.last_entry_ms is None
    ):
        return None
    added = entries_added - baseline.entries_added
    republishes = round(
        (last_entry_ms - baseline.last_entry_ms) / 1000.0 / floor.republish_period_seconds
    )
    if added <= 0 or republishes < 1:
        return None
    return -(-added // republishes)


def read_set_size(
    floor: FullSetFloor,
    *,
    length: int,
    first_entry_ms: int | None,
    last_entry_ms: int | None,
    entries_added: int | None = None,
    baseline: SetBaseline | None = None,
) -> SetReading:
    """The set size this tick raises the threshold on, as the largest of three
    readings it can trust, and what the next tick needs.

    - **the span** (``republished_set_size``): one reply, no history;
    - **since the last tick** (``set_size_since_last_tick``): the set now, for a
      set that stepped up or shrank;
    - **remembered**: the span as read before a gap or a shrinking set entered
      the window, for as long as that window is still retained.

    The largest, because each fails low in its own case and the case is
    transient: a gap reads the span low for a window's length and the
    since-last-tick reading low for one tick; a step up reads the span low for
    most of an hour. Each high failure is bounded as well, since a remembered
    reading dies with its anchor and a broken cap grows the stream past any
    fixed reading. The replayed numbers are in docs/BUS-HEALTH.md, *A window that
    is not uniform*.

    **A narrow trimmed window refuses the span** (CannObserv/broker#51). It
    counts republishes by the clock, so a producer republishing ``R`` times a
    period reads ``R`` times the set, and the threshold goes quiet by the same
    factor. A stream that has been trimmed (``entries-added`` past its length,
    out of the same reply) is at its cap, and at one republish a period
    ``retained_full_sets`` whole sets cannot be held in fewer than
    ``retained_full_sets - 1`` periods - the fencepost, so the tolerance is not
    a margin anyone chose. Narrower than that, the span is not a set size. A
    stream not yet trimmed is filling towards its cap, where a narrow window is
    ordinary and the reading is right, so the refusal does not reach it; nor
    does it reach a server that omits ``entries-added``, which this repo's Redis
    floor rules out.

    **At its cap, the length is a second equation.** One republish a period and
    a correctly floored cap hold the stream at ``retained_full_sets`` times the
    set - so on a narrow window a since-last-tick reading stands if that many of
    it fit in the length, and not otherwise. A set that just shrank passes: the
    window is narrow because the old, larger sets were cut, and the reading is
    the new size. A faster republish fails by a factor of ``R``. The one-equation
    shape #51 describes is what the refusal answers; this is the case where the
    stream supplies the second.

    Nothing left, a narrow window is **withheld** for one tick and goes to the
    mirrored default on the second (``SetReading.withheld``): the direction
    ``floor_in_force`` takes for a reply missing the ids, and loud about a
    faster republish, which is a mirror that stopped holding.

    Only the span is remembered. The since-last-tick reading can land mid-burst
    and read up to half a set high; kept, that would stand for a window's length
    rather than a tick.
    """
    remembered = baseline.remembered if baseline is not None else None
    if length <= 0 or first_entry_ms is None or last_entry_ms is None:
        return SetReading(
            carry=SetBaseline(
                entries_added=entries_added, last_entry_ms=last_entry_ms, remembered=remembered
            )
        )
    if remembered is not None and not remembered.still_retained(
        first_entry_ms=first_entry_ms, last_entry_ms=last_entry_ms
    ):
        remembered = None

    n = floor.retained_full_sets
    periods = _periods_spanned(floor, first_entry_ms=first_entry_ms, last_entry_ms=last_entry_ms)
    trimmed = entries_added is not None and entries_added > length
    narrow = trimmed and periods < n - 1

    readings: list[tuple[int, SetSource]] = []
    span: int | None = None
    if not narrow:
        span = republished_set_size(
            floor, length=length, first_entry_ms=first_entry_ms, last_entry_ms=last_entry_ms
        )
        readings.append((span, "span"))
    since = set_size_since_last_tick(
        floor, entries_added=entries_added, last_entry_ms=last_entry_ms, baseline=baseline
    )
    if since is not None and (not narrow or since * n <= length):
        readings.append((since, "since-last-tick"))
    if remembered is not None:
        readings.append((remembered.set_size, "remembered"))

    if span is not None and (remembered is None or periods <= n or span >= remembered.set_size):
        remembered = RememberedSet(set_size=span, anchor_ms=last_entry_ms)
    narrow_ticks = (baseline.narrow_ticks if baseline is not None else 0) + 1
    carry = SetBaseline(
        entries_added=entries_added,
        last_entry_ms=last_entry_ms,
        remembered=remembered,
        narrow_ticks=narrow_ticks if narrow and not readings else 0,
    )
    if not readings:
        return SetReading(narrow_periods=periods, carry=carry)
    # max() keeps the first of equals, so a tie is credited to the reading
    # that needs the least history
    set_size, source = max(readings, key=lambda reading: reading[0])
    return SetReading(
        set_size=set_size,
        source=source,
        narrow_periods=periods if narrow else None,
        carry=carry,
    )


def floor_in_force(check: StreamCheck, reading: SetReading) -> FlooredCap | None:
    """The cap this stream's full-set floor puts in force, or ``None``.

    ``None`` means the mirrored default governs - either because the floor is
    under it (``max(default, 10 x set)``, watcher's rule and not ``10 x set``,
    so a reading can only ever *raise* the threshold and never hand back the
    blindness CannObserv/broker#40 closed), or because there is no reading to
    raise it on: a reply without the ids, or a window ``read_set_size`` refused.
    Both are treated the way every other missing field here is: fall back, and
    fall back to the threshold that warns early rather than the one that goes
    quiet.
    """
    floor = check.full_set_floor
    if floor is None or reading.set_size is None or reading.source is None:
        return None
    maxlen = reading.set_size * floor.retained_full_sets
    if maxlen <= floor.default_maxlen:
        return None
    return FlooredCap(
        set_size=reading.set_size,
        retained_full_sets=floor.retained_full_sets,
        maxlen=maxlen,
        default_maxlen=floor.default_maxlen,
        source=reading.source,
    )


# How a finding names each reading's source, completing "10 x the N-entry set ...".
_SET_SOURCE_PHRASES: dict[SetSource, str] = {
    "span": "this stream's own span says it republishes",
    "since-last-tick": "it has republished since the last tick",
    "remembered": "read off an earlier window this stream still retains",
}


def evaluate_stream(
    check: StreamCheck,
    *,
    length: int,
    last_entry_ms: int | None,
    now_ms: int,
    first_entry_ms: int | None = None,
    set_reading: SetReading | None = None,
) -> list[Finding]:
    """Length and last-entry age for one stream.

    ``set_reading`` is ``read_set_size``'s answer for a row with a full-set
    floor. Without one the span alone is read off ``first_entry_ms`` - the
    single-reply reading, which is what a caller with no history has.
    """
    findings: list[Finding] = []
    floor = check.full_set_floor
    if set_reading is None and floor is not None:
        set_reading = read_set_size(
            floor, length=length, first_entry_ms=first_entry_ms, last_entry_ms=last_entry_ms
        )
    floored = floor_in_force(check, set_reading) if set_reading is not None else None
    warn_length = with_margin(floored.maxlen) if floored is not None else check.warn_length
    if set_reading is not None and set_reading.withheld:
        warn_length = None
    if warn_length is not None and length > warn_length:
        if floored is not None:
            diagnosis = (
                "the retention cap for this stream is not being applied - the cap in "
                f"force is the producer's full-set floor, {floored.retained_full_sets} x "
                f"the {floored.set_size}-entry set "
                f"{_SET_SOURCE_PHRASES[floored.source]}, not the mirrored "
                f"{floored.default_maxlen}"
            )
        elif (
            floor is not None and set_reading is not None and set_reading.narrow_periods is not None
        ):
            diagnosis = (
                "the retention cap for this stream is not being applied, or its producer "
                f"republishes more often than every {floor.republish_period_seconds:.0f}s: "
                f"the trimmed window spans {set_reading.narrow_periods} republish periods "
                f"where {floor.retained_full_sets} full sets need at least "
                f"{floor.retained_full_sets - 1}, so no set size can be read off it and the "
                f"mirrored {floor.default_maxlen} is the threshold (CannObserv/broker#51)"
            )
        else:
            diagnosis = "the retention cap for this stream is not being applied"
        findings.append(
            Finding(
                check="stream-length",
                subject=check.topic,
                message=f"XLEN {length} exceeds {warn_length} - {diagnosis}",
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
# The full-set streams' baseline beside it (CannObserv/broker#45), read and
# written through ``SetBaseline``. Its entries-added is the key above.
SET_LAST_ENTRY_KEY = "@set-last-entry-ms/{topic}"
SET_SIZE_KEY = "@set-size/{topic}"
SET_ANCHOR_KEY = "@set-anchor-ms/{topic}"
SET_NARROW_TICKS_KEY = "@set-narrow-ticks/{topic}"


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


def group_is_behind(*, last_generated_id: str | None, last_delivered_id: str | None) -> bool:
    """Whether a group has entries it has not been delivered - by *position*.

    The whole of CannObserv/broker#20 is in the word position. The three
    counters that look like they answer this do not:

    - ``XPENDING`` counts entries **delivered and not acked**. A consumer that
      has stopped calling ``XREADGROUP`` is delivered nothing, so it holds the
      healthy value, 0, forever. That is what reported a broker with a command
      stuck on it as having zero findings for hours on 2026-09-16.
    - ``XINFO GROUPS`` ``lag`` is wrong in both directions on this Redis
      (7.0.15). Measured after that reboot: ``watcher.blobs`` 152,
      ``archiver.revisions`` 147 and ``replicator.fetch`` 153, while the first
      two were at the stream's ``last-generated-id`` and the third was behind by
      exactly **one**. A lag-based check would have raised three findings, two
      false, and misstated the real one.
    - consumer ``idle`` read 869 s for every consumer on every group at the same
      moment - the time since the AOF load, not since each consumer's last read.
      So it is blind in the window right after a restart, which is when a
      consumer is likeliest not to have come back.

    Two ids and an inequality have none of those failure modes. ``None`` on
    either side is a reply that did not carry the field, which is a probe
    limitation and not a fault, and ``>=`` rather than ``!=`` is deliberate: the
    two ids come from two replies, so a delivery of an entry added between them
    puts the group *ahead* of the last id the probe read.
    """
    if last_generated_id is None or last_delivered_id is None:
        return False
    return _id_sort_key(last_delivered_id) < _id_sort_key(last_generated_id)


def evaluate_undelivered(
    check: StreamCheck,
    *,
    last_generated_id: str | None,
    last_delivered_id: str | None,
    oldest_undelivered_id: str | None,
    now_ms: int,
    pending: int,
) -> list[Finding]:
    """How long the group's oldest undelivered entry has been waiting.

    ``oldest_undelivered_id`` is the id of the first entry after the group's
    position, or ``None`` where there is none - which means two different things
    and the caller cannot tell them apart, so this does:

    - the group is not behind, and nothing was looked for: healthy;
    - the group **is** behind and the entries it is behind by no longer exist:
      they were trimmed or deleted before delivery, so there is nothing to date
      and nothing that will ever arrive. Its own finding, because reporting it
      as an age would send the reader after a stopped consumer instead of after
      whatever removed undelivered entries - which is the hazard
      ``content.replicate`` is carved out of every trim path for, happening on a
      stream that is not carved out.

    A row with no threshold says nothing at all: the consumer contract is what
    the threshold *is*, and a stream whose group nobody has sized a threshold
    for is one this repo has no opinion about yet.

    ``pending`` words the finding, and never decides it. A group holding
    nothing has nothing in a handler - for a consumer that acks after handling,
    as replicator's does (CannObserv/replicator#96) - so its consumer has
    stopped reading. A group holding a delivery has a consumer that took one and has not read
    since - inside one long attempt, re-claiming its own retry instead of
    reading (replicator's shape until CannObserv/replicator#98), or gone while
    holding it. The position
    cannot tell those apart, and the message says so rather than naming the
    first (CannObserv/broker#30).
    """
    if check.warn_undelivered_age_seconds is None:
        return []
    if not group_is_behind(
        last_generated_id=last_generated_id, last_delivered_id=last_delivered_id
    ):
        return []
    subject = f"{check.topic}/{check.pending_group}"
    if oldest_undelivered_id is None:
        return [
            Finding(
                check="group-undelivered-lost",
                subject=subject,
                message=f"the group is at {last_delivered_id} on a stream whose last id is "
                f"{last_generated_id}, and NOTHING remains after its position - the entries "
                "it had not been delivered were trimmed or deleted rather than consumed, so "
                "this group's consumer never received them and never will",
            )
        ]
    age = (now_ms - _entry_ms(oldest_undelivered_id)) / 1000.0
    if age <= check.warn_undelivered_age_seconds:
        return []
    head = (
        f"oldest UNDELIVERED entry {oldest_undelivered_id} is {age:.0f}s old "
        f"(warn over {check.warn_undelivered_age_seconds:.0f}s) - the group is at "
        f"{last_delivered_id}, the stream at {last_generated_id}. "
    )
    if pending == 0:
        body = (
            "Nothing was delivered, so XPENDING reads the healthy 0: this is a consumer that "
            "has stopped READING, not one that is slow to ack. Check it is running and "
            "connected (CLIENT LIST, `user=`)"
        )
    else:
        body = (
            f"Pending is {pending} - delivered and not acked - so the group's consumer took "
            "delivery and has not read since: inside one long handler, re-claiming its own "
            "retry instead of reading (the shape replicator had until "
            "CannObserv/replicator#98), or gone "
            "while holding it. A connected consumer is not the all-clear here; run "
            f"`XPENDING {check.topic} {check.pending_group} - + 10` twice - a delivery count "
            "that climbs is a consumer alive and retrying"
        )
    tail = '; see docs/UNDELIVERED-CONSUMERS.md, "Consumers that stopped reading"'
    return [Finding(check="group-undelivered", subject=subject, message=head + body + tail)]


def evaluate_pending(check: StreamCheck, *, pending_now: int, pending_prev: int) -> list[Finding]:
    """Two-tick rule: one tick of non-zero pending is in-flight delivery;
    non-zero across two consecutive ticks means the consumer is wedged or its
    database is down.

    The count is the group's ``pending`` field, which since CannObserv/broker#29
    comes out of the same ``XINFO GROUPS`` reply as its position rather than
    from an ``XPENDING`` of its own - the same number either way, which is why
    the message names the count and not a command.
    """
    if pending_now > 0 and pending_prev > 0:
        return [
            Finding(
                check="pending",
                subject=f"{check.topic}/{check.pending_group}",
                message=f"pending {pending_now} for two consecutive ticks "
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
    # The oldest *retained* entry, which with the newest is the span a full-set
    # stream's cap is read off (CannObserv/broker#44). Out of the reply the
    # length and the age already came from, so it costs no round trip.
    first_entry = info.get("first-entry")
    first_entry_ms = _entry_ms(first_entry[0]) if first_entry else None
    now_ms = int(time.time() * 1000)
    entries_added = int(info.get("entries-added", 0))
    set_reading: SetReading | None = None
    if check.full_set_floor is not None:
        # The one reading that needs history (CannObserv/broker#45, #51); its
        # baseline rides the state file beside the continuity one.
        set_reading = read_set_size(
            check.full_set_floor,
            length=length,
            first_entry_ms=first_entry_ms,
            last_entry_ms=last_entry_ms,
            entries_added=entries_added,
            baseline=SetBaseline.from_state(check.topic, previous_state),
        )
    findings.extend(
        evaluate_stream(
            check,
            length=length,
            last_entry_ms=last_entry_ms,
            now_ms=now_ms,
            first_entry_ms=first_entry_ms,
            set_reading=set_reading,
        )
    )

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
    if set_reading is not None:
        pending.update(set_reading.carry.to_state(check.topic))

    if check.pending_group is not None:
        key = f"{check.topic}/{check.pending_group}"
        # ONE reply for the group's whole state (CannObserv/broker#29).
        # ``XINFO GROUPS`` has always carried ``pending`` beside
        # ``last-delivered-id``, so the ``XPENDING`` that used to precede it was
        # a second round trip reading the same group a moment later - the shape
        # broker#13 took out of the DLQ scan, where two reads an ``XADD`` can
        # land between are not one observation. Nothing compared the two here
        # yet, so that half was cost rather than a bug; the finding below is the
        # part that gets better.
        #
        # The stream is known to exist. ``XINFO GROUPS`` raises "no such key" on
        # one deleted since the ``exists`` above, which is the race the
        # ``XINFO STREAM`` two lines up already answers the same way: the tick
        # ends as a ``broker`` finding with ``previous_state`` passed through
        # untouched, so the next tick - which sees the key simply absent -
        # reports the deletion as ``stream-reset`` off an intact baseline.
        groups = await client.xinfo_groups(check.topic)
        position = next(
            (g for g in groups if _decode(g.get("name", "")) == check.pending_group), None
        )
        if position is None:
            # Absence from the list, where it used to be ``XPENDING`` answering
            # NOGROUP (a ResponseError on real Redis, an IndexError out of
            # redis-py's parse_xpending on fakeredis - both the same condition).
            # Both servers report a group-less stream the same way here - an
            # empty list - which is why one branch now does: asserted against a
            # real 7.0 server by ``tests/deploy/test_bus_health_group_positions
            # .py::test_a_group_less_stream_answers_the_way_fakeredis_does``.
            findings.append(_evaluate_group_missing(check, groups))
        else:
            pending_now = int(position["pending"])
            pending[key] = pending_now
            findings.extend(
                evaluate_pending(
                    check,
                    pending_now=pending_now,
                    pending_prev=previous_state.get(key, 0),
                )
            )
            findings.extend(
                await _collect_undelivered(
                    client,
                    check,
                    position=position,
                    last_generated_id=info.get("last-generated-id"),
                )
            )
    return findings, pending


def _evaluate_group_missing(check: StreamCheck, groups: list[dict[str, object]]) -> Finding:
    """The group is absent from its stream's group list - and what else is on it.

    Three causes, and the old message could pick none of them: it asked
    ``XPENDING`` about one name and got NOGROUP, which says nothing about what
    else is there. The list the check now reads does (CannObserv/broker#29), and
    it separates the causes in both directions:

    - **Other groups, but not this one.** The probe asks for the name co-core's
      ``group_name()`` derives (cannobserv#384); a consumer running under
      another name looks healthy to itself and is probed by nobody. That is the
      one cause no other check on this node reports, and the names are its
      evidence - evidence, not a verdict (CR 1): on the streams carrying one
      group per consuming service a name beside the missing one is ordinary,
      and "never created it" is likeliest exactly where other consumers are
      present. All three causes stay in the message; the list narrows them.
    - **No groups at all.** Then nothing is reading the stream, and the reader
      is not sent hunting for a misnamed group that does not exist. Still two
      causes here - never created, or lost when the stream was recreated - and
      naming either alone would send whoever reads the alert after a wipe to the
      consumer's deployment instead of to the stream that came back empty.
    """
    present = sorted(_decode(g.get("name", "")) for g in groups)
    head = (
        f"consumer group {check.pending_group!r} does not exist on a stream that does, "
        "so its lag cannot be read. "
    )
    if present:
        body = (
            f"The groups that DO exist on it: {', '.join(repr(n) for n in present)}. All three "
            "causes stay open: one of those may be this consumer under a name group_name() does "
            "not derive (cannobserv#384), the cause no other check reports; on a stream carrying "
            "one group per consuming service they may simply be the other services'; and the "
            "consumer may equally have never created its own, or lost it when the stream was "
            "recreated. "
        )
    else:
        body = (
            "NO consumer group exists on that stream at all, so nothing on this node is "
            "reading it: the consumer never created it, or the group was lost when the "
            "stream was recreated. "
        )
    tail = 'See docs/BUS-HEALTH.md, "The bus-health probe"'
    return Finding(check="group-missing", subject=check.topic, message=head + body + tail)


async def _collect_undelivered(
    client: Redis,
    check: StreamCheck,
    *,
    position: dict[str, object],
    last_generated_id: str | bytes | None,
) -> list[Finding]:
    """Where the group stands against the end of its stream (broker#20).

    Both ids ride replies the tick has already paid for: ``last-generated-id``
    from the ``XINFO STREAM`` the length and continuity checks read, and the
    group's ``last-delivered-id`` from the ``XINFO GROUPS`` row the pending
    count comes out of (CannObserv/broker#29). A healthy tick therefore adds no
    read at all, and the ``XRANGE`` below is skipped entirely unless the group
    is behind.

    **No new privilege, and no group membership.** ``XINFO GROUPS`` and
    ``XRANGE`` are both read-only introspection this probe's ``brokeradmin``
    credential already holds; nothing here joins a group, which would silently
    take delivery of another service's messages. Pinned by
    ``tests/deploy/test_bus_health_units.py`` and, on the ACL itself, by
    ``test_the_probe_can_read_a_groups_position_without_joining_it``.

    Runs only where that row was found: a missing group is ``group-missing``,
    and saying so twice in two vocabularies helps nobody.
    """
    if check.warn_undelivered_age_seconds is None:
        return []
    if position.get("last-delivered-id") is None:
        return []
    last_delivered_id = _decode(position["last-delivered-id"])
    generated = None if last_generated_id is None else _decode(last_generated_id)

    oldest_undelivered_id = None
    if group_is_behind(last_generated_id=generated, last_delivered_id=last_delivered_id):
        # Exclusive range: the oldest entry the group has NOT been delivered is
        # the first one after its position. COUNT 1 because its id is the whole
        # answer - the payload is the consumer's business, not the probe's.
        entries = await client.xrange(check.topic, min=f"({last_delivered_id}", count=1)
        if entries:
            oldest_undelivered_id = _decode(entries[0][0])

    return evaluate_undelivered(
        check,
        last_generated_id=generated,
        last_delivered_id=last_delivered_id,
        oldest_undelivered_id=oldest_undelivered_id,
        now_ms=int(time.time() * 1000),
        pending=int(position["pending"]),
    )


async def _collect_memory(
    client: Redis, *, stream_lengths: Mapping[str, int] | None = None
) -> list[Finding]:
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
        stream_lengths=stream_lengths,
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
        state: dict[str, int] = {}
        stream_findings: list[Finding] = []
        stream_lengths: dict[str, int] = {}
        for check in STREAM_CHECKS:
            findings_for_stream, stream_pending = await _collect_stream(
                client, check, previous_state
            )
            stream_findings.extend(findings_for_stream)
            state.update(stream_pending)
            # The length baseline is the XINFO STREAM reply's own length, so the
            # memory finding names streams at no extra round trip (#61). Absent
            # for a stream with no key.
            length = stream_pending.get(CONTINUITY_LENGTH_KEY.format(topic=check.topic))
            if length is not None:
                stream_lengths[check.topic] = length
        # Built after the streams so it can name them; still reported first.
        findings = await _collect_memory(client, stream_lengths=stream_lengths)
        findings.extend(await _collect_persistence(client))
        findings.extend(stream_findings)
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
