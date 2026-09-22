"""Tests for the broker-side bus-health probe.

Moved from CannObserv/archiver's ``tests/core/test_bus_health.py`` under
archiver#193 D6, minus the two collectors that stayed with archiver (the
``changes_outbox`` probe and the dashboard's group-lag reader).

The pure ``evaluate_*`` functions carry the thresholds; the ``collect_*``
functions are exercised against fakeredis so the Redis command surface
(XINFO STREAM / XINFO GROUPS / SCAN) is real, not mocked. The two-tick
pending rule and the state file that carries it between oneshot runs get
their own coverage because the timer is stateless without them.
"""

from __future__ import annotations

import json
import re
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import get_args
from unittest.mock import AsyncMock, MagicMock

import pytest
from co_core.pure.adapters.bus.streams import (
    CONTENT_ARTIFACTS,
    CONTENT_FETCH,
    CONTENT_FETCH_POLICY,
    CONTENT_REPLICATE,
    CONTENT_REVISIONS,
    INFO_CHANGES,
    INFO_REGISTRY,
    INFO_WATCH_STATUS,
    StreamKind,
    dlq_name,
    stream_kind,
)
from fakeredis import aioredis as fakeredis_aio
from redis.exceptions import ResponseError

from src.broker import bus_health
from src.broker.bus_health import (
    BACKUP_WARN_MAX_AGE_SECONDS,
    BROKER_EVICTION_POLICY,
    CONTINUITY_ENTRIES_KEY,
    DISK_WARN_MIN_FREE_BYTES,
    FACT_PRODUCER_MAXLEN,
    FACT_WARN_LENGTH,
    LWW_PRODUCER_MAXLEN,
    LWW_REPUBLISH_PERIOD_SECONDS,
    LWW_RETAINED_FULL_SETS,
    LWW_WARN_LAST_ENTRY_AGE_SECONDS,
    REGISTRY_PRODUCER_MAXLEN,
    STREAM_CHECKS,
    FullSetFloor,
    StreamCheck,
    collect_broker_findings,
    evaluate_backup,
    evaluate_disk,
    evaluate_memory,
    evaluate_pending,
    evaluate_persistence,
    evaluate_stream,
    evaluate_undelivered,
    load_state,
    republished_set_size,
    save_state,
    with_margin,
)


@pytest.fixture
async def fake_redis():
    r = fakeredis_aio.FakeRedis()
    yield r
    await r.aclose()


class _DelegatingClient:
    """Forwards every call to a real client except the one a subclass overrides.

    The probe's behaviour against a server that answers *differently* - an older
    Redis with no ``entries-added``, a read that races a deletion - has no other
    seam, and a full mock would stop exercising the real command surface the
    rest of these tests are built on.
    """

    def __init__(self, delegate):
        self._delegate = delegate

    def __getattr__(self, name):
        return getattr(self._delegate, name)


# --- memory ---


def test_memory_healthy_below_fraction() -> None:
    assert evaluate_memory(used_memory=100, maxmemory=1000) == []


def test_memory_warns_at_fraction() -> None:
    findings = evaluate_memory(used_memory=750, maxmemory=1000)
    assert len(findings) == 1
    assert findings[0].check == "memory"


def test_memory_warns_when_maxmemory_unset() -> None:
    """maxmemory 0 makes noeviction inert (CannObserv/archiver#128) - that is a finding."""
    findings = evaluate_memory(used_memory=100, maxmemory=0)
    assert len(findings) == 1
    assert "maxmemory" in findings[0].message


def test_memory_healthy_under_the_required_policy() -> None:
    assert evaluate_memory(used_memory=100, maxmemory=1000, policy=BROKER_EVICTION_POLICY) == []


def test_memory_warns_on_a_volatile_eviction_policy() -> None:
    """The silent one, and the whole reason this check exists (CannObserv/broker#9).

    Replicator's dedupe keys are the only volatile keys on the instance, so a
    ``volatile-*`` policy makes that one namespace the entire eviction candidate
    set. Nothing reports the eviction, so without this the probe calls the
    broker healthy on every tick while the keys are being deleted.
    """
    findings = evaluate_memory(used_memory=100, maxmemory=1000, policy="volatile-lru")
    assert [f.check for f in findings] == ["eviction-policy"]
    assert "volatile-lru" in findings[0].message
    assert "dedupe" in findings[0].message


def test_memory_warns_on_an_allkeys_eviction_policy() -> None:
    """The other family, with a different consequence worth naming: it evicts
    stream entries, which is the loss the continuity check catches after the
    fact rather than before it."""
    findings = evaluate_memory(used_memory=100, maxmemory=1000, policy="allkeys-lru")
    assert [f.check for f in findings] == ["eviction-policy"]
    assert "stream entries" in findings[0].message


def test_memory_policy_unreported_is_not_a_finding() -> None:
    """A server that does not report it (fakeredis) is a probe limitation, the
    same rule ``evaluate_persistence`` follows for its missing fields."""
    assert evaluate_memory(used_memory=100, maxmemory=1000, policy=None) == []


def test_memory_reports_the_policy_and_the_absent_cap_together() -> None:
    """Both are ways the protection becomes inert and they are independent, so
    one must not hide the other behind an early return."""
    findings = evaluate_memory(used_memory=100, maxmemory=0, policy="volatile-ttl")
    assert {f.check for f in findings} == {"eviction-policy", "memory"}


async def test_collect_memory_reads_the_policy_from_the_section_it_already_fetches(
    monkeypatch,
) -> None:
    """No second call and no new grant: ``maxmemory_policy`` rides the same
    ``INFO memory`` the headroom check already pays for."""

    class Server:
        async def info(self, section):
            assert section == "memory"
            return {"used_memory": 1, "maxmemory": 1000, "maxmemory_policy": "volatile-lru"}

    findings = await bus_health._collect_memory(Server())
    assert [f.check for f in findings] == ["eviction-policy"]


# --- disk ---


def test_disk_healthy() -> None:
    total = 100 * 1024**3
    assert evaluate_disk(total=total, free=total // 2) == []


def test_disk_warns_at_used_fraction() -> None:
    total = 100 * 1024**3
    findings = evaluate_disk(total=total, free=total // 20)  # 95% used
    assert any(f.check == "disk" for f in findings)


def test_disk_warns_below_min_free() -> None:
    """Small disks: an absolute floor fires even under the fraction."""
    total = 10 * 1024**3
    free = DISK_WARN_MIN_FREE_BYTES - 1  # < 2 GiB free but only 80% used
    findings = evaluate_disk(total=total, free=free)
    assert any(f.check == "disk" for f in findings)


# --- stream length + last-entry age ---

_CHECK = StreamCheck(
    topic="t",
    warn_length=100,
    warn_last_entry_age_seconds=900.0,
)


def test_stream_healthy() -> None:
    now_ms = 10_000_000
    assert evaluate_stream(_CHECK, length=50, last_entry_ms=now_ms, now_ms=now_ms) == []


def test_stream_warns_over_length() -> None:
    now_ms = 10_000_000
    findings = evaluate_stream(_CHECK, length=101, last_entry_ms=now_ms, now_ms=now_ms)
    assert [f.check for f in findings] == ["stream-length"]


def test_stream_warns_on_stale_last_entry() -> None:
    now_ms = 10_000_000
    stale = now_ms - 901_000
    findings = evaluate_stream(_CHECK, length=50, last_entry_ms=stale, now_ms=now_ms)
    assert [f.check for f in findings] == ["stream-age"]


def test_stream_empty_skips_age() -> None:
    """A never-written or empty stream has no last entry to age-check; an empty
    registry publishes nothing by design (the corpus-size guard, #147)."""
    findings = evaluate_stream(_CHECK, length=0, last_entry_ms=None, now_ms=10_000_000)
    assert findings == []


def test_stream_without_age_threshold_skips_age() -> None:
    check = StreamCheck(topic="t", warn_length=100)
    findings = evaluate_stream(check, length=50, last_entry_ms=0, now_ms=10_000_000)
    assert findings == []


# --- two-tick pending ---


def test_pending_first_tick_is_grace() -> None:
    check = StreamCheck(topic="t", pending_group="g")
    assert evaluate_pending(check, pending_now=5, pending_prev=0) == []


def test_pending_two_consecutive_ticks_warn() -> None:
    check = StreamCheck(topic="t", pending_group="g")
    findings = evaluate_pending(check, pending_now=5, pending_prev=3)
    assert [f.check for f in findings] == ["pending"]


def test_pending_recovered_is_healthy() -> None:
    check = StreamCheck(topic="t", pending_group="g")
    assert evaluate_pending(check, pending_now=0, pending_prev=5) == []


def test_pending_message_names_no_specific_stream() -> None:
    """CR round 1, finding 2. The check runs for both archiver-owned groups, so
    a wedged artifacts consumer must not be reported as lost revisions - wrong
    stream, wrong remedy. The subject already carries topic/group."""
    check = _check_for(CONTENT_ARTIFACTS)
    (finding,) = evaluate_pending(check, pending_now=5, pending_prev=3)
    assert finding.subject == f"{CONTENT_ARTIFACTS}/archiver.artifacts"
    assert "revision" not in finding.message.lower()


# --- undelivered age: the consumer that stopped reading ---
#
# CannObserv/broker#20. After the 2026-09-16 reboot replicator never
# reconnected, `replicator.fetch` held one undelivered command for hours, and
# the probe reported 0 findings on every tick. Each existing signal is blind to
# a consumer that has stopped *reading*: pending counts what was delivered, so
# a consumer that never calls XREADGROUP keeps it at the healthy 0; last-entry
# age catches a stopped producer and exists only for the groupless streams; and
# `lag` is wrong in both directions on this Redis.

_GROUPED = StreamCheck(topic="t", pending_group="g", warn_undelivered_age_seconds=300.0)


def test_a_group_at_the_streams_last_id_is_caught_up() -> None:
    """Positions equal is the whole healthy case, and it is checked before
    anything is read: no XRANGE, no age, nothing to be wrong about."""
    assert (
        evaluate_undelivered(
            _GROUPED,
            last_generated_id="1000-0",
            last_delivered_id="1000-0",
            oldest_undelivered_id=None,
            now_ms=10_000_000,
            pending=0,
        )
        == []
    )


def test_a_group_behind_by_a_fresh_entry_is_healthy() -> None:
    """Being behind is not the finding - every delivery is momentarily behind.
    The finding is being behind *for longer than a delivery takes*."""
    now_ms = 10_000_000
    assert (
        evaluate_undelivered(
            _GROUPED,
            last_generated_id="9999999-0",
            last_delivered_id="9000000-0",
            oldest_undelivered_id=f"{now_ms - 1000}-0",
            now_ms=now_ms,
            pending=0,
        )
        == []
    )


def test_a_group_behind_by_a_stale_entry_warns() -> None:
    now_ms = 10_000_000
    (finding,) = evaluate_undelivered(
        _GROUPED,
        last_generated_id="9999999-0",
        last_delivered_id="9000000-0",
        oldest_undelivered_id=f"{now_ms - 301_000}-0",
        now_ms=now_ms,
        pending=0,
    )
    assert finding.check == "group-undelivered"
    assert finding.subject == "t/g"


def test_undelivered_entries_that_no_longer_exist_are_their_own_condition() -> None:
    """A stream trimmed past its group's position is not an age.

    The group is behind and the entries it is behind by are gone, so there is
    nothing to date and nothing that will ever be delivered. Reporting that as
    "0 seconds old" would read as healthy, and reporting it as an age at all
    would send the reader looking for a stopped consumer rather than for what
    deleted undelivered entries.
    """
    (finding,) = evaluate_undelivered(
        _GROUPED,
        last_generated_id="9999999-0",
        last_delivered_id="1000-0",
        oldest_undelivered_id=None,
        now_ms=10_000_000,
        pending=0,
    )
    assert finding.check == "group-undelivered-lost"


def test_a_group_row_without_a_threshold_says_nothing() -> None:
    check = StreamCheck(topic="t", pending_group="g")
    assert (
        evaluate_undelivered(
            check,
            last_generated_id="9999999-0",
            last_delivered_id="1000-0",
            oldest_undelivered_id=None,
            now_ms=10_000_000,
            pending=0,
        )
        == []
    )


def test_a_group_ahead_of_the_last_id_the_probe_read_is_caught_up() -> None:
    """The two ids come from two replies, so an XADD can land between them.

    ``XINFO STREAM`` is read first, then ``XINFO GROUPS``: a group that took
    delivery of an entry added in that window reports a position *newer* than
    the ``last-generated-id`` the probe holds. Compared for inequality that
    reads as behind, and the XRANGE after it finds nothing - which would be
    reported as entries deleted before delivery, on the healthiest possible
    broker.
    """
    assert (
        evaluate_undelivered(
            _GROUPED,
            last_generated_id="1000-0",
            last_delivered_id="1001-0",
            oldest_undelivered_id=None,
            now_ms=10_000_000,
            pending=0,
        )
        == []
    )


def test_positions_are_compared_as_numbers_not_strings() -> None:
    """``"9-0" > "10-0"`` lexicographically while ``9 < 10``.

    The same hazard CannObserv/broker#1 recorded against the watcher#285 group
    rename, and it bites here in the direction that goes quiet: a group at
    ``9-0`` on a stream whose last id is ``10-0`` is behind, and a string
    comparison calls it caught up.
    """
    now_ms = 10_000_000
    (finding,) = evaluate_undelivered(
        _GROUPED,
        last_generated_id="10-0",
        last_delivered_id="9-0",
        oldest_undelivered_id=f"{now_ms - 301_000}-0",
        now_ms=now_ms,
        pending=0,
    )
    assert finding.check == "group-undelivered"


def test_behind_and_holding_nothing_is_a_stopped_reader() -> None:
    """The 2026-09-16 shape: behind, and nothing delivered is outstanding.

    A consumer that acks after handling holds the entry it is handling, so for
    one a pending count of 0 means nothing is in flight, and what is left to
    explain the age is a consumer not reading. Replicator's does
    (CannObserv/replicator#96); archiver's and watcher's are not measured here,
    and one that acked first would be busy at 0.
    """
    now_ms = 10_000_000
    (finding,) = evaluate_undelivered(
        _GROUPED,
        last_generated_id="9999999-0",
        last_delivered_id="9000000-0",
        oldest_undelivered_id=f"{now_ms - 301_000}-0",
        now_ms=now_ms,
        pending=0,
    )
    assert "stopped READING" in finding.message


def test_behind_a_held_delivery_is_not_called_a_stopped_reader() -> None:
    """The states no handler duration sizes (CannObserv/broker#30).

    Replicator named two in CannObserv/replicator#96: one stalled-provider
    attempt, and recovery re-claiming its own failing entry every cycle so that
    ``XREADGROUP`` is never issued (CannObserv/replicator#98) - a live consumer,
    logging, whose group has not moved. Both hold a delivered entry. "XPENDING
    reads the healthy 0" is false there, and "check it is connected" sends the
    reader to the one thing that will look fine.
    """
    now_ms = 10_000_000
    (finding,) = evaluate_undelivered(
        _GROUPED,
        last_generated_id="9999999-0",
        last_delivered_id="9000000-0",
        oldest_undelivered_id=f"{now_ms - 301_000}-0",
        now_ms=now_ms,
        pending=1,
    )
    assert finding.check == "group-undelivered"
    assert "healthy 0" not in finding.message
    assert "stopped READING" not in finding.message
    assert "Pending is 1 - delivered and not acked" in finding.message
    assert "replicator's shape: CannObserv/replicator#98" in finding.message
    assert "XPENDING t g - + 10" in finding.message


# --- inventory ---


def test_an_undelivered_threshold_without_a_group_is_refused() -> None:
    """CR 1. The invariant the test below states, in its other direction.

    A threshold with no group is a row nothing can evaluate: the collector only
    reaches ``evaluate_undelivered`` where ``XINFO GROUPS`` found the group, and the
    evaluator would render ``subject`` as ``topic/None`` - a string an alert
    rule would carry. Refused at import time for the reason the config_state
    guard beside it is: this is a statement about the row, which cannot become
    true at runtime.
    """
    with pytest.raises(ValueError, match="undelivered"):
        StreamCheck(topic="t", warn_undelivered_age_seconds=300.0)


def test_every_probed_group_carries_an_undelivered_threshold() -> None:
    """A group row without one is a group nobody watches for a stopped reader.

    Asserted over the declared tuple rather than per row, so a sixth group
    arrives with the threshold or fails here - which is the half of broker#20
    that outlives the five groups on the node today.
    """
    missing = [
        c.topic
        for c in STREAM_CHECKS
        if c.pending_group is not None and c.warn_undelivered_age_seconds is None
    ]
    assert not missing, f"grouped streams with no undelivered threshold: {missing}"


def test_content_blobs_carries_no_retention_opinion() -> None:
    """The surviving half of the content.blobs boundary (CannObserv/broker#1
    Phase 5).

    The unqualified "never content.blobs" rule was Archiver's *role* boundary
    and came here verbatim; a neutral node has no role to be out of bounds of,
    so the group is now probed. What still holds is the part that was never
    about roles: this repo owns no cap for this stream, so it states no
    retention opinion on it - no length row, no age row. A regression here
    would be someone adding one from the LWW or fact cap, neither of which
    governs this stream.
    """
    check = _check_for("content.blobs")
    assert check.warn_length is None
    assert check.warn_last_entry_age_seconds is None
    assert check.pending_group == "watcher.blobs"


def test_inventory_covers_every_consumer_group_on_the_node() -> None:
    """Widened from Archiver's two to all five (CannObserv/broker#1 Phase 5).

    The exclusion was inherited from a probe that ran on Archiver's own host,
    where a downstream service's group lag was plausibly its own alerting
    problem. On a neutral node it is not: nobody else watches these, and this
    is the one place that can. Cost is three more group reads per tick.

    Pinned as an exact set rather than a subset, so a group silently dropped
    from the inventory fails here instead of going quiet in production.
    """
    groups = {c.pending_group for c in STREAM_CHECKS if c.pending_group}
    assert groups == {
        "archiver.revisions",
        "archiver.artifacts",
        "watcher.blobs",
        "replicator.fetch",
        "replicator.replicate",
    }


def test_group_names_are_derived_not_spelled() -> None:
    """cannobserv#384's whole payoff, and this repo's reason to depend on
    co-core rather than hardcode strings: a group name is computable from its
    stream name, so the broker needs no agreement with archiver about the
    literal. A regression here would be someone replacing ``group_name`` with a
    literal, which reads identically until archiver's convention moves.

    Checked for every probed group, not just Archiver's: the three added in
    Phase 5 name services this repo has no other contract with, which is
    exactly where a hand-typed literal would be tempting.
    """
    for check in STREAM_CHECKS:
        if check.pending_group is None:
            continue
        service, _, _ = check.pending_group.partition(".")
        assert check.pending_group == f"{service}.{check.topic.split('.', 1)[1]}"
    assert bus_health.REVISIONS_GROUP == f"archiver.{CONTENT_REVISIONS.split('.', 1)[1]}"


def _check_for(topic: str) -> StreamCheck:
    return next(c for c in STREAM_CHECKS if c.topic == topic)


def test_registry_threshold_tracks_its_own_producer_cap() -> None:
    """info.registry is excluded from archiver's operator-side XTRIM loop and
    capped on publish instead, so reusing the fact-stream threshold would let it
    reach 2.2x its cap before warning - defeating the "a breach means the trim
    contract broke" contract for the one stream whose retention floor is a
    consumer boot contract (CannObserv/archiver#141)."""
    assert _check_for(INFO_REGISTRY).warn_length == with_margin(REGISTRY_PRODUCER_MAXLEN)
    assert _check_for(INFO_REGISTRY).warn_length < FACT_WARN_LENGTH


def test_fact_stream_threshold_tracks_the_operator_xtrim_cap() -> None:
    """Derived from the mirrored cap, not written as a second literal: the
    repo split already costs one copy of each number (see the module's
    "Mirrored constants" comment), and a threshold spelled independently would
    make it two."""
    assert _check_for(INFO_CHANGES).warn_length == with_margin(FACT_PRODUCER_MAXLEN)
    assert FACT_WARN_LENGTH > FACT_PRODUCER_MAXLEN


@pytest.mark.parametrize("topic", [CONTENT_FETCH_POLICY, INFO_WATCH_STATUS])
def test_lww_threshold_catches_the_backlog_its_cap_was_cut_for(topic: str) -> None:
    """A cap lowered at home and not here does not fail safe
    (CannObserv/broker#40).

    Under the old 50k cap `content.fetch-policy` reached 29,770 entries, and
    replicator#85 measured a 950 s boot replay; watcher#292 cut both LWW caps to
    500 for it. A threshold still derived from 50k sat at 55k and would have let
    a broken cap climb through that whole range unreported. The other half: the
    node read 511 under `MAXLEN ~ 500` on 2026-09-22, so the margin has to
    absorb one macro node of approximate-trim overshoot. It does because these
    nodes fill by bytes (`stream-node-max-bytes`, Redis's 4096 default, which
    `deploy/redis.conf.broker` does not pin) at ~8-13 entries; filling by count
    (`stream-node-max-entries` 100) they would overshoot the 50-entry margin.
    """
    check = _check_for(topic)
    assert check.warn_length == with_margin(LWW_PRODUCER_MAXLEN)
    (finding,) = evaluate_stream(check, length=29_770, last_entry_ms=None, now_ms=0)
    assert finding.check == "stream-length"
    assert evaluate_stream(check, length=511, last_entry_ms=None, now_ms=0) == []


# --- the producer's full-set floor, read off the stream (CannObserv/broker#44) ---
#
# `LWW_PRODUCER_MAXLEN` mirrors watcher's *default*, and the default is not the
# whole rule: `resolve_stream_maxlen` floors each cap at `RETAINED_FULL_SETS`
# copies of the set being republished, so the cap in force is
# `max(500, 10 x set)`. Every case below is about which of the two governs.

_NOW_MS = 1_700_000_000_000
_PERIOD_MS = int(LWW_REPUBLISH_PERIOD_SECONDS * 1000)


def _spanning(periods: int) -> dict[str, int]:
    """The two ids a set size is read off, for a window holding `periods`
    republish intervals and ending now."""
    return {"first_entry_ms": _NOW_MS - periods * _PERIOD_MS, "last_entry_ms": _NOW_MS}


@pytest.mark.parametrize("topic", [CONTENT_FETCH_POLICY, INFO_WATCH_STATUS])
def test_a_set_past_50_entries_raises_the_lww_threshold_with_the_cap(topic: str) -> None:
    """Past ~50 entries per set the floor overtakes the mirrored 500, and a
    threshold that stayed at 550 would call a correctly applied cap broken on
    every tick (CannObserv/broker#44).

    A 62-entry set is capped at 620, and `MAXLEN ~` leaves one macro node of
    overshoot above it: 631 entries in a window spanning ten republishes.
    """
    check = _check_for(topic)
    assert 631 > with_margin(LWW_PRODUCER_MAXLEN)  # the standing false WARN #44 is about
    assert evaluate_stream(check, length=631, now_ms=_NOW_MS, **_spanning(10)) == []


def test_the_floor_does_not_hide_a_cap_that_stopped_being_applied() -> None:
    """An untrimmed stream grows its span in step with its length, so the set
    size the probe reads stays put and the threshold with it.

    This is the property that makes the reading safe to raise a threshold on:
    it measures entries *per republish*, which the cap does not change.
    """
    check = _check_for(CONTENT_FETCH_POLICY)
    (finding,) = evaluate_stream(check, length=41 * 62, now_ms=_NOW_MS, **_spanning(40))
    assert finding.check == "stream-length"
    assert "not being applied" in finding.message
    assert "full-set floor" in finding.message


def test_the_floor_never_lowers_the_mirrored_lww_threshold() -> None:
    """Below 50 entries per set the mirrored default is the cap in force, so
    the reading changes nothing - the state the node was in on 2026-09-22, with
    sets of 3 and 4. Read off the live broker that day: `content.fetch-policy`
    500 entries over 830 minutes (a 3-host set read as 4), `info.watch-status`
    504 over 625 (a 4-item set read as 5), both nowhere near the floor.

    The check is `max(500, 10 x set)`, not `10 x set`: a reading that lowered
    the threshold would hand back the blindness #40 closed.
    """
    check = _check_for(INFO_WATCH_STATUS)
    assert evaluate_stream(check, length=504, now_ms=_NOW_MS, **_spanning(125)) == []
    (finding,) = evaluate_stream(check, length=29_770, now_ms=_NOW_MS, **_spanning(7442))
    assert f"exceeds {with_margin(LWW_PRODUCER_MAXLEN)}" in finding.message


def test_a_window_holding_one_republish_holds_a_whole_one() -> None:
    """A span under one period cannot be divided into sets, and does not need
    to be: the floor is what makes the single reading safe to take at face
    value.

    Trimming inside one set would leave the stream below a full set, which is
    the partial-replay failure `RETAINED_FULL_SETS` exists to prevent, so the
    one republish a narrow window holds is a complete one.
    """
    floor = _check_for(CONTENT_FETCH_POLICY).full_set_floor
    assert floor is not None
    assert (
        republished_set_size(
            floor, length=700, first_entry_ms=_NOW_MS - 1_000, last_entry_ms=_NOW_MS
        )
        == 700
    )


def test_the_set_size_is_read_high_rather_than_low() -> None:
    """The oldest retained entries are a *partial* set - an approximate trim
    drops whole macro nodes, not whole republishes - so the window is one
    fragment followed by whole sets.

    Charging the fragment to the whole sets rounds the reading up, and up is
    the direction that cannot invent a broken cap.
    """
    floor = _check_for(CONTENT_FETCH_POLICY).full_set_floor
    assert floor is not None
    # 10 whole 62-entry sets plus an 11-entry fragment of an eleventh, over a
    # span of 10 periods: 631/10 read up to 64, never down to 62 or below
    assert republished_set_size(floor, length=631, **_spanning(10)) == 64


@pytest.mark.parametrize("set_size", [62, 100, 200, 1000])
def test_one_missed_republish_is_absorbed_and_two_are_not(set_size: int) -> None:
    """The direction this derivation fails in, and how far, pinned rather than
    left to be discovered.

    A gap inside the retained window counts as republishes that did not happen,
    so the set reads low and the threshold with it - the stale-low, warns-early
    direction, where the probe stays loud rather than going quiet. The 10%
    margin covers exactly one missed republish at `RETAINED_FULL_SETS` of 10,
    and only because the reading is ceilinged; two warn. Parametrized over the
    set sizes docs/BUS-HEALTH.md says it was measured at, so the claim there
    has an artifact rather than a memory.

    Two missed republishes is ten minutes of silence and `stream-age` needs
    fifteen, so between them sits a window with no finding naming the cause -
    bounded, unreachable until a set passes 50 entries, and recorded as
    CannObserv/broker#45 rather than closed here.
    """
    check = _check_for(CONTENT_FETCH_POLICY)
    assert LWW_WARN_LAST_ENTRY_AGE_SECONDS == 3 * LWW_REPUBLISH_PERIOD_SECONDS
    # the cap the floor puts in force, with one macro node of overshoot above it
    length = set_size * LWW_RETAINED_FULL_SETS + 11
    assert evaluate_stream(check, length=length, now_ms=_NOW_MS, **_spanning(10)) == []
    assert evaluate_stream(check, length=length, now_ms=_NOW_MS, **_spanning(11)) == []
    findings = evaluate_stream(check, length=length, now_ms=_NOW_MS, **_spanning(12))
    assert [f.check for f in findings] == ["stream-length"]


def test_a_floor_must_agree_with_the_row_it_sits_on() -> None:
    """The mirrored cap would otherwise have two spellings on one row - the
    `warn_length` the length check uses and the `maxlen` the floor compares
    against - and nothing would notice them diverging."""
    floor = FullSetFloor(default_maxlen=500, retained_full_sets=10, republish_period_seconds=300.0)
    with pytest.raises(ValueError, match="full-set floor"):
        StreamCheck(topic="t", warn_length=999, full_set_floor=floor)
    with pytest.raises(ValueError, match="full-set floor"):
        StreamCheck(topic="t", full_set_floor=floor)


def test_the_floor_constants_mirror_watchers() -> None:
    """Both are copies of numbers owned in CannObserv/watcher, so the source is
    named on each (docs/BUS-HEALTH.md, "Mirrored constants")."""
    for topic in (CONTENT_FETCH_POLICY, INFO_WATCH_STATUS):
        floor = _check_for(topic).full_set_floor
        assert floor is not None
        assert floor.default_maxlen == LWW_PRODUCER_MAXLEN
        assert floor.retained_full_sets == LWW_RETAINED_FULL_SETS
        assert floor.republish_period_seconds == LWW_REPUBLISH_PERIOD_SECONDS


def test_never_trimmed_stream_does_not_claim_a_broken_cap() -> None:
    """content.replicate is carved out of the trim set
    (capping a command stream orphans PEL entries), so it grows monotonically;
    a breach there is a volume milestone, not a retention failure."""
    check = _check_for(CONTENT_REPLICATE)
    assert check.never_trimmed
    (finding,) = evaluate_stream(check, length=check.warn_length + 1, last_entry_ms=None, now_ms=0)
    assert "never trimmed" in finding.message
    assert "not being applied" not in finding.message


def test_trimmed_stream_still_names_the_broken_cap() -> None:
    check = _check_for(INFO_CHANGES)
    (finding,) = evaluate_stream(check, length=check.warn_length + 1, last_entry_ms=None, now_ms=0)
    assert "never trimmed" not in finding.message


# --- collectors against fakeredis ---


async def test_collect_reports_stale_lww_stream(fake_redis) -> None:
    """An entry older than the age threshold on a groupless LWW stream warns."""
    await fake_redis.xadd(CONTENT_FETCH_POLICY, {"k": "v"}, id="1000-0")
    findings, _ = await collect_broker_findings(fake_redis, previous_state={})
    assert any(f.check == "stream-age" and f.subject == CONTENT_FETCH_POLICY for f in findings)


async def test_collect_reads_the_full_set_floor_off_the_stream(fake_redis) -> None:
    """The collector has to hand `evaluate_stream` the *first* entry as well.

    Without it every tick falls back to the mirrored default, and a set past 50
    entries is the standing false WARN CannObserv/broker#44 is about - so this
    pins the wiring, not just the arithmetic.
    """
    now_ms = int(time.time() * 1000)
    first_ms = now_ms - 10 * int(LWW_REPUBLISH_PERIOD_SECONDS * 1000)
    step = (now_ms - first_ms) // 630
    for i in range(631):
        await fake_redis.xadd(CONTENT_FETCH_POLICY, {"k": "v"}, id=f"{first_ms + i * step}-0")
    findings, _ = await collect_broker_findings(fake_redis, previous_state={})
    assert [f for f in findings if f.subject == CONTENT_FETCH_POLICY] == []


async def test_collect_reports_nonempty_dlq(fake_redis) -> None:
    """Resting state is depth 0 on every *.dlq key (CannObserv/archiver#162)."""
    await fake_redis.xadd("content.revisions.dlq", {"k": "v"})
    findings, _ = await collect_broker_findings(fake_redis, previous_state={})
    assert any(f.check == "dlq" and f.subject == "content.revisions.dlq" for f in findings)


async def test_collect_missing_group_is_a_finding(fake_redis) -> None:
    """A consumer group that should exist but does not is silent, so the probe
    must say it - without guessing why. A consumer that never ran is one of
    three causes (docs/BUS-HEALTH.md), and a message asserting it would send
    whoever reads the alert after a wipe to the consumer's deployment instead of
    to the group that was lost with its stream.

    An empty stream of groups rules out exactly one of the three - a consumer
    reading under another name (broker#29) - so the message names the other two
    and says the stream carries none.
    """
    await fake_redis.xadd(CONTENT_REVISIONS, {"k": "v"})
    findings, _ = await collect_broker_findings(fake_redis, previous_state={})
    (finding,) = [
        f for f in findings if f.check == "group-missing" and f.subject == CONTENT_REVISIONS
    ]
    assert "never provisioned" not in finding.message, finding.message
    assert "recreated" in finding.message, finding.message
    assert "NO consumer group exists" in finding.message, finding.message


async def test_a_missing_group_names_the_groups_that_are_there_instead(fake_redis) -> None:
    """broker#29: the evidence the old finding had to hedge about.

    "The consumer runs its group under a name other than the one ``group_name()``
    derives" is the one cause no other check reports - the consumer looks healthy
    to itself, and its real group is probed by nobody. Reading the group list
    rather than asking ``XPENDING`` about one name means the finding can print
    what IS on the stream, which is that cause's whole evidence.
    """
    await fake_redis.xadd(CONTENT_REVISIONS, {"k": "v"})
    await fake_redis.xgroup_create(CONTENT_REVISIONS, "archiver.content.revisions", id="0")

    findings, _ = await collect_broker_findings(fake_redis, previous_state={})
    (finding,) = [
        f for f in findings if f.check == "group-missing" and f.subject == CONTENT_REVISIONS
    ]
    assert "archiver.content.revisions" in finding.message, finding.message
    assert "NO consumer group exists" not in finding.message, finding.message


async def test_the_group_list_is_evidence_and_not_an_inference(fake_redis) -> None:
    """CR 1. A stream carrying one group per consuming service has other names
    on it by design, so the list must not be read as a rename.

    `content.blobs` broadcasts (docs/STREAMS.md), and there a group beside the
    missing one is ordinary rather than suspicious - while "the consumer never
    created its group" is the likeliest cause precisely when other consumers are
    present, which is the cause the first wording of this branch dropped. Three
    causes, and the reply narrows them without picking one: the names are the
    evidence, the inference is the reader's.
    """
    await fake_redis.xadd("content.blobs", {"k": "v"})
    await fake_redis.xgroup_create("content.blobs", "somewhere.blobs", id="0")

    findings, _ = await collect_broker_findings(fake_redis, previous_state={})
    (finding,) = [
        f for f in findings if f.check == "group-missing" and f.subject == "content.blobs"
    ]
    assert "somewhere.blobs" in finding.message, finding.message
    assert "never created" in finding.message, finding.message
    assert "per consuming service" in finding.message, finding.message


async def test_the_group_is_read_once_per_stream(fake_redis) -> None:
    """broker#29. Position and pending come out of one reply, not two.

    ``XINFO GROUPS`` has always carried ``pending``, so the ``XPENDING`` that
    used to precede it read the same group a round trip later - the shape
    broker#13 took out of the DLQ scan, where two reads an ``XADD`` can land
    between are not one observation. Asserted as "``XPENDING`` is not issued
    at all" rather than as a call count, because the cost is only half of it:
    the two halves of the group's state now cannot disagree.
    """

    class _CountingGroupReads(_DelegatingClient):
        def __init__(self, delegate):
            super().__init__(delegate)
            self.group_reads = 0

        async def xinfo_groups(self, topic, *a, **kw):
            self.group_reads += 1
            return await self._delegate.xinfo_groups(topic, *a, **kw)

        async def xpending(self, *a, **kw):
            raise AssertionError("XPENDING is the round trip broker#29 removed")

    await fake_redis.xadd(CONTENT_REVISIONS, {"k": "v"}, id="1000-0")
    await fake_redis.xgroup_create(CONTENT_REVISIONS, "archiver.revisions", id="0")
    client = _CountingGroupReads(fake_redis)

    findings, pending = await collect_broker_findings(client, previous_state={})

    # One read for the one grouped stream that exists here; the other four are
    # absent, and an absent stream is dormancy, not a group to check.
    assert client.group_reads == 1
    assert pending[f"{CONTENT_REVISIONS}/archiver.revisions"] == 0
    assert not any(f.check == "group-missing" and f.subject == CONTENT_REVISIONS for f in findings)


async def test_collect_pending_carries_state_between_ticks(fake_redis) -> None:
    await fake_redis.xadd(CONTENT_REVISIONS, {"k": "v"})
    await fake_redis.xgroup_create(CONTENT_REVISIONS, "archiver.revisions", id="0")
    await fake_redis.xreadgroup(
        "archiver.revisions", "c1", {CONTENT_REVISIONS: ">"}, count=10
    )  # deliver without ack -> pending=1

    findings, pending = await collect_broker_findings(fake_redis, previous_state={})
    key = f"{CONTENT_REVISIONS}/archiver.revisions"
    assert pending[key] == 1
    assert not any(f.check == "pending" for f in findings)  # first tick: grace

    findings, _ = await collect_broker_findings(fake_redis, previous_state=pending)
    assert any(f.check == "pending" for f in findings)  # second tick: warn


async def test_collect_warns_when_a_group_stopped_reading(fake_redis) -> None:
    """The 2026-09-16 state, reproduced: a group that exists, has a stream with
    entries, and never calls XREADGROUP.

    Every other signal reads healthy here - nothing was delivered, so pending
    is 0 - which is why this is the check the event asked for.
    """
    await fake_redis.xadd(CONTENT_REVISIONS, {"k": "v"}, id="1000-0")
    await fake_redis.xgroup_create(CONTENT_REVISIONS, "archiver.revisions", id="0")

    findings, _ = await collect_broker_findings(fake_redis, previous_state={})
    (finding,) = [f for f in findings if f.check == "group-undelivered"]
    assert finding.subject == f"{CONTENT_REVISIONS}/archiver.revisions"
    assert "stopped READING" in finding.message
    assert not any(f.check == "pending" for f in findings)


async def test_collect_words_the_finding_by_the_pending_count_it_already_read(fake_redis) -> None:
    """One entry delivered and never acked, a stale one behind it never offered.

    The pending count rides the same ``XINFO GROUPS`` row as the position
    (CannObserv/broker#29), so telling a held delivery from a stopped reader
    costs no read (CannObserv/broker#30).
    """
    await fake_redis.xadd(CONTENT_REVISIONS, {"k": "v"}, id="1000-0")
    await fake_redis.xadd(CONTENT_REVISIONS, {"k": "v"}, id="2000-0")
    await fake_redis.xgroup_create(CONTENT_REVISIONS, "archiver.revisions", id="0")
    await fake_redis.xreadgroup("archiver.revisions", "c1", {CONTENT_REVISIONS: ">"}, count=1)

    findings, _ = await collect_broker_findings(fake_redis, previous_state={})
    (finding,) = [f for f in findings if f.check == "group-undelivered"]
    assert "Pending is 1 - delivered and not acked" in finding.message
    assert "stopped READING" not in finding.message


async def test_collect_says_nothing_once_the_group_has_read(fake_redis) -> None:
    """A read clears it, and nothing about an ack is required: this check is
    about *delivery*, which is the half the pending count starts at."""
    await fake_redis.xadd(CONTENT_REVISIONS, {"k": "v"}, id="1000-0")
    await fake_redis.xgroup_create(CONTENT_REVISIONS, "archiver.revisions", id="0")
    await fake_redis.xreadgroup("archiver.revisions", "c1", {CONTENT_REVISIONS: ">"}, count=10)

    findings, _ = await collect_broker_findings(fake_redis, previous_state={})
    assert not any(f.check.startswith("group-undelivered") for f in findings)


async def test_collect_reports_undelivered_entries_that_were_trimmed_away(fake_redis) -> None:
    """The group is behind and what it is behind by is gone.

    Its own condition, not an age: there is nothing left to date, and the
    entries will never be delivered to anyone. This is the hazard
    ``content.replicate`` is carved out of every trim path for, arriving on a
    stream that is not carved out.
    """
    await fake_redis.xadd(CONTENT_REVISIONS, {"k": "v"}, id="1000-0")
    await fake_redis.xgroup_create(CONTENT_REVISIONS, "archiver.revisions", id="0")
    await fake_redis.xtrim(CONTENT_REVISIONS, maxlen=0)

    findings, _ = await collect_broker_findings(fake_redis, previous_state={})
    (finding,) = [f for f in findings if f.check == "group-undelivered-lost"]
    assert finding.subject == f"{CONTENT_REVISIONS}/archiver.revisions"


async def test_collect_fresh_registry_is_healthy(fake_redis) -> None:
    findings, _ = await collect_broker_findings(fake_redis, previous_state={})
    assert not any(f.subject == INFO_REGISTRY for f in findings)


async def test_collect_unreachable_broker_is_a_finding() -> None:
    class DownRedis:
        def __getattr__(self, name):
            async def _raise(*a, **kw):
                raise ConnectionError("refused")

            return _raise

    findings, pending = await collect_broker_findings(DownRedis(), previous_state={"x": 1})
    assert [f.check for f in findings] == ["broker"]
    assert pending == {"x": 1}  # state preserved so the grace tick is not reset


async def test_a_dlq_drained_between_ticks_is_reported_although_it_is_empty(
    fake_redis,
) -> None:
    """The blind spot CannObserv/broker#13 exists for.

    Evidence capture runs on the tick that first sees a non-resting queue, and
    the timer is every 10 minutes, so an entry written and deleted inside one
    interval left no dump, no finding and no trace. Depth alone cannot see it -
    the queue is empty at both observations. ``entries-added`` can: it is
    monotonic, it survives XDEL and XTRIM, and this repo already treats it as
    the authority on whether a stream was trimmed or wiped.
    """
    dlq = "content.fetch.dlq"
    for _ in range(3):
        entry = await fake_redis.xadd(dlq, {"k": "v"})
        await fake_redis.xdel(dlq, entry)
    assert await fake_redis.xlen(dlq) == 0

    findings, _ = await collect_broker_findings(
        fake_redis, previous_state={CONTINUITY_ENTRIES_KEY.format(topic=dlq): 1}
    )
    drained = [f for f in findings if f.check == "dlq-unobserved" and f.subject == dlq]
    assert drained, "an emptied queue that grew since the last tick must be reported"
    assert "at least 2" in drained[0].message, drained[0].message


async def test_a_dlq_entry_removed_after_being_seen_is_not_reported(fake_redis) -> None:
    """The ordinary, healthy drain must stay silent, or the check is noise.

    An entry that was present for a tick was captured to the evidence directory
    on that tick, so its later removal loses nothing. The counter was already
    advanced when it was observed, which is exactly why comparing against the
    stored total gets this right without needing to know what was deleted.
    """
    dlq = "content.fetch.dlq"
    entry = await fake_redis.xadd(dlq, {"k": "v"})
    _findings, state = await collect_broker_findings(fake_redis, previous_state={})
    await fake_redis.xdel(dlq, entry)

    findings, _ = await collect_broker_findings(fake_redis, previous_state=state)
    assert not [f for f in findings if f.check == "dlq-unobserved"]


async def test_the_unobserved_count_is_a_floor_rather_than_a_guess(fake_redis) -> None:
    """Three added, one still present: at least two went unseen, and the finding
    must not claim to know it was exactly two.

    Depth can include entries added *before* the last tick, which were captured
    then, so ``added - before - depth`` is a lower bound on what passed through
    unseen rather than an exact count. Stated as a floor because the alternative
    is a number that is sometimes wrong in the direction of crying wolf.
    """
    dlq = "content.replicate.dlq"
    for _ in range(2):
        entry = await fake_redis.xadd(dlq, {"k": "v"})
        await fake_redis.xdel(dlq, entry)
    await fake_redis.xadd(dlq, {"k": "survivor"})

    findings, _ = await collect_broker_findings(
        fake_redis, previous_state={CONTINUITY_ENTRIES_KEY.format(topic=dlq): 0}
    )
    unobserved = [f for f in findings if f.check == "dlq-unobserved"]
    assert unobserved, "a non-empty queue does not excuse the entries that vanished"
    assert "at least 2" in unobserved[0].message, unobserved[0].message
    # And the depth finding still fires: they are independent facts about the queue.
    assert [f for f in findings if f.check == "dlq" and f.subject == dlq]


async def test_a_first_sighting_of_an_empty_dlq_says_nothing(fake_redis) -> None:
    """No stored total means no baseline, and inventing one would report every
    queue's whole history on the first tick after a deploy."""
    await fake_redis.xadd("content.blobs.dlq", {"k": "v"})
    await fake_redis.xtrim("content.blobs.dlq", maxlen=0)

    findings, state = await collect_broker_findings(fake_redis, previous_state={})
    assert not [f for f in findings if f.check == "dlq-unobserved"]
    assert state[CONTINUITY_ENTRIES_KEY.format(topic="content.blobs.dlq")] == 1, (
        "but it must record the baseline"
    )


async def test_entries_added_going_backwards_on_a_dlq_is_a_worse_finding(fake_redis) -> None:
    """A counter that fell means the KEY was deleted, not the entries drained.

    ``entries-added`` only resets when the stream itself goes, so this is the
    queue's identity being reset - and any evidence dump still on disk now
    belongs to a stream that no longer exists. Reported apart from the drain
    case because the response is different: the drain is triage, this is
    "something deleted a dead-letter queue".
    """
    dlq = "content.revisions.dlq"
    await fake_redis.xadd(dlq, {"k": "v"})

    findings, _ = await collect_broker_findings(
        fake_redis, previous_state={CONTINUITY_ENTRIES_KEY.format(topic=dlq): 5}
    )
    reset = [f for f in findings if f.check == "stream-reset" and f.subject == dlq]
    assert reset, "entries-added 5 -> 1 is a deleted stream, not a drain"


async def test_a_server_without_entries_added_still_reports_the_depth(fake_redis) -> None:
    """The continuity half is additive, and it must not be able to take the
    load-bearing half with it.

    ``entries-added`` is a Redis 7.0 field and this repo's floor is a
    client-side assertion, not something the probe can assume. Reading it out of
    the same ``XINFO STREAM`` the depth comes from means one failure mode covers
    both, so the field's absence is handled *after* the depth finding and its
    evidence capture, never before.
    """
    await fake_redis.xadd("content.fetch.dlq", {"k": "v"})

    class _NoEntriesAdded(_DelegatingClient):
        async def xinfo_stream(self, topic, *a, **kw):
            info = dict(await self._delegate.xinfo_stream(topic, *a, **kw))
            info.pop("entries-added", None)
            return info

    findings, state = await collect_broker_findings(_NoEntriesAdded(fake_redis), previous_state={})

    assert [f.subject for f in findings if f.check == "dlq"] == ["content.fetch.dlq"]
    assert not [f for f in findings if f.check in {"dlq-unobserved", "stream-reset"}]
    assert CONTINUITY_ENTRIES_KEY.format(topic="content.fetch.dlq") not in state


async def test_a_dlq_key_that_vanished_is_reported_once(fake_redis) -> None:
    """The silent half of the reset case, and the worse one.

    ``entries-added`` cannot report its own stream's deletion, and the
    counter-went-backwards finding needs the queue recreated *before the next
    tick* to have anything to compare - so a queue deleted and left deleted used
    to pass unremarked while its evidence dumps sat on disk claiming to describe
    it. Every legitimate disposal leaves the key: ``XDEL`` per entry since
    broker#12, and the ``XTRIM MAXLEN 0`` it replaced.
    """
    dlq = "content.fetch.dlq"
    await fake_redis.xadd(dlq, {"k": "v"})
    _findings, state = await collect_broker_findings(fake_redis, previous_state={})
    assert CONTINUITY_ENTRIES_KEY.format(topic=dlq) in state

    await fake_redis.delete(dlq)  # not a drain - the key itself

    findings, after = await collect_broker_findings(fake_redis, previous_state=state)
    gone = [f for f in findings if f.check == "stream-reset" and f.subject == dlq]
    assert gone, "a dead-letter queue whose key is gone must not be silence"
    assert "replicator" in gone[0].message, gone[0].message

    # Once. The baseline is not carried forward, so the next tick has nothing to
    # compare and a permanently-absent queue does not become a permanent finding.
    assert CONTINUITY_ENTRIES_KEY.format(topic=dlq) not in after
    findings, _ = await collect_broker_findings(fake_redis, previous_state=after)
    assert not [f for f in findings if f.check == "stream-reset"]


async def test_a_queue_that_goes_between_the_scan_and_the_read_keeps_its_baseline(
    fake_redis,
) -> None:
    """The vanished-key finding must survive a ``DEL`` that races the tick.

    ``SCAN`` returns the key and the ``XINFO STREAM`` for it then fails, which
    is what a deletion landing between the two looks like from here. That tick
    cannot judge - the key was in the scan, so the vanished rule declines - so
    the baseline has to be carried forward for the *next* tick to judge with.
    Dropping it there would make exactly the deletion this check exists for
    unreportable, on any tick unlucky enough to straddle it.
    """
    dlq = "content.fetch.dlq"
    await fake_redis.xadd(dlq, {"k": "v"})
    _findings, state = await collect_broker_findings(fake_redis, previous_state={})
    entries_key = CONTINUITY_ENTRIES_KEY.format(topic=dlq)
    assert entries_key in state

    class _ReadRaces(_DelegatingClient):
        async def xinfo_stream(self, topic, *a, **kw):
            if topic == dlq:
                raise ResponseError("ERR no such key")
            return await self._delegate.xinfo_stream(topic, *a, **kw)

    findings, during = await collect_broker_findings(_ReadRaces(fake_redis), previous_state=state)
    assert not [f for f in findings if f.check == "stream-reset"], "the scan still returned it"
    assert during[entries_key] == state[entries_key], "the baseline must survive the race"

    await fake_redis.delete(dlq)
    findings, _ = await collect_broker_findings(fake_redis, previous_state=during)
    assert [f.check for f in findings if f.subject == dlq] == ["stream-reset"]


async def test_a_declared_stream_going_absent_is_not_a_vanished_dlq(fake_redis) -> None:
    """The vanished-key rule is keyed on the ``*.dlq`` suffix, because it rests on
    dead-letter disposal always leaving the key. A fact stream has no such
    guarantee - ``evaluate_stream_continuity`` owns those - so it must not be
    swept up by a prefix match on the shared state key."""
    for _ in range(3):
        await fake_redis.xadd(CONTENT_FETCH, {"k": "v"})
    _findings, state = await collect_broker_findings(fake_redis, previous_state={})

    await fake_redis.delete(CONTENT_FETCH)

    findings, _ = await collect_broker_findings(fake_redis, previous_state=state)
    reset = [f for f in findings if f.check == "stream-reset"]
    assert not [f for f in reset if "dead-letter" in f.message], (
        "the *.dlq suffix filter is what keeps the DLQ rule off a fact stream"
    )


async def test_a_declared_stream_that_vanished_is_reported(fake_redis) -> None:
    """broker#10's failure mode, on the nine streams that carry the data.

    ``_collect_stream`` returns early when the key does not exist, calling it
    dormancy - true of a stream nothing has written **yet**, and false of one
    that was written and then destroyed. That early return happens before the
    continuity check, before the group check, and before the baselines are
    written, so a ``DEL`` or a ``FLUSHDB`` produced ``finding_count: 0`` and then
    erased the only record that the stream had ever existed. Verified against a
    real server: every subsequent tick was silent too.

    Which is exactly what ``evaluate_stream_continuity`` exists to prevent - it
    was only ever reached while the stream was still there to read a counter
    from. CannObserv/broker#13 closed the identical hole for dead-letter queues
    and left this one open, which is the wrong half to close first: these carry
    the cluster's data.
    """
    await fake_redis.xadd(CONTENT_FETCH, {"k": "v"})
    _findings, state = await collect_broker_findings(fake_redis, previous_state={})
    assert state[CONTINUITY_ENTRIES_KEY.format(topic=CONTENT_FETCH)] == 1

    await fake_redis.delete(CONTENT_FETCH)
    findings, after = await collect_broker_findings(fake_redis, previous_state=state)

    vanished = [f for f in findings if f.check == "stream-reset" and f.subject == CONTENT_FETCH]
    assert vanished, "a declared stream that was written and is now absent is not dormancy"
    # Fires once: the baseline is not carried forward, so the next tick has
    # nothing to compare and says nothing further.
    assert CONTINUITY_ENTRIES_KEY.format(topic=CONTENT_FETCH) not in after
    again, _ = await collect_broker_findings(fake_redis, previous_state=after)
    assert not [f for f in again if f.check == "stream-reset"]


async def test_a_stream_nothing_has_written_yet_is_still_dormancy(fake_redis) -> None:
    """The other half, and the reason the early return is there. No baseline
    means the stream was never seen, which on this cluster is an ordinary state -
    ``content.replicate`` sat at zero entries for the whole of broker#7."""
    findings, state = await collect_broker_findings(fake_redis, previous_state={})
    assert not [f for f in findings if f.check == "stream-reset"]
    assert CONTINUITY_ENTRIES_KEY.format(topic=CONTENT_FETCH) not in state


async def test_the_state_file_keeps_pending_and_dlq_totals_apart(fake_redis, tmp_path) -> None:
    """One flat file, and a dead-letter queue's baseline uses the key convention
    the declared streams already use.

    `@entries-added/<topic>` was already there for `evaluate_stream_continuity`;
    the `@` namespaces it away from the pending counters, which are keyed
    `<topic>/<group>`. Adding a second convention for DLQs would have meant two
    spellings of one idea in one file, and this test is what pins that it did
    not happen.
    """
    await fake_redis.xadd(CONTENT_REVISIONS, {"k": "v"})
    await fake_redis.xgroup_create(CONTENT_REVISIONS, "archiver.revisions", id="0")
    await fake_redis.xreadgroup("archiver.revisions", "c1", {CONTENT_REVISIONS: ">"}, count=10)
    await fake_redis.xadd("content.fetch.dlq", {"k": "v"})

    state_path = tmp_path / "state.json"
    await bus_health.run_once(fake_redis, state_path=state_path, disk_usage=_healthy_disk)
    stored = json.loads(state_path.read_text())

    assert stored[f"{CONTENT_REVISIONS}/archiver.revisions"] == 1
    assert stored[CONTINUITY_ENTRIES_KEY.format(topic="content.fetch.dlq")] == 1
    assert all(isinstance(v, int) for v in stored.values())


async def test_collect_tolerates_a_non_stream_dlq_key(fake_redis) -> None:
    """A stray non-stream key named *.dlq must not raise
    WRONGTYPE out of the DLQ scan - that would surface as "broker unreachable"
    and discard every other finding on the tick."""
    await fake_redis.set("stray.dlq", "not-a-stream")
    await fake_redis.xadd("content.fetch.dlq", {"k": "v"})

    findings, _ = await collect_broker_findings(fake_redis, previous_state={})

    assert not any(f.check == "broker" for f in findings)
    assert [f.subject for f in findings if f.check == "dlq"] == ["content.fetch.dlq"]


# --- DLQ triage: who owns it, and the evidence that must outlive the entries ---
#
# CannObserv/broker#1 Phase 5 split the old single "drainer" role. Detection,
# evidence capture and escalation are the broker's, because they are mechanical
# and suffix-keyed; triage and the XTRIM belong to whoever can read the payload,
# which is the stream's own consumer. These tests pin both halves.


def _dlq_finding(findings, subject: str):
    return next(f for f in findings if f.check == "dlq" and f.subject == subject)


def test_every_dlq_drainer_is_derived_from_its_topic() -> None:
    """The assignment is per-stream data; the *key* is still computed.

    ``dlq_name`` is co-core's, the same helper the writers use, so the mapping
    cannot drift into naming a queue no service actually writes.
    """
    for key in bus_health.DLQ_DRAINERS:
        topic = key.removesuffix(".dlq")
        assert dlq_name(topic) == key


async def test_dlq_finding_names_its_drainer(fake_redis) -> None:
    """A finding with no addressee is how content.fetch.dlq reached 110
    (CannObserv/archiver#162). The journald line is the only artifact anyone
    sees, so the owner has to be in it."""
    await fake_redis.xadd("content.fetch.dlq", {"k": "v"})
    findings, _ = await collect_broker_findings(fake_redis, previous_state={})
    assert "replicator" in _dlq_finding(findings, "content.fetch.dlq").message


async def test_dlq_with_no_named_drainer_falls_to_the_broker(fake_redis) -> None:
    """The backstop, and the reason the mapping is allowed to be incomplete.

    A ``*.dlq`` key nobody claims is exactly the "DLQ with nobody named"
    failure, and the broker is the only party that can see one - the per-service
    ACL users cannot SCAN the instance. So an unknown queue is reported as
    unassigned rather than skipped.
    """
    await fake_redis.xadd("unknown.topic.dlq", {"k": "v"})
    findings, _ = await collect_broker_findings(fake_redis, previous_state={})
    message = _dlq_finding(findings, "unknown.topic.dlq").message
    assert "no drainer assigned" in message
    assert "backstop" in message


async def test_dlq_evidence_is_captured_before_anyone_can_trim(fake_redis, tmp_path) -> None:
    """Audit, back up, trim, verify - in that order, because reversing it
    destroys the evidence the trim needed justifying with. That first step is
    the one an operator has to remember; here it happens on the tick that first
    sees the depth."""
    await fake_redis.xadd("content.fetch.dlq", {"k": "v"}, id="5-0")
    findings, _ = await collect_broker_findings(
        fake_redis, previous_state={}, evidence_dir=tmp_path
    )

    captured = sorted((tmp_path / "content.fetch.dlq").iterdir())
    assert [p.name for p in captured] == ["5-0.json"]
    # An operator-facing line; "1 entries" reads as a bug in the probe.
    assert "1 entry captured" in _dlq_finding(findings, "content.fetch.dlq").message
    assert json.loads(captured[0].read_text()) == [{"id": "5-0", "fields": {"k": "v"}}]
    assert str(captured[0]) in _dlq_finding(findings, "content.fetch.dlq").message


async def test_dlq_evidence_is_not_recaptured_while_the_queue_is_unchanged(
    fake_redis, tmp_path
) -> None:
    """A DLQ rests non-empty for as long as triage takes. Re-dumping the same
    entries every ten minutes would bury the one dump that matters."""
    await fake_redis.xadd("content.fetch.dlq", {"k": "v"}, id="5-0")
    for _ in range(3):
        findings, _ = await collect_broker_findings(
            fake_redis, previous_state={}, evidence_dir=tmp_path
        )

    assert [p.name for p in (tmp_path / "content.fetch.dlq").iterdir()] == ["5-0.json"]
    assert "already captured" in _dlq_finding(findings, "content.fetch.dlq").message


async def test_dlq_evidence_capture_is_incremental(fake_redis, tmp_path) -> None:
    """Growth after a capture is new evidence, and only the new entries are
    new. Capturing the whole queue again on every growth turns a queue that
    fills one entry at a time into a quadratic pile of dumps."""
    await fake_redis.xadd("content.fetch.dlq", {"n": "1"}, id="5-0")
    await collect_broker_findings(fake_redis, previous_state={}, evidence_dir=tmp_path)
    await fake_redis.xadd("content.fetch.dlq", {"n": "2"}, id="6-0")
    await collect_broker_findings(fake_redis, previous_state={}, evidence_dir=tmp_path)

    topic_dir = tmp_path / "content.fetch.dlq"
    assert sorted(p.name for p in topic_dir.iterdir()) == ["5-0.json", "6-0.json"]
    assert json.loads((topic_dir / "6-0.json").read_text()) == [{"id": "6-0", "fields": {"n": "2"}}]


async def test_dlq_evidence_high_water_compares_ids_numerically(fake_redis, tmp_path) -> None:
    """``"9-0" > "10-0"`` lexicographically while ``9 < 10``.

    Recorded in CannObserv/broker#1 against the watcher#285 rename and it
    applies here for the same reason: a string comparison would read the
    9-0 capture as ahead of the queue and silently never capture 10-0 - wrong
    in exactly the direction that loses evidence.
    """
    await fake_redis.xadd("content.fetch.dlq", {"n": "9"}, id="9-0")
    await collect_broker_findings(fake_redis, previous_state={}, evidence_dir=tmp_path)
    await fake_redis.xadd("content.fetch.dlq", {"n": "10"}, id="10-0")
    await collect_broker_findings(fake_redis, previous_state={}, evidence_dir=tmp_path)

    assert (tmp_path / "content.fetch.dlq" / "10-0.json").exists()


async def test_dlq_evidence_failure_still_reports_the_depth(fake_redis, tmp_path) -> None:
    """Capture is best-effort; the finding is not. A full disk or a bad
    StateDirectory must not swallow the one signal that says a DLQ is not at
    rest, and the operator has to be told the backup did not happen before they
    reach for XTRIM."""
    blocked = tmp_path / "blocked"
    blocked.write_text("not a directory")
    await fake_redis.xadd("content.fetch.dlq", {"k": "v"})

    findings, _ = await collect_broker_findings(fake_redis, previous_state={}, evidence_dir=blocked)

    message = _dlq_finding(findings, "content.fetch.dlq").message
    assert "capture FAILED" in message
    assert not any(f.check == "broker" for f in findings)


async def test_run_once_captures_evidence_beside_its_state_file(
    fake_redis, tmp_path, monkeypatch
) -> None:
    """The timer passes only --state-file, so the evidence directory is derived
    from it and lands inside systemd's StateDirectory rather than somewhere the
    unit's User= may not own."""
    monkeypatch.setattr(bus_health.logger, "warning", MagicMock())
    await fake_redis.xadd("content.fetch.dlq", {"k": "v"}, id="5-0")

    await bus_health.run_once(
        fake_redis, state_path=tmp_path / "state.json", disk_usage=_healthy_disk
    )

    assert (tmp_path / bus_health.DLQ_EVIDENCE_DIRNAME / "content.fetch.dlq" / "5-0.json").exists()


# --- state file ---


def test_state_round_trip(tmp_path) -> None:
    path = tmp_path / "state.json"
    save_state(path, {"a/b": 3})
    assert load_state(path) == {"a/b": 3}


def test_state_missing_file_is_empty(tmp_path) -> None:
    assert load_state(tmp_path / "absent.json") == {}


def test_state_corrupt_file_is_empty(tmp_path) -> None:
    path = tmp_path / "state.json"
    path.write_text("not json{")
    assert load_state(path) == {}


# --- logging surface ---


def _healthy_disk(path: str) -> tuple[int, int, int]:
    total = 100 * 1024**3
    return total, total // 2, total // 2


async def test_run_once_logs_each_finding_at_warning(fake_redis, tmp_path, monkeypatch) -> None:
    """Spies the module logger rather than using caplog: configure_logging()
    replaces root.handlers, which defeats pytest's capture handler."""
    await fake_redis.xadd("content.fetch.dlq", {"k": "v"})
    warning_spy, info_spy = MagicMock(), MagicMock()
    monkeypatch.setattr(bus_health.logger, "warning", warning_spy)
    monkeypatch.setattr(bus_health.logger, "info", info_spy)

    findings = await bus_health.run_once(
        fake_redis,
        state_path=tmp_path / "state.json",
        disk_usage=_healthy_disk,
    )

    assert findings
    # One line per finding, plus the summary line which escalates to WARNING
    # while any finding exists (same persistent-visibility contract as archiver#112).
    assert warning_spy.call_count == len(findings) + 1
    info_spy.assert_not_called()


async def test_run_once_healthy_logs_info_summary(fake_redis, tmp_path, monkeypatch) -> None:
    warning_spy, info_spy = MagicMock(), MagicMock()
    monkeypatch.setattr(bus_health.logger, "warning", warning_spy)
    monkeypatch.setattr(bus_health.logger, "info", info_spy)

    findings = await bus_health.run_once(
        fake_redis,
        state_path=tmp_path / "state.json",
        disk_usage=_healthy_disk,
    )

    assert findings == []
    warning_spy.assert_not_called()
    info_spy.assert_called_once()
    assert info_spy.call_args.kwargs["extra"]["finding_count"] == 0


# --- timer entrypoint ---


@pytest.fixture
def stub_main_deps(monkeypatch, tmp_path):
    """Neutralise everything main() touches outside the probe itself, and hand
    back the spies the entrypoint contracts are asserted on."""
    monkeypatch.setenv("BROKER_REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setattr(bus_health, "configure_logging", lambda: None)

    client = AsyncMock()
    from_url = MagicMock(return_value=client)
    monkeypatch.setattr(bus_health.Redis, "from_url", from_url)

    async def _noop_run_once(*a, **kw):
        return []

    monkeypatch.setattr(bus_health, "run_once", _noop_run_once)
    return SimpleNamespace(
        from_url=from_url,
        client=client,
        state_file=tmp_path / "state.json",
    )


def test_main_bounds_the_redis_socket(stub_main_deps) -> None:
    """A hung (not refusing) broker would otherwise block past systemd's
    TimeoutStartSec and fail the unit, instead of producing the WARN-only
    "broker unreachable" finding this module promises - the probe's contract
    breaking in exactly the degraded state it exists to observe."""
    assert bus_health.main(["--state-file", str(stub_main_deps.state_file)]) == 0

    kwargs = stub_main_deps.from_url.call_args.kwargs
    assert kwargs["socket_connect_timeout"] > 0
    assert kwargs["socket_timeout"] > 0


def test_main_releases_the_client_pool(stub_main_deps) -> None:
    """A pool reclaimed during loop teardown emits "Event loop is closed" noise
    into the journald stream this unit exists to keep clean. Archiver's copy
    disposed a database engine here too; this process holds no database
    credential, so there is only one pool left to release."""
    assert bus_health.main(["--state-file", str(stub_main_deps.state_file)]) == 0

    stub_main_deps.client.aclose.assert_awaited_once()


def test_main_without_a_broker_url_reports_and_exits_clean(stub_main_deps, monkeypatch) -> None:
    """The one contract that inverts on the move. In archiver an unset URL meant
    *dormancy* - a legitimate, INFO-level configuration. Here the unit exists
    only to watch a broker and runs on a host that is one, so an unset URL is a
    misconfiguration and is logged at ERROR. Still exit 0: a WARN-only unit that
    starts failing on a config mistake trains an operator to ignore it."""
    monkeypatch.delenv("BROKER_REDIS_URL")
    error_spy = MagicMock()
    monkeypatch.setattr(bus_health.logger, "error", error_spy)

    assert bus_health.main(["--state-file", str(stub_main_deps.state_file)]) == 0

    stub_main_deps.from_url.assert_not_called()
    error_spy.assert_called_once()


async def test_run_once_persists_state(fake_redis, tmp_path) -> None:
    state_path = tmp_path / "state.json"
    await bus_health.run_once(
        fake_redis,
        state_path=state_path,
        disk_usage=_healthy_disk,
    )
    assert json.loads(state_path.read_text()) == {}


# --- stream-kind invariants (cannobserv#384, co-core >=0.13.1) --------------
#
# ``pending_group`` was a hand-kept field whose correctness rested on the
# author knowing the three-kind taxonomy. ``stream_kind`` makes that taxonomy
# machine-readable, so the rule "a config/state stream never carries a group"
# stops being a comment and becomes a constructor guard.


def test_stream_check_rejects_a_pending_group_on_a_config_state_stream() -> None:
    """A config/state stream must never carry a consumer group.

    A group there accumulates a PEL nothing drains: every worker needs every
    message, so nobody acks on behalf of the others. co-core states the rule;
    this makes STREAM_CHECKS unable to express a violation of it.
    """
    with pytest.raises(ValueError, match="config_state"):
        StreamCheck(INFO_REGISTRY, warn_length=10, pending_group="archiver.registry")


def test_stream_check_allows_a_pending_group_on_a_fact_stream() -> None:
    """The guard must not overreach: fact streams are exactly where groups live.

    The group name is deliberately *not* a conventional one. The guard keys on
    the topic's kind and must have no opinion about the group's spelling -
    asserting with ``archiver.revisions`` would leave both behaviours
    consistent with a pass.
    """
    check = StreamCheck(CONTENT_REVISIONS, warn_length=10, pending_group="not-a-convention")
    assert check.pending_group == "not-a-convention"


def test_stream_check_allows_a_pending_group_on_a_command_stream() -> None:
    """``command`` is the third kind, and it takes exactly one group.

    Non-conventional name for the same reason as the fact-stream case above: a
    guard keyed on the group's *spelling* rather than the stream's *kind* would
    pass an ``archiver.``- or ``replicator.``-prefixed name either way.
    """
    check = StreamCheck(CONTENT_REPLICATE, warn_length=10, pending_group="also-not-a-convention")
    assert check.pending_group == "also-not-a-convention"


@pytest.mark.parametrize("topic", [c.topic for c in STREAM_CHECKS])
def test_every_probed_topic_is_classifiable(topic: str) -> None:
    """``StreamCheck``'s guard fails *open* on a ``ValueError`` from ``stream_kind``.

    That swallow is unavoidable - co-core publishes no public set of canonical
    topics to test membership against (``_STREAM_KINDS`` is private) - so its
    safety rests on ``ValueError`` meaning "not canonical" and nothing else. If
    a future co-core stopped classifying a topic archiver probes, the guard
    would quietly stop guarding that stream and no other test would notice.
    This is the tripwire.

    The domain is ``STREAM_CHECKS`` rather than a hand-listed set of co-core
    constants, because that is exactly where the guard runs. A hand list cannot
    notice a stream being added, and would assert about ``content.blobs``,
    which this probe is forbidden to touch at all.

    The kinds come from ``get_args(StreamKind)`` rather than a copied tuple, so
    co-core legitimately adding a fourth kind does not fail this test for the
    wrong reason.
    """
    assert stream_kind(topic) in get_args(StreamKind)


@pytest.mark.parametrize(
    "check",
    [c for c in STREAM_CHECKS if stream_kind(c.topic) == "config_state"],
    ids=lambda c: c.topic,
)
def test_every_config_state_check_probes_last_entry_age(check: StreamCheck) -> None:
    """A groupless stream's only liveness signal is the age of its last entry.

    Replaces an earlier assertion that no config/state check carries a
    ``pending_group``. That became **unfalsifiable** once ``__post_init__``
    started raising on exactly that: ``STREAM_CHECKS`` is built at import, so a
    violation makes the module fail to import and this file fail at
    *collection* - the test could never go red, only vanish.

    This is the invariant the constructor does *not* enforce, and it is the one
    with teeth (CannObserv/archiver#128). A config/state stream has no consumer group, so
    it is invisible to every pending-based check; without an age threshold
    a producer that stopped publishing would look identical to one that is
    merely quiet. Adding a config/state stream to the inventory without
    ``warn_last_entry_age_seconds`` therefore buys a probe that cannot detect
    the failure it exists for.

    Known limit: parametrising over ``STREAM_CHECKS`` means this cannot catch a
    config/state stream dropped from the inventory entirely - that case has no
    entry to iterate. Detecting it would need a hand-maintained list of
    canonical topics, which is the coupling
    ``test_every_probed_topic_is_classifiable`` deliberately removed.
    """
    assert check.warn_last_entry_age_seconds is not None, (
        f"{check.topic} is groupless, so last-entry age is its only liveness probe"
    )


# ---------------------------------------------------------------------------
# The probe's own connection bounds
# ---------------------------------------------------------------------------


class _StopProbe(Exception):
    """Sentinel: stop ``main`` once the client kwargs have been captured."""


def test_probe_client_is_built_with_the_documented_bounds(monkeypatch) -> None:
    """``main`` must apply both timeouts - the WARN-only contract depends on it.

    Without them a *hung* broker (as opposed to a refusing one) blocks until
    systemd's ``TimeoutStartSec`` kills the unit, converting the "broker
    unreachable" finding this probe exists to report into a failed unit that
    reports nothing.

    Replaces the module's ``Redis`` name rather than setting ``from_url`` on the
    class: patching an attribute of the shared ``redis`` class mutates it for
    every importer.
    """
    captured: dict[str, object] = {}

    class _FakeRedis:
        @staticmethod
        def from_url(url, **kwargs):
            captured.update(kwargs)
            raise _StopProbe

    monkeypatch.setenv("BROKER_REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setattr(bus_health, "Redis", _FakeRedis)
    monkeypatch.setattr(bus_health, "configure_logging", lambda: None)

    with pytest.raises(_StopProbe):
        bus_health.main(["--state-file", "/dev/null"])

    assert captured["socket_connect_timeout"] == bus_health.SOCKET_CONNECT_TIMEOUT_SECONDS
    assert captured["socket_timeout"] == bus_health.SOCKET_TIMEOUT_SECONDS


def test_probe_takes_no_retry_so_its_stated_per_call_bound_is_the_real_one() -> None:
    """A retry policy here would silently double every bound in the unit file.

    Measured on redis-py 7.4.1 against a black-holed address:
    ``socket_connect_timeout`` bounds a single *attempt*, not the call, so the
    wait is ``(retries + 1) x socket_connect_timeout`` - 5.01 s at retries=0,
    10.03 s at retries=1. redis-py's default is 0 retries (its stock ``Retry``
    object is configured for zero, which reads as a policy and behaves as none),
    and ``deploy/broker-bus-health.service`` reasons explicitly from "5s connect
    / 10s read" per call when sizing ``TimeoutStartSec=60`` against ~25 calls
    per tick.

    So the probe *relies* on that default. Adding a retry - a reasonable-looking
    change now that every participant reaches this node across a tailnet - would
    halve the effective headroom without touching either number the unit cites.
    """
    client = bus_health.Redis.from_url(
        "redis://localhost:6379/14",
        socket_connect_timeout=bus_health.SOCKET_CONNECT_TIMEOUT_SECONDS,
        socket_timeout=bus_health.SOCKET_TIMEOUT_SECONDS,
    )
    assert client.connection_pool.make_connection().retry._retries == 0


def test_unit_backstop_exceeds_a_single_call_worst_case() -> None:
    """Pin the unit's ``TimeoutStartSec`` against the module's own bounds.

    The unit's comment does this arithmetic in prose across a file boundary no
    test spanned. It is a weak assertion on purpose - the exact call count per
    tick is not worth pinning - but it catches the case that would actually bite:
    someone raising a timeout in this module past the backstop in that unit, so
    every tick against a slow broker is killed before it reports.
    """
    unit = (
        Path(__file__).resolve().parents[1] / "deploy" / "broker-bus-health.service"
    ).read_text()
    match = re.search(r"^TimeoutStartSec=(\d+)$", unit, re.MULTILINE)
    assert match, "the unit lost its TimeoutStartSec backstop"
    backstop = int(match.group(1))
    worst_call = bus_health.SOCKET_CONNECT_TIMEOUT_SECONDS + bus_health.SOCKET_TIMEOUT_SECONDS
    assert worst_call < backstop, (
        f"a single Redis call can take {worst_call}s against a backstop of {backstop}s; "
        "the unit would be killed before the probe could report anything"
    )


# --- notifier check-in (CannObserv/broker#3) ---
#
# The probe's findings were an audience of zero: a WARN line in journald on a
# node nobody is logged into. The check-in is what gives them a reader - and,
# more importantly, what makes SILENCE detectable, since a dead probe, a stopped
# timer or a dead node all produce zero findings and zero traffic.


@pytest.fixture
def checkin_env(monkeypatch):
    """Both variables set, plus a spy standing in for the HTTP call."""
    monkeypatch.setenv("NOTIFIER_MONITOR_ID", "01JMONITOR")
    monkeypatch.setenv("NOTIFIER_API_KEY", "k3y")
    calls = []

    def _spy(url, data, headers, timeout):
        calls.append({"url": url, "data": json.loads(data), "headers": headers})
        return 200

    monkeypatch.setattr(bus_health, "_http_post", _spy)
    return calls


async def test_checkin_is_inert_when_unconfigured(fake_redis, tmp_path, monkeypatch) -> None:
    """Unset is the default and must cost nothing. The probe predates the
    check-in and has to keep working without it - a node that has not been wired
    to notifier is not misconfigured."""
    monkeypatch.delenv("NOTIFIER_MONITOR_ID", raising=False)
    monkeypatch.delenv("NOTIFIER_API_KEY", raising=False)
    monkeypatch.setattr(
        bus_health, "_http_post", MagicMock(side_effect=AssertionError("must not post"))
    )
    monkeypatch.setattr(bus_health.logger, "info", MagicMock())

    await bus_health.run_once(
        fake_redis, state_path=tmp_path / "state.json", disk_usage=_healthy_disk
    )


async def test_checkin_posts_every_tick_even_with_no_findings(
    fake_redis, tmp_path, monkeypatch, checkin_env
) -> None:
    """The arrival IS the signal. A findings-only push would be silent in every
    case where the probe itself is the thing that died."""
    monkeypatch.setattr(bus_health.logger, "info", MagicMock())

    findings = await bus_health.run_once(
        fake_redis, state_path=tmp_path / "state.json", disk_usage=_healthy_disk
    )

    assert findings == []
    assert len(checkin_env) == 1
    assert checkin_env[0]["data"]["status"] == "ok"
    assert checkin_env[0]["headers"]["X-API-Key"] == "k3y"


async def test_checkin_reports_alert_and_carries_the_findings(
    fake_redis, tmp_path, monkeypatch, checkin_env
) -> None:
    """`status` is the probe's own judgement - notifier does not learn this
    repo's taxonomy - and the findings ride in `variables` for the monitor's
    template to render."""
    monkeypatch.setattr(bus_health.logger, "warning", MagicMock())
    await fake_redis.xadd("content.fetch.dlq", {"k": "v"})

    await bus_health.run_once(
        fake_redis, state_path=tmp_path / "state.json", disk_usage=_healthy_disk
    )

    sent = checkin_env[0]["data"]
    assert sent["status"] == "alert"
    assert sent["variables"]["finding_count"] == 1
    assert sent["variables"]["source"]
    finding = sent["variables"]["findings"][0]
    assert {"check", "subject", "message"} <= finding.keys()


def test_the_checkin_url_cannot_be_pointed_at_the_dev_endpoint() -> None:
    """Structural, and it is the reason the base URL is a constant rather than
    configuration.

    `notifier:9001` is `notifier_dev`, running against DEV_DATABASE_URL, and its
    `/health` is **byte-identical** to production's - same status, same build -
    so a wrong port cannot be caught by the obvious check. Worse, this monitor
    alarms on the ABSENCE of check-ins, so a one-character typo would not degrade
    it, it would invert it: the production monitor goes silent and reports the
    broker dead while the broker is fine.

    The operator therefore supplies a monitor id, never a host or a port. Same
    move as `databases 1` against the db15 vector - make the wrong destination
    unnameable rather than merely discouraged.
    """
    url = bus_health._checkin_url("01JMONITOR")
    assert url.startswith("http://notifier:9000/")
    assert "9001" not in url


async def test_a_failed_checkin_is_a_warning_and_never_a_failed_unit(
    fake_redis, tmp_path, monkeypatch, checkin_env
) -> None:
    """WARN-only is the probe's contract and the check-in does not get to break
    it. A monitoring unit that starts failing on its own transport trains an
    operator to ignore it - which is the failure this whole issue exists to
    prevent, arriving through the fix."""
    monkeypatch.setattr(bus_health, "_http_post", MagicMock(return_value=503))
    warning_spy = MagicMock()
    monkeypatch.setattr(bus_health.logger, "warning", warning_spy)
    monkeypatch.setattr(bus_health.logger, "info", MagicMock())

    findings = await bus_health.run_once(
        fake_redis, state_path=tmp_path / "state.json", disk_usage=_healthy_disk
    )

    assert findings == []  # the tick itself was clean
    assert any("check-in" in str(c).lower() for c in warning_spy.call_args_list)


async def test_a_checkin_that_raises_does_not_take_the_tick_with_it(
    fake_redis, tmp_path, monkeypatch, checkin_env
) -> None:
    """A DNS failure, a DERP outage or a notifier restart must not lose the
    findings the tick already collected - they are still going to journald,
    which is the floor this repo never gives up."""
    monkeypatch.setattr(bus_health, "_http_post", MagicMock(side_effect=OSError("no route")))
    monkeypatch.setattr(bus_health.logger, "warning", MagicMock())
    await fake_redis.xadd("content.fetch.dlq", {"k": "v"})

    findings = await bus_health.run_once(
        fake_redis, state_path=tmp_path / "state.json", disk_usage=_healthy_disk
    )

    assert [f.check for f in findings] == ["dlq"]


async def test_half_configured_is_reported_not_ignored(fake_redis, tmp_path, monkeypatch) -> None:
    """One variable without the other is a config mistake, and the failure it
    would otherwise produce is silence - the same shape as the flag-plus-URL
    startup guards the participants carry."""
    monkeypatch.setenv("NOTIFIER_MONITOR_ID", "01JMONITOR")
    monkeypatch.delenv("NOTIFIER_API_KEY", raising=False)
    monkeypatch.setattr(
        bus_health, "_http_post", MagicMock(side_effect=AssertionError("must not post"))
    )
    error_spy = MagicMock()
    monkeypatch.setattr(bus_health.logger, "error", error_spy)
    monkeypatch.setattr(bus_health.logger, "info", MagicMock())

    await bus_health.run_once(
        fake_redis, state_path=tmp_path / "state.json", disk_usage=_healthy_disk
    )

    error_spy.assert_called_once()


# --- the broker losing data (CannObserv/broker#10) ---
#
# Every other threshold here is an UPPER bound, so a broker that has lost
# entries looks exceptionally healthy. On 2026-09-10 a `databases 1` restart
# replayed a historical FLUSHDB against db0, the broker came up holding 4% of
# its entries, and the probe ticked twice reporting finding_count 0 - correctly,
# by its own rules.


def test_a_trim_is_not_a_loss() -> None:
    """The tolerance question, and why raw length cannot be the signal.

    Three streams shrink as normal operation: `info.changes` rides archiver's
    periodic XTRIM, `info.registry` is capped on every publish (archiver#141)
    and can drop a large fraction in one tick, and the LWW streams carry a
    producer-side maxlen. A length-based rule either misses the wipe or cries
    wolf on all three.
    """
    check = _check_for(INFO_REGISTRY)
    findings = bus_health.evaluate_stream_continuity(
        check, entries_added=2721, entries_added_prev=2600, length=116, length_prev=2605
    )
    assert findings == []


def test_entries_added_going_backwards_is_a_reset() -> None:
    """The signal that actually separates the two.

    `entries-added` is monotonic for the life of a stream object: a trim removes
    entries while it keeps climbing. It can only fall if the stream was
    destroyed and recreated - which is what a FLUSHDB, a FLUSHALL, a restore
    from a stale snapshot, or a DEL followed by a fresh XADD all look like from
    here.
    """
    findings = bus_health.evaluate_stream_continuity(
        _check_for(CONTENT_FETCH),
        entries_added=30,
        entries_added_prev=948,
        length=30,
        length_prev=948,
    )
    assert [f.check for f in findings] == ["stream-reset"]
    assert "948" in findings[0].message and "30" in findings[0].message


def test_a_never_trimmed_stream_must_never_shrink() -> None:
    """The cheap floor, for the streams where any decrease is a fault by
    definition. `content.replicate` is carved out of every trim path, so nothing
    legitimate can shorten it - and it is the least tolerant stream on the bus."""
    findings = bus_health.evaluate_stream_continuity(
        _check_for(CONTENT_REPLICATE),
        entries_added=40,
        entries_added_prev=40,
        length=12,
        length_prev=40,
    )
    assert [f.check for f in findings] == ["stream-shrank"]


def test_continuity_needs_a_previous_tick() -> None:
    """First tick after a deploy, a state-file loss, or a new stream. Nothing to
    compare against is not a finding - the two-tick pending rule takes the same
    position for the same reason."""
    assert (
        bus_health.evaluate_stream_continuity(
            _check_for(CONTENT_FETCH),
            entries_added=30,
            entries_added_prev=None,
            length=30,
            length_prev=None,
        )
        == []
    )


async def test_collect_detects_a_wiped_stream_across_ticks(fake_redis, tmp_path) -> None:
    """End to end against a real stream object, because the whole check rests on
    what `XINFO STREAM` reports for `entries-added` after a recreate."""
    for _ in range(5):
        await fake_redis.xadd(CONTENT_FETCH, {"k": "v"})
    _, state = await collect_broker_findings(fake_redis, previous_state={})

    await fake_redis.delete(CONTENT_FETCH)  # the wipe
    await fake_redis.xadd(CONTENT_FETCH, {"k": "v"})

    findings, _ = await collect_broker_findings(fake_redis, previous_state=state)
    assert any(f.check == "stream-reset" and f.subject == CONTENT_FETCH for f in findings)


async def test_continuity_state_survives_a_broker_outage(fake_redis, tmp_path) -> None:
    """An unreachable broker must not reset the baseline, or the tick after an
    outage compares against nothing and a wipe during the outage goes unseen.
    Same contract the pending counters already have."""

    class DownRedis:
        def __getattr__(self, _name):
            async def _raise(*a, **kw):
                raise OSError("down")

            return _raise

    for _ in range(5):
        await fake_redis.xadd(CONTENT_FETCH, {"k": "v"})
    _, state = await collect_broker_findings(fake_redis, previous_state={})
    assert any(k.startswith("@") for k in state)

    _, after_outage = await collect_broker_findings(DownRedis(), previous_state=state)
    assert after_outage == state


# --- the backup, and the persistence that feeds it (CannObserv/broker#4) ---
#
# src/broker/backup.py writes a state file; the probe reads it and turns
# silence into a finding, the same way the notifier check-in turned the probe's
# own silence into one. Three states matter: no success on record, a success
# too old, and a failure newer than the last success. A fourth is the
# snapshot's own age - the job can succeed hourly while shipping the same file.

_T0 = datetime(2026, 9, 10, 16, 0, tzinfo=UTC)


def _backup_state(**overrides) -> dict:
    state = {
        "last_success_at": "2026-09-10T15:00:03Z",
        "snapshot_at": "2026-09-10T14:55:11Z",
        "object": "gs://b/co-broker/20260910T145511Z.rdb.gz",
        "outcome": "uploaded",
    }
    state.update(overrides)
    return state


def test_backup_fresh_is_healthy() -> None:
    assert evaluate_backup(_backup_state(), now=_T0) == []


def test_backup_never_run_on_an_installed_unit_is_a_finding() -> None:
    """The state directory exists (systemd made it on first start) but no state
    was ever written: the unit is installed and has never completed a run."""
    (finding,) = evaluate_backup(None, now=_T0, installed=True)
    assert finding.check == "backup"
    assert "never" in finding.message


def test_backup_absent_where_the_unit_is_not_installed_is_not_a_finding() -> None:
    """Dev clones, CI, and a node whose backup is not wired yet. No state
    directory means no unit; that is a deploy checklist item, not a probe
    finding every ten minutes."""
    assert evaluate_backup(None, now=_T0, installed=False) == []


def test_backup_stale_success_is_a_finding() -> None:
    late = _T0 + timedelta(seconds=BACKUP_WARN_MAX_AGE_SECONDS + 1)
    (finding,) = evaluate_backup(_backup_state(), now=late)
    assert finding.check == "backup"
    assert "last successful backup" in finding.message


def test_backup_failure_newer_than_success_is_a_finding() -> None:
    state = _backup_state(last_failure_at="2026-09-10T15:30:00Z", last_error="NotFound: bucket")
    (finding,) = evaluate_backup(state, now=_T0)
    assert finding.check == "backup"
    assert "NotFound: bucket" in finding.message


def test_backup_old_failure_before_a_newer_success_is_not_a_finding() -> None:
    state = _backup_state(last_failure_at="2026-09-10T14:30:00Z", last_error="transient")
    assert evaluate_backup(state, now=_T0) == []


def test_backup_corrupt_state_reads_as_never_run() -> None:
    (finding,) = evaluate_backup({"last_success_at": "not a time"}, now=_T0)
    assert finding.check == "backup"


def test_persistence_healthy() -> None:
    info = {
        "rdb_last_bgsave_status": "ok",
        "aof_last_write_status": "ok",
        "aof_last_bgrewrite_status": "ok",
    }
    assert evaluate_persistence(info, now=_T0) == []


@pytest.mark.parametrize(
    "field", ["rdb_last_bgsave_status", "aof_last_write_status", "aof_last_bgrewrite_status"]
)
def test_persistence_error_is_a_finding(field: str) -> None:
    """A failed BGSAVE is the backup's blind spot seen from the other side: the
    file the job ships stops changing. A failed AOF write is worse. Both are one
    INFO section the probe already pays for."""
    info = {
        "rdb_last_bgsave_status": "ok",
        "aof_last_write_status": "ok",
        "aof_last_bgrewrite_status": "ok",
        field: "err",
    }
    (finding,) = evaluate_persistence(info, now=_T0)
    assert finding.check == "persistence"
    assert field in finding.message


def test_persistence_missing_fields_are_not_findings() -> None:
    """fakeredis and a server without the section: a probe limitation, not a fault."""
    assert evaluate_persistence({}, now=_T0) == []


_OK = {
    "rdb_last_bgsave_status": "ok",
    "aof_last_write_status": "ok",
    "aof_last_bgrewrite_status": "ok",
}


def test_persistence_overdue_save_is_a_finding() -> None:
    """The backup's blind spot, seen from the server: changes have been waiting
    longer than any `save` rule should allow, so dump.rdb - and therefore every
    hourly backup - is the same stale file. Read from INFO rather than from the
    backup's state file, because only the server can tell "nothing to save"
    from "not saving"."""
    info = {
        **_OK,
        "rdb_changes_since_last_save": 5,
        "rdb_last_save_time": int((_T0 - timedelta(hours=4)).timestamp()),
    }
    (finding,) = evaluate_persistence(info, now=_T0)
    assert finding.check == "persistence"
    assert "5 changes" in finding.message
    assert "backup" in finding.message


def test_persistence_idle_server_is_not_overdue() -> None:
    """No changes means no save is due, however old the last one is. The
    state-side rule this replaced would have called an idle broker broken."""
    info = {
        **_OK,
        "rdb_changes_since_last_save": 0,
        "rdb_last_save_time": int((_T0 - timedelta(hours=10)).timestamp()),
    }
    assert evaluate_persistence(info, now=_T0) == []


def test_persistence_recent_save_with_pending_changes_is_not_overdue() -> None:
    info = {
        **_OK,
        "rdb_changes_since_last_save": 500,
        "rdb_last_save_time": int((_T0 - timedelta(minutes=10)).timestamp()),
    }
    assert evaluate_persistence(info, now=_T0) == []


async def test_run_once_reports_the_backup_when_told_where_its_state_is(
    fake_redis, tmp_path
) -> None:
    state_path = tmp_path / "backup" / "state.json"
    state_path.parent.mkdir()
    state_path.write_text(json.dumps(_backup_state(last_success_at="2026-01-01T00:00:00Z")))
    findings = await bus_health.run_once(
        fake_redis,
        state_path=tmp_path / "state.json",
        disk_usage=_healthy_disk,
        backup_state_path=state_path,
    )
    assert any(f.check == "backup" for f in findings)


async def test_run_once_treats_a_missing_state_directory_as_not_installed(
    fake_redis, tmp_path
) -> None:
    findings = await bus_health.run_once(
        fake_redis,
        state_path=tmp_path / "state.json",
        disk_usage=_healthy_disk,
        backup_state_path=tmp_path / "absent" / "state.json",
    )
    assert not any(f.check == "backup" for f in findings)


async def test_run_once_skips_the_backup_check_when_not_told(fake_redis, tmp_path) -> None:
    """Dev and CI run the probe with no backup unit beside it; only the deployed
    unit passes the path."""
    findings = await bus_health.run_once(
        fake_redis, state_path=tmp_path / "state.json", disk_usage=_healthy_disk
    )
    assert not any(f.check == "backup" for f in findings)


def test_main_passes_the_backup_state_path_through(stub_main_deps, monkeypatch) -> None:
    seen: dict = {}

    async def _spy_run_once(client, **kwargs):
        seen.update(kwargs)
        return []

    monkeypatch.setattr(bus_health, "run_once", _spy_run_once)
    argv = ["--state-file", str(stub_main_deps.state_file), "--backup-state-file", "/x/state.json"]
    assert bus_health.main(argv) == 0
    assert seen["backup_state_path"] == Path("/x/state.json")


# --- the doc pointer a finding hands its reader ---

_DOC_POINTER = re.compile(r'docs/(?P<doc>[A-Z][A-Z-]*\.md), "(?P<title>[^"]+)"')
_DOCS = Path(__file__).resolve().parent.parent / "docs"


def _headings(doc: Path) -> list[str]:
    """Markdown headings, backticks dropped, skipping fenced code - whose `#`
    lines are shell comments, and one could otherwise stand in for a heading."""
    headings, fenced = [], False
    for line in doc.read_text().splitlines():
        if line.startswith("```"):
            fenced = not fenced
        elif not fenced and line.startswith("#"):
            headings.append(line.lstrip("#").strip().replace("`", ""))
    return headings


async def _finding_with_a_doc_pointer(fake_redis, check: str) -> bus_health.Finding:
    if check == "eviction-policy":
        (finding,) = evaluate_memory(used_memory=100, maxmemory=1000, policy="volatile-lru")
        return finding
    previous_state: dict[str, int] = {}
    if check == "dlq":
        await fake_redis.xadd("content.revisions.dlq", {"k": "v"})
    elif check == "dlq-unobserved":
        dlq = "content.fetch.dlq"
        for _ in range(3):
            await fake_redis.xdel(dlq, await fake_redis.xadd(dlq, {"k": "v"}))
        previous_state[CONTINUITY_ENTRIES_KEY.format(topic=dlq)] = 1
    elif check == "group-missing":
        await fake_redis.xadd(CONTENT_REVISIONS, {"k": "v"})
    findings, _ = await collect_broker_findings(fake_redis, previous_state=previous_state)
    return next(f for f in findings if f.check == check)


@pytest.mark.parametrize("check", ["eviction-policy", "dlq", "dlq-unobserved", "group-missing"])
async def test_the_doc_section_a_finding_points_at_exists(fake_redis, check) -> None:
    """A finding that says ``see docs/X.md, "Title"`` is the operator's first
    step, and the only one this repo writes for them at the moment it matters.

    These pointers are prose inside f-strings, so nothing else checks them: the
    2026-09-11 split of STREAMS.md moved the section the eviction-policy finding
    names, and its message was retargeted by hand. The next split will not
    necessarily be done by someone who greps for it.
    """
    finding = await _finding_with_a_doc_pointer(fake_redis, check)
    pointers = _DOC_POINTER.findall(finding.message)
    assert pointers, f"{check} carries no docs pointer: {finding.message}"
    for doc, title in pointers:
        path = _DOCS / doc
        assert path.is_file(), f"{check} points at docs/{doc}, which does not exist"
        assert any(h.startswith(title) for h in _headings(path)), (
            f"{check} points at docs/{doc}, {title!r} - no heading there starts that way"
        )
