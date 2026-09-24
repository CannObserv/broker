# Bus health

What the bus-health probe watches, the per-stream contracts it holds each stream
to, and what it cannot see. Which stream is which, who produces and consumes it,
and who drains its DLQ is [STREAMS.md](STREAMS.md) - including the table whose
`Health primitive` column this file expands. Its `Producer durability under OOM`
column is expanded in [MEMORY-PROTECTION.md](MEMORY-PROTECTION.md); the probe's
`backup` and `persistence` findings are [RECOVERY.md](RECOVERY.md)'s.

Moved out of STREAMS.md on 2026-09-11, when the two together ran past the
per-doc context budget.

## Per-stream monitoring contracts

The `info.changes` row's `Health primitive` cell in [STREAMS.md](STREAMS.md) spent a while naming a primitive that did not
exist; CannObserv/archiver#112 (badge + journald line) and the bus-health probe
below closed that gap. Left as a reminder of the failure class: a health column an
operator would assume is wired up must either be real or carry a ⚠️.

**`content.fetch-policy` is monitoring-blind to consumer-group lag, and always
will be (CannObserv/archiver#128 / cannobserv#285).** Every worker needs every message, so
the consumer reads groupless (`co_core_aio.bus.AsyncBusTailReader`, in-memory
cursor, replayed from `0-0` at boot) - a group here would accumulate a PEL
nothing drains. Consequence: **`XPENDING` reports nothing for this stream whether
or not a single consumer is alive.** Any dashboard or alert that reads "no
pending entries" as healthy will read this stream as healthy while it is dead.
Use last-entry age instead - it at least catches a producer that stopped
republishing.

Note the *permanently* in `content.fetch-policy`'s STREAMS.md row. `info.changes` is groupless today too, but
only because its consumer isn't built; it gains a group and becomes
lag-monitorable, exactly as `content.revisions` just did.
`content.fetch-policy` does not.

For the same reason it has **no DLQ**. `dead_letter()` is a method on
`AsyncBusConsumer` - it copies the frame to `dlq_name(topic)` and acks the
original. A groupless tail reader has no ack and no delivery accounting, so
nothing can write `content.fetch-policy.dlq` and nothing would trigger one.

**Retention on this stream is the producer's, not the broker's.** A stream whose
producer republishes its full set on a timer grows without bound unless trimmed;
`BusPublish.maxlen` (co-core >=0.7.7) rides the trim on each publish, and the
knob sits with Watcher (CannObserv/watcher#292, both LWW streams) because the
consumer's replay-from-`0-0` boot depends on the retention policy - it is a
contract property, not broker tuning. The broker's exposure is the
shared-instance blast radius, which `maxmemory` bounds and the probe watches.

**`info.registry` retention is different in kind** (CannObserv/archiver#141): consumers
boot by replaying from `0-0`, so the floor is "at least one full snapshot plus
the deltas since" - a consumer contract, not operator housekeeping. It is
capped on every publish via `BusPublish.maxlen` (`ARCHIVER_REGISTRY_STREAM_MAXLEN`,
default 50k, sized from key count × sets retained - never from the
`info.changes` number) and **`XTRIM`med by nobody** - not archiver's drain loop,
and not a person at a `redis-cli`: `brokeradmin` trims `*.dlq` keys only
(CannObserv/broker#34). Snapshot period: `ARCHIVER_REGISTRY_SNAPSHOT_INTERVAL`,
default 3600s; operator republish-now: `POST
/api/v1/tools/republish-registry-announcements`.

**Retention, stream side.** With no consumer yet, entries accumulate on
`info.changes`. The Archiver outbox publisher caps it with a periodic
`XTRIM ... MAXLEN ~ N`; `N` is `ARCHIVER_REDIS_STREAM_MAXLEN` (default 100000).
A timer rather than the per-`XADD` trim `info.registry` uses is a **choice, not
an absence** (`BusPublish` has carried `maxlen` since cannobserv#285,
CannObserv/archiver#138): a fact stream nothing replays needs housekeeping, not
a consumer contract.

## The bus-health probe

`broker-bus-health.{service,timer}` - a periodic oneshot (`OnUnitActiveSec=10min`),
WARN-only to journald, running `python -m src.broker.bus_health`. It is
deliberately a standalone unit on the broker's own host rather than a check
inside any participant's publisher loop: an `ExecStartPre` fires once at process
start and unbounded growth is an after-start condition, and anything riding a
producer's loop stops reporting exactly when that producer is down.

Per tick it probes:

- `used_memory` vs `maxmemory` (WARN at 75% - before the `noeviction` cap
  starts refusing `XADD` instance-wide), `maxmemory 0` (inert ceiling), and
  **`maxmemory-policy` other than `noeviction`** - the third way the protection
  goes inert, and the only one whose damage is otherwise reported by nobody
  ([MEMORY-PROTECTION.md](MEMORY-PROTECTION.md)). All from the one `INFO memory`
  the first check already pays for. The 75% finding also names the three
  longest streams, by entries not bytes, out of the `XINFO STREAM` replies the
  length checks below already read (CannObserv/broker#61) - a naming aid, never
  a threshold;
- `XLEN` per stream, each threshold derived as **that stream's own retention
  cap + 10%** - so a breach means the retention mechanism broke, not that
  traffic grew. Three caps apply and they are not interchangeable: 110k for
  `info.changes` on archiver's operator-side `XTRIM`
  (`ARCHIVER_REDIS_STREAM_MAXLEN`), 55k for `info.registry` (capped on publish
  instead, `ARCHIVER_REGISTRY_STREAM_MAXLEN`), and for the two LWW streams
  `max(500, 10 x the set Watcher republishes)` + 10% - the 550 while the sets are small,
  which is the state the node is in today, and the floor past ~50 entries per
  set. See *Mirrored constants* and *The one cap that is read, not mirrored*
  below. **The five `content.*` streams get no length threshold**: nothing trims
  them, so there is no cap to mirror and a breach could only mean traffic grew.
  `maxmemory` is their only bound and the memory check above is the finding for
  it, naming the longest streams when it fires. Until CannObserv/broker#60 four
  of them borrowed the 110k, which archiver's `trim_topics` allowlist
  (CannObserv/archiver#239) never applied to them - and the `maxmemory`
  derivation on CannObserv/archiver#231 summed those borrowed numbers as
  retention. [STREAMS.md](STREAMS.md) marks each row
  **No retention cap**, and a test holds the probe to it;
- last-entry age for the groupless streams (15 min for the two `*/5` LWW
  streams; 2h for `info.registry`'s hourly snapshot, skipped while the stream
  is empty - the corpus-size guard);
- the `pending` count of **all five** consumer groups on the node -
  `archiver.revisions`, `archiver.artifacts`, `watcher.blobs`,
  `replicator.fetch`, `replicator.replicate` - WARN on non-zero across two
  consecutive ticks, with the count carried in
  `StateDirectory=broker-bus-health`. Widened from Archiver's two by
  CannObserv/broker#1 Phase 5: the exclusion was inherited from a probe running
  on Archiver's own host, where a downstream service's group lag was plausibly
  its own alerting problem. On a neutral node it is not - nobody else watches
  these, and this is the one place that can;
- **a consumer group that does not exist** on one of those five streams
  (`group-missing`) - absent from the stream's `XINFO GROUPS` reply, so its lag
  cannot be read at all and a stalled consumer is invisible to the rule above.
  WARN on **every** tick it is absent, with no two-tick grace: nothing about it
  is transient. Three causes, and the finding names those the reply has not
  ruled out rather than guessing:
  - the consumer has never run against this broker, so it never created the
    group (co-core's `ensure_group`, when the consumer starts);
  - the consumer runs its group under a name other than the one co-core's
    `group_name()` derives, which is the name the probe asks for
    (cannobserv#384). The consumer looks healthy, and its real group is probed
    by nobody - the one cause no other check reports. **The finding prints the
    groups that DO exist on the stream** (CannObserv/broker#29): evidence for
    this cause, not a verdict - where a stream carries one group per consuming
    service the other names are ordinary. An empty list rules it out and says
    so;
  - the group was lost while the stream was not: a stream deleted or flushed
    and then recreated by its producer's next `XADD` comes back without its
    groups. That case follows a `stream-reset` finding on the same stream - on
    the same tick if the stream was recreated between two ticks, on an earlier
    one if a tick caught it absent - and the two together are the signature
    (*Detecting loss* below).

  Only a stream that exists is checked - one nothing has written yet is
  dormant, not a fault - and a stream the probe reads without a group cannot
  trip it: `info.changes` until its consumer exists, and the config/state
  streams, where `StreamCheck` refuses a group at import time. A missing group
  records no pending count, so when it comes back its two-tick rule starts
  again from zero;
- **the age of the oldest entry a group has not been DELIVERED** - WARN over 5
  minutes on each of the five groups. The check a pending count cannot make,
  and the one the 2026-09-16 event asked for; see
  [UNDELIVERED-CONSUMERS.md](UNDELIVERED-CONSUMERS.md);
- every `*.dlq` key via `SCAN` - WARN on any non-zero depth, with the drainer
  named and the entries captured; see *Who drains a DLQ* in [STREAMS.md](STREAMS.md).
  The disposal primitive is `XDEL <queue> <id>`, per entry. It is deliberately not
  `XTRIM MAXLEN 0`, which was the only tool the broker had before
  CannObserv/broker#12 and which takes every *other* entry with it - on a queue
  that reached 110, emptying it to remove one triaged frame destroys 109 audits
  nobody did. `XTRIM` stays granted for the retention trims in the [STREAMS.md](STREAMS.md) table;
- **`entries-added` going backwards on any stream** - the one check here that is
  not an upper bound. See *Detecting loss* below. On a **dead-letter queue** it
  means the queue was deleted rather than drained, so any evidence dump still on
  disk describes a stream that no longer exists;
- **a `*.dlq` key that a tick's `SCAN` no longer returns**, after a tick that
  recorded it (`stream-reset`, the same name and the same meaning - this queue's
  identity is gone). `entries-added` cannot report its own stream's deletion, and
  every legitimate disposal leaves the key: `XDEL` per entry, and the
  `XTRIM MAXLEN 0` it replaced. Fires once, because the baseline is not carried
  into the tick that reports it;
- **`entries-added` climbing past what the depth accounts for, on a dead-letter
  queue** (`dlq-unobserved`, CannObserv/broker#13) - entries arrived and were
  removed between two ticks, so evidence capture never saw them and those
  payloads are gone. Reported as a floor, `added - <last tick's added> - depth`,
  because depth can include entries captured on an earlier tick. Depth and
  `entries-added` are read from one `XINFO STREAM` rather than an `XLEN` and an
  `XINFO`, so an `XADD` cannot land between them and push the floor above the
  truth. It makes the *existence* of a drained failure undeniable, which is what
  the per-queue drain grants (CannObserv/broker#12) made worth having;
- `/` disk headroom (WARN at 90% used or under 2 GiB free) - which on this node
  is the AOF's headroom, and is the check whose meaning the move restored.

`content.blobs` carries a group row and nothing else. The unqualified "never
`content.blobs`" rule was Archiver's *role* boundary and a neutral node has no
role to be out of bounds of, so `watcher.blobs` is probed like any other group.
What survives is the half that was never about roles: this repo owns no
retention cap for that stream, so it states no opinion on its length or its age
- neither the fact cap nor the LWW cap governs it, and inventing a threshold
with no owner is how a probe starts crying wolf.

The unit holds **no** database credential: the `changes_outbox` half of the
old combined probe stayed in archiver with the table it queries.
`tests/deploy/test_bus_health_units.py` pins that, the consumer-group
abstention, installed-copy parity, and what the unit inherits (broker#37).

## A consumer that stopped reading

Why a pending count cannot see a consumer that stopped calling
`XREADGROUP`, and what the probe compares instead, are
[UNDELIVERED-CONSUMERS.md](UNDELIVERED-CONSUMERS.md).

## Behaviour under the `noeviction` cap

What the cap does to the producers when it is reached, and why the policy
beside it is `noeviction`, are [MEMORY-PROTECTION.md](MEMORY-PROTECTION.md).
The probe's own two checks on them - the 75% warning and the
`eviction-policy` finding - are in *The bus-health probe* above.

## Detecting loss - the one check that is not an upper bound

Every other threshold in the probe is a ceiling: length against a retention cap,
memory against `maxmemory`, DLQ depth against zero, disk against a fraction. So
until CannObserv/broker#10, **a broker that had lost data looked healthier than
one under load.** That was not hypothetical - a `databases 1` restart on
2026-09-10 replayed a historical `FLUSHDB` against db0, the broker came up
holding 4% of its entries, and the probe ticked twice reporting
`finding_count: 0`, correctly by its own rules.

**Length cannot be the signal.** Three streams shrink as normal operation:
`info.changes` rides archiver's periodic `XTRIM`; `info.registry` is capped on
every publish (CannObserv/archiver#141) and can drop most of itself in a single
tick - it sat at ~2,600 entries in early September and at 116 by the 10th,
entirely legitimately, one generation per item with older generations
superseded; and the two LWW streams carry a producer-side `maxlen`. Any
percentage threshold would either miss a wipe or fire on `info.registry` every
snapshot.

**`entries-added` is what separates them.** It is monotonic for the life of a
stream *object*: a trim removes entries while it keeps climbing, and it can only
fall if the stream was destroyed and recreated. Verified against Redis 7.0.15
rather than assumed:

| Operation | `length` | `entries-added` |
|---|---|---|
| 50 x `XADD` | 50 | 50 |
| `XTRIM MAXLEN 10` | **10** | **50** - unchanged, so no finding |
| `FLUSHDB` then one `XADD` | 1 | **1** - went backwards, so a finding |

A `FLUSHDB`, a `FLUSHALL`, a restore from a stale snapshot and a `DEL` followed
by a fresh `XADD` are indistinguishable from here, which is the point: the probe
is not diagnosing a cause, it is refusing to call an empty broker healthy.

`content.replicate` additionally gets the cheaper rule - it is carved out of
every trim path, so **any** decrease in its length is a fault.

Both baselines are carried between oneshot runs in the same `StateDirectory`
file as the pending counters, under `@`-prefixed keys, and an unreachable broker
passes them through untouched - otherwise the tick after an outage would compare
against nothing and a wipe *during* the outage would go unseen.

## Mirrored constants - the cost of the repo split

`src/broker/bus_health.py` derives each `XLEN` warning threshold from the
retention cap that stream is *supposed* to be held at, plus 10%. Before
CannObserv/archiver#193 D6 two of those caps were **imported**; across a repo boundary
they cannot be, so all three are mirrored here with their owner named - and,
since CannObserv/broker#44, the two further numbers the LWW cap's floor is made
of:

| Constant in `src/broker/bus_health.py` | Source of truth |
|---|---|
| `CHANGES_PRODUCER_MAXLEN` (100k) | `DEFAULT_STREAM_MAXLEN`, `CannObserv/archiver:src/core/changes/publisher.py` - **reaches `info.changes` only**, the one stream in archiver's `trim_topics` allowlist; `FACT_PRODUCER_MAXLEN` until CannObserv/broker#60, a name that got it lent to four `content.*` streams it never trims |
| `REGISTRY_PRODUCER_MAXLEN` (50k) | `DEFAULT_REGISTRY_STREAM_MAXLEN`, `CannObserv/archiver:src/core/changes/registry_snapshot.py` |
| `LWW_PRODUCER_MAXLEN` (500) | Watcher's two `DEFAULT_*_STREAM_MAXLEN`, `src/core/{fetch_policy,watch_status}.py` (CannObserv/watcher#292) |
| `LWW_RETAINED_FULL_SETS` (10) | `RETAINED_FULL_SETS`, `CannObserv/watcher:src/core/bus.py` (CannObserv/watcher#292) - the multiplier on the floor the line above is only the *default* of |
| `LWW_REPUBLISH_PERIOD_SECONDS` (300) | Watcher's `*/5 * * * *` republish (CannObserv/watcher#264, #265; `info.watch-status` reads `WATCHER_WATCH_STATUS_REPUBLISH_CRON` and defaults to it) - already load-bearing as 3x the LWW age threshold before it was spelled out |
| `DLQ_DRAINERS` (who triages each `*.dlq`) | *Who drains a DLQ* in [STREAMS.md](STREAMS.md) - an assignment, so it has no computable source; the keys are still derived through co-core's `dlq_name()` |

`LWW_PRODUCER_MAXLEN` was already a mirror before the split - there was never an
import to lose - which is why the pattern was tolerable enough to extend to the
other two caps.

**Only a raised cap fails safe.** Raised at home and not here, the threshold
goes stale-*low* and warns early. Lowered, it goes stale-*high* and can hide the
backlog the cut was for (CannObserv/broker#40: watcher#292's 29,770 is under
55k).

## The one cap that is read, not mirrored

`LWW_PRODUCER_MAXLEN` is a **default, not the whole rule**. Watcher's
`resolve_stream_maxlen` floors each LWW cap at `RETAINED_FULL_SETS` (10) copies
of the set it is about to republish, so the cap in force is
`max(500, 10 x set)`. The sets were 3 (`content.fetch-policy`) and 4
(`info.watch-status`) on 2026-09-22 and the default governs; from about 54
entries per set a threshold of 550 would have said *the retention cap for this
stream is not being applied* while the cap was being applied correctly, just
higher - and a standing WARN with the wrong cause trains an operator to ignore
the LWW rows, which is the blindness CannObserv/broker#40 closed
(CannObserv/broker#44).

**The third term cannot be mirrored.** It is the size of Watcher's corpus, it
changes with no edit anywhere, and that is precisely the failure a mirror cannot
cover. So `republished_set_size` reads it off one `XINFO STREAM` reply - the one
the length and age checks already make - as `length / the republishes the span
holds`. Three properties carry it:

- it is entries per republish **whether or not the cap is applied**, because an
  untrimmed stream grows its span in step with its length, so a broken cap still
  climbs through the threshold instead of carrying it along;
- it **rounds up twice** - the oldest retained entries are a partial set, and
  the division charges that fragment to the whole ones before the remainder is
  ceilinged. Up delays a real breach by a tick or two; down would invent one;
- it **never lowers the threshold**: `max(default, floor)` is Watcher's rule and
  the probe's.

**What it reads on this node.** Measured 2026-09-22, against the live broker as
`brokeradmin` - `XINFO STREAM` is the read the length and age checks already
make, so this needs no grant nobody holds and no round trip nobody pays:

| Stream | `XLEN` | span | set read | cap in force |
|---|---|---|---|---|
| `content.fetch-policy` | 500 | 830 min (166 republishes) | 4, for a 3-host set | the mirrored 500 |
| `info.watch-status` | 504 | 625 min (125 republishes) | 5, for a 4-item set | the mirrored 500 |

One over the true set in both rows, which is the rounding working: the reading
is an upper bound on the set and therefore on the cap, and at these sizes it
changes nothing at all - `10 x 5` is far under 500.

**It fails on a window that is not uniform.** The reading assumes every
republish in the retained window was the same size and arrived on time. Two
things break that, and both read the set *low* - the warns-early direction, not
the quiet one.

**A gap**, first: republishes that did not happen are counted as if they had.
Measured at set sizes from 62 to 1,000, the behaviour is the same at all of
them:

| Missed republishes | Reading | Length finding |
|---|---|---|
| 0 | one or two over the true set | silent |
| 1 | ~9% under | silent - the ceiling is what carries it |
| 2 | ~17% under | **WARN**, with no `stream-age` beside it |
| 3+ | further under | WARN, and `stream-age` now fires too |

So one missed republish is absorbed and two are not, while `stream-age` waits
for three (`LWW_WARN_LAST_ENTRY_AGE_SECONDS`, 15 min). The ten-minute gap
between those is a window where this check reports a broken cap and nothing
beside it names the real cause.

**A set that changes size** is the second, and it is the one Watcher's own
comment on `RETAINED_FULL_SETS` tells us to expect - the window then holds sets
of two sizes and the reading averages them. Simulated against a retained window
trimming at `max(500, 10 x set)`, for a set that jumps in a single republish:

| Jump | False WARNs |
|---|---|
| 1.10x, 1.25x | none - absorbed |
| 1.50x | 4 ticks (~20 min) |
| 2x | 6 ticks (~30 min) |
| 3x and above | 7 ticks (~35 min) |

Growth that *accumulates* - an item at a time, the way a registry fills - never
trips it: the two sizes in the window differ by too little to move the average
past the margin. It takes a step change, which on this cluster means a bulk
import or a restore.

Both are bounded the same way: the old window trims out within
`RETAINED_FULL_SETS` periods, so neither is standing the way
CannObserv/broker#44's was, and neither is reachable until a set passes 50
entries. That is why they are recorded rather than closed:
CannObserv/broker#45.

A period *lengthened* at home and not here reads the same way, permanently
rather than transiently, and is the same mirror failure as any other row in the
table above.

## Who watches what, after the split

| Signal | Lives in | Why there |
|---|---|---|
| Broker memory and eviction policy, per-stream `XLEN`, last-entry age, group `pending`, undelivered age per group, DLQ depth and `entries-added` continuity, disk, persistence status, backup freshness | **this repo** (`broker-bus-health.timer`) | Every one of them measures the broker's host |
| `information.changes_outbox` depth / age / dead-lettered | **archiver** (`archiver-bus-health.timer`) | Queries archiver's database |
| The dashboard bus panel's group lag | **archiver** (`collect_group_lag`) | `XPENDING` from a client is an ordinary call, and the panel is archiver's UI |
| Redis >= 7.0 floor at service start | **each participant** (`check_redis_floor.sh`) | A client-side assertion about the broker it is about to talk to |
