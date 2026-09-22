# Bus health

What the bus-health probe watches, the per-stream contracts it holds each stream
to, and what it cannot see. Which stream is which, who produces and consumes it,
and who drains its DLQ is [STREAMS.md](STREAMS.md) - including the table whose
`Health primitive` and `Producer durability under OOM` columns this file expands.
The probe's `backup` and `persistence` findings are [RECOVERY.md](RECOVERY.md)'s.

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
knob sits with Watcher (CannObserv/watcher#265: `maxlen` 50k on both LWW
streams) because the consumer's replay-from-`0-0` boot depends on the retention
policy - it is a contract property, not broker tuning. The broker's exposure is
the shared-instance blast radius, which `maxmemory` bounds and the probe
watches.


**`info.registry` retention is different in kind** (CannObserv/archiver#141): consumers
boot by replaying from `0-0`, so the floor is "at least one full snapshot plus
the deltas since" - a consumer contract, not operator housekeeping. It is
therefore **excluded from the periodic `XTRIM` loop** and capped on every
publish via `BusPublish.maxlen` instead (`ARCHIVER_REGISTRY_STREAM_MAXLEN`,
default 50k, sized from key count × sets retained - never from the
`info.changes` number). Snapshot period: `ARCHIVER_REGISTRY_SNAPSHOT_INTERVAL`,
default 3600s; operator republish-now: `POST
/api/v1/tools/republish-registry-announcements`.

**Retention, stream side.** With no consumer yet, entries accumulate on
`info.changes`. The Archiver outbox publisher caps the stream operator-side via
a periodic `XTRIM ... MAXLEN ~ N`; `N` is `ARCHIVER_REDIS_STREAM_MAXLEN`
(default 100000). Operator-side rather than co-core's XADD-time trim is a
**choice, not an absence** - `BusPublish` has carried `maxlen`/`approximate`
since cannobserv#285 (CannObserv/archiver#138), and `info.registry` uses it, because a
config/state stream's retention is a consumer contract. `info.changes` is a fact
stream nothing replays, so its cap is housekeeping and belongs on the operator's
cadence.


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
  (see below). All from the one `INFO memory` the first check already pays for;
- `XLEN` per stream, each threshold derived as **that stream's own retention
  cap + 10%** - so a breach means the retention mechanism broke, not that
  traffic grew. Three caps apply and they are not interchangeable: 110k for
  fact streams on archiver's operator-side `XTRIM`
  (`ARCHIVER_REDIS_STREAM_MAXLEN`), 55k for `info.registry` (capped on publish
  instead, `ARCHIVER_REGISTRY_STREAM_MAXLEN`), 55k for the two LWW streams
  (Watcher's producer-side `maxlen`). See *Mirrored constants* below.
  `content.replicate` is the exception: never trimmed by design, so its breach
  message says "volume milestone", not "broken cap";
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
  and the one the 2026-09-16 event asked for; see *A consumer that stopped
  reading* below;
- every `*.dlq` key via `SCAN` - WARN on any non-zero depth, with the drainer
  named and the entries captured; see *Who drains a DLQ* in [STREAMS.md](STREAMS.md);
The disposal primitive is `XDEL <queue> <id>`, per entry. It is deliberately not
`XTRIM MAXLEN 0`, which was the only tool the broker had before
CannObserv/broker#12 and which takes every *other* entry with it - on a queue
that reached 110, emptying it to remove one triaged frame destroys 109 audits
nobody did. `XTRIM` stays granted for the retention trims in the [STREAMS.md](STREAMS.md) table.

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
abstention, and installed-copy parity.

## A consumer that stopped reading

Provenance: CannObserv/broker#20.

**A group whose consumer is gone was invisible to every other check here**, and
the probe said so out loud: after the node's reboot on 2026-09-16
(redis-server up 15:26:34Z, AOF intact) replicator never reconnected,
`replicator.fetch` held one undelivered command from 15:27:00Z onwards, and the
tick at 15:38 and every tick after it reported **0 findings**. It was found by
hand.

Each existing signal is blind to a consumer that has stopped *reading*:

- **`pending` counts what was delivered and not acked.** A consumer that never
  calls `XREADGROUP` is delivered nothing, so its pending count sits at `0` -
  which is the healthy value. The two-tick rule above catches a consumer wedged
  *after* delivery; this is the half before it.
- **Last-entry age catches a stopped producer**, and exists only for the
  permanently-groupless streams.
- **Consumer `idle` is useless right after a restart** - exactly when it is most
  wanted. It read 869 s for every consumer on every group at 15:41: the time
  since the AOF load, not since each consumer's last read.

### Positions, not counters - and why not `lag`

`XINFO GROUPS` reports a `lag` per group, which looks like the answer and is
not. Measured that morning:

| Group | `lag` | Where it actually stood |
|---|---|---|
| `watcher.blobs` | 152 | at the stream's `last-generated-id` - caught up |
| `archiver.revisions` | 147 | at the stream's `last-generated-id` - caught up |
| `replicator.fetch` | 153 | behind by **one** entry - the real fault |

A lag-based check would have raised three findings, two of them false, and
misstated the third. The cause is structural rather than a quirk: `lag` is
`entries-added` minus the group's `entries-read`, and **`entries-read` does not
survive a reload** - `test_a_reload_keeps_the_position_and_loses_lags_input`
restarts a server to show it. `last-delivered-id` does survive, because it is
what the consumer's next read resumes from.

So the check compares positions and dates one entry:

1. `XINFO STREAM <stream>` -> `last-generated-id`, which the length and
   continuity checks already read;
2. `XINFO GROUPS <stream>` -> that group's `last-delivered-id`, plus the
   `pending` count and the group names the checks above read out of the same
   reply - one round trip, one observation (CannObserv/broker#29);
3. equal - or the group *ahead*, which happens when an `XADD` lands between the
   two replies - and the group is caught up, whatever `lag` says. Nothing
   further is read;
4. behind, and `XRANGE <stream> (<last-delivered-id> + COUNT 1` gives the oldest
   entry it has not been offered. The timestamp in that id is its age.

**The threshold is 5 minutes on every group, and it is not a mirrored
constant.** Every other threshold in this probe copies a retention cap owned in
another repo; this one is owned here, because it describes the consumer's read
loop as this node can observe it. All five groups are blocking `XREADGROUP`
readers, so delivery is immediate - `replicator.fetch` answered the 14:18:00Z
command at 14:18:01Z - and five minutes is two orders of magnitude of slack over
that, and ~55x the slowest handler: `content.replicate`'s, 5.4 s at the 64 MiB
blob ceiling (CannObserv/replicator#96, broker#30). A consumer that ever moves
to a schedule rather than a blocking read needs its own value on its row: that
schedule's period plus margin, with the source named the way a mirrored constant
names its owner. `test_every_probed_group_carries_an_undelivered_threshold`
fails if a sixth group arrives without one, and `StreamCheck` refuses the other
direction - a threshold on a row with no group - at import.

**What no duration sizes: a consumer alive and not reading**, since a reader
inside a handler is not reading - one stalled-provider attempt, or recovery
re-claiming its own failing entry every cycle so `XREADGROUP` never runs
(CannObserv/replicator#98). Both hold a delivered entry, so the finding words
itself by the group's `pending` count: at 0, a stopped reader (for a consumer
that acks after handling, as replicator's does); above 0, run `XPENDING <stream>
<group> - + 10` twice. A delivery count that climbs is a consumer alive and
retrying; one that stands still is one long attempt or a dead holder, which
`CLIENT LIST` separates.

**A stream trimmed past its group's position is its own finding**
(`group-undelivered-lost`), not an age. The group is behind and the entries it
is behind by are gone - trimmed or deleted before delivery - so there is nothing
to date and nothing that will ever arrive. Reported as an age it would read as
either healthy or as a stopped consumer, and the remedy is neither: it is the
hazard `content.replicate` is carved out of every trim path for, happening on a
stream that is not carved out.

**It costs no grant, no round trip, and joins nothing.** `XINFO STREAM`,
`XINFO GROUPS` and `XRANGE` are all read-only introspection `brokeradmin`
already held, so the check shipped without touching `deploy/redis-acl.conf`;
since CannObserv/broker#29 that `XINFO GROUPS` is the one the pending count
comes from; `test_the_probe_can_read_a_groups_position_without_joining_it`
asserts both halves - that the reads are permitted, and that `XREADGROUP` and
`XGROUP CREATE` are still refused. A probe that joined a group would take delivery of another
service's messages, which is the rule it exists on the other side of.

**Corroboration, not contract.** Zero `user=replicator` connections in
`CLIENT LIST` was the first visible sign on the day, and it is the right thing
to check next when this finding fires on a group holding nothing. It is not the
check itself: a connection count is not what a consumer promises, and a
connected process that has stopped reading looks identical to a healthy one.

## Behaviour under the `noeviction` cap - verified for all three producers

CannObserv/broker#1 R5 asked whether the cap protects the broker at the cost of
breaking its clients. It does not: **all three producers survive `OOM command
not allowed`, and none of them drops or dead-letters.** Measured against
scratch instances at `maxmemory 1mb`, never against this broker - the cap is
instance-wide, so forcing it here would be an outage.

Per-stream answers are in the `Producer durability under OOM` column of [STREAMS.md](STREAMS.md).
Three broker-level facts came out of that work and belong here rather than in
any one participant's repo:

**Only `denyoom` commands are refused, which makes an OOM a *publishing*
incident rather than a total one.** `XADD` and `SET` are refused; `XREADGROUP`,
`XACK`, `XAUTOCLAIM`, `XPENDING`, `XRANGE`, `XREAD`, `XLEN`, `EXISTS` and
`PING` are all admitted at the cap. So consumers keep draining their backlogs
throughout, which is why every loop re-arms instead of wedging, and why the
bus-health probe keeps reporting - every command it issues is on the admitted
side.

**`XGROUP CREATE ... MKSTREAM` is `denyoom`; `XGROUP CREATE` against an
existing key is not.** A cold boot into a latched cap therefore succeeds
wherever the stream already exists and fails at `ensure_group` only where it
does not. First-boot hazard only, and it self-heals when the cap clears - but
it presents as a service that will not start rather than as a memory incident.

**OOM is a threshold, not a latch**, and the client's argument buffer counts
toward `used_memory` when a `denyoom` command runs. So at the boundary a large
entry can be refused and free enough on the error reply to put usage back under
the cap, and a naive fill-then-probe sees `XADD` succeed two commands after it
was refused. Anyone reproducing this should fill to the first refusal, then
lower `maxmemory` below the current `used_memory` so the state holds still.

### `noeviction` is load-bearing beyond refusing writes (CannObserv/broker#9)

The policy matters as much as the cap, and for a reason that is not visible from
`deploy/redis.conf.broker`.

**Replicator's `replicator:cmd:*` keys are the only volatile keys on this
instance.** Every other tenant writes streams, and a stream never carries a TTL.
So under any `volatile-*` policy - `volatile-lru`, `volatile-ttl`,
`volatile-random` - that one namespace is the *entire* eviction candidate set,
and memory pressure would evict precisely it and nothing else: every stream,
every consumer group and every PEL left intact, and every issuer none the wiser.

The two failure modes are not comparable, and the worse one is the one that
looks safer:

| | `noeviction` at the cap | any `volatile-*` at the cap |
|---|---|---|
| What happens | the `denyoom` write is refused | replicator's dedupe keys are deleted |
| How it presents | `OOM command not allowed`, instance-wide | **nothing** - eviction is not an error anyone sees |
| What it costs | producers retry through it; verified for all three (broker#1 R5) | a TTL window of duplicate fetches against live origins |
| How you learn | immediately, from every producer's journal | from the origins, or not at all |

So "evict something rather than refuse writes" - the obvious change to reach for
under memory pressure - buys a silent failure in exchange for a loud one, and
picks the one namespace on the instance nobody would choose to lose. If a future
tuning pass moves off `noeviction`, that trade has to be made deliberately, and
`allkeys-*` is not the escape either: it evicts stream entries, which is the
loss `## Detecting loss` exists to catch after the fact.

All three claims are checked rather than trusted, and the third is the one
that matters at 3am:

- `test_tracked_config_sets_an_explicit_nonzero_maxmemory` pins the policy in
  the tracked config, with the hazard in the failing test's own docstring so
  whoever changes it deliberately reads why;
- `test_the_dedupe_keys_are_the_only_volatile_keys_on_the_instance` pins the
  keyspace claim against the live broker - the hazard changes shape the moment
  a second service writes a key with a TTL, and that is the day this section
  stops being true;
- **the probe reports a wrong policy every tick**, which is what closes the gap
  the other two leave. A `CONFIG SET maxmemory-policy volatile-lru` is live,
  persisted nowhere, and reaches nothing that runs on a schedule - so until
  this check existed, the only detector was a test suite someone had to
  remember to run on the node. The finding names the family, because the two
  fail differently: `volatile-*` evicts only the dedupe keys and reports it to
  nobody; `allkeys-*` evicts stream entries, which *Detecting loss* catches,
  but only afterwards.

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
they cannot be, so all three are mirrored here with their owner named:

| Constant in `src/broker/bus_health.py` | Source of truth |
|---|---|
| `FACT_PRODUCER_MAXLEN` (100k) | `DEFAULT_STREAM_MAXLEN`, `CannObserv/archiver:src/core/changes/publisher.py` |
| `REGISTRY_PRODUCER_MAXLEN` (50k) | `DEFAULT_REGISTRY_STREAM_MAXLEN`, `CannObserv/archiver:src/core/changes/registry_snapshot.py` |
| `LWW_PRODUCER_MAXLEN` (50k) | Watcher's producer-side `BusPublish.maxlen` (CannObserv/watcher#265) |
| `DLQ_DRAINERS` (who triages each `*.dlq`) | *Who drains a DLQ* in [STREAMS.md](STREAMS.md) - an assignment, so it has no computable source; the keys are still derived through co-core's `dlq_name()` |

The third was already a mirror before the split - there was never an import to
lose - which is why the pattern was tolerable enough to extend to the other two.

**The failure mode is bounded and it is the safe direction.** A cap raised in
its home repo and not here leaves this probe's threshold stale-*low*, so it
warns early. It never goes quiet. The reverse - lowering a cap without lowering
the threshold - widens the blind window but cannot hide a stream that has
stopped being trimmed at all, which is the condition the check exists for.

## Who watches what, after the split

| Signal | Lives in | Why there |
|---|---|---|
| Broker memory and eviction policy, per-stream `XLEN`, last-entry age, group `pending`, undelivered age per group, DLQ depth and `entries-added` continuity, disk, persistence status, backup freshness | **this repo** (`broker-bus-health.timer`) | Every one of them measures the broker's host |
| `information.changes_outbox` depth / age / dead-lettered | **archiver** (`archiver-bus-health.timer`) | Queries archiver's database |
| The dashboard bus panel's group lag | **archiver** (`collect_group_lag`) | `XPENDING` from a client is an ordinary call, and the panel is archiver's UI |
| Redis >= 7.0 floor at service start | **each participant** (`check_redis_floor.sh`) | A client-side assertion about the broker it is about to talk to |
