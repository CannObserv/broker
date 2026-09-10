# The cluster stream inventory

Every Redis Stream on this broker: who produces it, who consumes it, which
health primitive applies, who writes its DLQ - and who drains it. Plus the one
thing on this instance that is not a stream, under *Non-stream keys on `db0`*.

Moved here from `CannObserv/archiver:deploy/README.md` under
[archiver#193](https://github.com/CannObserv/archiver/issues/193) D6. It was
always a *cluster-wide* document living in one participant's repo; the broker's
own repo is where it belongs.

**This file describes contracts, not archiver's implementation of them.** Where
a row names a service's constant or module, that is a pointer across a repo
boundary, and the pointer is the only thing keeping the two in step.

## Streams on this broker

Cells marked *(target)* describe an arrangement not yet running. The `DLQ`
column names who **writes** each one; who **drains** it is a separate question,
answered under *Who drains a DLQ* below.

| Stream | Producer → consumer | Kind | Consumer group | Health primitive | DLQ (writer / **drainer**) | Producer durability under OOM |
|---|---|---|---|---|---|---|
| `info.changes` | Archiver → Replicator *(target)* | event | none *yet* - Replicator adds one | producer-side outbox stats (depth / oldest-unpublished age / dead-lettered count, CannObserv/archiver#112): dashboard badge + the drain loop's periodic journald line, plus archiver's own reduced bus-health timer re-running the same query from outside the publisher process - the surface that keeps reporting when the publisher is down. Group lag once consumed | `info.changes.dlq` *(target)* / **Replicator** *(prospective - nothing writes it until Replicator adds a group)* | **retries indefinitely** - transactional outbox, OOM classified transient |
| `content.fetch` | Watcher → Replicator | command | `replicator.fetch` (exactly one - competing consumers) | `XPENDING` / group lag | `content.fetch.dlq` / **Replicator** | **retries indefinitely, no ceiling** - verified CannObserv/watcher#288. The row stays `pending_publish` on all three publish paths and republishes under the same `command_id`, which Replicator dedupes. There is no attempt counter on that outbox and the reaper scans `IN_FLIGHT` only, so a long outage does not garbage-collect the backlog |
| `content.blobs` | Replicator → Watcher | fact | one per consuming service | group lag per group | `content.blobs.dlq` / **Watcher** | **retries indefinitely, no ceiling** - verified CannObserv/replicator#79. A refused publish is `Outcome.RETRY`: nothing acked, no fact, nothing dead-lettered, PEL entry intact, and the delivery ceiling is never consulted. Store-then-publish means the bytes are already on disk, so the retry after the cap lifts is a no-op |
| `content.revisions` | Watcher → **Archiver** *(producer target: CannObserv/watcher#253)* | fact | `archiver.revisions` (one per consuming service) | `XPENDING` / group lag - **the first group Archiver owns**; probed by this repo's bus-health probe: non-zero pending across two consecutive ticks WARNs (healthy steady state is 0 - a state to name, not a number to guess) | `content.revisions.dlq` - written by the ingest consumer's quarantine path / **Archiver** | **retries indefinitely** - verified CannObserv/watcher#288. `OutOfMemoryError` is in `_TRANSIENT_PUBLISH_ERRORS`, and the transient branch is exempt from `MAX_PUBLISH_ATTEMPTS`; `mark_failure` backs off 60 s -> 1 h |
| `content.artifacts` | Replicator → **Archiver** *(consumer live - CannObserv/archiver#170)* | fact, broadcast (both replicate outcomes share it, so an issuer sees success and failure in one group) | `archiver.artifacts` (one per consuming service) | `XPENDING` / group lag; issuer-side, `information.replication_commands` rows still `state='requested'` past the reap horizon - the reaper logs each abandonment at WARNING | `content.artifacts.dlq` - written by this consumer's quarantine path / **Archiver** | **retries indefinitely** - verified CannObserv/replicator#79, same `Outcome.RETRY` path as `content.blobs` |
| `content.fetch-policy` | Watcher → Replicator workers *(producer live - full set republished on `*/5 * * * *`, capped by producer-side `BusPublish.maxlen` 50k, CannObserv/watcher#265)* | config/state, broadcast, last-write-wins per host key | **none, permanently - by design** | **last-entry age via `XINFO STREAM`** - probed by this repo's bus-health probe, WARN over 15 min (3× the republish period) | **none applies** | **self-correcting** - full set is republished on a timer |
| `info.registry` | **Archiver** → Watcher *(consumer live - CannObserv/watcher#254)* | config/state, broadcast, last-write-wins per `info_item_id`, `generation`-ordered | **none, permanently - by design** (every consumer needs every message; a group accumulates a PEL nothing drains) | **last-entry age via `XINFO STREAM`** - on a non-empty corpus the snapshot guarantees ≥1 entry/hour, so an age over ~2× the snapshot interval means the producer is down; an empty or never-announced registry publishes nothing, so the alarm needs a corpus-size guard. See CannObserv/archiver#147 | **none applies** - a state message has nothing to close; quarantine is terminal and the next full set supersedes | **split by path**: deltas ride the transactional outbox and retry indefinitely (OOM transient); snapshots have **no retry** - one lost to an outage is corrected by the next period, not a re-attempt |
| `content.replicate` | **Archiver** → Replicator *(producer live - CannObserv/archiver#169; consumer shipped for `gcs`, CannObserv/replicator#34)* | command | `replicator.replicate` (exactly one - competing consumers, `content.fetch`'s posture) | `XPENDING` / group lag, plus the issuer-side view `information.replication_commands` gives: rows still `state='requested'` past the reaper horizon, which the reaper (CannObserv/archiver#170) closes as `abandoned` and logs at WARNING | `content.replicate.dlq` - Replicator's to write; Archiver provisions nothing here / **Replicator** | **retries indefinitely** - transactional outbox, OOM classified transient. **Never XTRIMmed by Archiver**: capping a command stream deletes commands the consumer group has not delivered and orphans the PEL entries naming them, so the topic is carved out of the drain loop's trim set (`no_trim_topics`) |
| `info.watch-status` | Watcher → **Archiver** *(consumer live - CannObserv/archiver#151; producer live - CannObserv/watcher#264, republish `*/5 * * * *`, producer-side `maxlen` 50k)* | config/state, broadcast, last-write-wins per `info_item_id` | **none, permanently - by design** - Archiver tails groupless (`AsyncBusTailReader`), resuming from its own `bus_tail_cursors` row rather than a full `0-0` replay | consumer-side: staleness of the `watch_status` cache vs the producer's republish period; broker-side last-entry age probed by this repo's bus-health probe, WARN over 15 min | **none, matching `content.fetch-policy`** - **two** skip paths, both durable (the skip advances the persisted cursor) and both logged at ERROR: a frame that will not *decode*, and a decoded message the registry can never *write* (a value outside a column's domain, a constraint violation). With no DLQ and a cursor that only advances on success, retrying either forever would stall the stream silently; the periodic republish is what supersedes a skip. Everything else (broker or DB down) rewinds and retries rather than skipping | **self-correcting** - coalesced level signals, full republish on a timer (CannObserv/watcher#264) |

⚠️ **`content.replicate` is the one stream where a test message is not free.**
Every other topic here carries a fact or a piece of state - the worst a stray
one costs is a confusing dashboard. A replicate command asks another service to
write bytes into a **permanent** store, and one of the providers (archive.org)
cannot be deleted at all. Two rules follow, and neither is enforceable by the
broker:

- **Never point a dev or test process at the production broker.**
  `ARCHIVER_DEV_REDIS_URL` is unset by default and prod's `ARCHIVER_REDIS_URL`
  is never inherited (the Redis analogue of the DB `_test` guard); CannObserv/archiver#157 is what
  the DB half of that lesson cost, and CannObserv/archiver#162 is what a DLQ full of test residue
  costs to clean up.
- **The "Replicate now" button (CannObserv/archiver#171) writes for real.** It is an
  operator action on the live dashboard - port 8000 - and it enqueues a genuine
  command against the item's latest revision. It is `hx-confirm`-guarded for
  that reason. There is no dry-run.

**`content.revisions` is Archiver's first consumer role** - every other row is a
stream it operates for someone else. Two operational consequences: the
`archiver.revisions` group is the first thing on this broker whose *lag* is
Archiver's own problem (a stalled consumer means revisions stop being recorded,
silently, while the stream keeps growing), and `content.revisions.dlq` is the
first DLQ this service writes rather than merely provisions. Group membership is
gated on `ARCHIVER_BUS_CONSUMER=1`, set only in `deploy/archiver.service` - a
second process in the group silently takes half the revisions.

## Non-stream keys on `db0` (CannObserv/broker#9, CannObserv/replicator#80)

This file had no row for anything that is not a stream, which is how an audit
came to find these by scanning the keyspace rather than by reading.

| Pattern | Owner | Kind | Lifetime | Commands used | What it is |
|---|---|---|---|---|---|
| `replicator:cmd:<stream suffix>:<command_id>` | Replicator | string, **volatile** | `REPLICATOR_DEDUPE_TTL_SECONDS`, default 86400 | `SET .. NX EX`, `EXISTS` | De-duplication of `content.fetch` / `content.replicate` commands. Written **after** the handler completes; read by an `EXISTS` **before** the next one runs. Reasoning: [`CannObserv/replicator:docs/CONVENTIONS.md#the-replicatorcmd-keys`](https://github.com/CannObserv/replicator/blob/main/docs/CONVENTIONS.md#the-replicatorcmd-keys) |

**This is the only non-stream key pattern any service writes here.** A new one
belongs in this table before it belongs on the broker.

**One namespace per command stream**, and the suffix is the same one co-core's
`group_name` puts after the service - so `content.fetch` gives both the group
`replicator.fetch` and the keys `replicator:cmd:fetch:<id>`. Today that means
two namespaces, `fetch` and `replicate`.

*Losing them costs re-work, never correctness.* Set-after-success means a key
can only short-circuit work already known to have finished, so an empty
namespace costs a re-fetch, a content-addressed re-store that is a no-op, and a
duplicate fact the issuer contract already requires consumers to tolerate. The
framing that matters: a `db0` that has lost these has lost the streams and the
groups' **PELs** with them, and the PEL is Replicator's only durable record of
intent - it has no database and no outbox. These keys are the cheapest thing in
that blast radius.

> **The plural is load-bearing, and getting it wrong is silent.** The ACL granted
> `~replicator:cmd:fetch:*` until broker#9 - one segment of the namespace rather
> than the namespace. Nothing could have observed the gap: the replicate loop
> completes no commands while no alias table is provisioned, so its namespace is
> **empty rather than absent**, and a grant derived from what was seen on the
> wire cannot see a namespace with no traffic. The moment that loop completes
> one - which is what broker#7 exists to make happen - the `EXISTS` before the
> handler is denied, replicator#82 classifies `NOPERM` transient, and the loop
> backs off and retries forever without ever running a handler. Nothing lost,
> nothing progressing. Fixed live 2026-09-10 to `~replicator:cmd:*`;
> `tests/deploy/test_redis_acl.py` now derives the namespaces from co-core's
> command taxonomy, so a third command stream cannot arrive without a grant or
> a red test. Same lesson as the `+exists` omission that wedged the fetch loop
> the same day: **an observed inventory is only as good as its attribution**,
> and a namespace with no traffic yet is the blind spot.

Measured on this broker 2026-09-10: **37 keys on `db0`, 27 of them dedupe keys
with TTLs and 10 streams without**, average TTL remaining ~13.5 h; every dedupe
key under the `fetch` segment, the `replicate` segment empty. Replicator's own
audit the day before found the same 27 with TTLs spanning 534 s to 84,713 s.
The counts come from `INFO keyspace` and `SCAN MATCH`, which is all `brokeradmin`
holds - it has no `+ttl` and no `+type`, deliberately, and the average is the
one `INFO` reports.

## Who drains a DLQ (CannObserv/archiver#162)

Three roles, and no two of them are reliably the same service:

- **Writer** - whichever service's consumer calls `AsyncBusConsumer.dead_letter()`
  on that topic; the `DLQ` column above names it per stream. Archiver writes
  `content.revisions.dlq` and `content.artifacts.dlq`, Replicator writes
  `content.fetch.dlq` and `content.replicate.dlq`, and the groupless config/state
  streams can write none at all.
- **Drainer - the stream's own consumer, per stream.** Named in the `DLQ`
  column above, so an unowned queue reads as a blank cell rather than something
  to infer. **Settled by CannObserv/broker#1 Phase 5**, replacing "Archiver, for
  every DLQ on this broker" - a claim that was a corollary of operating the
  instance (CannObserv/archiver#109, CannObserv/archiver#162) and lost its
  premise when CannObserv/archiver#193 D6 moved the broker to a neutral node.

  Two things forced it rather than tidiness. **D3's per-service ACL users make
  the old assignment unimplementable**: draining `content.fetch.dlq` would need
  Archiver granted `~content.fetch.dlq` plus `+xrange +xtrim`, and finding a
  queue nobody told it about would need instance-wide `SCAN` - a cluster-wide
  hole through the one model whose payoff is that Archiver cannot name
  `content.blobs`. The consumer-drains rule needs **no** extra grant: every
  service already holds `~<its own topic>.dlq`. And **triage is not mechanical**
  - "residue, or a real permanent failure?" is a question about the payload, and
  the consumer is the party that can read it. The #162 drain settles that: those
  110 were Replicator's writes, of Watcher's commands, caused by Archiver's test
  suite, and nothing about operating the instance would have told you so.

- **Broker - detection, evidence, escalation, and the backstop.** The mechanical
  half of the old drainer role stays cluster-wide, because it is suffix-keyed
  and needs no payload semantics, and because the broker is the only party that
  can `SCAN` for a queue nobody claims. `src/broker/bus_health.py` warns on any
  non-zero `*.dlq` depth, names the drainer from `DLQ_DRAINERS` (a mirror of the
  column above), and **dumps the entries to `dlq-evidence/` under the unit's
  `StateDirectory` on first sight**. A `*.dlq` key with no named drainer is
  reported as unassigned rather than skipped - a DLQ with nobody named is a DLQ
  nobody empties, which is exactly how `content.fetch.dlq` reached 110.
- **Polluter** - whoever put junk in it, which is automatically none of the
  above. The 110 were Replicator's writes, of Watcher's commands, caused by
  Archiver's test suite (CannObserv/archiver#157).

**Resting state is depth 0 on every `*.dlq` key.** That is the invariant worth
holding: a non-zero depth then means a real dead-letter awaiting triage, rather
than a number an operator has to know the backstory of before ignoring it. This
repo's bus-health probe scans every `*.dlq` key each tick and WARNs on any
non-zero depth; a dashboard rendering of the same numbers is archiver's
(CannObserv/archiver#147), alongside the other streams that cannot use group
lag.

Draining is never in-band cleanup. Audit, back up, trim, verify - in that order,
because reversing it destroys the evidence you needed to justify the trim.

**The back-up step is already done.** It is the one an operator under time
pressure skips, so the probe does it on the tick that first sees the depth: the
entries are at
`/var/lib/broker-bus-health/dlq-evidence/<topic>/<last-id>.json` on the broker
node, and the finding names the exact path. Capture is incremental by stream id,
so a queue that keeps growing accumulates one dump per growth rather than
re-dumping itself every ten minutes. Read those before the `XRANGE` below;
re-dump only if you want the entries in `redis-cli`'s own framing.

```bash
redis-cli XLEN content.fetch.dlq                    # what you are about to delete
redis-cli --no-raw XRANGE content.fetch.dlq - + > /var/tmp/fetch-dlq-$(date +%F).txt
# read it: every payload residue, or is a real permanent failure hiding in there?
redis-cli XINFO STREAM content.fetch.dlq | grep -A1 last-generated-id
redis-cli XTRIM content.fetch.dlq MINID <last-generated-id, +1ms>
redis-cli XLEN content.fetch.dlq                    # -> 0
```

`XTRIM MINID`, not `DEL`: the boundary confines the deletion to the entries you
actually audited - anything dead-lettered while you were reading carries a higher
id and survives - and the key plus any consumer groups stay in place.

Worked example, the CannObserv/archiver#162 drain (2026-08-19): 110 entries, every one a
`content_fetch` command against `example.com`, all inside one 18-minute window on
2026-08-13, zero non-residue payloads, zero consumer groups on the key.
`XTRIM content.fetch.dlq MINID 1786635782730-0` removed exactly those 110 and
left the key at depth 0.



## Orphaned consumer registrations, one time only (CannObserv/archiver#156)

Archiver's group consumers used to name themselves `{hostname}:{pid}`, so every
restart that received a message left a registration behind and nothing reaped it.
Seven had accumulated on `archiver.revisions` by 2026-08-27, six of them dead.
The consumers are now named for their group (`archiver-revisions-1`,
`archiver-artifacts-1`), stable across restarts, so **this cannot recur** - which
is why the cleanup is a one-time procedure here rather than a startup reaper.

Order matters: **deploy, restart, then reap.** Reaping before the restart leaves
the running process's own registration behind.

⚠️ **`XGROUP DELCONSUMER` destroys that consumer's pending entries.** Never reap
a consumer with a non-zero PEL - that is a stranded message needing `XAUTOCLAIM`,
not an orphan. The loop below therefore re-reads `pending` per consumer and skips
any that is non-zero, rather than trusting a check the operator ran beforehand;
`XGROUP DELCONSUMER` returns how many entries it destroyed, and by the time you
can read that number they are already gone.

It also skips the **current** consumer by name rather than matching the old
`{hostname}:{pid}` shape. A `watcher:` filter would be specific to this VM's
hostname and would silently match nothing anywhere else, which reads identical to
"already clean".

```bash
reap_orphans() {   # stream group live-consumer-name
  redis-cli XINFO CONSUMERS "$1" "$2" \
    | awk '/^name$/{getline n} /^pending$/{getline p; print n, p}' \
    | while read -r name pending; do
        if   [ "$name" = "$3" ];  then echo "keep $name (current consumer)"
        elif [ "$pending" != 0 ]; then echo "SKIP $name - pending=$pending, stranded not orphan"
        else redis-cli XGROUP DELCONSUMER "$1" "$2" "$name" >/dev/null && echo "reaped $name"
        fi
      done
}

reap_orphans content.revisions archiver.revisions archiver-revisions-1
reap_orphans content.artifacts archiver.artifacts archiver-artifacts-1
```

`archiver.artifacts` carried 0 registrations as of 2026-08-27 - its stream has
never delivered an entry, and registration happens on delivery - so that second
call is expected to print nothing. It is in the procedure anyway because the
group used the same pre-fix naming and would have leaked identically once
replication traffic started.

**Do not verify by looking for `archiver-revisions-1`.** Registration happens on
*delivery*: an `XREADGROUP` that returns zero entries does not register the
consumer, so on a quiet stream the new name is correctly absent and appears when
traffic next arrives. Verify from the journal instead:

```bash
sudo journalctl -u archiver -n 200 | grep 'Bus consumer starting'
```

## Per-stream monitoring contracts

The `info.changes` health row spent a while naming a primitive that did not
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

Note the *permanently* in that row. `info.changes` is groupless today too, but
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
- `XPENDING` on **all five** consumer groups on the node - `archiver.revisions`,
  `archiver.artifacts`, `watcher.blobs`, `replicator.fetch`,
  `replicator.replicate` - WARN on non-zero across two consecutive ticks, with
  the count carried in `StateDirectory=broker-bus-health`. Widened from
  Archiver's two by CannObserv/broker#1 Phase 5: the exclusion was inherited
  from a probe running on Archiver's own host, where a downstream service's
  group lag was plausibly its own alerting problem. On a neutral node it is not
  - nobody else watches these, and this is the one place that can;
- every `*.dlq` key via `SCAN` - WARN on any non-zero depth, with the drainer
  named and the entries captured; see *Who drains a DLQ* above;
- **`entries-added` going backwards on any stream** - the one check here that is
  not an upper bound. See *Detecting loss* below;
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

## Behaviour under the `noeviction` cap - verified for all three producers

CannObserv/broker#1 R5 asked whether the cap protects the broker at the cost of
breaking its clients. It does not: **all three producers survive `OOM command
not allowed`, and none of them drops or dead-letters.** Measured against
scratch instances at `maxmemory 1mb`, never against this broker - the cap is
instance-wide, so forcing it here would be an outage.

Per-stream answers are in the `Producer durability under OOM` column above.
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
| `DLQ_DRAINERS` (who triages each `*.dlq`) | *Who drains a DLQ* above, in this file - an assignment, so it has no computable source; the keys are still derived through co-core's `dlq_name()` |

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
| Broker memory and eviction policy, per-stream `XLEN`, last-entry age, `XPENDING`, DLQ depth, disk, persistence status, backup freshness | **this repo** (`broker-bus-health.timer`) | Every one of them measures the broker's host |
| `information.changes_outbox` depth / age / dead-lettered | **archiver** (`archiver-bus-health.timer`) | Queries archiver's database |
| The dashboard bus panel's group lag | **archiver** (`collect_group_lag`) | `XPENDING` from a client is an ordinary call, and the panel is archiver's UI |
| Redis >= 7.0 floor at service start | **each participant** (`check_redis_floor.sh`) | A client-side assertion about the broker it is about to talk to |
