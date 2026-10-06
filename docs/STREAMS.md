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

How each stream is monitored - the probe and its thresholds, the per-stream
monitoring contracts, loss detection, *Mirrored constants* and *Who watches
what* - is [BUS-HEALTH.md](BUS-HEALTH.md). The `noeviction` contract is
[MEMORY-PROTECTION.md](MEMORY-PROTECTION.md).

## Streams on this broker

Cells marked *(target)* describe an arrangement not yet running. The `DLQ`
column names who **writes** each one; who **drains** it is a separate question,
answered in [DLQ-DRAINING.md](DLQ-DRAINING.md).

**`Health primitive` says `XPENDING` + undelivered age, and no longer group
lag.** `lag` is `entries-added` minus a group's `entries-read`, and that input
does not survive a reload - measured on this Redis, three groups reported lags
of 152, 147 and 153 while two of them stood at their stream's
`last-generated-id` (CannObserv/broker#20). The broker's primitive is the pair
of positions instead; `lag` survives as a dashboard number in archiver, where it
is read rather than alarmed on. `XPENDING` in that column names the **signal** -
a group's pending count - not the command this repo's probe issues: it reads the
same number out of `XINFO GROUPS`, beside the position (CannObserv/broker#29).
[UNDELIVERED-CONSUMERS.md](UNDELIVERED-CONSUMERS.md).

**The `Producer → consumer` column is enforced, not only documented.** Since
CannObserv/broker#14 each service holds a `+xadd` **selector** naming the
streams this column says it produces, plus the dead-letter queues it writes,
and a `+xtrim` selector, if any, naming no more than that - since
CannObserv/broker#41 archiver's, on `info.changes`, is the only one - and holds
neither command on its root permission set - where a Redis key pattern applies
it to every stream the service can name, including the ones it only reads. A row
that says **Never XTRIMmed** is enforced the same way: no `+xtrim` selector on
the instance names that stream (CannObserv/broker#34). The consumer half is
enforced the same way since CannObserv/broker#43: the four group commands
(`XREADGROUP`, `XACK`, `XAUTOCLAIM`, `XGROUP CREATE`) ride a **consume
selector** naming only the streams this column says the service consumes in a
group, so a producer cannot take delivery in its consumer's group and `XACK` the
entry away. Processor keeps them on its root, which names only the stream it
consumes and its own queue. The other three services' reads are selectors of
their own, on the streams each owner named; the three groupless tails
(`info.watch-status`, `info.registry`, `content.fetch-policy`) are reads, not
group commands. `tests/deploy/test_redis_acl.py` parses this column and that
phrase and asks redis to prove both halves, so a row changed here without the
grant following fails a test rather than degrading a service quietly.

**A row that says No retention cap is a fact about behaviour, not about
grants** (CannObserv/broker#60). Nothing trims the eight `content.*` streams:
no producer passes a `maxlen` (CannObserv/watcher#317, CannObserv/replicator#106;
the processing pair has none by contract, CannObserv/broker#62, nor has
`content.persist`, CannObserv/broker#64) and archiver's
trim allowlist is `info.changes` alone (CannObserv/archiver#239).
`maxmemory` is their only bound, so the probe gives them no `XLEN` threshold -
`test_the_probe_and_the_inventory_agree_on_which_streams_have_no_cap`, in
the same file, pins the phrase to exactly the rows the probe leaves
unthresholded. All of them but `content.derived` are
also **Never XTRIMmed**: CannObserv/broker#41 took Watcher's and Replicator's
`+xtrim` off, never issued (CannObserv/watcher#327, CannObserv/replicator#119),
so no selector names any of them. No selector names `content.derived` either;
its row keeps the phrase off because a cap there would ride its producer's
publish. Whether each stream gets a cap is its
producer's call, filed on its producer's repo - `content.replicate`'s is
CannObserv/archiver#267.

| Stream | Producer → consumer | Kind | Consumer group | Health primitive | DLQ (writer / **drainer**) | Producer durability under OOM |
|---|---|---|---|---|---|---|
| `info.changes` | Archiver → Replicator *(target)* | event | none *yet* - Replicator adds one | producer-side outbox stats (depth / oldest-unpublished age / dead-lettered count, CannObserv/archiver#112): dashboard badge + the drain loop's periodic journald line, plus archiver's own reduced bus-health timer re-running the same query from outside the publisher process - the surface that keeps reporting when the publisher is down. `XPENDING` + undelivered age once a group exists | `info.changes.dlq` *(target)* / **Replicator** *(prospective - nothing writes it until Replicator adds a group)* | **retries indefinitely** - transactional outbox, OOM classified transient |
| `content.fetch` | Watcher → Replicator | command | `replicator.fetch` (exactly one - competing consumers) | `XPENDING` + **undelivered age**. **No retention cap**: nothing trims it, so no `XLEN` threshold - `maxmemory` is its only bound (CannObserv/broker#60). **Never XTRIMmed, and the broker refuses it since CannObserv/broker#41**: a cap on a command stream deletes commands `replicator.fetch` has not been delivered. Watcher's `+xtrim` here came from the observed inventory and was never issued; it came off once CannObserv/watcher#327 settled the stream uncapped | `content.fetch.dlq` / **Replicator** | **retries indefinitely, no ceiling** - verified CannObserv/watcher#288. The row stays `pending_publish` on all three publish paths and republishes under the same `command_id`, which Replicator dedupes. There is no attempt counter on that outbox and the reaper scans `IN_FLIGHT` only, so a long outage does not garbage-collect the backlog |
| `content.blobs` | Replicator → Watcher | fact | one per consuming service | `XPENDING` + **undelivered age** per group. **No retention cap**: nothing trims it, so no `XLEN` threshold - `maxmemory` is its only bound (CannObserv/broker#60). **Never XTRIMmed since CannObserv/broker#41**: a trim past `watcher.blobs`'s position is lost delivery (`group-undelivered-lost`), and CannObserv/replicator#119 settled the stream uncapped. If a trigger in Replicator's `docs/STREAMS.md` fires, a cap comes back as Replicator's named request for an `XTRIM MINID` below every group's settled position | `content.blobs.dlq` / **Watcher** | **retries indefinitely, no ceiling** - verified CannObserv/replicator#79. A refused publish is `Outcome.RETRY`: nothing acked, no fact, nothing dead-lettered, PEL entry intact, and the delivery ceiling is never consulted. Store-then-publish means the bytes are already on disk, so the retry after the cap lifts is a no-op |
| `content.revisions` | Watcher → **Archiver** *(producer target: CannObserv/watcher#253)* | fact | `archiver.revisions` (one per consuming service) | `XPENDING` + **undelivered age** - **the first group Archiver owns**; probed by this repo's bus-health probe: non-zero pending across two consecutive ticks WARNs (healthy steady state is 0 - a state to name, not a number to guess). **No retention cap**: nothing trims it, so no `XLEN` threshold - `maxmemory` is its only bound (CannObserv/broker#60). **Never XTRIMmed since CannObserv/broker#41**: CannObserv/watcher#327 settled the stream uncapped, and a cap would come back as Watcher's named request for an `XTRIM MINID` below `archiver.revisions`'s position | `content.revisions.dlq` - written by the ingest consumer's quarantine path / **Archiver** | **retries indefinitely** - verified CannObserv/watcher#288. `OutOfMemoryError` is in `_TRANSIENT_PUBLISH_ERRORS`, and the transient branch is exempt from `MAX_PUBLISH_ATTEMPTS`; `mark_failure` backs off 60 s -> 1 h |
| `content.artifacts` | Replicator → **Archiver** *(consumer live - CannObserv/archiver#170)* | fact, broadcast - **four event types**: both replicate outcomes (`replication_complete` / `replication_failed`) and, since cannobserv v0.19.6, both persist outcomes (`blob_persisted` / `persist_failed`, cannobserv#493, CannObserv/broker#64), so an issuer sees success and failure of either command in one group. The persist pair carries no domain echo and no URL, by contract; Archiver correlates on `command_id` | `archiver.artifacts` (one per consuming service) | `XPENDING` + **undelivered age**; issuer-side, `information.replication_commands` rows still `state='requested'` past the reap horizon - the reaper logs each abandonment at WARNING. **No retention cap**: nothing trims it, so no `XLEN` threshold - `maxmemory` is its only bound (CannObserv/broker#60). **Never XTRIMmed since CannObserv/broker#41**, with `content.blobs`'s reason and request path (CannObserv/replicator#119) | `content.artifacts.dlq` - written by this consumer's quarantine path / **Archiver** | **retries indefinitely** - verified CannObserv/replicator#79, same `Outcome.RETRY` path as `content.blobs` |
| `content.fetch-policy` | Watcher → Replicator workers *(producer live - full set republished on `*/5 * * * *`, capped by producer-side `BusPublish.maxlen` 500, floored at 10 full sets, CannObserv/watcher#265, #292)* | config/state, broadcast, last-write-wins per host key | **none, permanently - by design** | **last-entry age via `XINFO STREAM`** - probed by this repo's bus-health probe, WARN over 15 min (3× the republish period); **and `XLEN`** against the cap in force, which is the 500 or the 10-full-set floor, whichever is larger - the set size is read off this stream rather than mirrored, so the threshold follows the corpus (CannObserv/broker#44, #45, #51; [BUS-HEALTH.md](BUS-HEALTH.md), *The one cap that is read, not mirrored*) | **none applies** | **self-correcting** - full set is republished on a timer. **Never XTRIMmed - capped only on publish** (CannObserv/broker#41): the cap is the `MAXLEN` on each `XADD`, which an ACL does not see, so it needs no trim grant and no identity holds one |
| `info.registry` | **Archiver** → Watcher *(consumer live - CannObserv/watcher#254)* | config/state, broadcast, last-write-wins per `info_item_id`, `generation`-ordered | **none, permanently - by design** (every consumer needs every message; a group accumulates a PEL nothing drains) | **last-entry age via `XINFO STREAM`** - on a non-empty corpus the snapshot guarantees ≥1 entry/hour, so an age over ~2× the snapshot interval means the producer is down; an empty or never-announced registry publishes nothing, so the alarm needs a corpus-size guard. See CannObserv/archiver#147 | **none applies** - a state message has nothing to close; quarantine is terminal and the next full set supersedes | **split by path**: deltas ride the transactional outbox and retry indefinitely (OOM transient); snapshots have **no retry** - one lost to an outage is corrected by the next period, not a re-attempt. **Never XTRIMmed - capped only on publish, and the broker refuses the rest**: consumers boot by replaying from `0-0`, so retention has a floor - one full snapshot plus every delta since (CannObserv/archiver#141) - and it is a boot contract, not housekeeping. Cut under it and the next consumer to boot converges to a partial set and reports success. The producer holds the floor with a `MAXLEN` on every publish (`ARCHIVER_REGISTRY_STREAM_MAXLEN`, sized from key count × sets retained); a trim from anywhere else cannot see where the last snapshot starts. No identity here holds `+xtrim` on it - not archiver, and not the operator's `acladmin`, whose trim stops at `~*.dlq` (CannObserv/broker#34, #52) |
| `content.replicate` | **Archiver** → Replicator *(producer live - CannObserv/archiver#169; consumer shipped for `gcs`, CannObserv/replicator#34)* | command | `replicator.replicate` (exactly one - competing consumers, `content.fetch`'s posture) | `XPENDING` + **undelivered age**, plus the issuer-side view `information.replication_commands` gives: rows still `state='requested'` past the reaper horizon, which the reaper (CannObserv/archiver#170) closes as `abandoned` and logs at WARNING. **No retention cap**: nothing trims it, so no `XLEN` threshold - `maxmemory` is its only bound (CannObserv/broker#60). | `content.replicate.dlq` - Replicator's to write; Archiver provisions nothing here / **Replicator** | **retries indefinitely** - transactional outbox, OOM classified transient. **Never XTRIMmed, and the broker now refuses it**: capping a command stream deletes commands the consumer group has not delivered and orphans the PEL entries naming them. The topic is absent from archiver's trim allowlist (`trim_topics`, CannObserv/archiver#239) *and* from every `+xtrim` selector in `deploy/redis-acl.conf` (CannObserv/broker#14), so the rule no longer rests on one participant's source - it is refused to archiver, which could trim it, and to replicator, whose PEL entries would be the ones orphaned |
| `content.persist` | **Archiver** → Replicator *(producer live 2026-10-01 - issuance switched on by CannObserv/archiver#283; consumer live 2026-09-26 - CannObserv/replicator#114; the contract is cannobserv#493, the design of record replicator's `docs/plans/2026-09-25-content-addressed-persist-and-storage-naming.md`)* | command - `ContentPersistCommand`, one per observed revision: copy the raw bytes by digest into Replicator's private, content-addressed permanent store. Outcomes ride `content.artifacts` | `replicator.persist` (exactly one - competing consumers, `content.replicate`'s posture) | `XPENDING` + **undelivered age** - probed from before the first command (CannObserv/broker#64), on a threshold sized by Replicator's own timing of the handler since CannObserv/broker#76 ([UNDELIVERED-CONSUMERS.md](UNDELIVERED-CONSUMERS.md)); `group-missing` every tick if Replicator's group leaves the stream. A stopped reader matters beyond latency here: every persist races the temp tier's 7-day TTL (MUST-7), so an undelivered one is a revision aging toward `blob_expired`. **No retention cap**: nothing trims it, so no `XLEN` threshold - `maxmemory` is its only bound | `content.persist.dlq` - Replicator's to write, from the loop's `dead_letter` / **Replicator** | *(unverified)* Archiver issues through its transactional outbox, persisting the command before publishing it (MUST-2), so `content.replicate`'s retry-indefinitely posture is the expectation; archiver#283 put it in service on 2026-10-01 and nothing has yet shown it under `OOM`. **Never XTRIMmed, and the broker refuses it from day one**: no `+xtrim` selector in `deploy/redis-acl.conf` names it, and Archiver's grant is `+xadd` alone, in a selector, so the issuer can neither cap the stream nor take delivery in Replicator's group (broker#43's hole, never opened here; #43 closed it on the older streams) |
| `info.watch-status` | Watcher → **Archiver** *(consumer live - CannObserv/archiver#151; producer live - CannObserv/watcher#264, republish `*/5 * * * *`, producer-side `maxlen` 500, floored at 10 full sets - CannObserv/watcher#292)* | config/state, broadcast, last-write-wins per `info_item_id` | **none, permanently - by design** - Archiver tails groupless (`AsyncBusTailReader`), resuming from its own `bus_tail_cursors` row rather than a full `0-0` replay | consumer-side: staleness of the `watch_status` cache vs the producer's republish period; broker-side last-entry age probed by this repo's bus-health probe, WARN over 15 min, **and `XLEN`** against the cap in force - the 500 or the 10-full-set floor, whichever is larger, with the set size read off this stream rather than mirrored (CannObserv/broker#44, #45, #51; [BUS-HEALTH.md](BUS-HEALTH.md), *The one cap that is read, not mirrored*) | **none, matching `content.fetch-policy`** - **two** skip paths, both durable (the skip advances the persisted cursor) and both logged at ERROR: a frame that will not *decode*, and a decoded message the registry can never *write* (a value outside a column's domain, a constraint violation). With no DLQ and a cursor that only advances on success, retrying either forever would stall the stream silently; the periodic republish is what supersedes a skip. Everything else (broker or DB down) rewinds and retries rather than skipping | **self-correcting** - coalesced level signals, full republish on a timer (CannObserv/watcher#264). **Never XTRIMmed - capped only on publish**, as `content.fetch-policy` (CannObserv/broker#41) |
| `content.process` | Watcher → Processor *(live in shadow since 2026-10-03 - Watcher issues beside its own local extraction, `WATCHER_EXTRACT_MODE=shadow` (CannObserv/watcher#325); Processor consumes since 2026-10-02 (CannObserv/processor#1); the role was Observo's until CannObserv/broker#75; the contract is cannobserv#486, the design of record watcher's `docs/plans/2026-09-24-observo-extraction-and-diff-design.md` and processor's `docs/specs/2026-09-29-processor-service-design.md`)* | command - `ContentProcessCommand`, one `source_spec` per occasion | `processor.process` (exactly one - competing consumers, `content.fetch`'s posture) | `XPENDING` + **undelivered age** - probed by this repo's bus-health probe since before either end ran (CannObserv/broker#62). `processor.process` exists since 2026-10-02, created at `$` with `MKSTREAM` by `processor ensure-group` before Watcher's first command (CannObserv/broker#75), so the group outlives any outage of Processor's: "Processor is down" reads from this side as the **undelivered age** once Watcher has written, not as `group-missing` (the design surfaces it on Watcher's as `processing_timeout`). **No retention cap**: nothing trims it, so no `XLEN` threshold - `maxmemory` is its only bound | `content.process.dlq` - Processor's to write, from the driver's `dead_letter` on an undecodable command / **Processor** | **inferred, not stated** - Watcher's `process_commands` outbox shipped with the `fetch_commands` discipline, persist-before-publish plus the every-minute publish sweep (CannObserv/watcher#325, migration `2bc94dabe269`), so `content.fetch`'s retry-indefinitely posture is the expectation; Watcher has not stated how it classifies `OOM` on this publish. **Never XTRIMmed, and the broker refuses it from day one**: a cap on a command stream deletes commands the worker pool has not been delivered and orphans the PEL entries naming them, so no `+xtrim` selector in `deploy/redis-acl.conf` names it - `content.replicate`'s posture rather than `content.fetch`'s, whose producer keeps an unissued trim from the observed inventory |
| `content.derived` | Processor → Watcher *(live in shadow since 2026-10-03 - producer CannObserv/processor#1, first consumer CannObserv/watcher#325)* | fact, broadcast (both processing outcomes share it - `ProcessingCompleteEvent`, including the "spec bound nothing" result, and `ProcessingFailedEvent` - so an issuer sees success and failure in one group, `content.blobs`'s posture; an issuer discards facts for command ids it did not issue) | `watcher.derived` (one per consuming service) | `XPENDING` + **undelivered age** per group, probed as above; `watcher.derived` exists since 2026-10-03. **No retention cap**: nothing trims it, so no `XLEN` threshold - `maxmemory` is its only bound (CannObserv/broker#62). No `+xtrim` selector names it either, and the row still does not say Never XTRIMmed: a cap here is Processor's call and would ride its publish, which needs no grant, so the phrase would promise more than the broker enforces | `content.derived.dlq` - shared by every consuming service (drainers filter on `dlq.group`) / **Watcher**, as the first consumer | **retries by reclaim - NOPERM shown here, OOM stated** (CannObserv/broker#75, the way CannObserv/watcher#245 and CannObserv/replicator#19 stated theirs). A planned rehearsal on 2026-10-04 withheld Processor's grant on this stream for 112 s: the refused publish left the command pending, and the reclaim published `processing_complete` 659 s after delivery, at attempt 1, with no strike and no dead letter (`deploy/redis-acl.conf`, the processor stanza). The same path covers `OOM`: Processor classifies `OOM command not allowed` on this `XADD` as transient, publishes nothing and leaves the `content.process` entry pending, and the reclaim **re-runs** the extraction - deterministic, into a write-if-absent store - rather than re-publishing a cached result, so no dedupe key. A re-run after an `XADD` that landed and an `XACK` that did not publishes the fact twice; Watcher's consumer keeps the first terminal fact per `command_id` and drops later ones (CannObserv/watcher#325, `81e4083`). Not shown on this broker: the first traffic (2026-10-03) met no `OOM`. Processor showed it on a scratch 7.0.15 under these grants with `maxmemory 1` (broker#75), and rehearsing it here would refuse every participant's writes, not only Processor's |

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

## Participants, hosts and paths

Where each participant runs, and how its packets reach this broker. The four
live nodes are in `pdx` since 2026-09-15, which closes the cross-region interval
broker#1 R6 made unavoidable (CannObserv/broker#8). Processor's VM is a
fifth, consuming since 2026-10-02 (CannObserv/broker#75, which re-homed the
role #62 had declared for Observo): `co-processor`, on the tailnet as
`tag:processor` since 2026-09-29, admitted to `tag:broker` on 6379 by a policy
rule the same day. It runs Tailscale with `--accept-dns=true` since
CannObserv/processor#8, so its bus URL names the broker as `broker`, like the
other three.

| Service | Tailnet node | VM | Region | Tailnet address | Path to broker |
|---|---|---|---|---|---|
| `archiver` | `archiver` | `co-registrar` | pdx | `100.109.138.101` | direct |
| `watcher` | `watcher` | `co-watcher` | pdx | `100.66.24.24` | direct |
| `replicator` | `replicator` | `co-replicator` | pdx | `100.114.136.20` | direct |
| `processor` | `co-processor` *(consuming since 2026-10-02, CannObserv/processor#1)* | `co-processor` | not recorded | `100.110.22.56` | direct - a hairpin through the exe.dev NAT both VMs share, both ways since the service runs (CannObserv/processor#15, [NETWORK-PATHS.md](NETWORK-PATHS.md)) |
| broker | `broker` | `co-broker` | pdx | `100.97.91.19` | - |

**This table is checked against the live broker.** `CLIENT LIST` reports each
connection's peer address and `user=`, so a participant that moves reconnects
from an address this table does not name and
`test_every_connected_participant_is_where_the_docs_say` goes red on the node
until the row follows. A host table here has rotted silently before: the one in
[ACL-CUTOVER.md](ACL-CUTOVER.md) kept watcher in `lax` and replicator on
watcher's VM for days after both had moved.

The measured latency from each participant, the path beside every number, and
the accepted DERP risk: [NETWORK-PATHS.md](NETWORK-PATHS.md).

## Non-stream keys on `db0`

Provenance: CannObserv/broker#9, CannObserv/replicator#80.

This file had no row for anything that is not a stream, which is how an audit
came to find these by scanning the keyspace rather than by reading.

| Pattern | Owner | Kind | Lifetime | Commands used | What it is |
|---|---|---|---|---|---|
| `replicator:cmd:<stream suffix>:<command_id>` | Replicator | string, **volatile** | `REPLICATOR_DEDUPE_TTL_SECONDS`, default 86400 | `SET .. NX EX`, `EXISTS` | De-duplication of `content.fetch` / `content.replicate` / `content.persist` commands. Written **after** the handler completes; read by an `EXISTS` **before** the next one runs. Reasoning: [`CannObserv/replicator:docs/CONVENTIONS.md#the-replicatorcmd-keys`](https://github.com/CannObserv/replicator/blob/main/docs/CONVENTIONS.md#the-replicatorcmd-keys) |

**This is the only non-stream key pattern any service writes here.** A new one
belongs in this table before it belongs on the broker. Processor adds none
(CannObserv/broker#62, #75): it keeps no dedupe keys, because a redelivered
command re-runs a deterministic extraction whose output is written
content-addressed and if-absent - and its ACL user holds neither `+set` nor
`+exists`, which is what keeps that a property of the broker rather than of
Processor's source.

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
The counts come from `INFO keyspace` and `SCAN MATCH`, which is all the probe's
`brokeradmin` and the operator's `acladmin` hold for this - neither has `+ttl`
or `+type`, deliberately, and the average is the one `INFO` reports.

## Who drains a DLQ

Moved to [DLQ-DRAINING.md](DLQ-DRAINING.md) when this file passed its context
budget (2026-09-29): the writer / drainer / backstop roles, the capture, and the
`XTRIM MINID` drain procedure with its worked example.

## Consumer registrations

The one-time reap of orphaned consumer registrations, and why it cannot
recur: [CONSUMER-REGISTRATIONS.md](CONSUMER-REGISTRATIONS.md).
