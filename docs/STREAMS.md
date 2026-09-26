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
answered under *Who drains a DLQ* below.

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
and a `+xtrim` selector naming no more than that, and holds neither command on
its root permission set - where a Redis key pattern applies it to every stream
the service can name, including the ones it only reads. A row that says
**Never XTRIMmed** is enforced the same way: no `+xtrim` selector on the
instance names that stream (CannObserv/broker#34).
`tests/deploy/test_redis_acl.py` parses this column and that phrase and asks
redis to prove both halves, so a row changed here without the grant following
fails a test rather than degrading a service quietly.

**A row that says No retention cap is a fact about behaviour, not about
grants** (CannObserv/broker#60). Nothing trims the eight `content.*` streams:
no producer passes a `maxlen` (CannObserv/watcher#317, CannObserv/replicator#106;
the processing pair has none by contract, CannObserv/broker#62, nor has
`content.persist`, CannObserv/broker#64) and archiver's
trim allowlist is `info.changes` alone (CannObserv/archiver#239).
`maxmemory` is their only bound, so the probe gives them no `XLEN` threshold -
`test_the_probe_and_the_inventory_agree_on_which_streams_have_no_cap`, in
the same file, pins the phrase to exactly the rows the probe leaves
unthresholded. `content.replicate`, `content.process` and `content.persist` are
also **Never XTRIMmed**; Watcher's `+xtrim` selector still names `content.fetch` and
`content.revisions`, and Replicator's names `content.blobs` and
`content.artifacts`, unissued; no selector names `content.derived`, whose cap
would ride its producer's publish. Whether each stream gets a cap is its
producer's call, filed on its producer's repo - `content.replicate`'s is
CannObserv/archiver#267.

| Stream | Producer → consumer | Kind | Consumer group | Health primitive | DLQ (writer / **drainer**) | Producer durability under OOM |
|---|---|---|---|---|---|---|
| `info.changes` | Archiver → Replicator *(target)* | event | none *yet* - Replicator adds one | producer-side outbox stats (depth / oldest-unpublished age / dead-lettered count, CannObserv/archiver#112): dashboard badge + the drain loop's periodic journald line, plus archiver's own reduced bus-health timer re-running the same query from outside the publisher process - the surface that keeps reporting when the publisher is down. `XPENDING` + undelivered age once a group exists | `info.changes.dlq` *(target)* / **Replicator** *(prospective - nothing writes it until Replicator adds a group)* | **retries indefinitely** - transactional outbox, OOM classified transient |
| `content.fetch` | Watcher → Replicator | command | `replicator.fetch` (exactly one - competing consumers) | `XPENDING` + **undelivered age**. **No retention cap**: nothing trims it, so no `XLEN` threshold - `maxmemory` is its only bound (CannObserv/broker#60). | `content.fetch.dlq` / **Replicator** | **retries indefinitely, no ceiling** - verified CannObserv/watcher#288. The row stays `pending_publish` on all three publish paths and republishes under the same `command_id`, which Replicator dedupes. There is no attempt counter on that outbox and the reaper scans `IN_FLIGHT` only, so a long outage does not garbage-collect the backlog |
| `content.blobs` | Replicator → Watcher | fact | one per consuming service | `XPENDING` + **undelivered age** per group. **No retention cap**: nothing trims it, so no `XLEN` threshold - `maxmemory` is its only bound (CannObserv/broker#60). | `content.blobs.dlq` / **Watcher** | **retries indefinitely, no ceiling** - verified CannObserv/replicator#79. A refused publish is `Outcome.RETRY`: nothing acked, no fact, nothing dead-lettered, PEL entry intact, and the delivery ceiling is never consulted. Store-then-publish means the bytes are already on disk, so the retry after the cap lifts is a no-op |
| `content.revisions` | Watcher → **Archiver** *(producer target: CannObserv/watcher#253)* | fact | `archiver.revisions` (one per consuming service) | `XPENDING` + **undelivered age** - **the first group Archiver owns**; probed by this repo's bus-health probe: non-zero pending across two consecutive ticks WARNs (healthy steady state is 0 - a state to name, not a number to guess). **No retention cap**: nothing trims it, so no `XLEN` threshold - `maxmemory` is its only bound (CannObserv/broker#60). | `content.revisions.dlq` - written by the ingest consumer's quarantine path / **Archiver** | **retries indefinitely** - verified CannObserv/watcher#288. `OutOfMemoryError` is in `_TRANSIENT_PUBLISH_ERRORS`, and the transient branch is exempt from `MAX_PUBLISH_ATTEMPTS`; `mark_failure` backs off 60 s -> 1 h |
| `content.artifacts` | Replicator → **Archiver** *(consumer live - CannObserv/archiver#170)* | fact, broadcast - **four event types**: both replicate outcomes (`replication_complete` / `replication_failed`) and, since cannobserv v0.19.6, both persist outcomes (`blob_persisted` / `persist_failed`, cannobserv#493, CannObserv/broker#64), so an issuer sees success and failure of either command in one group. The persist pair carries no domain echo and no URL, by contract; Archiver correlates on `command_id` | `archiver.artifacts` (one per consuming service) | `XPENDING` + **undelivered age**; issuer-side, `information.replication_commands` rows still `state='requested'` past the reap horizon - the reaper logs each abandonment at WARNING. **No retention cap**: nothing trims it, so no `XLEN` threshold - `maxmemory` is its only bound (CannObserv/broker#60). | `content.artifacts.dlq` - written by this consumer's quarantine path / **Archiver** | **retries indefinitely** - verified CannObserv/replicator#79, same `Outcome.RETRY` path as `content.blobs` |
| `content.fetch-policy` | Watcher → Replicator workers *(producer live - full set republished on `*/5 * * * *`, capped by producer-side `BusPublish.maxlen` 500, floored at 10 full sets, CannObserv/watcher#265, #292)* | config/state, broadcast, last-write-wins per host key | **none, permanently - by design** | **last-entry age via `XINFO STREAM`** - probed by this repo's bus-health probe, WARN over 15 min (3× the republish period); **and `XLEN`** against the cap in force, which is the 500 or the 10-full-set floor, whichever is larger - the set size is read off this stream rather than mirrored, so the threshold follows the corpus (CannObserv/broker#44, #45, #51; [BUS-HEALTH.md](BUS-HEALTH.md), *The one cap that is read, not mirrored*) | **none applies** | **self-correcting** - full set is republished on a timer |
| `info.registry` | **Archiver** → Watcher *(consumer live - CannObserv/watcher#254)* | config/state, broadcast, last-write-wins per `info_item_id`, `generation`-ordered | **none, permanently - by design** (every consumer needs every message; a group accumulates a PEL nothing drains) | **last-entry age via `XINFO STREAM`** - on a non-empty corpus the snapshot guarantees ≥1 entry/hour, so an age over ~2× the snapshot interval means the producer is down; an empty or never-announced registry publishes nothing, so the alarm needs a corpus-size guard. See CannObserv/archiver#147 | **none applies** - a state message has nothing to close; quarantine is terminal and the next full set supersedes | **split by path**: deltas ride the transactional outbox and retry indefinitely (OOM transient); snapshots have **no retry** - one lost to an outage is corrected by the next period, not a re-attempt. **Never XTRIMmed - capped only on publish, and the broker refuses the rest**: consumers boot by replaying from `0-0`, so retention has a floor - one full snapshot plus every delta since (CannObserv/archiver#141) - and it is a boot contract, not housekeeping. Cut under it and the next consumer to boot converges to a partial set and reports success. The producer holds the floor with a `MAXLEN` on every publish (`ARCHIVER_REGISTRY_STREAM_MAXLEN`, sized from key count × sets retained); a trim from anywhere else cannot see where the last snapshot starts. No identity here holds `+xtrim` on it - not archiver, and not `brokeradmin`, whose trim stops at `~*.dlq` (CannObserv/broker#34) |
| `content.replicate` | **Archiver** → Replicator *(producer live - CannObserv/archiver#169; consumer shipped for `gcs`, CannObserv/replicator#34)* | command | `replicator.replicate` (exactly one - competing consumers, `content.fetch`'s posture) | `XPENDING` + **undelivered age**, plus the issuer-side view `information.replication_commands` gives: rows still `state='requested'` past the reaper horizon, which the reaper (CannObserv/archiver#170) closes as `abandoned` and logs at WARNING. **No retention cap**: nothing trims it, so no `XLEN` threshold - `maxmemory` is its only bound (CannObserv/broker#60). | `content.replicate.dlq` - Replicator's to write; Archiver provisions nothing here / **Replicator** | **retries indefinitely** - transactional outbox, OOM classified transient. **Never XTRIMmed, and the broker now refuses it**: capping a command stream deletes commands the consumer group has not delivered and orphans the PEL entries naming them. The topic is absent from archiver's trim allowlist (`trim_topics`, CannObserv/archiver#239) *and* from every `+xtrim` selector in `deploy/redis-acl.conf` (CannObserv/broker#14), so the rule no longer rests on one participant's source - it is refused to archiver, which could trim it, and to replicator, whose PEL entries would be the ones orphaned |
| `content.persist` | **Archiver** → Replicator *(target - issuer CannObserv/archiver#276 item 2, processor CannObserv/replicator#114 step 6, shipping disabled behind `REPLICATOR_PERSIST_ENABLED`; the contract is cannobserv#493, the design of record replicator's `docs/plans/2026-09-25-content-addressed-persist-and-storage-naming.md`)* | command - `ContentPersistCommand`, one per observed revision: copy the raw bytes by digest into Replicator's private, content-addressed permanent store. Outcomes ride `content.artifacts` | `replicator.persist` (exactly one - competing consumers, `content.replicate`'s posture) | `XPENDING` + **undelivered age** - probed ahead of the consumer (CannObserv/broker#64): dormant while nothing has written the stream, and `group-missing` every tick once Archiver has and Replicator's group is not on it. A stopped reader matters beyond latency here: every persist races the temp tier's 7-day TTL (MUST-7), so an undelivered one is a revision aging toward `blob_expired`. **No retention cap**: nothing trims it, so no `XLEN` threshold - `maxmemory` is its only bound | `content.persist.dlq` - Replicator's to write, from the loop's `dead_letter` / **Replicator** | *(target)* Archiver issues through its transactional outbox, persisting the command before publishing it (MUST-2), so `content.replicate`'s retry-indefinitely posture is the expectation, to be verified when archiver#276 item 2 ships. **Never XTRIMmed, and the broker refuses it from day one**: no `+xtrim` selector in `deploy/redis-acl.conf` names it, and Archiver's grant is `+xadd` alone, in a selector, so the issuer can neither cap the stream nor take delivery in Replicator's group (broker#43's hole, not opened here) |
| `info.watch-status` | Watcher → **Archiver** *(consumer live - CannObserv/archiver#151; producer live - CannObserv/watcher#264, republish `*/5 * * * *`, producer-side `maxlen` 500, floored at 10 full sets - CannObserv/watcher#292)* | config/state, broadcast, last-write-wins per `info_item_id` | **none, permanently - by design** - Archiver tails groupless (`AsyncBusTailReader`), resuming from its own `bus_tail_cursors` row rather than a full `0-0` replay | consumer-side: staleness of the `watch_status` cache vs the producer's republish period; broker-side last-entry age probed by this repo's bus-health probe, WARN over 15 min, **and `XLEN`** against the cap in force - the 500 or the 10-full-set floor, whichever is larger, with the set size read off this stream rather than mirrored (CannObserv/broker#44, #45, #51; [BUS-HEALTH.md](BUS-HEALTH.md), *The one cap that is read, not mirrored*) | **none, matching `content.fetch-policy`** - **two** skip paths, both durable (the skip advances the persisted cursor) and both logged at ERROR: a frame that will not *decode*, and a decoded message the registry can never *write* (a value outside a column's domain, a constraint violation). With no DLQ and a cursor that only advances on success, retrying either forever would stall the stream silently; the periodic republish is what supersedes a skip. Everything else (broker or DB down) rewinds and retries rather than skipping | **self-correcting** - coalesced level signals, full republish on a timer (CannObserv/watcher#264) |
| `content.process` | Watcher → Observo *(target - issuer CannObserv/watcher#325, processor CannObserv/observo#629, neither built; the contract is cannobserv#486, the design of record watcher's `docs/plans/2026-09-24-observo-extraction-and-diff-design.md`)* | command - `ContentProcessCommand`, one `source_spec` per occasion | `observo.process` (exactly one - competing consumers, `content.fetch`'s posture) | `XPENDING` + **undelivered age** - probed by this repo's bus-health probe ahead of the consumer (CannObserv/broker#62): dormant while nothing has written the stream, and `group-missing` every tick once Watcher has and Observo's group is not on it, which from this side is "Observo is down" (the design surfaces it on Watcher's as `processing_timeout`). **No retention cap**: nothing trims it, so no `XLEN` threshold - `maxmemory` is its only bound | `content.process.dlq` - Observo's to write, from the driver's `dead_letter` on an undecodable command / **Observo** | *(target)* Watcher's `process_commands` outbox is the `fetch_commands` discipline - persist-before-publish plus the every-minute publish sweep (design §2) - so `content.fetch`'s retry-indefinitely posture is the expectation, to be verified when watcher#325 ships. **Never XTRIMmed, and the broker refuses it from day one**: a cap on a command stream deletes commands the worker pool has not been delivered and orphans the PEL entries naming them, so no `+xtrim` selector in `deploy/redis-acl.conf` names it - `content.replicate`'s posture rather than `content.fetch`'s, whose producer keeps an unissued trim from the observed inventory |
| `content.derived` | Observo → Watcher *(target - producer CannObserv/observo#629, first consumer CannObserv/watcher#325)* | fact, broadcast (both processing outcomes share it - `ProcessingCompleteEvent`, including the "spec bound nothing" result, and `ProcessingFailedEvent` - so an issuer sees success and failure in one group, `content.blobs`'s posture; an issuer discards facts for command ids it did not issue) | `watcher.derived` (one per consuming service) | `XPENDING` + **undelivered age** per group, probed ahead of the consumer as above. **No retention cap**: nothing trims it, so no `XLEN` threshold - `maxmemory` is its only bound (CannObserv/broker#62). No `+xtrim` selector names it either, and the row still does not say Never XTRIMmed: a cap here is Observo's call and would ride its publish, which needs no grant, so the phrase would promise more than the broker enforces | `content.derived.dlq` - shared by every consuming service (drainers filter on `dlq.group`) / **Watcher**, as the first consumer | **unverified** - Observo's durability under `OOM command not allowed` is observo#629's to state, the way CannObserv/watcher#245 and CannObserv/replicator#19 stated theirs. The design's rule for a transient failure - publish nothing and let the pending entry be reclaimed - covers a refused publish only if `OOM` is classified transient, and whether a completed extraction is re-run or re-published on the reclaim is the same question |

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
broker#1 R6 made unavoidable (CannObserv/broker#8). Observo's primary is a
fifth, declared ahead of its consumer (CannObserv/broker#62): its row carries
the address Observo's own `docs/reference/tailscale.md` states, under
`tag:observo-primary` since observo#588, admitted to `tag:broker` by a policy
rule added on 2026-09-24. Observo runs Tailscale with `--accept-dns=false`, so
its bus URL names the broker by address, `100.97.91.19`, not as `broker`.

| Service | Tailnet node | VM | Region | Tailnet address | Path to broker |
|---|---|---|---|---|---|
| `archiver` | `archiver` | `co-registrar` | pdx | `100.109.138.101` | direct |
| `watcher` | `watcher` | `co-watcher` | pdx | `100.66.24.24` | direct |
| `replicator` | `replicator` | `co-replicator` | pdx | `100.114.136.20` | direct |
| `observo` | `observo-primary` *(consumer target - CannObserv/observo#629)* | Observo's primary host (exe.dev; its VM name is not recorded in this repo) | not recorded | `100.105.63.31` | direct, once formed - the first pings rode DERP (sea) ([NETWORK-PATHS.md](NETWORK-PATHS.md)) |
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
belongs in this table before it belongs on the broker. Observo adds none
(CannObserv/broker#62): it keeps no dedupe keys, because its output is written
content-addressed and if-absent, so a redelivered command is idempotent by
construction - and its ACL user holds neither `+set` nor `+exists`, which is
what keeps that a property of the broker rather than of Observo's source.

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

## Who drains a DLQ

Provenance: CannObserv/archiver#162.

Three roles, and no two of them are reliably the same service:

- **Writer** - whichever service's consumer calls `AsyncBusConsumer.dead_letter()`
  on that topic; the `DLQ` column above names it per stream. Archiver writes
  `content.revisions.dlq` and `content.artifacts.dlq`, Replicator writes
  `content.fetch.dlq` and `content.replicate.dlq`, Observo will write
  `content.process.dlq` and Watcher `content.derived.dlq` once the processing
  pair's consumers ship (CannObserv/broker#62), and the groupless config/state
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
  `content.blobs`. The consumer-drains rule needs a far smaller grant: every
  service already holds `~<its own topic>.dlq`. **It is not "no extra grant",
  which is what this said until CannObserv/broker#12** - a key pattern is not a
  deletion grant, and for five of the six queues below the named drainer could
  write its queue and not empty it. Each drainer now holds a **selector**,
  `(+xdel ~<its own>.dlq)`, which is the only ACL grammar that scopes a command
  to a pattern; the root permission set never holds `+xdel`, so the grant cannot
  reach the stream the queue is a copy of. And **triage is not mechanical**
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
redis-cli XINFO STREAM content.fetch.dlq | grep -A1 last-generated-id   # the boundary, FIRST
redis-cli --no-raw XRANGE content.fetch.dlq - <last-generated-id> > /var/tmp/fetch-dlq-$(date +%F).txt
# read it: every payload residue, or is a real permanent failure hiding in there?
redis-cli XTRIM content.fetch.dlq MINID <last-generated-id, +1ms>
redis-cli XLEN content.fetch.dlq                    # -> 0, or what landed since the boundary
```

`XTRIM MINID`, not `DEL`: the boundary confines the deletion to the entries you
actually audited, and the key plus any consumer groups stay in place. **The
boundary is taken before the read, and the dump stops at it.** Taken after -
this runbook's order until CannObserv/broker#59 - it lands above anything
dead-lettered while you were reading, and the trim removes those unread. Taken
first, a later entry carries a higher id and survives, and everything the trim
removes is in the dump. The same hazard is why archiver's triage disposes by
`XDEL` of named ids (CannObserv/archiver#238).

**For `*.dlq` keys only.** Two rows above say **Never XTRIMmed** -
`content.replicate` and `info.registry` - and this procedure pointed at either
is refused: `brokeradmin`, the credential it runs as, holds `+xtrim` on
`~*.dlq` and nothing else (CannObserv/broker#34). The refusal is the backstop;
the reasons are in the rows.

Worked example, the CannObserv/archiver#162 drain (2026-08-19): 110 entries, every one a
`content_fetch` command against `example.com`, all inside one 18-minute window on
2026-08-13, zero non-residue payloads, zero consumer groups on the key.
`XTRIM content.fetch.dlq MINID 1786635782730-0` removed exactly those 110 and
left the key at depth 0.



The one-time reap of orphaned consumer registrations, and why it cannot
recur: [CONSUMER-REGISTRATIONS.md](CONSUMER-REGISTRATIONS.md).
