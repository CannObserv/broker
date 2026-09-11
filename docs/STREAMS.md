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
monitoring contracts, loss detection, the `noeviction` contract - is
[BUS-HEALTH.md](BUS-HEALTH.md).

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

## Non-stream keys on `db0`

Provenance: CannObserv/broker#9, CannObserv/replicator#80.

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

## Who drains a DLQ

Provenance: CannObserv/archiver#162.

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



## Orphaned consumer registrations, one time only

Provenance: CannObserv/archiver#156.

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
