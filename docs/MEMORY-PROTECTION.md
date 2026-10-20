# Memory protection

What the `maxmemory` cap does to the producers on this instance when it is
reached, and why the policy beside it is `noeviction` and not an eviction
policy. Provenance for the second: CannObserv/broker#9.

The cap and the policy themselves are in [../deploy/redis.conf.broker](../deploy/redis.conf.broker);
what the bus-health probe does with both - the 75% warning and the
`eviction-policy` finding - is [BUS-HEALTH.md](BUS-HEALTH.md).

Moved out of BUS-HEALTH.md on 2026-09-22, when it ran past the per-doc
context budget. It was there because CannObserv/broker#1 R5's answer had no
other home in this repo, not because it describes the probe.

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

### `content.derived`: retries by reclaim

The `Producer durability under OOM` cell for `content.derived` in
[STREAMS.md](STREAMS.md), moved here when that file passed its context budget
again (CannObserv/broker#114). **Retries by reclaim - NOPERM shown here, OOM
stated** (CannObserv/broker#75, the way CannObserv/watcher#245 and
CannObserv/replicator#19 stated theirs).

A planned rehearsal on 2026-10-04 withheld Processor's grant on this stream for 112 s: the refused publish left the command pending, and the reclaim published `processing_complete` 659 s after delivery, at attempt 1, with no strike and no dead letter (`deploy/redis-acl.conf`, the processor stanza). The same path covers `OOM`: Processor classifies `OOM command not allowed` on this `XADD` as transient, publishes nothing and leaves the `content.process` entry pending, and the reclaim **re-runs** the extraction - deterministic, into a write-if-absent store - rather than re-publishing a cached result, so no dedupe key. A re-run after an `XADD` that landed and an `XACK` that did not publishes the fact twice; Watcher's consumer keeps the first terminal fact per `command_id` and drops later ones (CannObserv/watcher#325, `81e4083`). Not shown on this broker: the first traffic (2026-10-03) met no `OOM`. Processor showed it on a scratch 7.0.15 under these grants with `maxmemory 1` (broker#75), and rehearsing it here would refuse every participant's writes, not only Processor's

## The streams `maxmemory` alone bounds

Moved out of [STREAMS.md](STREAMS.md), whose rows say **No retention cap**, when
it passed its context budget again (CannObserv/broker#114).

**A row that says No retention cap is a fact about behaviour, not about
grants** (CannObserv/broker#60). Nothing trims the nine `content.*` streams
or the headless-Watcher pair, `info.watch-events` and `info.watch-check`:
no producer passes a `maxlen` (CannObserv/watcher#317, CannObserv/replicator#106;
the processing pair has none by contract, CannObserv/broker#62, nor have
`content.persist` and `content.capture`, CannObserv/broker#64, #114, nor a fact Watcher publishes,
CannObserv/watcher#327) and archiver's
trim allowlist is `info.changes` alone (CannObserv/archiver#239).
`maxmemory` is their only bound, so the probe gives them no `XLEN` threshold -
`test_the_probe_and_the_inventory_agree_on_which_streams_have_no_cap`, in
`tests/deploy/test_redis_acl.py`, pins the phrase to exactly the rows the probe leaves
unthresholded. All of them but `content.derived` are
also **Never XTRIMmed**: CannObserv/broker#41 took Watcher's and Replicator's
`+xtrim` off, never issued (CannObserv/watcher#327, CannObserv/replicator#119),
so no selector names any of them. No selector names `content.derived` either;
its row keeps the phrase off because a cap there would ride its producer's
publish. Whether each stream gets a cap is its
producer's call, filed on its producer's repo - `content.replicate`'s is
CannObserv/archiver#267.

## `noeviction` is load-bearing beyond refusing writes

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
`allkeys-*` is not the escape either.

**Archiver's argument against it is the sharper one: silence**
(CannObserv/archiver#234). Eviction takes a whole key, so `info.registry` comes
back with the next delta but not its replay-from-`0-0` floor, and a consumer
booting before the next snapshot converges to a partial set and **reports
success**. *Detecting loss* flags the reset after the fact; the wrong answer is
in the consumer. The dedupe keys cost re-work; this costs correctness.

All three claims are checked rather than trusted, and the third is the one
that matters at 3am:

- `test_tracked_config_sets_an_explicit_nonzero_maxmemory` pins the policy in
  the tracked config, with the hazard in the failing test's own docstring so
  whoever changes it deliberately reads why;
- `test_the_dedupe_keys_are_the_only_volatile_keys_on_the_instance` pins the
  keyspace claim against the live broker: the day a second service writes a key
  with a TTL is the day this section stops being true;
- **the probe reports a wrong policy every tick**, which is what closes the gap
  the other two leave. A `CONFIG SET maxmemory-policy volatile-lru` is live and
  persisted nowhere, so until this check existed the only detector was a test
  suite someone had to remember to run on the node. The finding names the
  family, because the two fail differently, as above.
