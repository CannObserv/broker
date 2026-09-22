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
