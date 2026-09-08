# deploy/

Systemd artifacts for the broker VM.

| Unit / file | Type | Purpose |
|---|---|---|
| `redis-server.dropin.conf` | service drop-in (**not currently installed**) | The tracked statement of the broker's tuning: AOF `everysec`, `maxmemory-policy noeviction`, explicit `--maxmemory`. Those settings are in force on the node, but from `/etc/redis/redis.conf`, not from here - see *Not reconciled* below |
| `broker-bus-health.service` | service (oneshot) | One WARN-only health tick: memory, per-stream `XLEN`, last-entry age, `XPENDING`, DLQ depth, disk. Never blocks anything |
| `broker-bus-health.timer` | timer | Runs the probe every 10 min. Enable with `systemctl enable --now broker-bus-health.timer` |

Both parity tests (`tests/deploy/`) compare the repo copy against
`/etc/systemd/system/` and **skip** when the file is absent, so CI and dev
clones pass and only a host actually running the broker is asserted on.

## Not reconciled with the node yet (broker#1 Phase 5)

`redis-server.dropin.conf` arrived from archiver, where a drop-in was the only
mechanism tuning the broker. **On this node it is not the mechanism.** Phase 2
appended the four settings straight to `/etc/redis/redis.conf` and gave the
drop-in slot to something else:

| Concern | Where it actually lives |
|---|---|
| `bind`, `requirepass`, `appendonly`, `appendfsync`, `maxmemory`, `maxmemory-policy` | appended to `/etc/redis/redis.conf` |
| `After=tailscaled.service` + the `/proc/net/fib_trie` wait (R1's boot race) | `/etc/systemd/system/redis-server.service.d/broker.conf` |

So **`broker.conf` is taken, by a different file.** Installing this repo's
drop-in under that name would delete the tailnet ordering and re-open the race
observo#473 cost two weeks. Phase 1 step 1 posed the choice - "keep layering on
Debian's package unit or own the whole thing on a dedicated host" - and Phase 2
answered it in practice without the repo following. Phase 5 settles it and
brings the two into line; until then `tests/deploy/` asserts only the invariant
that survives either answer (the cap is explicit and non-zero) and skips the
installed-copy comparison.

## Install (the health timer)

```bash
sudo cp deploy/broker-bus-health.service deploy/broker-bus-health.timer \
    /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now broker-bus-health.timer

# verify the tuning is in force, whatever supplies it:
redis-cli -u "$BROKER_REDIS_URL" CONFIG GET appendonly        # -> yes
redis-cli -u "$BROKER_REDIS_URL" CONFIG GET maxmemory-policy  # -> noeviction
redis-cli -u "$BROKER_REDIS_URL" CONFIG GET maxmemory         # -> must NOT be 0
```

## Changing the cap

Prefer applying it live - no restart, no dropped client connections - then
persist it wherever the node currently keeps it (today: `/etc/redis/redis.conf`;
see *Not reconciled* above), and keep this repo's tracked statement in step:

```bash
redis-cli -u "$BROKER_REDIS_URL" CONFIG SET maxmemory <value>   # applies now
sudo sed -i 's/^maxmemory .*/maxmemory <value>/' /etc/redis/redis.conf
# then edit ExecStart in deploy/redis-server.dropin.conf to match
```

Pass the value **exactly as the config file spells it** - `CONFIG SET` accepts
the same unit suffixes, so there is no byte conversion to get wrong.

`CONFIG SET` is not persisted (no `CONFIG REWRITE`), which is what keeps the
tracked file authoritative. The flip side is that it can drift the *running* broker from the tracked file
in either direction, and no file-parity test can see that.
Two live-value checks cover the gap from opposite sides: each participant's
`check_redis_floor.sh` reads `maxmemory` at its own service start (warn-only),
and this repo's bus-health probe reports `maxmemory 0` as a finding every tick.

## `maxmemory` is load-bearing, not decoration

`noeviction` with the default `maxmemory 0` is *inert*: there is no ceiling to
refuse writes at, so an untrimmed stream never produces the retryable write
errors the "a stream broker must never evict" reasoning assumes - it grows until
the kernel OOM-killer takes `redis-server`, costing the whole broker plus an
AOF-replay restart (CannObserv/archiver#128). The explicit cap converts that
into bounded, instance-wide `OOM command not allowed` errors.

**Those errors are only survivable because archiver classifies them as
transient.** `_TRANSIENT_PUBLISH_ERRORS` in
`CannObserv/archiver:src/core/changes/publisher.py` retries through them instead
of dead-lettering valid events. The cap and that classification are one decision
now made in two repositories with no test spanning them; each side names the
other in a comment. **Do not change either alone** (archiver#193 R5).

**The cap changes the failure mode for every producer, not just archiver's.**
Once it is reached, `XADD` is refused instance-wide - Watcher's `content.fetch`
and Replicator's `content.blobs` included. Archiver rides that out because its
outbox retries indefinitely; **whether the other producers have an equivalent
durable retry is their own property, and this repo does not assert it.** A
producer that publishes straight from a request handler with no outbox will
*drop* on OOM. Raised on CannObserv/watcher#245 and CannObserv/replicator#19 so
each producer's durability under OOM is a stated assumption rather than an
assumed one; the `Producer durability under OOM` column in
[`../docs/STREAMS.md`](../docs/STREAMS.md) records the current answer.

AOF needs no separate cap: `auto-aof-rewrite-percentage 100` /
`auto-aof-rewrite-min-size 64mb` self-bound the file at roughly 2x the dataset,
so bounded retention bounds the AOF. The independent exposure is fork/COW at
rewrite time, which `maxmemory` also caps.

## Environment

`/etc/broker/.env` carries:

- `BROKER_REDIS_URL` - what the probe connects to. Include the `default:`
  username explicitly (`redis://default:<password>@...`): the empty-username
  form authenticates for redis-py and **fails** for `redis-cli`, which sends a
  two-argument `AUTH "" <password>`, so every shell tool degrades silently while
  the services look green (CannObserv/archiver#195).
- `GOOGLE_APPLICATION_CREDENTIALS` - the read-only `co-pypi-reader` key the
  `uv run` in `ExecStart` needs to resolve `co-core` from the wheelhouse.

The probe holds **no** database credential and joins **no** consumer group.
Both are asserted by `tests/deploy/test_bus_health_units.py`.
