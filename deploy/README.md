# deploy/

Systemd artifacts for the broker VM.

| Unit / file | Type | Purpose |
|---|---|---|
| `redis-server.dropin.conf` | service drop-in | Tuning for the stock `redis-server.service`: AOF `everysec`, `maxmemory-policy noeviction`, and an explicit `--maxmemory` cap. Installs as `broker.conf`; layers on the package unit rather than replacing it |
| `broker-bus-health.service` | service (oneshot) | One WARN-only health tick: memory, per-stream `XLEN`, last-entry age, `XPENDING`, DLQ depth, disk. Never blocks anything |
| `broker-bus-health.timer` | timer | Runs the probe every 10 min. Enable with `systemctl enable --now broker-bus-health.timer` |

Both parity tests (`tests/deploy/`) compare the repo copy against
`/etc/systemd/system/` and **skip** when the file is absent, so CI and dev
clones pass and only a host actually running the broker is asserted on.

## Install

```bash
sudo mkdir -p /etc/systemd/system/redis-server.service.d
sudo cp deploy/redis-server.dropin.conf \
    /etc/systemd/system/redis-server.service.d/broker.conf
sudo cp deploy/broker-bus-health.service deploy/broker-bus-health.timer \
    /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl restart redis-server
sudo systemctl enable --now broker-bus-health.timer

# verify the tuning took:
redis-cli CONFIG GET appendonly        # -> yes
redis-cli CONFIG GET maxmemory-policy  # -> noeviction
redis-cli CONFIG GET maxmemory         # -> must NOT be 0
```

The install filename is `broker.conf`. Archiver's copy installed as
`archiver.conf` on the shared VM; a node still carrying that name is a node that
was never cut over (archiver#193 D6).

## Changing the cap

Prefer applying it live - no restart, no dropped client connections - and let
the unit supply it from the next restart onward:

```bash
# edit ExecStart in deploy/redis-server.dropin.conf, then:
sudo cp deploy/redis-server.dropin.conf \
    /etc/systemd/system/redis-server.service.d/broker.conf
sudo systemctl daemon-reload
redis-cli CONFIG SET maxmemory <value from ExecStart>   # applies now, no restart
```

Pass the value **exactly as `ExecStart` spells it** - `CONFIG SET` accepts the
same unit suffixes, so there is no byte conversion to get wrong and no second
copy of the number to drift.

`CONFIG SET` is not persisted (no `CONFIG REWRITE`), which is what keeps the
unit authoritative. The flip side is that it can drift the *running* broker from
the tracked file in either direction, and the file-parity test cannot see that.
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
