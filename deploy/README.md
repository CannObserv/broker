# deploy/

Everything the broker node runs, tracked. Three of these are Redis's, two are
the health probe's.

| File | Installs as | Purpose |
|---|---|---|
| `redis.conf.broker` | appended to `/etc/redis/redis.conf` | The tuning: bind, `requirepass`, AOF `everysec`, `maxmemory-policy noeviction`, explicit `maxmemory`. `__REQUIREPASS__` is substituted at install time from `/etc/redis/broker-password` |
| `redis-server.service.d/broker.conf` | `/etc/systemd/system/redis-server.service.d/broker.conf` | Unit **ordering only**: `After=tailscaled.service`, widened restart limits, and the `ExecStartPre=+` wait |
| `wait-for-tailnet-addr.sh` | `/usr/local/sbin/` | R1's boot-race insurance. Probes `/proc/net/fib_trie`, never `ip addr` |
| `broker-bus-health.service` | `/etc/systemd/system/` | One WARN-only health tick: memory, per-stream `XLEN`, last-entry age, `XPENDING`, DLQ depth + evidence capture, disk. Never blocks anything |
| `broker-bus-health.timer` | `/etc/systemd/system/` | Runs the probe every 10 min |
| `redis-acl.conf` + `render-acl.sh` | `/etc/redis/users.acl` | Per-service ACL users (D3, broker#2). **Not yet installed** - it needs `aclfile` in redis.conf, which is immutable, so it rides the restart window in broker#5 |

`tests/deploy/` asserts all of it: the installed copies match these files
(skipping when absent, so CI and dev clones pass), and
`test_live_broker_matches_tracked_config.py` reads the running config back
through `CONFIG GET` so a `CONFIG SET` that no file records still gets caught.

## The ACL users need a restart, and that was not obvious

`aclfile` is an **immutable** config. `CONFIG SET aclfile` fails with "can't set
immutable config", so turning it on costs one restart of an instance three
services depend on. Everything *after* that is live - `ACL SETUSER` applies
immediately and `ACL SAVE` persists - which is why a separate file still beats
`user` lines in `redis.conf`: an ACL that can only be changed by a cohort-wide
restart is an ACL nobody will dare tighten.

The alternative is `ACL SETUSER` plus `CONFIG REWRITE`, which needs no restart
and does **not** destroy this repo's delimited block (checked, rather than
assumed - the comments survive and the users land in a generated section below
them). It is still the worse option: it splits the ACL's source of truth across
a file this repo only partly tracks, and `CONFIG REWRITE` normalises unrelated
directives (`dir` and `logfile` get rewritten), which the live-config test would
then see as drift.

So the ACL cutover rides broker#5's window alongside `databases 1`.

Two things the tracked file cannot be written without knowing, both found by
`tests/deploy/test_redis_acl.py` loading it into a throwaway server rather than
by reading it:

- **An aclfile permits no comments and no blank lines.** Redis aborts startup
  on one - and refuses the *whole file*, not the offending line. `render-acl.sh`
  strips them, so the reasoning can live with the rules where it belongs.
- **`CLIENT SETINFO` does not exist before Redis 7.2.** This broker is 7.0.15,
  so granting `+client|setinfo` pre-emptively is rejected and takes every user
  down with it. It becomes a required step of any upgrade to >= 7.2 instead.

## Installing the ACL users

```bash
# passwords file: __ARCHIVER_PW__=... one per line, 0400 root:root
sudo deploy/render-acl.sh /etc/redis/broker-acl-passwords \
    | sudo install -m 0640 -o root -g redis /dev/stdin /etc/redis/users.acl
# then, in the restart window, add `aclfile /etc/redis/users.acl` to redis.conf
```

### The cutover order, and why only step 1 needs the window

**`default` is the sharp edge, in both directions.** Omitting it from an aclfile
silently makes it `nopass` - an anonymous client is served while
`CONFIG GET requirepass` still returns the password, which is R2 arriving as a
side effect of turning on the mechanism meant to prevent it. And setting it
`off` at first load locks out all three services, because the restart lands
before any of them has moved onto its own credential. So the tracked file
declares `default` **enabled, with today's password**, and retiring it is the
last step rather than the first.

1. **In the window** - install `/etc/redis/users.acl`, add `aclfile` and
   `databases 1` to `redis.conf`, restart. Nothing changes for any service:
   every URL still says `default:` and `default` still has the same password.
   Confirm the tailnet wait fired, `NRestarts=0`, and that an anonymous
   `redis-cli` is refused.
2. **Rolling, no window** - flip each service's URL to its own credential, one
   at a time, verifying each before the next.
3. **Rolling** - flip the probe's `BROKER_REDIS_URL` to `brokeradmin`.
4. **Live** - `ACL SETUSER default off` then `ACL SAVE`, run as `default`
   (`brokeradmin` deliberately has no `+acl`). Reversing it is
   `ACL SETUSER default on >...` plus `ACL SAVE`; both directions are pinned by
   `test_disabling_default_is_live_and_reversible`.

Steps 2 to 4 are reversible and need no restart, which is the point of putting
the irreversible-feeling step last. And all three participants classify `NOPERM`
as transient, so a grant that is too narrow degrades to a backing-off publisher
rather than dead-lettering valid events - bought deliberately (archiver#193
Phase 1, replicator#82) and the reason a wrong rule here is recoverable.

## Why the tuning is in `redis.conf` and not in the drop-in

broker#1 Phase 1 step 1 left this open - "keep layering on Debian's package unit
or own the whole thing on a dedicated host" - and this repo arrived from archiver
carrying a drop-in that overrode `ExecStart` to state the tuning as CLI flags.
Phase 2 had already answered it the other way on the node, and Phase 5 settled
it in the repo's favour of what was actually deployed.

The deciding argument is `requirepass`. A drop-in states its settings as
`ExecStart` arguments, which puts the password in `argv` - visible in `ps` and
in journald. The secret has to live in `redis.conf` regardless, so splitting the
rest of the tuning across two mechanisms buys nothing and costs a second place
to look. The drop-in slot is therefore used for the one thing a config file
cannot express, unit ordering, and `test_the_dropin_does_not_override_execstart`
keeps it that way.

## Install

```bash
# Redis tuning (first install only; the block is delimited in redis.conf)
sudo sed "s/__REQUIREPASS__/$(sudo cat /etc/redis/broker-password)/" \
    deploy/redis.conf.broker | sudo tee -a /etc/redis/redis.conf >/dev/null

# Unit ordering + the boot-race wait
sudo install -m 0755 deploy/wait-for-tailnet-addr.sh /usr/local/sbin/
sudo install -m 0644 -D deploy/redis-server.service.d/broker.conf \
    /etc/systemd/system/redis-server.service.d/broker.conf

# The health timer
sudo install -m 0644 deploy/broker-bus-health.service \
    deploy/broker-bus-health.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now broker-bus-health.timer
```

Then verify against the running broker rather than against the files:

```bash
set -a; . /etc/broker/.env; set +a
uv run pytest tests/deploy            # every skip becomes a real assertion here
```

**Restarting `redis-server` is a cohort-wide event.** All three services connect
to this instance, so anything needing a restart waits for a window rather than
being applied in passing.

## Changing the cap

Prefer applying it live - no restart, no dropped client connections - then
persist it in both places:

```bash
redis-cli -u "$BROKER_REDIS_URL" CONFIG SET maxmemory <value>   # applies now
sudo sed -i 's/^maxmemory .*/maxmemory <value>/' /etc/redis/redis.conf
sed -i 's/^maxmemory .*/maxmemory <value>/' deploy/redis.conf.broker
```

Pass the value **exactly as the config file spells it** - `CONFIG SET` accepts
the same unit suffixes, so there is no byte conversion to get wrong.

`CONFIG SET` is not persisted (no `CONFIG REWRITE`), which is what keeps the
tracked file authoritative, and it can drift the running broker from that file
in either direction. Three checks cover the gap from different sides:
`test_live_broker_matches_tracked_config.py` compares the running values against
this repo, each participant's `check_redis_floor.sh` reads `maxmemory` at its own
service start (warn-only), and the bus-health probe reports `maxmemory 0` as a
finding every tick.

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

> The cap moved from `redis-server.dropin.conf` to `redis.conf.broker` in
> Phase 5. Archiver's half of the seam still names the old path until
> CannObserv/archiver#196 lands - the decision is unchanged, only the filename
> moved. A broken pointer is what this mechanism is *for*: there is no test
> spanning the two repos, so the pair of comments is the whole seam.

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

`/etc/broker/.env` (`root:exedev`, `0640`) carries:

- `BROKER_REDIS_URL` - what the probe connects to. Include the `default:`
  username explicitly (`redis://default:<password>@localhost:6379/0`): the
  empty-username form authenticates for redis-py and **fails** for `redis-cli`,
  which sends a two-argument `AUTH "" <password>`, so every shell tool degrades
  silently while the services look green (CannObserv/archiver#195). `localhost`
  rather than `broker`, because this probe runs *on* the broker and `redis.conf`
  binds loopback as well as the tailnet address, deliberately.
- `GOOGLE_APPLICATION_CREDENTIALS` - the read-only `co-pypi-reader` key the
  `uv run` in `ExecStart` needs to resolve `co-core` from the wheelhouse.

The probe holds **no** database credential and joins **no** consumer group.
Both are asserted by `tests/deploy/test_bus_health_units.py`.

## DLQ evidence

The probe writes captured DLQ entries to
`/var/lib/broker-bus-health/dlq-evidence/<topic>/<last-id>.json`, inside the
unit's `StateDirectory`. That is the "back up" step of the drain runbook, done
on the tick that first sees a non-resting queue rather than by an operator
under time pressure. Delete a topic's dumps once its triage is finished; the
high-water mark is read back from the filenames, so deleting them correctly
re-arms capture rather than leaving a gap. See
[`../docs/STREAMS.md`](../docs/STREAMS.md), *Who drains a DLQ*.
