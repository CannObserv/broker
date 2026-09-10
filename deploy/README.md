# deploy/

Everything the broker node runs, tracked. Three of these are Redis's, two are
the health probe's, two are the backup's.

| File | Installs as | Purpose |
|---|---|---|
| `redis.conf.broker` | appended to `/etc/redis/redis.conf` | The tuning: bind, `requirepass`, AOF `everysec`, `maxmemory-policy noeviction`, explicit `maxmemory`. `__REQUIREPASS__` is substituted at install time from `/etc/redis/broker-password` |
| `redis-server.service.d/broker.conf` | `/etc/systemd/system/redis-server.service.d/broker.conf` | Unit **ordering only**: `After=tailscaled.service`, widened restart limits, and the `ExecStartPre=+` wait |
| `wait-for-tailnet-addr.sh` | `/usr/local/sbin/` | R1's boot-race insurance. Probes `/proc/net/fib_trie`, never `ip addr` |
| `broker-bus-health.service` | `/etc/systemd/system/` | One WARN-only health tick: memory, per-stream `XLEN`, last-entry age, `XPENDING`, DLQ depth + evidence capture, disk. Never blocks anything |
| `broker-bus-health.timer` | `/etc/systemd/system/` | Runs the probe every 10 min |
| `redis-acl.conf` + `render-acl.sh` | `/etc/redis/users.acl` | Per-service ACL users (D3, broker#2). Live since broker#5's window on 2026-09-10; the shared `default` password was retired the same day. Changes are made live with `ACL SETUSER` + `ACL SAVE` as `acladmin`, then mirrored here - `aclfile` is immutable, so the file itself is only re-read at a restart |
| `broker-backup.service` | `/etc/systemd/system/` | Ships `dump.rdb` to `gs://co-gcs-broker-backup`, verified and create-only, holding **no Redis credential**; root confined to read-only everything but its state directory (broker#4). See [`../docs/RECOVERY.md`](../docs/RECOVERY.md) |
| `broker-backup.timer` | `/etc/systemd/system/` | Hourly, `Persistent=true` |

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

So the ACL cutover rode broker#5's window on 2026-09-10. `databases 1` was to
ride the same one and did not - see the incident in `docs/RESTART-WINDOW.md`;
it waits on a `BGREWRITEAOF` that itself waits on broker#4.

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
`off` at the first load locks out every service still on `default:`, because
that restart lands before any of them has moved onto its own credential. So on
this cluster the file went in reading `on` with the then-current password,
retiring it was the last step rather than the first, and **the tracked file now
says `off`** - still carrying its password, because `off` is a flag that leaves
the password set intact and the rollback `ACL SETUSER default on` would
otherwise enable a `nopass` user with `+@all`. A new cluster repeats the order:
`on` for the first load, migrate, `off` live.

1. **In the window** - install `/etc/redis/users.acl`, add `aclfile` to
   `redis.conf` (and `databases 1` only after a `BGREWRITEAOF` - see the
   runbook's step 1a-bis), restart. Nothing changes for any service:
   every URL still says `default:` and `default` still has the same password.
   Confirm the tailnet wait fired, `NRestarts=0`, and that an anonymous
   `redis-cli` is refused.
2. **Rolling, no window** - flip each service's URL to its own credential, one
   at a time, verifying each before the next.
3. **Rolling** - flip the probe's `BROKER_REDIS_URL` to `brokeradmin`.
4. **Live** - `ACL SETUSER default off` then `ACL SAVE`, run as **`acladmin`**,
   the break-glass user that exists because `+acl` would otherwise belong to
   nobody once `default` is off, freezing every grant on the broker
   permanently. Done 2026-09-10. Reversing it, widening any grant afterwards,
   and re-opening `default` for a restart window are all done the same way.
   See `docs/RESTART-WINDOW.md` step 4.

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

# The backup (broker#4): its env names the bucket and the writer key's path.
# The timer is enabled only once the key is at that path - see docs/RECOVERY.md,
# "Provisioning the bucket and the writer".
sudo install -m 0644 deploy/broker-backup.service deploy/broker-backup.timer /etc/systemd/system/
sudo install -m 0400 -o root -g root /dev/null /etc/broker/backup.env
printf 'BROKER_BACKUP_BUCKET=co-gcs-broker-backup\nGOOGLE_APPLICATION_CREDENTIALS=/etc/broker/co-broker-backup.json\n' \
    | sudo tee /etc/broker/backup.env >/dev/null
sudo systemctl daemon-reload
sudo systemctl enable --now broker-backup.timer      # after the key exists
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

- `BROKER_REDIS_URL` - what the probe connects to:
  `redis://brokeradmin:<password>@localhost:6379/0`. The username is
  load-bearing twice over: the empty-username form means `default`, which is
  disabled since 2026-09-10, and even while it worked it **failed** for
  `redis-cli`, which sends a two-argument `AUTH "" <password>`, so every shell
  tool degraded silently while the services looked green
  (CannObserv/archiver#195). `localhost` rather than `broker`, because this
  probe runs *on* the broker and `redis.conf` binds loopback as well as the
  tailnet address, deliberately.
- `GOOGLE_APPLICATION_CREDENTIALS` - the read-only `co-pypi-reader` key the
  `uv run` in `ExecStart` needs to resolve `co-core` from the wheelhouse.

The probe holds **no** database credential and joins **no** consumer group.
Both are asserted by `tests/deploy/test_bus_health_units.py`.

`/etc/broker/notifier.env` (`0400 root:root`, **optional**) carries the check-in
credential - see *The notifier check-in* below.

`/etc/broker/backup.env` (`0400 root:root`, **required by the backup unit**,
which loads nothing else) carries `BROKER_BACKUP_BUCKET` and the
`GOOGLE_APPLICATION_CREDENTIALS` path of the **writer** key. Not in
`/etc/broker/.env`, and the reason cuts both ways: that file is readable by
`exedev`, which must not hold a credential that can write to the backup
bucket; and the backup unit must not inherit a Redis URL it has no use for.
`BROKER_BACKUP_PREFIX` is optional and defaults to the hostname.

## The notifier check-in (broker#3)

Every tick, the probe posts to notifier whether or not it found anything:

```
POST http://notifier:9000/api/v1/monitors/<id>/checkin
X-API-Key: <key>
{"status": "ok" | "alert", "variables": {"source", "finding_count", "findings"}}
```

**The report goes every tick regardless of `finding_count`, and that is the
design rather than chattiness.** A findings-only push is silent in exactly the
cases that matter most - a dead probe, a stopped timer, a wedged `uv run` or a
dead node all produce zero findings and zero traffic, which is indistinguishable
from a healthy broker. Notifier alarms on the *absence* of a report, so the
arrival is the signal. `status` is the probe's own judgement (`finding_count > 0`
maps to `alert`) because the alternative is notifier learning this repo's
taxonomy.

**Configure it with two variables, and never a host or a port:**

| Variable | Where |
|---|---|
| `NOTIFIER_MONITOR_ID` | `/etc/broker/notifier.env` |
| `NOTIFIER_API_KEY` | `/etc/broker/notifier.env` |

```bash
sudo install -m 0400 -o root -g root /dev/null /etc/broker/notifier.env
sudo tee /etc/broker/notifier.env >/dev/null <<'ENV'
NOTIFIER_MONITOR_ID=<the monitor's own id - NOT the tenant_id>
NOTIFIER_API_KEY=<key>
ENV
sudo systemctl start broker-bus-health.service
journalctl -u broker-bus-health -n 5 -o cat --no-pager
```

`0400 root:root`, and **not** `/etc/broker/.env`. That file is `0640
root:exedev`, so the unit's own `User=` can read it at any time, running or not.
systemd reads an `EnvironmentFile` as root *before* dropping privileges, so a
root-only file still reaches the process while staying unreadable to the
account. The process seeing its own environment is unavoidable; a second copy in
a file `exedev` can `cat` is not.

Both unset is the supported default and costs nothing - the `EnvironmentFile`
line carries a leading `-`. One set without the other is a config mistake and is
logged at ERROR, because the failure it would otherwise produce is silence.

**The host and port are not configurable, deliberately.** `notifier:9001` is
`notifier_dev` running against `DEV_DATABASE_URL`, the tailnet policy currently
admits it alongside `:9000`, and its `/health` is byte-identical to production's
- same status, same build - so a wrong port cannot be caught by the obvious
check. Since this monitor alarms on the absence of check-ins, a one-character
typo would not degrade it but **invert** it: reports land in the dev database,
the production monitor receives nothing, and it declares a healthy broker dead.
So the operator supplies a monitor id and the base URL is a constant in
`src/broker/bus_health.py`. Same move as `databases 1` against the db15 vector -
make the wrong destination unnameable rather than merely discouraged.

A failed check-in is a WARN line and never a failed unit. The probe is WARN-only
by contract, and a monitoring unit that starts failing on its own transport
trains an operator to ignore it.

### Two things to check when a check-in fails

**`HTTP 404` means the monitor id is wrong, and the likeliest cause is a
`tenant_id`.** Both are ULIDs of the same shape and both appear in the monitor's
own JSON, so they are easy to transpose. Read it back and compare:

```bash
sudo sh -c 'set -a; . /etc/broker/notifier.env; set +a
  curl -s -H "X-API-Key: $NOTIFIER_API_KEY" \
    http://notifier:9000/api/v1/monitors/$NOTIFIER_MONITOR_ID' | python3 -m json.tool
```

`last_checkin_at`, `last_status` and `last_variables` should reflect the most
recent tick. This is the only end-to-end confirmation - a clean journald line
proves the POST returned 2xx, not that notifier recorded anything useful.

**`enabled: false` means the dead-man's timer is not running.** Notifier's
`sweep_monitors` selects `enabled is True` only, so a disabled monitor records
check-ins and reports state while alarming on nothing when they stop. Its
check-in route does *not* gate on the flag, so `status: alert` still dispatches
- which makes the failure asymmetric and easy to miss: findings reach a person,
silence does not. Silence is the half this probe exists for.

## The backup (broker#4)

`broker-backup.service` copies `dump.rdb`, verifies the copy with
`redis-check-rdb`, gzips it, and creates
`gs://co-gcs-broker-backup/co-broker/<snapshot time>.rdb.gz` with
`if_generation_match=0` - never an overwrite, and the identity holds no
`delete`, so the bucket's 30-day lifecycle rule is the only thing that removes
a snapshot. It holds **no Redis credential**: the server rewrites `dump.rdb`
atomically at its `save` points, so the file is the interface.

The probe reads the job's state file (`/var/lib/broker-backup/state.json`)
every tick and reports, in order of precedence: never completed a run; the last
attempt failed (with the error); the last success older than three hours; the
snapshot itself older than three hours while the job succeeds - which is Redis's
`save` points having stopped, and is also reported directly as a `persistence`
finding from `rdb_last_bgsave_status`. A missing state *directory* means the
unit is not installed on this node and is not a finding.

**A restored snapshot is ignored under `appendonly yes` unless it is staged as
the AOF base** - `python -m src.broker.restore` does that, and
[`../docs/RECOVERY.md`](../docs/RECOVERY.md) is the runbook around it, the
rehearsal record, and the provisioning commands for the bucket and its writer.

## DLQ evidence

The probe writes captured DLQ entries to
`/var/lib/broker-bus-health/dlq-evidence/<topic>/<last-id>.json`, inside the
unit's `StateDirectory`. That is the "back up" step of the drain runbook, done
on the tick that first sees a non-resting queue rather than by an operator
under time pressure. Delete a topic's dumps once its triage is finished; the
high-water mark is read back from the filenames, so deleting them correctly
re-arms capture rather than leaving a gap. See
[`../docs/STREAMS.md`](../docs/STREAMS.md), *Who drains a DLQ*.
