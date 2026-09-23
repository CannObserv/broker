# deploy/

Everything the broker node runs, tracked. Four of these are Redis's, two are
the health probe's, two are the backup's, and five protect the node's memory.

| File | Installs as | Purpose |
|---|---|---|
| `redis.conf.broker` | appended to `/etc/redis/redis.conf` | The tuning: bind, `requirepass`, AOF `everysec`, `maxmemory-policy noeviction`, explicit `maxmemory`. `__REQUIREPASS__` is substituted at install time from `/etc/redis/broker-password` |
| `redis-server.service.d/broker.conf` | `/etc/systemd/system/redis-server.service.d/broker.conf` | Unit **ordering only**: `After=tailscaled.service`, widened restart limits, and the `ExecStartPre=+` wait |
| `wait-for-tailnet-addr.sh` | `/usr/local/sbin/` | R1's boot-race insurance. Probes `/proc/net/fib_trie`, never `ip addr` |
| `broker-bus-health.service` | `/etc/systemd/system/` | One WARN-only health tick: memory and eviction policy, per-stream length, last-entry age, per-group pending count and undelivered age, DLQ depth + evidence capture + `entries-added` continuity, disk, persistence status, backup freshness. Never blocks anything |
| `broker-bus-health.timer` | `/etc/systemd/system/` | Runs the probe every 10 min |
| `redis-acl.conf` + `render-acl.sh` | `/etc/redis/users.acl` | Per-service ACL users (D3, broker#2). Live since broker#5's window on 2026-09-10; the shared `default` password was retired the same day. Changes are made live with `ACL SETUSER` + `ACL SAVE` as `acladmin`, then mirrored here - `aclfile` is immutable, so the file itself is only re-read at a restart |
| `broker-backup.service` | `/etc/systemd/system/` | Ships `dump.rdb` to `gs://co-gcs-broker-backup`, verified and create-only, holding **no Redis credential**; root confined to read-only everything but its state directory (broker#4). See [`../docs/RECOVERY.md`](../docs/RECOVERY.md) |
| `broker-backup.timer` | `/etc/systemd/system/` | Hourly, `Persistent=true` |
| `sysctl.d/60-broker-memory.conf` | `/etc/sysctl.d/` | `vm.min_free_kbytes` 64 MiB: the reserve atomic allocations draw on (broker#21) |
| `system.slice.d/broker-memory.conf` | `/etc/systemd/system/system.slice.d/` | `MemoryLow=` for the slice - without it the two below protect nothing, because this node has no `memory_recursiveprot` |
| `redis-server.service.d/memory.conf` | `/etc/systemd/system/redis-server.service.d/` | `MemoryLow=1G`, twice `maxmemory`: protection from reclaim, not a limit. `OOMScoreAdjust=-900`, below dev tooling. `broker.conf` beside it stays ordering-only |
| `tailscaled.service.d/memory.conf` | `/etc/systemd/system/tailscaled.service.d/` | `MemoryLow=128M` and `OOMScoreAdjust=-900` for the network path |
| `earlyoom.default` | `/etc/default/earlyoom` | A per-process OOM killer weighted against the bus and the way in (`--avoid`, -300) and toward dev tooling (`--prefer`) - a ranking, not an exclusion |

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
ride the same one and did not - see the incident in `docs/RESTART-WINDOW.md`.
It needs a `BGREWRITEAOF` first, which broker#4's backup made acceptable the
same day; the remaining window is a matter of scheduling.

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
   See `docs/ACL-CUTOVER.md` step 4.

Steps 2 to 4 are reversible and need no restart, which is the point of putting
the irreversible-feeling step last. And all three participants classify `NOPERM`
as transient, so a grant that is too narrow degrades to a backing-off publisher
rather than dead-lettering valid events - bought deliberately (archiver#193
Phase 1, replicator#82) and the reason a wrong rule here is recoverable.

## Changing a grant, and the half of it that is not a command

The mechanism is deliberately live. An ACL that can only be changed by a
cohort-wide restart is an ACL nobody will dare tighten, which is the whole
reason the grants are in a separate `aclfile` rather than in `redis.conf`:

```bash
pw()   { sudo sed -n "s/^__${1}_PW__=//p" /etc/redis/broker-acl-passwords; }
# $1 is the ACL user. The password reaches redis-cli's ENVIRONMENT and never its
# argv - CannObserv/broker#47, and the reason no line here says `-u`.
rcli() { local u=$1; shift
         REDISCLI_AUTH="$(pw "${u^^}")" redis-cli --user "$u" -h 127.0.0.1 -p 6379 "$@"; }

rcli acladmin ACL SETUSER <user> <rule>   # applies now
rcli acladmin ACL SAVE                    # -> /etc/redis/users.acl
# then mirror the same rule into deploy/redis-acl.conf, with its reason, and commit
```

**Credentials never reach a command line.** `REDISCLI_AUTH` is the whole of it:
a `redis://user:<plaintext>@host` URL lands the secret in `argv`, readable from
`ps` by any local user for the life of the call, in root's shell history, and -
under `sudo` - in journald, which is what cost CannObserv/archiver#251 a
rotation. Verified on a scratch 7.0.15: `REDISCLI_AUTH` authenticates
identically and emits no "may not be safe" warning, so a `--no-auth-warning` in
a diff here is itself the tell that the old form is back.
`tests/deploy/test_runbook_credentials.py` fails on either spelling.

**`ACL SETUSER` ADDS; it does not replace.** `-xadd`, `clearselectors` and the
like are how a rule comes *off*. The obvious shortcut - `ACL SETUSER <user>
reset <the whole line>` - works and costs something that is not obvious: `reset`
sets an explicit `sanitize-payload` flag that a user loaded from an aclfile does
not carry, `ACL SAVE` then writes it into `/etc/redis/users.acl`, and
`test_live_acl_matches_tracked_acl.py` goes red on a difference that is
behaviourally nil (it is the default, stated; only `RESTORE` reads it, which no
user here holds). **There is no keyword that clears it** - verified on a scratch
7.0.15 - so the repair is `ACL DELUSER` and recreate, which terminates every
connection authenticated as those users. Done during broker#14, pipelined into
one `redis-cli` invocation so the gap was sub-millisecond: all three services
reconnected, `ACL LOG` recorded no denial, and no group lost its position. Cheap
here, but a disconnect is not nothing - prefer the delta rules, and keep `reset`
for the case where the whole line is being re-declared anyway.

**A password comes off by digest, and the plaintext is the fallback.** The same
"ADDS, does not replace" applies to `>secret`, so a rotation is two rules in one
`SETUSER` - and `#<64-hex>` / `!<64-hex>` are the forms of both that put nothing
on a command line. Verified on a scratch 7.0.15: `!<hash>` removes exactly that
password and keeps the others, `#<hash>` adds one that then authenticates, and
neither disturbs an `off` flag. The digest is
`printf %s "$pw" | sha256sum | cut -d' ' -f1` over the value in
`/etc/redis/broker-acl-passwords` - the `cut` is not optional, `sha256sum`
prints `<hash>  -` and redis refuses the trailing filename:

```bash
rcli acladmin ACL SETUSER <user> "#<new-sha256>" "!<old-sha256>"   # rotate, no plaintext
rcli acladmin ACL SAVE
rcli brokeradmin ACL GETUSER <user>        # -> exactly one hash, and it is the new one
```

`>newsecret <oldsecret` is the plaintext spelling of the same pair. It is the
fallback, for the case where the digest is not to hand - not the default, and
on a rotation it is often not even available: a hash-only handoff leaves this
node holding no plaintext for that user at all (CannObserv/archiver#251).

**The third line is the one that gets skipped.** Until broker#11 nothing
checked it, and it rested on someone remembering four times: eleven corrections
during the broker#2 cutover, step 4 of the restart window, and broker#9's key
pattern. `tests/deploy/test_live_acl_matches_tracked_acl.py` now compares every
tracked user's rules against the running broker, loading the tracked file into
the throwaway `redis-server` the parse tests already spawn - a byte comparison
is out, because `ACL SAVE` rewrites in Redis's canonical form (`#<sha256>` for
passwords, the rule string reordered, `-@admin` folded into `-@dangerous`).
This is the ACL's counterpart to reading `maxmemory` back through `CONFIG GET`.

It costs `brokeradmin` the read-only `+acl|getuser`. Two limits, both
deliberate and both recorded on the grant itself:

- **It cannot enumerate.** `+acl|getuser` permits `ACL GETUSER` alone -
  `ACL USERS`, `ACL LIST`, `ACL WHOAMI` and `ACL CAT` are each denied
  separately - so a user the tracked file never declared is invisible to a
  per-name lookup. An untracked identity that is actually *in use* is still
  caught, by the `user=` field in `CLIENT LIST`.
- **It cannot see a grant that is wrong in both places.** Both real ACL bugs
  in this epic were exactly that. Where a grant has a derivable source, prefer
  a test over the source - `test_replicator_can_name_every_dedupe_namespace`
  derives the dedupe namespaces from co-core's command taxonomy, and that is
  what caught broker#9.

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

# Memory protection (broker#21). All live - nothing on the bus restarts:
# daemon-reload applies MemoryLow= to running units (checked on a scratch unit).
sudo install -m 0644 -D deploy/sysctl.d/60-broker-memory.conf /etc/sysctl.d/60-broker-memory.conf
sudo sysctl -p /etc/sysctl.d/60-broker-memory.conf
for d in system.slice.d/broker-memory.conf redis-server.service.d/memory.conf tailscaled.service.d/memory.conf; do
    sudo install -m 0644 -D "deploy/$d" "/etc/systemd/system/$d"
done
sudo systemctl daemon-reload
# OOMScoreAdjust= applies at exec; take it live rather than restarting the bus.
for u in redis-server tailscaled; do
    sudo choom -n -900 -p "$(systemctl show -p MainPID --value "$u")"
done
sudo apt-get install -y earlyoom
sudo install -m 0644 deploy/earlyoom.default /etc/default/earlyoom
sudo systemctl restart earlyoom
```

Then verify against the running broker rather than against the files:

```bash
set -a; . /etc/broker/.env; set +a
uv run pytest tests/deploy            # every skip becomes a real assertion here
```

**Restarting `redis-server` is a cohort-wide event.** All three services connect
to this instance, so anything needing a restart waits for a window rather than
being applied in passing.

## Memory protection (broker#21)

On 2026-09-16 the then-2 GB node ran out of memory under dev tooling (broker#17).
Nothing was OOM-killed. The kernel failed **atomic** allocations in `kswapd0`,
`tailscaled` and `ksoftirqd`, so the bus's network path degraded while every
process stayed alive, and the probe was silent for most of an hour. The VM is 8
GB now; these defences are the part that does not depend on size, and three
things about this node shaped them:

- **Dev tooling runs in `init.scope`, not `user.slice`.** exe.dev's agent starts
  sessions itself, so VSCode Server and Claude Code are outside any slice
  systemd can cap. A `user.slice` `MemoryMax=` contains nothing here - which is
  why the containment is a per-process killer (earlyoom) plus protection for
  what matters (`MemoryLow=`), not a limit on what does not.
- **cgroup2 has no `memory_recursiveprot`.** A service's protection is capped by
  its slice's, and `system.slice` defaults to 0. Check
  `/sys/fs/cgroup/system.slice/redis-server.service/memory.low`, not
  `systemctl show`, which reports the configured value either way.
- **Everything exe.dev starts is at `oom_score_adj` -1000.** `exe-init` and
  `sshd` run there, and every session process inherits it, so dev tooling reads
  `oom_score` 0. earlyoom 1.7 floors a `--prefer` match at 300 and an unmatched
  one (`claude`, anything run from a shell) stays at 0, while a unit at the
  default adj 0 reads ~667 and `--avoid` only takes it to ~367. As first
  installed, earlyoom's dry run would have killed tailscaled and redis-server
  before any of it. Both units now run at -900: below every dev process, above
  -1000 so the kernel's own OOM killer - which cannot touch dev tooling at all -
  still has a restartable last resort instead of a panic that `kernel.panic = 0`
  turns into a hung node. What still ranks **above** dev tooling is small
  adj-0 system daemons (polkitd, cron, logind): collateral earlyoom works through
  first, freeing little. Check the order with
  `earlyoom --dryrun -d -m 99,99 -s 100,100 <the regexes>`, which kills nothing.
- **earlyoom's regexes are unquoted.** The unit runs `earlyoom $EARLYOOM_ARGS`,
  which systemd splits on whitespace without interpreting quotes; the package's
  own quoted example would never match. `journalctl -u earlyoom -b` prints both
  regexes at start - read them there.

**`vm.overcommit_memory = 1`, and the warning it answers (broker#26).** Redis
logs `WARNING Memory overcommit must be enabled!` at every start on any value
but 1 - `checkOvercommit`, `src/syscheck.c` - and step 1d of the restart runbook
walks a reader straight into it. On this kernel that warning is **inert**, which
is the whole reason the setting is cheap: measured with untouched private
anonymous mappings on 6.12.93, mode 0 granted 7.29 GiB (more than
`MemAvailable`, ~6.3 GiB) and refused only 8.75 GiB (`MemTotal` + 1 GiB). Mode 0
here does not consider free memory at all, there is no swap, and a fork commits
at most redis's own private memory, which `maxmemory` bounds - so it could not
have refused a save or an AOF rewrite at any dataset size the cap allows. The
jemalloc issue the warning cites is about mode **2**; jemalloc's own
`os_overcommits_proc` treats 0 and 1 alike.

What the change costs is where a too-large allocation fails: at first touch,
as an earlyoom or kernel OOM kill in the order above, rather than up front with
`ENOMEM`. That only reaches a process asking for more than the whole machine at
once. **The running server keeps its warning** - it is logged at start, so the
log goes quiet at the next restart, not now. And **re-measure after a kernel
change**: "mode 0 ignores free memory" is this kernel's behaviour, not a
guarantee.

**One VSCode Server build at a time.** After the 2026-09-16 reboot two builds
ran side by side, about 300 MiB of dev baseline for nothing. When the client
updates, close the old window rather than leaving both connected.

`tests/deploy/test_memory_protection.py` pins all of it: the reserve's floor,
overcommit at the value redis asks for, redis's protection against twice the
tracked cap, the slice covering its children, the bus's score below dev
tooling's, both regexes against the real process names, and - on this node -
installed parity, every tracked sysctl key read back from `/proc/sys`, and
earlyoom running and enabled.

## Changing the cap

Prefer applying it live - no restart, no dropped client connections - then
persist it in both places:

```bash
# `pw` and `rcli` as defined under "Changing a grant" above.
rcli default CONFIG SET maxmemory <value>   # applies now; window-only, see below
sudo sed -i 's/^maxmemory .*/maxmemory <value>/' /etc/redis/redis.conf
sed -i 's/^maxmemory .*/maxmemory <value>/' deploy/redis.conf.broker
```

**`CONFIG SET` is `default`'s, not `brokeradmin`'s.** This line handed the probe's
own `BROKER_REDIS_URL` straight to `redis-cli` until CannObserv/broker#47 swept
the credential out of `argv`, and that URL is `brokeradmin`, which holds
`+config|get` and nothing else - so it has been a `NOPERM` since the 2026-09-10
cutover and nothing said so. Raising the cap therefore opens a window:
`ACL SETUSER default on` as `acladmin`, the `CONFIG SET`, then `off` again -
[`../docs/ACL-CUTOVER.md`](../docs/ACL-CUTOVER.md) step 4, both directions. The
restart the section's first sentence saves is still saved; only the identity
was wrong.

Pass the value **exactly as the config file spells it** - `CONFIG SET` accepts
the same unit suffixes, so there is no byte conversion to get wrong.

**Raising it moves two more files.** `redis-server.service.d/memory.conf` holds
`MemoryLow=` at twice the cap, and `system.slice.d/broker-memory.conf` at least
that plus tailscaled's; `test_memory_protection.py` fails until both follow.
Both apply live:

```bash
sudo install -m 0644 deploy/redis-server.service.d/memory.conf /etc/systemd/system/redis-server.service.d/memory.conf
sudo install -m 0644 deploy/system.slice.d/broker-memory.conf /etc/systemd/system/system.slice.d/broker-memory.conf
sudo systemctl daemon-reload                     # no restart; read back /sys/fs/cgroup/.../memory.low
```

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
- `GOOGLE_APPLICATION_CREDENTIALS` - the read-only `co-pypi-reader` key. **It
  is the operator's, not the probe's:** it authenticates the wheelhouse sync in
  AGENTS.md, and it is here so that sync is one `source` away. The unit unsets
  it. `uv run` resolves `co-core` from `./.wheelhouse`, a local directory, and
  needs no credential (CannObserv/broker#37).

**No unit syncs the wheelhouse.** CI syncs on every run; on the node the sync is
manual, and it is due before any `uv sync` or `uv run` that follows a `co-core`
pin change. Miss it and the probe's `ExecStart` fails with a resolution error.

The probe holds **no** database credential, joins **no** consumer group, and
inherits no variable from `/etc/broker/.env` that it does not read. All three
are asserted by `tests/deploy/test_bus_health_units.py`; the last one against
the live file, so a variable added there on the node fails it.

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
attempt failed (with the error); the last success older than three hours. The
case where the job succeeds every hour while shipping the same stale file is
judged from the server instead - a `persistence` finding when `INFO
persistence` shows changes left unsaved for over three hours, or any of its
`*_status` fields not `ok` - because only the server can tell an idle broker
from one whose save points have stopped. A missing state *directory* means the
unit is not installed on this node and is not a finding. After a failed run the
state file's `outcome` is `failed`; the last success stays on record beside it,
which is what the staleness rule reads.

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

The **disposal** step is `XDEL <queue> <id>`, per entry, and since broker#12
each named drainer can do it for its own queues without an operator - a
selector, `(+xdel ~<its own>.dlq)`, which cannot reach the stream the queue
copies from. `brokeradmin` holds `(+xdel +xtrim ~*.dlq)` as the backstop for a
queue nobody claimed - `+xtrim` joined that selector in broker#14, having been a
root grant riding `~*`, where the one identity that can see every stream on the
instance could also cap any of them. Before that the only tool was `XTRIM MAXLEN 0`, which empties
the queue: on one that has reached 110 entries, removing a single triaged frame
took the other 109 with it.

**Capture and disposal have a 10-minute gap between them, and what closes it is
a counter rather than a faster tick** (broker#13). Capture happens on a probe
tick, so an entry written and deleted inside one interval leaves no dump - and
depth cannot see it either, because the queue is empty at both observations. The
probe therefore records each queue's `entries-added`, which is monotonic and
survives `XDEL`: a counter that advanced by more than the depth accounts for
means entries passed through unseen, and the probe says so (`dlq-unobserved`).
It reports a **floor**, `added - <last tick's added> - depth`, not a count -
depth can include entries captured on an earlier tick, so subtracting it can
only understate. The baseline is the `@entries-added/<queue>` key in
`StateDirectory=broker-bus-health`; no baseline (first tick after a deploy, or a
lost state file) means no comparison and no finding.

The payloads are still gone; what the check buys is that their *existence*
cannot be. The queue's **identity** going is a different and worse finding, and
it is reported under the same name the declared streams use, `stream-reset` -
one condition should not need two names in an alert rule. It arrives two ways:
`entries-added` going backwards, which only happens when the stream was deleted
and recreated, and the key being **absent** from a tick's scan after a tick that
had it. Both mean any evidence dump still on disk describes a queue that no
longer exists. Disposal never does this: `XDEL` and `XTRIM MAXLEN 0` both leave
the key behind.
