# deploy/

Everything the broker node runs, tracked. Four of these are Redis's, two are
the health probe's, two are the backup's, two are the drift check's, and five
protect the node's memory.

| File | Installs as | Purpose |
|---|---|---|
| `redis.conf.broker` | appended to `/etc/redis/redis.conf` | The tuning: bind, `requirepass`, AOF `everysec`, `maxmemory-policy noeviction`, explicit `maxmemory`. `__REQUIREPASS__` is substituted at install time with a value minted there and kept nowhere - nobody's password since broker#52 |
| `redis-server.service.d/broker.conf` | `/etc/systemd/system/redis-server.service.d/broker.conf` | Unit **ordering only**: `After=tailscaled.service`, widened restart limits, and the `ExecStartPre=+` wait |
| `wait-for-tailnet-addr.sh` | `/usr/local/sbin/` | R1's boot-race insurance. Probes `/proc/net/fib_trie`, never `ip addr` |
| `broker-bus-health.service` | `/etc/systemd/system/` | One WARN-only health tick: memory and eviction policy, per-stream length, last-entry age, per-group pending count and undelivered age, DLQ depth + evidence capture + `entries-added` continuity, disk, persistence status, backup freshness. Never blocks anything. Runs `/srv/broker/live`, a release; a deploy installs a changed copy (#107) |
| `broker-bus-health.timer` | `/etc/systemd/system/` | Runs the probe every 10 min |
| `redis-acl.conf` + `render-acl.sh` | `/etc/redis/users.acl` | Per-service ACL users (D3, broker#2). Live since broker#5's window on 2026-09-10; the shared `default` password was retired the same day, and `default` made a tombstone on 2026-09-29 (broker#52). Changes are made live with `ACL SETUSER` + `ACL SAVE` as `acladmin`, then mirrored here - `aclfile` is immutable, so the file itself is only re-read at a restart |
| `broker-backup.service` | `/etc/systemd/system/` | Ships `dump.rdb` to `gs://co-gcs-broker-backup`, verified and create-only, holding **no Redis credential** (broker#4). Ships the service users' ACL digests beside each snapshot, refusing any plaintext line (broker#72). Runs `/srv/broker/live`, a release, with `/home` hidden; a deploy installs a changed copy (#108). Runs as `broker-backup`, a static user with no capability: root only copies `dump.rdb` in, and the key and passwords file arrive as credentials (#109). See [`../docs/RECOVERY.md`](../docs/RECOVERY.md) |
| `broker-backup.timer` | `/etc/systemd/system/` | Hourly, `Persistent=true` |
| `broker-drift.service` | `/etc/systemd/system/` | Does live lag `origin/main` in code that runs? Asks GitHub, checks in to co-status's `co-broker-drift` once a run (#110). `DynamicUser=`, the co-status key as a `LoadCredential=`, **no Redis credential** and no `.env`. Runs `/srv/broker/live`; see *The drift check* below |
| `broker-drift.timer` | `/etc/systemd/system/` | Hourly, 5 min after boot. Enabled by the deploy that first installs it, once it verifies |
| `sysctl.d/60-broker-memory.conf` | `/etc/sysctl.d/` | `vm.min_free_kbytes` 64 MiB: the reserve atomic allocations draw on (broker#21) |
| `system.slice.d/broker-memory.conf` | `/etc/systemd/system/system.slice.d/` | `MemoryLow=` for the slice - without it the two below protect nothing, because this node has no `memory_recursiveprot` |
| `redis-server.service.d/memory.conf` | `/etc/systemd/system/redis-server.service.d/` | `MemoryLow=1G`, twice `maxmemory`: protection from reclaim, not a limit. `OOMScoreAdjust=-900`: out of earlyoom's reach, and the kernel's last resort after dev tooling and the small daemons. `broker.conf` beside it stays ordering-only |
| `tailscaled.service.d/memory.conf` | `/etc/systemd/system/tailscaled.service.d/` | `MemoryLow=128M` and `OOMScoreAdjust=-900` for the network path |
| `earlyoom.default` | `/etc/default/earlyoom` | A per-process OOM killer weighted against the bus and the way in (`--avoid`, -300). **Installed and disabled**: broker#58 disabled it when dev tooling sat at exe.dev's -1000, out of its reach, and it stays disabled now that broker#71 put sessions at 0, where the kernel takes them before the bus (operator's decision, 2026-10-06). Configured, so turning it on is one command |
| `needrestart.conf.d/broker.conf` | `/etc/needrestart/conf.d/` | `$nrconf{restart} = 'l'`: needrestart **lists** restarts after an apt run, never performs them. Stock Ubuntu mode restarts automatically, so a `libc6` security update would restart `redis-server` outside a window (broker#65) |
| `apt/preferences.d/tailscale.pref` | `/etc/apt/preferences.d/` | Scopes `pkgs.tailscale.com`, which patching-hosts' maintenance lane follows: `Package: *` at 100, below Ubuntu's 500, and `tailscale` + `tailscale-archive-keyring` at 600. Unpinned, the site ties Ubuntu and could replace an Ubuntu package (broker#67) |

`tests/deploy/` asserts all of it: the installed copies match these files, and
`test_live_broker_matches_tracked_config.py` reads the running config back
through `CONFIG GET` so a `CONFIG SET` that no file records still gets caught.
Off the node they skip, so CI and dev clones pass. "Off the node" is
`on_broker_node()` in `tests/deploy/conftest.py`, never the absence of the file
under test: on the node a missing copy **fails**, so deleting a drop-in cannot
quiet its own guard (broker#79).

The `broker-*` units above reach the node through `../scripts/deploy.sh`, which
installs a changed copy from the release it switches to: *Deploying*, below.

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
  down with it. It becomes a required step of any upgrade to >= 7.2 instead,
  for the consumers: not `brokeradmin`, whose probe sends none since #102.

## Installing the ACL users

```bash
# passwords file, 0400 root:root, one line per placeholder, every one a digest
# since broker#52 - nothing on this node holds a Redis password in plaintext:
#   __ARCHIVER_PW_SHA256__=<64-hex>       one only its service holds (broker#49)
#   __ACLADMIN_PW_SHA256__=<64-hex>       one this node authenticates as, from
#                                         /etc/credstore.encrypted/broker-acladmin
# `__X_PW__=<plaintext>` still renders - for the interval a new service's
# password waits to be handed off - and the live suite fails while one exists.
# Every user renders as #<sha256>; the output carries no plaintext.
sudo deploy/render-acl.sh /etc/redis/broker-acl-passwords \
    | sudo install -m 0640 -o root -g redis /dev/stdin /etc/redis/users.acl
# then, in the restart window, add `aclfile /etc/redis/users.acl` to redis.conf
```

### The cutover order, and why only step 1 needs the window

**`default` is the sharp edge, in both directions.** Omitting it from an aclfile
silently makes it `nopass` - an anonymous client is served while `CONFIG GET
requirepass` still returns the password, which is R2 arriving as a side effect
of turning on the mechanism meant to prevent it. And setting it `off` at the
first load locks out every service still on `default:`, because that restart
lands before any of them has moved onto its own credential. So on this cluster
the file went in reading `on` with the then-current password, retiring it was
the last step rather than the first, and **the tracked file now says `off`** -
and since broker#52 grants no read command and no operator command, behind the
digest of a value nobody kept. Its one grant, writes on keys, is the AOF
replay's (broker#101). It still carries a password, because `off` is a flag that
leaves the password set intact, and a line with none would be `nopass` the
moment anything flipped it `on`. A new cluster repeats the order: `on` with the
shared grants for the first load, migrate, then the tombstone live.

1. **In the window** - install `/etc/redis/users.acl`, add `aclfile` to
   `redis.conf` (and `databases 1` only after a `BGREWRITEAOF` - see the
   runbook's step 1a-bis), restart. Nothing changes for any service:
   every URL still says `default:` and `default` still has the same password.
   Confirm the tailnet wait fired, `NRestarts=0`, and that an anonymous
   `redis-cli` is refused.
2. **Rolling, no window** - flip each service's URL to its own credential, one
   at a time, verifying each before the next.
3. **Rolling** - flip the probe's `BROKER_REDIS_URL` to `brokeradmin` (the
   user alone; the password is the unit's encrypted credential since
   broker#52).
4. **Live** - `ACL SETUSER default off` then `ACL SAVE`, run as **`acladmin`**,
   the operator user that exists because `+acl` would otherwise belong to
   nobody once `default` is off, freezing every grant on the broker
   permanently. Done 2026-09-10; `default` became a tombstone, and the
   window's commands `acladmin`'s, on 2026-09-29 (broker#52). Widening any
   grant afterwards is done the same way. See `docs/ACL-CUTOVER.md` step 4.

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
# $1 is the ACL user - acladmin, or the probe's brokeradmin. Each password is a
# systemd-creds credential encrypted to this node, decrypted by root on demand
# with no prompt (broker#52). It reaches redis-cli's ENVIRONMENT and never its
# argv - CannObserv/broker#47, and the reason no line here says `-u`.
cred() { sudo -n systemd-creds decrypt --name="broker-$1" "/etc/credstore.encrypted/broker-$1" -; }
# Refuses rather than connecting with an empty password: a failed decrypt would
# otherwise be a WRONGPASS naming the user in ACL LOG, the evidence log.
rcli() { local u=$1 p; shift
         p="$(cred "$u")" && [ -n "$p" ] || { echo "rcli: no credential for $u" >&2; return 1; }
         REDISCLI_AUTH="$p" redis-cli --user "$u" -h localhost -p 6379 "$@"; }

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
`SETUSER` - and `#<64-hex>` / `!<64-hex>` are the forms of both that put no
*secret* on a command line. The digest is on it; the digest is not the
credential, which is the whole of the distinction. Verified on a scratch 7.0.15: `!<hash>` removes exactly that
password and keeps the others, `#<hash>` adds one that then authenticates, and
neither disturbs an `off` flag. The digest is
`printf %s "$pw" | sha256sum | cut -d' ' -f1` over the plaintext - the `cut`
is not optional, `sha256sum` prints `<hash>  -` and redis refuses the trailing
filename. For a user `/etc/redis/broker-acl-passwords` holds by digest
(`__X_PW_SHA256__`), the old digest is that line's value:

```bash
rcli acladmin ACL SETUSER <user> "#<new-sha256>" "!<old-sha256>"   # rotate, no plaintext
rcli acladmin ACL SAVE
rcli acladmin ACL GETUSER <user>           # -> exactly one hash, and it is the new one
```

`>newsecret <oldsecret` is the plaintext spelling of the same pair. It is the
fallback, for the case where the digest is not to hand - not the default, and
on a rotation it is often not even available: a hash-only handoff leaves this
node holding no plaintext for that user at all (CannObserv/archiver#251).

**A rotation is not done until the passwords file says so,** or the next
re-render reinstates the old credential. For a **service** user the new line is
its digest, and a digest may sit on a command line; the old line's value,
plaintext or digest, is matched by `.*` and never spelled:

```bash
sudo sed -i 's/^__<USER>_PW\(_SHA256\)\?__=.*/__<USER>_PW_SHA256__=<new-sha256>/' \
    /etc/redis/broker-acl-passwords      # GNU sed -i keeps the 0400 root:root
sudo grep -c '^__<USER>_PW_SHA256__=<new-sha256>$' \
    /etc/redis/broker-acl-passwords      # -> 1; sed says nothing when no line matched
```

The two **node** users - `acladmin`, `brokeradmin` - have a digest line too
since broker#52, and one more place their password lives: the encrypted
credential `rcli` and the probe authenticate from. Rotating one is four checked steps,
[docs/NODE-CREDENTIALS.md](../docs/NODE-CREDENTIALS.md). `default`'s
line is the digest of a value nobody kept, and there is nothing to rotate.

`test_the_nodes_passwords_file_renders_the_credentials_that_are_live` renders
the node's file under `sudo -n` and compares every user's digest with
`ACL GETUSER`, so a forgotten line fails the suite rather than waiting for a
rebuild (CannObserv/broker#49). The service users `archiver`, `watcher` and
`replicator` are held by digest alone since 2026-09-23; nothing on this node
needs their plaintext. `processor` never had a plaintext here: its
handoff was hash-only (CannObserv/broker#75), and `provisioner`'s is
(CannObserv/broker#78) - the shape a service minted since
the backup refuses plaintext lines takes - [docs/ACL-CUTOVER.md](../docs/ACL-CUTOVER.md),
step 1. The three operator lines joined on 2026-09-29 (broker#52).

**The third line is the one that gets skipped.** Until broker#11 nothing
checked it, and it rested on someone remembering four times: eleven corrections
during the broker#2 cutover, step 4 of the restart window, and broker#9's key
pattern. `tests/deploy/test_live_acl_matches_tracked_acl.py` now compares every
tracked user's rules against the running broker, loading the tracked file into
the throwaway `redis-server` the parse tests already spawn - a byte comparison
is out, because `ACL SAVE` rewrites in Redis's canonical form (`#<sha256>` for
passwords, the rule string reordered, `-@admin` folded into `-@dangerous`).
This is the ACL's counterpart to reading `maxmemory` back through `CONFIG GET`.

**The second line gets skipped too, and a restart is where it shows.** Memory
is not the file: an `ACL SETUSER` mirrored but never saved passes the
comparison above, then reverts when the broker restarts. On the node, the same
module reads the broker's `aclfile` through `sudo -n` - digests and rules only,
nothing `ACL SAVE` writes is a credential - loads it into a second throwaway
server, and compares every tracked user's rules *and* digests with the live
ACL (CannObserv/broker#54).

It runs as `acladmin` since broker#52, the operator's credential decrypted
through `sudo -n`; until then it cost the probe's `brokeradmin` a
`+acl|getuser` that put every digest behind a credential readable without sudo.
One limit, deliberate:

- **It enumerates now**, which the probe's grant never could: `ACL USERS`
  catches a user the tracked file never declared, idle or not. `CLIENT LIST`'s
  `user=` and the saved file each still catch what they did - who is connected,
  and what a restart would load.
- **It cannot see a grant that is wrong in both places.** Both real ACL bugs
  in this epic were exactly that. Where a grant has a derivable source, prefer
  a test over the source - `test_replicator_can_name_every_dedupe_namespace`
  derives the dedupe namespaces from co-core's command taxonomy, and that is
  what caught broker#9.

### Removing a user

Three writes, in this order, and the passwords line is the one that gets
forgotten: render-acl.sh ignores a line whose placeholder the tracked file no
longer carries, so nothing fails while the line sits there - except
`test_the_nodes_passwords_file_names_only_declared_users` - and the backup ships
it hourly as the digest of a user no rebuild creates.

```bash
rcli acladmin ACL DELUSER <user>          # -> (integer) 1; drops its connections
rcli acladmin ACL SAVE
# then delete its stanza from deploy/redis-acl.conf, commit, and push
sudo sed -i '/^__<USER>_PW\(_SHA256\)\?__=/d' /etc/redis/broker-acl-passwords
sudo grep -c '^__<USER>_PW' /etc/redis/broker-acl-passwords   # -> 0
```

The tracked file goes before the passwords line, not after. A rebuild renders
main's tracked file over the newest shipped digests, and a placeholder with no
line is the one mismatch render-acl.sh refuses. An extra line, the other way
round, renders. `citest` went this way on 2026-10-06 (CannObserv/broker#53).
Rehearse the `DELUSER` on a throwaway server loaded from the tracked file first,
as any grant change.

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
# `requirepass` is minted here and kept nowhere else: it is nobody's password
# (broker#52; the aclfile's `default` line overrides it), and its only job is
# the last-resort restart with `aclfile` commented out. Minted inside the root
# shell, so the value is only ever a shell variable and awk's environment -
# never an argv, including sudo's.
sudo sh -c 'R=$(LC_ALL=C tr -dc "A-Za-z0-9" < /dev/urandom | head -c 40); export R
    awk "/^requirepass / { sub(/__REQUIREPASS__/, ENVIRON[\"R\"]) } 1" deploy/redis.conf.broker >> /etc/redis/redis.conf'
sudo grep -c '^requirepass [A-Za-z0-9]\{40\}$' /etc/redis/redis.conf   # -> 1

# On a rebuild, the passwords file comes first, from the bucket: docs/RECOVERY.md
# step 2 (`restore --digests`, broker#72). It refuses a file that exists, and the
# mint below appends, so the mint must not run before it.
# The two node credentials, minted into systemd's store, and their digest lines
# in the passwords file - before the ACL render: docs/NODE-CREDENTIALS.md,
# "On a new or rebuilt node". /etc/broker/.env's BROKER_REDIS_URL names the user alone.

# Unit ordering + the boot-race wait
sudo install -m 0755 deploy/wait-for-tailnet-addr.sh /usr/local/sbin/
sudo install -m 0644 -D deploy/redis-server.service.d/broker.conf \
    /etc/systemd/system/redis-server.service.d/broker.conf

# The health timer. The unit runs /srv/broker/live (#107), which the first
# deploy makes (Deploying, below): install and enable it BEFORE that deploy.
# The deploy verifies the probe by its next natural tick, which a timer not
# active never gives, so it refuses before the switch while the timer is off
# (#122). Ticks before live exists fail at chdir: a missed check-in, nothing
# worse. Its user comes first: a User= that does not exist fails every tick,
# and the deploy refuses it before the switch (#121, #122).
sudo useradd --system --no-create-home --shell /usr/sbin/nologin broker-bus-health
id broker-bus-health                                 # its own group alone
sudo install -m 0644 deploy/broker-bus-health.service \
    deploy/broker-bus-health.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now broker-bus-health.timer

# The backup (broker#4). The timer is enabled only once the key is in place -
# see docs/RECOVERY.md, "Provisioning the bucket and the writer". The unit runs
# /srv/broker/live (#108): a run before the first deploy fails at chdir. Its
# user, env and key come BEFORE that deploy, which forces one backup run and
# switches back if it fails; it refuses a User= that does not exist (#122).
# backup.env is the restore's, and the #108 unit's a switch back restores; this
# unit names its bucket and takes the key as a credential.
sudo useradd --system --no-create-home --shell /usr/sbin/nologin broker-backup
id broker-backup                                     # its own group alone: never redis
sudo install -m 0644 deploy/broker-backup.service deploy/broker-backup.timer /etc/systemd/system/
sudo install -m 0400 -o root -g root /dev/null /etc/broker/backup.env
printf 'BROKER_BACKUP_BUCKET=co-gcs-broker-backup\nGOOGLE_APPLICATION_CREDENTIALS=/etc/broker/co-broker-backup.json\n' \
    | sudo tee /etc/broker/backup.env >/dev/null
sudo systemctl daemon-reload
sudo systemctl enable --now broker-backup.timer      # after the key exists

# The drift check (#110): its key BEFORE the deploy that installs its units,
# which enables the timer once it verifies, and whose first run checks in at
# once. The copy: The drift check, below.

# then ~/broker/scripts/deploy.sh, detached (Deploying, below)

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
# earlyoom: installed and configured, NOT running (broker#58). The install starts
# it on stock arguments, so disable it in the same step.
sudo apt-get install -y earlyoom
sudo install -m 0644 deploy/earlyoom.default /etc/default/earlyoom
sudo systemctl disable --now earlyoom

# needrestart lists, never restarts (broker#65). Takes effect at the next apt run;
# `sudo needrestart -b -m u` prints "Disabling Ubuntu mode" once it is read.
sudo install -m 0644 -D deploy/needrestart.conf.d/broker.conf /etc/needrestart/conf.d/broker.conf

# Tailscale's origin scoped to the packages it was added for (broker#67).
# `apt-cache policy tailscale` shows 600 once it is read.
sudo install -m 0644 -D deploy/apt/preferences.d/tailscale.pref /etc/apt/preferences.d/tailscale.pref
```

Then verify against the running broker rather than against the files:

```bash
set -a; . /etc/broker/.env; set +a
uv run pytest tests/deploy -rs        # the only skips: test_redis_acl.py's unassigned-DLQ rows
```

**Restarting `redis-server` is a cohort-wide event.** Every service connects
to this instance, so anything needing a restart waits for a window rather than
being applied in passing.

## Deploying (broker#22, #106)

`scripts/deploy.sh` builds a **release**: a root-owned, read-only `git archive`
of one CI-green commit on `origin/main`, with only the wheels `uv.lock` names
and a venv of its own. It then points `/srv/broker/live` at the release. The
design is `docs/plans/2026-10-17-22-release-deploy.md`. A unit runs a release
once its `WorkingDirectory=` is `/srv/broker/live`.

- **The probe has run `live` since #107.** A merge alone no longer reaches it:
  a change to `src/`, `uv.lock`, `pyproject.toml` or its unit is live only once
  deployed.
- **The backup has run `live` since #108**, as `broker-backup` since #109. A
  deploy that moves `live` forces one backup run, so each ships one more
  digests object; the bucket's lifecycle rule ages it out. No unit reads
  `~/broker` now, and its venv is the dev tree's alone.

```bash
# Once, on a new or rebuilt node: root's, and writable by root alone. The
# deploy refuses any other owner or mode.
sudo install -d -o root -g root -m 755 /srv/broker

# From the checkout, as exedev. Never as root, and never from a release, which
# has no .git. It syncs the wheelhouse itself, with the key path in /etc/broker/.env.
# The script and that sync run from the checkout's working tree; the release is
# `git archive` of the commit. So ~/broker stays on a clean main: a branch or an
# edit there changes the deploy's logic, never what it deploys (#111).
~/broker/scripts/deploy.sh                 # origin/main
~/broker/scripts/deploy.sh <sha12>         # an older main commit: how a rollback is spelled
~/broker/scripts/deploy.sh --skip-ci ...   # GitHub cannot answer: an emergency, journaled
~/broker/scripts/deploy.sh --build-only    # build and check releases/<sha12>, and stop (below)
journalctl -t broker-deploy -n 20          # CI passed for ..., live -> <sha12> (was ...), ...
                                           # live is on <sha12>: the verified verdict (#122)
```

**In order:**

1. **CI gate.** The commit's `push` run of `ci.yml` on `main` must be
   `success`, and so must every job in it. GitHub is asked unauthenticated, and
   a pending run is waited for, up to 600 s.
   - A commit behind the tip with no run is refused at once.
   - So is a commit a workflow pushed: GitHub never runs CI for a
     `GITHUB_TOKEN` push, such as the weekly context measurement. The refusal
     names the commit under it.
   - `audit.yml` is not gated.
2. **Build** `releases/<sha12>` at its final path, or reuse it when it is
   complete, root's, and still imports. A release `live` links is never rebuilt.
   Then two refusals before the switch, each journaled (#122):
   - a release service whose `User=` does not exist (`getent passwd`), unless
     it is `DynamicUser=yes`: every run of it would fail;
   - a deploy the probe's tick will verify (step 5) while
     `broker-bus-health.timer` is not active, as when the deploy would add the
     probe's unit before Install enabled its timer. No tick would come.
3. **Switch** `live` with a `sudo` rename.
4. **Install** each `deploy/broker-*.{service,timer}` that differs from its
   installed copy, with one `daemon-reload`. A new unit is installed, never
   enabled, with one exception: `broker-drift.timer` (step 6).
5. **Verify** each unit the deploy changed: one whose unit file it installed,
   or one that runs `live` when `live` moved.
   - The probe is verified by its **next natural tick**, never a forced one,
     because of the two-tick `pending` rule. The tick must exit 0 and log the
     sha12.
   - The backup is verified by one forced run, after any running one finishes.
     The run must report `uploaded` or `unchanged`, with that sha12.
   - The drift check is not verified, on purpose: its hourly run and
     `co-broker-drift` are its check (#110). The deploy does import
     `src.broker.drift` from the release before it switches.
   - Each proof is journaled, then `live is on <sha12>` (#122): a detached
     deploy's outcome is in `journalctl -t broker-deploy`, not only its log.
   - On failure, `live` and the units switch back, and the old build is proved
     the same way.
   - A quiet deploy can take up to 15 minutes waiting for the probe's tick,
     and twice that when it switches back.
   - The deploy reads the tick's summary line from the system journal. `exedev`
     can read that journal because it is in `adm`.
6. **Enable `broker-drift.timer`** if this deploy installed it new, after it
   verified. `--now` starts it, and its first run fires at once
   (`journalctl -u broker-drift -n 5`). A switched-back deploy never enables
   it, and one installed before is never enabled again, so an operator's
   `systemctl disable --now` sticks. A failed enable is a note.
7. **Compare** the host configs, then **prune** to 5 releases plus `live`,
   newest first by last build or deploy. The release `live` replaced is never
   pruned by the deploy that replaced it: it is the rollback target.

**Accepted limit** (#111): **the switch races `daemon-reload`**, about 0.25 s
measured once (#107). A tick in that gap runs the new `live` under the previous
unit file. Only a deploy that changes a unit has the gap. Reloading before the
switch would reverse the race, not remove it, and would put unit changes ahead
of the switch that today's switch back and signal handling assume (#122).

**`--build-only`** (#122, #108's option (b)) runs steps 1 and 2's build and
stops, journaling `built <sha12> (--build-only); live stays on ...`. Nothing is
switched, installed, verified, backed up or pruned, and step 2's refusals are
skipped: they guard a switch. It is how a rebuild gets a release to run
`restore` from before Install makes the units' users (docs/RECOVERY.md, step 2).
- **`live`, and so the drift check, are unchanged.** The drift check reports
  the `REVISION` of the release its unit runs, which is `live`'s.
- **Pruning.** The release counts as built at that moment, so it is the newest,
  and is pruned only once 5 newer releases exist. A deploy of the same commit
  reuses it. The release `live` replaced is spared whatever the count, so no
  number of build-only builds pushes the rollback target out.

**Run it detached when an agent runs it.** A deploy can outlast a 10-minute
tool call: up to 600 s waiting for CI, then up to 900 s for a probe tick, and
that twice when it switches back. Run it in the background and read
`journalctl -t broker-deploy`. A deploy ended by a signal says where it stopped:
- before the switch: nothing switched, exit 1;
- after the switch: what `live` names, `unverified`, in the journal too, exit 4;
- while pruning: still verified, exit 0.

**Exit codes:**

- `0`: deployed and verified.
- `1`: refused before anything switched, or switched back to a build that answers.
- `4`: nothing answers. Read `journalctl -t broker-deploy` and each unit's own journal.

**It never installs a host config, and never restarts `redis-server`.** It
compares the files Install puts in place verbatim, and a difference is a note.
`redis.conf.broker` and `redis-acl.conf` are installed derived, under an
`/etc/redis` that `exedev` cannot read. They are compared with the release
`live` ran before, and a change is a note pointing at the restart window.

## Memory protection (broker#21)

On 2026-09-16 the then-2 GB node ran out of memory under dev tooling (broker#17).
Nothing was OOM-killed. The kernel failed **atomic** allocations in `kswapd0`,
`tailscaled` and `ksoftirqd`, so the bus's network path degraded while every
process stayed alive, and the probe was silent for most of an hour. The VM is 8
GB now; these defences are the part that does not depend on size, and these
things about this node shaped them:

- **Dev tooling runs in `init.scope`, not `user.slice`.** exe.dev's agent starts
  sessions itself, so VSCode Server and Claude Code are outside any slice
  systemd can cap. A `user.slice` `MemoryMax=` contains nothing here - which is
  why the defences are protection for what matters (`MemoryLow=`), a reserve
  (`vm.min_free_kbytes`) and a kill order, not a limit on what does not.
- **cgroup2 has no `memory_recursiveprot`.** A service's protection is capped by
  its slice's, and `system.slice` defaults to 0. Check
  `/sys/fs/cgroup/system.slice/redis-server.service/memory.low`, not
  `systemctl show`, which reports the configured value either way.
- **exe.dev's own processes sit at `oom_score_adj` -1000; its sessions no
  longer do.** `exe-init` and `sshd` run there, and nothing can kill them. Until
  broker#71 every session process inherited it too - VSCode Server, `claude`,
  SocratiCode's `npx` - an `exe-init` bug exe.dev confirmed, so neither killer
  could take the processes that exhausted the node in broker#17. The fixed
  `exe-init` (`14fd603`) went in on 2026-10-06, and sessions read 0. The kernel's
  order measured that day: the user `systemd` and `(sd-pam)` (adj 100, ~734, a
  few MiB), then `claude` (690), `MainThread` (683), `node`, `npm exec`, then the
  small daemons at ~666, and the bus last. `test_live_sessions_are_killed_before_the_bus`
  pins it, and fails if a session reads -1000 again.
- **earlyoom stays installed and disabled.** broker#58 disabled it when it could
  reach only small daemons: anything above -1000 whose `oom_score` stays above 0
  after `--avoid` - the session `dbus-daemon`, `(sd-pam)`, cron, polkitd, logind,
  timesyncd and journald (at -250 it still scores 502, 202 after `--avoid`) -
  tens of MiB, journald's evidence included. Since broker#71 `--prefer` reaches
  dev tooling (a 2026-10-06 dry run ended on `MainThread`), but the kernel
  already takes it before the bus, and it stays off by the operator's decision of
  that date. What it would add is acting at `-m` available instead of at
  exhaustion. Its config is kept current: `systemctl enable --now earlyoom` is
  the whole change.
- **The bus is out of earlyoom's reach and near-last for the kernel.** A unit at
  the default adj 0 reads ~667, which `--avoid` only takes to ~367, so as first
  installed earlyoom's dry run would have killed tailscaled and redis-server.
  Both run at -900 now: `oom_score` under 300, so `--avoid` puts them below the
  badness-0 victim earlyoom starts each scan with, and never picks. Not -1000,
  so the kernel's own OOM killer still has a restartable victim instead of a
  panic that `kernel.panic = 0` turns into a hung node. Its order, measured
  2026-09-24: every small daemon, then `tailscaled` (~72), then `redis-server`
  (~68) - the network path before the bus - with only the system `dbus-daemon`
  (~67, also -900) behind them. Check the order with
  `earlyoom --dryrun -d -m 99,99 -s 100,100 <the regexes>`, which kills nothing,
  and read its `new victim` lines, not the badness column.
- **earlyoom's regexes carry no backslash.** The unit runs
  `earlyoom $EARLYOOM_ARGS`, which systemd splits on whitespace, stripping quotes
  and dropping a backslash for the character after it: `\.` would arrive as `.`.
  When it runs, `journalctl -u earlyoom -b` prints both regexes at start - read
  them there.

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
as an OOM kill in the order above, rather than up front with `ENOMEM`. That only
reaches a process asking for more than the whole machine at once - and if that
process is dev tooling, which neither killer can take, the kill lands on the
daemons and then the bus instead. **The running server keeps its warning** - it
is logged at start, so the log goes quiet at the next restart, not now. And
**re-measure after a kernel change**: "mode 0 ignores free memory" is this
kernel's behaviour, not a guarantee.

**One VSCode Server build at a time.** After the 2026-09-16 reboot two builds
ran side by side, about 300 MiB of dev baseline for nothing. When the client
updates, close the old window rather than leaving both connected.

`tests/deploy/test_memory_protection.py` pins all of it: the reserve's floor,
overcommit at the value redis asks for, redis's protection against twice the
tracked cap, the slice covering its children, the bus's score out of earlyoom's
reach, both regexes against the real process names and systemd's split, and -
on this node - installed parity, every tracked sysctl key read back from
`/proc/sys`, the host class (exe.dev's roots at -1000, every `--prefer` match
above it and ranked ahead of the bus), and earlyoom stopped and disabled.

## Changing the cap

Prefer applying it live - no restart, no dropped client connections - then
persist it in both places:

```bash
# `cred` and `rcli` as defined under "Changing a grant" above.
rcli acladmin CONFIG SET maxmemory <value>  # applies now
sudo sed -i 's/^maxmemory .*/maxmemory <value>/' /etc/redis/redis.conf
sed -i 's/^maxmemory .*/maxmemory <value>/' deploy/redis.conf.broker
```

**`CONFIG SET` is `acladmin`'s, and only since broker#52.** This line handed the
probe's own `BROKER_REDIS_URL` straight to `redis-cli` until CannObserv/broker#47
swept the credential out of `argv`, and that URL is `brokeradmin`, which never
held `CONFIG SET` - so it was a `NOPERM` from the 2026-09-10 cutover and nothing
said so. Until #52 raising the cap then meant `ACL SETUSER default on`, the
`CONFIG SET`, and `off` again; the operator holds the command by name now, and
`default` holds nothing.

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
  `redis://brokeradmin@localhost:6379/0`, **the user alone**. The password is
  the unit's `LoadCredentialEncrypted=` credential (broker#52), and a password
  put back here would override it - redis-py lets the URL win - and be readable
  by the account again; `tests/deploy/test_bus_health_units.py` reads this file
  for one, verdict only. The username is
  load-bearing twice over: the empty-username form means `default`, which is
  disabled since 2026-09-10, and even while it worked it **failed** for
  `redis-cli`, which sends a two-argument `AUTH "" <password>`, so every shell
  tool degraded silently while the services looked green
  (CannObserv/archiver#195). `localhost` rather than `broker`, because this
  probe runs *on* the broker and `redis.conf` binds loopback as well as the
  tailnet address, deliberately.
- `GOOGLE_APPLICATION_CREDENTIALS` - the read-only `co-pypi-reader` key. **It
  is the operator's and the deploy's, not the probe's:** it authenticates the
  wheelhouse sync in AGENTS.md and `scripts/deploy.sh`'s, and it is here so
  either is one `source` away. The unit unsets it. The probe runs a release
  whose venv was resolved at build, so nothing resolves at start
  (CannObserv/broker#37, #107).

**The deploy syncs the wheelhouse; no unit does.** CI syncs on every run.
`scripts/deploy.sh` syncs before each build and copies into the release only the
wheels `uv.lock` names. In the checkout the sync is manual, and it is due before any `uv sync`
or `uv run` there that follows a `co-core` pin change. Miss it and that command
fails with a resolution error.

**A locked wheel is held to a committed sha256** (CannObserv/broker#125).
uv records no digest for a wheelhouse file, so `uv sync --locked` installs any
file with that name. `pyproject.toml`'s `[tool.broker.wheelhouse-sha256]` pins
one per file `uv.lock` names, and a pin change moves it in the same diff. The
sync fetches a file again when it misses its pin, and refuses a download that
misses too. The deploy refuses a release whose copy misses its pin, journaled
(`refused <sha12>: <file> has sha256 ...`), before `uv sync`, and journals a
pass as `wheelhouse for <sha12>: <files> match their pinned sha256`. Each digest is of
a file fetched over GCS's TLS: it stops a later swap, local or in transit, not
one made at pin time.

**GCS's TLS trust is certifi's bundle, not the system store** (CannObserv/broker#124).
The backup, the restore and the wheelhouse sync reach GCS over `requests`, which
verifies against the venv's `certifi`, so a system-store remediation such as
exe.dev's for eMudhra never reaches them. A CA distrust there is
`uv lock --upgrade-package certifi` and a deploy; `pyproject.toml`'s
`constraint-dependencies` floor holds it through a re-lock, and
`tests/test_tls_trust.py` checks it. The drift check's `urllib` reads the system
store.

The probe holds **no** database credential, joins **no** consumer group, and
inherits no variable from `/etc/broker/.env` that it does not read. All three
are asserted by `tests/deploy/test_bus_health_units.py`; the last one against
the live file, so a variable added there on the node fails it.

`/etc/broker/status.env` (`0400 root:root`, **optional**) carries the check-in
credential - see *The co-status check-in* below.

`/etc/broker/status-checkin.key` (`0400 root:root`) carries the same key alone,
for the drift check's `LoadCredential=` - see *The drift check* below.

`/etc/broker/backup.env` (`0400 root:root`) carries `BROKER_BACKUP_BUCKET` and
the `GOOGLE_APPLICATION_CREDENTIALS` path of the **writer** key. Every
`restore` in [`../docs/RECOVERY.md`](../docs/RECOVERY.md) reads it as root, and
so does the #108 backup unit a switch back restores. The current unit reads no
env file (#109): it names the bucket itself and takes the key as a
credential, so the file keeps naming the key's own path. Not in
`/etc/broker/.env`, and the reason cuts both ways: that file is readable by
`exedev`, which must not hold a credential that can write to the backup
bucket; and the backup must not inherit a Redis URL it has no use for.
`BROKER_BACKUP_PREFIX` is optional and defaults to the hostname; the unit
takes it from a drop-in, `sudo systemctl edit broker-backup.service`.

## The co-status check-in (broker#3, #66)

Every tick, the probe posts to co-status whether or not it found anything:

```
POST http://status:9000/api/v1/monitors/<id>/checkin
X-API-Key: <key>
{"status": "ok" | "alert", "variables": {"source", "revision", "finding_count", "findings"},
 "metadata": {"fault": ["<check>:<subject>", ...]}}
```

**`variables.revision` is the code the tick ran** (broker#105): a release's
`REVISION` file, `dev` from a checkout, or `unknown` when the file is there but
unreadable or empty: a broken release, never a reason to skip the report. The
`Bus health summary` line carries it too. It is never in `metadata`, where every deploy would open a new fault.

**`metadata.fault` is the one value co-status compares** (CannObserv/status#28,
broker#84): the sorted, duplicate-free `check:subject` of this tick's findings,
`[]` on `ok`. Since 2026-10-09 co-status reports a run of `alert` check-ins
once (`co-broker` has no `renotify_seconds` until CannObserv/status#39 sets a
daily reminder), sends *has cleared* on the first `ok`, and opens a new fault -
reporting at once, with no *cleared* for the old one - whenever this value
changes. So a finding that arrives mid-fault reports, and so does one that
clears while another stays open: that smaller report is the only notice a
partial clear gets. A key that only grew until the next `ok` would hear
additions alone, need its own state, and stay silent through a finding's flap
after its first appearance; a flap here costs at most one report a tick, the
rate every `alert` had before 2026-10-09.

The monitor lived in notifier until CannObserv/notifier#83 moved dead-man's
timers into co-status (`CannObserv/status`, tailnet node `status`). co-status
kept the wire contract - path, body, the 202, the monitor id - so broker#66
changed the host and the key and nothing else.

Broker's access to notifier is gone, not dormant (broker#70). Notifier deleted
the `co-broker` tenant on 2026-10-02 (CannObserv/status#2), which revoked the
old key. Its file, `/etc/broker/notifier.env.pre-status-66`, was shredded on
2026-10-05. `notifier` is no longer in this node's tailnet netmap. From here, a
`notifier:9000` timeout is the expected state and not a fault, so do not ask
for `tag:broker -> tag:notifier:9000` back. `tag:broker -> tag:status:9000` is
the rule that stays.

**The report goes every tick regardless of `finding_count`, and that is the
design rather than chattiness.** A findings-only push is silent in exactly the
cases that matter most - a dead probe, a stopped timer, a wedged tick or a
dead node all produce zero findings and zero traffic, which is indistinguishable
from a healthy broker. co-status alarms on the *absence* of a report, so the
arrival is the signal. `status` is the probe's own judgement (`finding_count > 0`
maps to `alert`) because the alternative is co-status learning this repo's
taxonomy.

**Configure it with two variables, and never a host or a port:**

| Variable | Where |
|---|---|
| `STATUS_MONITOR_ID` | `/etc/broker/status.env` |
| `STATUS_API_KEY` | `/etc/broker/status.env` |

```bash
sudo install -m 0400 -o root -g root /dev/null /etc/broker/status.env
sudo tee /etc/broker/status.env >/dev/null <<'ENV'
STATUS_MONITOR_ID=<the monitor's own id - NOT the tenant_id>
STATUS_API_KEY=<key>
ENV
sudo systemctl start broker-bus-health.service
journalctl -u broker-bus-health -n 5 -o cat --no-pager
```

`0400 root:root`, and **not** `/etc/broker/.env`. That file is `0640
root:exedev`, so the operator's account can read it at any time, running or not.
systemd reads an `EnvironmentFile` as root *before* dropping privileges, so a
root-only file still reaches the process while staying unreadable to every
account. The probe's own user, since #121, reads neither file. The process
seeing its own environment is unavoidable; a second copy in a file `exedev` can
`cat` is not.

Both unset is the supported default and costs nothing - the `EnvironmentFile`
line carries a leading `-`. One set without the other is a config mistake and is
logged at ERROR, because the failure it would otherwise produce is silence.

**The host and port are not configurable, deliberately.** `status:9001` is
`status_dev`, against its own database. Since this monitor alarms on the absence
of check-ins, a wrong destination would not degrade it but **invert** it:
reports land where nothing alarms, the production monitor receives nothing, and
it declares a healthy broker dead. The retired `notifier:9000` is the same trap
one host over. So the operator supplies a monitor id and the base URL is a
constant in `src/broker/checkin.py`, which the drift check posts from too. Same move as `databases 1` against the
db15 vector - make the wrong destination unnameable rather than merely
discouraged.

That reasoning outlived the move; one of notifier's did not. Notifier's `:9001`
was admitted to this node and its `/health` was byte-identical to production's,
so the wrong port could not be caught by looking. co-status's `/health` names
its `environment`, and the tailnet admits `tag:broker` to `:9000` only. Neither
is a reason to make the host configurable: the constant is what keeps a typo
from being possible at all, and a per-tick `/health` assertion would be one more
request whose failure is silent.

A failed check-in is a WARN line and never a failed unit. The probe is WARN-only
by contract, and a monitoring unit that starts failing on its own transport
trains an operator to ignore it.

### Two things to check when a check-in fails

**`HTTP 404` means the monitor id is wrong, and the likeliest cause is a
`tenant_id`.** Both are ULIDs of the same shape and both appear in the monitor's
own JSON, so they are easy to transpose. Read it back and compare:

```bash
sudo sh -c 'set -a; . /etc/broker/status.env; set +a
  printf "X-API-Key: %s\n" "$STATUS_API_KEY" |
    curl -s -H @- http://status:9000/api/v1/monitors/$STATUS_MONITOR_ID' | python3 -m json.tool
```

The key goes to `curl` on stdin, not as `-H "X-API-Key: $KEY"`: that form
expands into `curl`'s `argv`, readable from `ps` for as long as it runs.
`printf` is a shell builtin, so it has no `argv` of its own (broker#47's rule,
applied to an HTTP header).

`last_checkin_at`, `last_status` and `last_variables` should reflect the most
recent tick, and `fault_key` the last `alert`'s `metadata.fault` while a fault is
open (`null` after an `ok`). This is the only end-to-end confirmation - a clean
journald line proves the POST returned 2xx, not that co-status recorded anything
useful.

**`enabled: false` means the dead-man's timer is not running.** co-status's
sweep selects enabled monitors only, so a disabled monitor records check-ins and
reports state while alarming on nothing when they stop. Its check-in route does
*not* gate on the flag, so an `alert` that opens a fault still dispatches -
which makes the failure asymmetric and easy to miss: findings reach a person,
silence does not.
Silence is the half this probe exists for. The cutover relies on the same
asymmetry: the monitor is imported disabled, the first check-in lands on it,
and only then is it enabled.

## The drift check (broker#110)

Since #107 a merge reaches nothing the probe runs until `scripts/deploy.sh`.
`broker-drift.timer` asks, hourly, whether that has been left undone:
`python -m src.broker.drift`, from `/srv/broker/live`, asks GitHub how far the
release's `REVISION` is behind `main`, and checks in once to co-status's
`co-broker-drift` monitor (id `01M5CVMEFPKRGP2Y48F7QTPP7D`, tenant `co-broker`,
CannObserv/status#51). A unit of its own, not a probe finding, so a lag you mean
to keep is silenced without silencing the bus (#22 D6). The rules are
`src/broker/drift.py`, ported from CannObserv/processor's.

**What runs** is `src/`, `deploy/broker-*`, `scripts/deploy.sh`,
`pyproject.toml` and `uv.lock`, defined once in `drift.py`. The rest of
`deploy/` is installed by hand and never runs from `live`, and docs, tests, CI
and the skills never run at all. The commit the skills hook pushes every day,
and the weekly context measurement (a workflow's push, which gets no CI run),
are therefore `ok` at any age.

Each run logs one `drift check` record (`journalctl -u broker-drift -n 5 -o cat`)
with `build`, `main`, `kind`, `body` and `checkin`, and sends `kind`, `live`,
`main` and `body` on every check-in. Every `alert` carries `metadata.fault`, its
`kind`, so a change of fault reports at once; an `ok` carries no metadata.

| `kind` | Means | Do |
|---|---|---|
| `ok` | live is `main`, behind only in what never runs, or behind in code for 8 h or less since the push | nothing |
| `lag` | behind in code for more than 8 h since the push (timed from that push's CI run); the body names `main`'s CI | deploy `main`'s tip, `~/broker/scripts/deploy.sh`; or, to keep the lag, silence it (below) |
| `off_main` | GitHub says live is not on `main`, or does not know it | deploy `origin/main`: a deploy never builds off `main`, so a hand-made link or a rewritten `main` is the likeliest cause |
| `unstamped` | live has no `REVISION` (`dev`), an unreadable or empty one (`unknown`), or one that is not a commit id; sent as `live: unknown`, the body says which | deploy again: the release is a broken one |
| `test` | `--test-alert`, by hand | the next run sends *has cleared* |

**GitHub silent** (a rate limit, a 5xx, a timeout) sends **no check-in** and
exits 1, so a long outage ends in `missing`, never a false `ok`. The record
says `checkin: null` and the body quotes GitHub. A 422 from co-status (a
template preview that failed, nothing recorded) or any answer but 202 is a
failed run too: exit 1, `checkin: failed: ...`, no retry; the next hour is it.

**`missing`** on `co-broker-drift` (2.5 h with no check-in) means the drift
check is not reporting: the timer is stopped, the node is down, the tailnet path
is gone, the key is wrong (`checkin: failed: 401 ...`), or GitHub was silent two
runs running. It says nothing about the Redis broker, which `co-broker` watches.

**Silencing a lag you mean to keep:** `sudo systemctl stop broker-drift.timer`,
and `start` it when the lag ends. `co-broker-drift` reads `missing` after
2.5 h, as intended: a silenced check should look silenced.

**Rolling back past #110** leaves the timer running a release with no
`src/broker/drift.py`: every run fails and the monitor goes `missing`. The
deploy never removes a unit; it says the drift units are not in the release.
Stop it with `sudo systemctl disable --now broker-drift.timer`, and enable it
again by hand after deploying forward, since the deploy enables it only the
first time it installs it.

**Budget:** GitHub is asked unauthenticated, as the deploy gate asks it: 60
requests an hour per address, shared with `scripts/deploy.sh`. A run costs 1 in
sync or behind in docs, 2 behind in code, and at most 10 past the grace.

**No Redis.** The unit holds no Redis credential, loads no `EnvironmentFile=`,
and the module imports no Redis client, so `ACL LOG` cannot gain an entry from
it. `tests/deploy/test_drift_units.py` and `tests/test_drift.py` pin both.

### The key

The drift check presents `co-broker`'s production key, `co-broker-checkin`, the
one the probe sends from `/etc/broker/status.env`. `LoadCredential=` needs it
in a file of its own, `/etc/broker/status-checkin.key`, `0400 root:root`,
holding the key alone. Copy it inside the root shell, so the key is only ever a
shell variable and `printf`'s output, never an `argv` (`printf` and `[` are
builtins). The copy refuses unless the sourced value is the line's raw text: an
empty, quoted, `$`-expanded or repeated `STATUS_API_KEY=` could parse
differently here than under systemd's `EnvironmentFile=`, which is what the
probe sends:

```bash
sudo sh -c 'set -eu; umask 077
  set -a; . /etc/broker/status.env; set +a
  raw=$(sed -n "s/^STATUS_API_KEY=//p" /etc/broker/status.env)
  if [ -z "$raw" ] || [ "$raw" != "$STATUS_API_KEY" ]; then
    echo "status.env: STATUS_API_KEY is empty, quoted, expanded or repeated; nothing copied"; exit 1; fi
  install -m 0400 -o root -g root /dev/null /etc/broker/status-checkin.key
  printf "%s\n" "$raw" > /etc/broker/status-checkin.key
  if [ "$(tr -d "\n" < /etc/broker/status-checkin.key)" = "$raw" ]; then
    echo "status-checkin.key: matches status.env"; else echo "status-checkin.key: DIFFERS"; exit 1; fi'
sudo stat -c '%U:%G %a' /etc/broker/status-checkin.key      # root:root 400
```

The unit reads it at each start: nothing to reload. Absent, the unit's
`SetCredential=` fallback runs the check, logs `not sent: missing the
status-checkin-key credential`, and exits 1. The backup's `/etc/broker`
allowlist hides it from the backup, and the probe's user cannot read it (#121).

**Rotating `co-broker-checkin`** is now two files: put the new key in
`/etc/broker/status.env` (*The co-status check-in*, above), then run the copy
above again, which replaces `/etc/broker/status-checkin.key` from it. Until
both are done, the probe or the drift check presents the old key and gets a 401.

### Reading it, and the test alert

```bash
sudo systemctl start broker-drift.service             # one run now: drift check, kind, checkin: 202
                                                      # (OnUnitActiveSec=1h: the hourly run moves to now)
journalctl -u broker-drift -n 5 -o cat --no-pager
systemctl list-timers broker-drift.timer              # the next run

# co-status's view: last_checkin_at, last_status, last_variables, fault_key.
# The key reaches curl on stdin, never its argv.
sudo sh -c 'printf "X-API-Key: %s\n" "$(cat /etc/broker/status-checkin.key)" |
  curl -s -H @- http://status:9000/api/v1/monitors/01M5CVMEFPKRGP2Y48F7QTPP7D' | python3 -m json.tool

# One deliberate alert (kind: test, metadata.fault: test), asking GitHub nothing.
# A transient unit holding the same credential and the unit's own Environment=,
# since the key is root's alone. It reports at once, enabled or not; the next
# `systemctl start broker-drift.service` sends "has cleared".
sudo systemd-run --quiet --pipe --wait --collect -p DynamicUser=yes \
  -p LoadCredential=status-checkin-key:/etc/broker/status-checkin.key \
  -p "$(systemctl show -p Environment broker-drift.service)" -p WorkingDirectory=/srv/broker/live \
  /srv/broker/live/.venv/bin/python -m src.broker.drift --test-alert
```

No dev or test run may use the production key or this monitor id: co-status's
check-in route has no dry run, so a test `ok` resets the monitor's timer and a
test `alert` pages. The suite stubs both calls, and `tests/conftest.py` points
both destinations at nowhere for every test.

## The backup (broker#4)

`broker-backup.service` copies `dump.rdb`, verifies the copy with
`valkey-check-rdb` or `redis-check-rdb` (#102), gzips it, and creates
`gs://co-gcs-broker-backup/co-broker/<snapshot time>.rdb.gz` with
`if_generation_match=0` - never an overwrite, and the identity holds no
`delete`, so the bucket's 30-day lifecycle rule is the only thing that removes
a snapshot. It holds **no Redis credential**: the server rewrites `dump.rdb`
atomically at its `save` points, so the file is the interface.

It runs as `broker-backup` (#109), a static system user in no supplementary
group, with no capability. `/var/lib/redis` is `0750 redis:redis`, so the
user cannot read `dump.rdb`; the unit's one `+` line, `install -p`, copies it
as root into the unit's `0700` runtime directory, and the user reads the copy.
The writer key and `/etc/redis/broker-acl-passwords` arrive through
`LoadCredential=`. The `redis` group is not an option: it holds write on
`dump.rdb` and on `/etc/redis`. Neither is `DynamicUser=`, under which the probe
could not read `state.json` (#22 B1). A missing key or passwords file is an
empty credential, and a failed copy is no start failure: each fails inside the
run and is recorded, so the probe reports it on its next tick.

Each run also ships the service users' ACL digests, `<run time>.digests`, so a
rebuilt node re-admits every service without reissuing a password (broker#72).
A plaintext line in the passwords file fails the run; a digest is not a
credential. [`../docs/RECOVERY.md`](../docs/RECOVERY.md), *The backup*.

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
[`../docs/RECOVERY.md`](../docs/RECOVERY.md) is the runbook around it and the
provisioning commands for the bucket and its writer.

## DLQ evidence

The probe writes captured DLQ entries to
`/var/lib/broker-bus-health/dlq-evidence/<topic>/<last-id>.json`, inside the
unit's `StateDirectory`. That is the "back up" step of the drain runbook, done
on the tick that first sees a non-resting queue rather than by an operator
under time pressure. Delete a topic's dumps once its triage is finished; the
high-water mark is read back from the filenames, so deleting them correctly
re-arms capture rather than leaving a gap. See
[`../docs/DLQ-DRAINING.md`](../docs/DLQ-DRAINING.md).

The **disposal** step is `XDEL <queue> <id>`, per entry, and since broker#12
each named drainer can do it for its own queues without an operator - a
selector, `(+xdel ~<its own>.dlq)`, which cannot reach the stream the queue
copies from. The operator's `acladmin` holds `(+xdel +xtrim ~*.dlq)` as the
backstop for a queue nobody claimed - `brokeradmin`'s until broker#52, which left
the probe unable to delete anything. `+xtrim` joined that selector in broker#14,
having been a root grant riding `~*`, where the one identity that can see every
stream on the instance could also cap any of them. Before that the only tool was `XTRIM MAXLEN 0`, which empties
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
