# The cohort restart window

Runbook for CannObserv/broker#5, and the CannObserv/broker#2 cutover it enables.

> **Status, 2026-09-10.** The `aclfile` half of the window ran at 00:46 UTC,
> steps 2 and 3 followed rolling, and **step 4 retired the shared password at
> 14:18 UTC**, and **`databases 1` landed at 20:10 UTC**. Both halves of R4 are
> closed and this runbook is now a record rather than a plan. It is kept because
> the next cluster needs the shape, and because step 1a-bis is the one nobody
> should rediscover.
>
> The order that worked, all in one window with `default` re-enabled for it:
> `SAVE`, ship a backup off-node, `BGREWRITEAOF` and verify the base number
> advanced, add the directive to both files, restart, verify against a
> before-snapshot, `default off`. Zero `DB index is out of range` on the
> restart, every group at its position, 2 seconds of downtime.
>
> Consequence for every command below: `$U` (`default:`) was the identity of the
> first window and **no longer authenticates between windows**. Read as
> `brokeradmin`, change ACLs as `acladmin`, and for anything needing
> `CONFIG SET`, `BGREWRITEAOF` or a shutdown, open the window by re-enabling
> `default` and close it by disabling it again - step 4, both directions.
>
> ```bash
> pw() { sudo sed -n "s/^__${1}_PW__=//p" /etc/redis/broker-acl-passwords; }
> B="redis://brokeradmin:$(pw BROKERADMIN)@localhost:6379/0"   # read, sweep, diagnose
> A="redis://acladmin:$(pw ACLADMIN)@localhost:6379/0"         # ACL changes only
> U="redis://default:$(pw DEFAULT)@localhost:6379/0"           # window-only; refused while off
> ```

Restarting `redis-server` on `co-broker` disconnects all three participants, so
it is a cohort-wide event rather than a maintenance detail. This window carries
everything that needs one, and nothing that does not.

**The window itself changes nothing for any service.** Every URL still says
`default:` when it ends, and `default` still has the same password. That is
deliberate: it puts the risky, reversible work *outside* the window, where it can
be done one service at a time with a rollback after each.

| | |
|---|---|
| **Expected downtime** | under a minute - one `systemctl restart` |
| **Blast radius** | all three participants lose the bus for that minute |
| **Reversible** | yes, at every step |
| **Data touched** | none by steps 1b to 1d and 2 to 4. Step 1a-bis rewrites the AOF and discards its history, deliberately, after a fresh snapshot has been shipped off-node |

---

## What the window is for

Two directives, on two different axes of the same problem (R4: *a test credential
cannot reach production topic names*).

| Directive | Closes | Why it needs a restart |
|---|---|---|
| `databases 1` | the database-index axis - `SELECT 15` fails outright, so a repointed `REPLICATOR_TEST_REDIS_URL` has nowhere to land | `databases` is startup-only |
| `aclfile /etc/redis/users.acl` | the topic-name axis - a `citest` credential that cannot **name** a production topic | `aclfile` is an **immutable** config; `CONFIG SET aclfile` fails |

Redis ACLs cannot partition by database index at all, which is why neither
substitutes for the other. Doing them together means R4 closes in one event.

Also decided in this window and requiring no change: **`maxmemory` stays at
512 MB.** D5 asked for it to be re-derived rather than inherited and it was -
~27% of this node's 1.9 GiB, room for an AOF-rewrite fork's copy-on-write,
~19x current usage. Recorded so a future window does not re-litigate it. It is
live-settable via `CONFIG SET` anyway, so it never needs a window.

---

## Preconditions

- [ ] All three service owners agree the window. The cohort is pre-production
      with a single hourly Watched Item, so the quiesce is effectively free.
- [ ] `uv run pytest` green on `co-broker` with the env sourced (**0 skipped**
      is the expected shape here; skips mean an artifact is missing).
- [ ] The backup pipeline is healthy before anything depends on it:
      `journalctl -u broker-backup -n 1 -o cat --no-pager` says `uploaded` or
      `unchanged` with a `snapshot_at` inside the last hour, and the probe's
      last tick raised no `backup` finding. A rotated key is found here, with
      nothing open, not inside the window.
- [ ] `deploy/redis-acl.conf` is the version you intend to install. Read it -
      the grants are the security boundary, and the `content.blobs` omission on
      `archiver` is the one nobody should "fix".
- [ ] The dry run below has been done, on this node, with the **real** passwords
      file.

---

## Before the window - no downtime, do this first

### 1. Mint the ACL passwords

Six, one per placeholder, plus `default`. `__DEFAULT_PW__` is **the current
`requirepass` value**, not a new one - that is what makes the first restart a
no-op for every service, and it stays on the line after `default` is retired so
that the rollback can never land on `nopass`.

```bash
sudo install -m 0400 -o root -g root /dev/null /etc/redis/broker-acl-passwords
for p in ARCHIVER WATCHER REPLICATOR BROKERADMIN ACLADMIN CITEST; do
    echo "__${p}_PW__=$(openssl rand -base64 32 | tr -dc 'A-Za-z0-9' | head -c 40)"
done | sudo tee -a /etc/redis/broker-acl-passwords >/dev/null
echo "__DEFAULT_PW__=$(sudo cat /etc/redis/broker-password)" \
    | sudo tee -a /etc/redis/broker-acl-passwords >/dev/null
sudo chmod 0400 /etc/redis/broker-acl-passwords
```

`tr -dc 'A-Za-z0-9'` is not fussiness: these end up in `redis://user:pass@host`
URLs in three `.env` files, and a `/`, `+`, `@` or `#` in a password is a URL
that parses as something else. `default:` already cost this cohort one silent
outage (CannObserv/archiver#195); do not spend another on percent-encoding.

### 2. Dry-run the real file against a throwaway server

`tests/deploy/test_redis_acl.py` already does this with throwaway passwords on
every test run. This repeats it with the **real** passwords file, which is the
only thing that catches a malformed passwords file.

```bash
sudo install -m 0600 -o root -g root /dev/null /root/users.acl.check
sudo deploy/render-acl.sh /etc/redis/broker-acl-passwords \
    | sudo tee /root/users.acl.check >/dev/null

sudo redis-server --port 6399 --bind 127.0.0.1 --save '' --appendonly no \
    --daemonize yes --pidfile /root/aclcheck.pid --logfile /root/aclcheck.log \
    --aclfile /root/users.acl.check
sleep 1
redis-cli -p 6399 PING                                  # -> NOAUTH  (not PONG!)
redis-cli -u "redis://acladmin:$(sudo sed -n 's/^__ACLADMIN_PW__=//p' /etc/redis/broker-acl-passwords)@127.0.0.1:6399" ACL LIST
sudo kill "$(sudo cat /root/aclcheck.pid)"              # no user holds +shutdown, by design
sudo shred -u /root/users.acl.check /root/aclcheck.log
```

**`PING` must return `NOAUTH`.** If it returns `PONG`, stop - see *The `nopass`
trap* below. `ACL LIST` must show seven users: `archiver`, `watcher`,
`replicator`, `brokeradmin`, `acladmin`, `citest`, and `default` - **`off`**
since step 4, still carrying its password hash.

### 3. Note where each service lives

The rolling steps happen on each service's host, not here.

| Service | Host | Env file |
|---|---|---|
| archiver | tailnet `archiver` (VM `co-registrar`, pdx) | `/etc/archiver/.env` |
| watcher | tailnet `watcher` (lax) | `/etc/watcher/.env` |
| replicator | the `watcher` VM - it has no tailnet node of its own | `/etc/replicator/.env` |

Replicator's placement is inferred from two facts rather than read off a
manifest: there is no `replicator` peer in `tailscale status`, and broker#1
Phase 3's Gap 1 found replicator's unit pulling the shared VM's local
`redis-server` back up after the cutover. **Confirm it before step 2** rather
than trusting this table.

---

## The window

### Step 1a - install the ACL file

```bash
sudo deploy/render-acl.sh /etc/redis/broker-acl-passwords \
    | sudo install -m 0640 -o root -g redis /dev/stdin /etc/redis/users.acl
```

Installing it does nothing on its own - Redis does not read it until `aclfile`
is set. That is why this is safe to do before the restart.

### Step 1a-bis - `BGREWRITEAOF` FIRST. This is not optional.

**`databases 1` against this broker's AOF wipes db0.** Found the hard way on
2026-09-10; the incident is recorded at the bottom of this file, and both the
failure and this fix are reproduced in a scratch instance rather than reasoned
about.

The mechanism, in one line: **a command recorded against a database index that
no longer exists executes against whatever database is currently selected, which
is db0.** On replay, `SELECT 15` fails with `DB index is out of range`, the
replay does not abort, the current database stays db0 - and the very next
command from that section lands there instead.

For most commands that is harmless pollution. For the one this epic itself
recorded, it is catastrophic: **broker#1 D4 said "flush `db15`", that flush was
done on 2026-09-08, and `FLUSHDB` is now sitting in the AOF.** Under
`databases 1` it replays as a full wipe of db0, and everything written before it
is gone. Only entries appended *after* the flush survive.

So the two halves of R4's fix are hostile in this order and safe in the other.
Purge the history first. `BGREWRITEAOF` belongs to no user but `default`, so
this is where the window is opened - `ACL SETUSER default on` as `acladmin`,
step 4's rollback - and step 4 is repeated to close it once the verification in
1d is done.

The rewrite destroys the AOF history, which is what recovered the incident
below. That is acceptable now that broker#4 ships an hourly snapshot to
`co-gcs-broker-backup`, with one step first. The backup job ships whatever
`dump.rdb` is, and only a save point rewrites that file, so a snapshot shipped
by hand is up to an hour old unless you take the save yourself - `default` is
on at this point, and `SAVE` is synchronous and sub-second on this dataset.
The object is named by the save's own time, so the run reports `uploaded` and
the journal line's `snapshot_at` is the minute just gone:

```bash
redis-cli -u "$U" --no-auth-warning SAVE                   # -> OK
sudo systemctl start broker-backup.service
journalctl -u broker-backup -n 1 -o cat --no-pager         # THIS run's line, not the state file:
#   "message": "Backup uploaded: gs://co-gcs-broker-backup/co-broker/<now>.rdb.gz", ..., "snapshot_at": "<now>"
```

`SAVE` rather than `BGSAVE`, deliberately: a background save leaves a child
running, and a `BGREWRITEAOF` issued while one runs is only *scheduled*. If the
line says `unchanged`, the save did not happen - read the `SAVE` reply. If the
unit fails, systemd prints one line and the journal carries the error; the
state file says `outcome: failed` too, but the journal is this run's and the
file is cumulative. The restore, should it ever be needed, is
`docs/RECOVERY.md`, *Same node, lost data*.

Now the rewrite:

```bash
sudo ls /var/lib/redis/appendonlydir/                      # note the base number, <n>
redis-cli -u "$U" --no-auth-warning BGREWRITEAOF           # -> Background append only file rewriting started
while redis-cli -u "$U" --no-auth-warning INFO persistence | tr -d '\r' \
      | grep -E '^aof_rewrite_(in_progress|scheduled):' | grep -qv ':0$'; do sleep 1; done
redis-cli -u "$U" --no-auth-warning INFO persistence \
    | grep -E '^aof_(rewrites|rewrite_scheduled|rewrite_in_progress|last_bgrewrite_status):'
#   aof_rewrites:1                  <- a counter, up by one from what it was
#   aof_rewrite_scheduled:0
#   aof_rewrite_in_progress:0
#   aof_last_bgrewrite_status:ok    <- also what a server that never rewrote reports; proves nothing alone
sudo ls -la /var/lib/redis/appendonlydir/
#   appendonly.aof.<n+1>.base.rdb at roughly the dataset's size (~500 KB today; the old base was
#   89 bytes) and a near-empty incr. THE BASE NUMBER ADVANCING IS THE CHECK.
```

The rewrite regenerates the base from the current in-memory dataset, which
contains db0 and nothing else - no `SELECT`, no historical `FLUSHDB`. Verified:
with the rewrite, a restart under `databases 1` keeps every key and logs **zero**
`DB index is out of range`. Without it, the same restart loses everything older
than the flush.

**Do not skip the verification.** If `aof_rewrites` did not go up by one, or the
base file's number did not advance, the landmine is still armed and the next
step is the one that steps on it. A `NOPERM` or `WRONGPASS` on the
`BGREWRITEAOF` line means the window was never opened - `default` is still
off - and everything after it printed the never-ran defaults.

**As executed, 2026-09-10 20:06 UTC.** The AOF held **61 `SELECT` commands
naming db 0, 14 and 15** plus one `FLUSHDB` - db14 as well as the db15 this
runbook anticipated, so the blast radius was wider than the incident write-up
assumed. After the rewrite: `aof_rewrites` 0 to 1, base `appendonly.aof.1` to
`appendonly.aof.2`, base size 89 bytes to 508,520, incr 42.9 MB to **0**, and
zero `SELECT` or `FLUSHDB` left in the new incr file. The server logged
`Removing the history file appendonly.aof.1.incr.aof`, which is the 42.9 MB
carrying the flush. Confirm it the same way:

```bash
sudo grep -ac '^SELECT'  /var/lib/redis/appendonlydir/appendonly.aof.*.incr.aof   # -> 0
sudo grep -ac '^FLUSHDB' /var/lib/redis/appendonlydir/appendonly.aof.*.incr.aof   # -> 0
```

### Step 1b - add the two directives

To **both** `/etc/redis/redis.conf` (inside the `CannObserv/broker#1` block, or
appended at the end - Redis takes the last occurrence) and
`deploy/redis.conf.broker` in this repo, identically:

```
# broker#5. `SELECT 15` now fails outright, so a repointed test URL has nowhere
# to land. Redis ACLs cannot partition by database index, so this axis of R4 can
# only be closed here.
databases 1

# broker#2. Immutable config - this is why the ACL users need a window at all.
# Everything after this is live: ACL SETUSER applies immediately, ACL SAVE
# persists here.
aclfile /etc/redis/users.acl
```

`deploy/redis.conf.broker` is the tracked statement of what is deployed, and
`tests/deploy/test_live_broker_matches_tracked_config.py` compares every
directive in it against `CONFIG GET`. Editing only one of the two files makes
that test fail, which is the point.

### Step 1c - restart

```bash
sudo systemctl restart redis-server
```

### Step 1d - verify, in this order

**`redis-server` does not log to journald on this node.** `logfile` is
`/var/log/redis/redis-server.log`, so `journalctl -u redis-server` holds exactly
one entry in the unit's whole history - the one time the tailnet wait actually
had to wait. Every check below that reads the server's own output therefore
reads the **file**. This is not pedantry: the earlier version of this runbook
grepped the journal for `DB index is out of range`, which is the single check
that would have caught the 2026-09-10 incident, and it would have returned
"0 occurrences" on an empty journal and read as a pass.

```bash
# THE CHECK THAT MATTERS MOST, and it is in the file, not the journal.
sudo grep -c 'DB index is out of range' /var/log/redis/redis-server.log   # -> 0
sudo tail -20 /var/log/redis/redis-server.log
#   * DB loaded from base file appendonly.aof.<n>.base.rdb    <- the snapshot was read
#   * Done loading RDB, keys loaded: <N>, keys expired: 0
#   * Ready to accept connections
# `Creating AOF base file ... on server start` instead means it loaded NOTHING.

systemctl show redis-server -p NRestarts        # -> 0

# R1's boot race. The wait prints ONLY when it actually waited, so silence here
# means the address was already up - which it is on any restart that is not a
# cold boot. The check that always means something is the bind:
sudo ss -ltnp | grep 6379                       # -> 127.0.0.1:6379 AND 100.97.91.19:6379
journalctl -u redis-server --no-pager | grep -i 'tailnet address'   # only after a real wait

# Anonymous must still be refused.
redis-cli PING                                   # -> NOAUTH

pw() { sudo sed -n "s/^__${1}_PW__=//p" /etc/redis/broker-acl-passwords; }
U="redis://default:$(pw DEFAULT)@localhost:6379/0"       # window-only; open at this point
B="redis://brokeradmin:$(pw BROKERADMIN)@localhost:6379/0"
redis-cli -u "$U" --no-auth-warning SELECT 15    # -> ERR DB index is out of range
redis-cli -u "$U" --no-auth-warning SELECT 1     # -> ERR too: `databases 1` means db0 alone
redis-cli -u "$U" --no-auth-warning SELECT 0     # -> OK

# The data, against the snapshot taken before the window. Lengths alone are not
# enough - entries-added is the counter that tells a trim from a wipe.
for s in info.changes info.registry info.watch-status content.fetch \
         content.fetch-policy content.blobs content.revisions \
         content.artifacts content.replicate content.fetch.dlq; do
    printf '%-22s XLEN=%s entries-added=%s\n' "$s" \
        "$(redis-cli -u "$B" --no-auth-warning XLEN $s)" \
        "$(redis-cli -u "$B" --no-auth-warning XINFO STREAM $s 2>/dev/null | grep -A1 entries-added | tail -1)"
done
for s in content.fetch content.blobs content.revisions content.artifacts content.replicate; do
    echo "== $s"; redis-cli -u "$B" --no-auth-warning XINFO GROUPS $s
done

set -a; . /etc/broker/.env; set +a
uv run pytest tests/deploy -q                    # the live-config test now covers both new directives
```

**Read the diff, do not eyeball the numbers.** The two LWW streams move on their
own - `info.watch-status` grows on the `*/5` republish and
`content.fetch-policy` is trimmed by its producer's `maxlen` - so a length that
changed is not a finding. `entries-added` going **backwards** is, on any stream;
so is a group's `last-delivered-id` moving backwards.

Then confirm each service reconnected: `CLIENT LIST` grouped by `user=` should
show all three, and `ACL LOG` should be empty (a restart clears it). They
recover on their own retry loops without intervention - all three classify a
broker outage as transient.

**Do not start a second `redis-server` against this config to "test" it.** It
inherits `dir /var/lib/redis` and `appendonly yes`, so it opens the live AOF for
append alongside the running server. Trial-parse with a throwaway directory
instead: `redis-server /etc/redis/redis.conf --dir "$(mktemp -d)" --port 6398
--appendonly no --logfile ''`. And note that extra CLI arguments are read as
config lines *numbered past the end of the file*, so a `FATAL CONFIG FILE ERROR`
at a line the file does not have is the argument, not the config.

### Rollback for step 1

Remove the two directives from `/etc/redis/redis.conf`, `systemctl restart
redis-server`. `/etc/redis/users.acl` can stay - it is inert without `aclfile`.
No data is involved in the rollback itself. The AOF history that step 1a-bis
rewrote is gone either way; the snapshot shipped just before it is what stands
behind a restart that comes up wrong (`docs/RECOVERY.md`, *Same node, lost
data*).

---

## After the window - rolling, reversible, no downtime

Do these one at a time, verifying between. Nothing below needs a restart of
anything except the service being flipped.

### Step 2 - each service onto its own credential

For each of archiver, watcher, replicator, on its own host:

```bash
# in /etc/<service>/.env, change the bus URL's credential:
#   redis://default:<shared>@broker:6379/0
#   -> redis://<service>:<its password>@broker:6379/0
sudo systemctl restart <service>
journalctl -u <service> -n 50 -o cat --no-pager | grep -iE 'noperm|error|redis'
```

Verify before moving to the next service:

- No `NOPERM` in the journal.
- The service's streams still advance (`XLEN` from this node).
- Its consumer group's `pending` returns to 0.
- `check_redis_floor.sh` reports the version rather than
  `could not read redis_version` - that is `+info` working, and it is warn-only,
  so nothing else will tell you.

**A `NOPERM` here is recoverable and expected to be survivable.** All three
participants classify it as transient (archiver#193 Phase 1, replicator#82, and
watcher's loops classify by nothing so they back off by default), so a missing
grant degrades to a backing-off publisher rather than dead-lettering valid
events. Widen the grant live:

```bash
redis-cli -u "$A" --no-auth-warning ACL SETUSER <service> +<command>
redis-cli -u "$A" --no-auth-warning ACL SAVE      # persists to /etc/redis/users.acl
```

Then add the same grant to `deploy/redis-acl.conf` and commit it, or the next
render silently reverts it.

This exact path ran on 2026-09-10, unplanned. `replicator` had been left without
`+exists` (the command was in its observed inventory and attributed to the wrong
client), and the moment replicator#85 brought its group loops back every fetch
command was delivered, denied, retried and never acked. `XPENDING` climbed 2 to
3, the probe's two-tick rule fired, the notifier alert reached a person, `ACL
LOG` named the command in one query, and the `SETUSER` above drained the PEL to
0 with no restart. Nothing dead-lettered, because replicator#82 classifies
`NOPERM` as transient. That is the recovery story, exercised.

### Step 3 - the probe onto `brokeradmin`

```bash
# /etc/broker/.env: BROKER_REDIS_URL -> redis://brokeradmin:<pw>@localhost:6379/0
sudo systemctl start broker-bus-health.service
journalctl -u broker-bus-health -n 5 -o cat --no-pager   # -> finding_count: 0
```

`brokeradmin` is narrowed to read-and-trim: no `XADD`, no `CONFIG SET`, no
`FLUSHDB`, and nothing that can CHANGE an ACL. It reads one - `+acl|log` and,
since broker#11, `+acl|getuser` - and read-versus-change is the line `acladmin`
sits on the other side of. If the probe reports `broker unreachable or probe
failed: NoPermissionError`, it is a missing grant, not an outage.

### Step 4 - retire the shared password

**Done 2026-09-10 14:18 UTC**, as `acladmin`. Recorded as it was run, so the
next cluster - or this one after broker#4 rebuilds it - has the shape.

**Last, and only once steps 2 and 3 are confirmed for all four clients.** The
check is not "the services look fine"; it is that no connection is
authenticated as `default`:

```bash
redis-cli -u "$B" --no-auth-warning CLIENT LIST | grep -oE 'user=[^ ]+' | sort | uniq -c
#   4 user=archiver  1 user=brokeradmin  3 user=replicator  3 user=watcher  - and no user=default
redis-cli -u "$B" --no-auth-warning ACL LOG 5             # quiet: nothing newer than the last fix
redis-cli -u "$A" --no-auth-warning ACL LIST | grep '^user acladmin'   # precondition, not optional
```

```bash
redis-cli -u "$A" --no-auth-warning ACL SETUSER default off
redis-cli -u "$A" --no-auth-warning ACL SAVE
```

Then verify every axis, not only the one that changed:

```bash
redis-cli --no-auth-warning PING                           # -> NOAUTH Authentication required.
redis-cli -u "$U" --no-auth-warning PING                   # -> WRONGPASS ... or user is disabled
for u in archiver watcher replicator brokeradmin acladmin citest; do
    redis-cli -u "redis://$u:$(pw "$(echo "$u" | tr a-z A-Z)")@localhost:6379/0" --no-auth-warning PING   # -> PONG, each
done
redis-cli -u "$B" --no-auth-warning CLIENT LIST | grep -c 'flags=b'    # same count as before the flip
sudo grep '^user default' /etc/redis/users.acl             # -> user default off #<hash> ~* &* +@all
```

`ACL SETUSER default off` does **not** disconnect clients already authenticated
as `default` on this Redis (7.0): they keep working until they reconnect, and
then fail. That is why the `CLIENT LIST` check comes first - a straggler would
break at its next restart rather than now, and look healthy in between.

Then update `deploy/redis-acl.conf` to `user default off` - keeping its
`>__DEFAULT_PW__` - and commit, so the tracked file matches what `ACL SAVE`
wrote. `tests/deploy/test_redis_acl.py` pins the line as declared, `off`, and
carrying a password.

Reversing step 4 - and, from now on, **opening any restart window**, since no
other user can `BGREWRITEAOF`, `CONFIG SET` or shut the server down:

```bash
redis-cli -u "$A" --no-auth-warning ACL SETUSER default on     # the password survives 'off'
redis-cli -u "$A" --no-auth-warning ACL SAVE
# ... the window ...
redis-cli -u "$A" --no-auth-warning ACL SETUSER default off
redis-cli -u "$A" --no-auth-warning ACL SAVE
```

**Why `acladmin` exists at all**, because this was nearly got wrong: `ACL
SETUSER` requires `+acl`, and until that user was added **no user had it** -
not `brokeradmin`, not any service. `default off` would therefore have frozen
every grant on the broker permanently. That breaks more than this step's
rollback; it breaks the recovery story the whole cutover rests on, since
step 2's "a `NOPERM` is survivable, widen the grant live" would have required
editing `users.acl` and restarting - a cohort-wide event, for a typo.

`acladmin` is deliberately *not* `brokeradmin` with more grants. `brokeradmin`
holds `~*` because `INFO` and the DLQ sweep need it, so its narrow command list
is the only boundary it has, and `+acl` would let it grant itself `+xadd`.
Verified live: `acladmin` can run `ACL LIST` and `ACL SETUSER` and is refused
`XADD`, `XLEN`, `INFO`, `CONFIG SET` and `FLUSHALL`.

**Keep a shell on the node** through step 4 regardless. The last-resort recovery
is still `redis-server`'s own config - restart with the `aclfile` line commented
out, restoring `requirepass` behaviour - and that is a cohort-wide event, which
is exactly why `acladmin` is the path you want to reach for first.

---

## The `nopass` trap

Worth its own section because it is silent, it is the opposite of what the
mechanism is for, and every check anyone would think to run reports success.

**An aclfile that does not mention `default` makes it `nopass`.** Verified on a
scratch instance: `requirepass` set in `redis.conf`, aclfile with no `default`
line, and an anonymous client gets `PONG` - while `CONFIG GET requirepass` still
returns the password. The ACL subsystem takes ownership of `default` the moment
an aclfile exists, and its built-in default is `nopass ~* &* +@all`.

That is R2 - a tailnet-bound broker reachable by every node the policy admits,
including the user-owned `observo-primary` - arriving as a *side effect of
enabling the mechanism meant to prevent it*.

`deploy/redis-acl.conf` therefore always declares `default`, and
`test_default_is_declared_disabled_and_still_carries_a_password` fails if the
line is ever removed - or if it loses its password, which would turn the
rollback `ACL SETUSER default on` into the same trap by another door. **The
one-line check is `redis-cli PING` with no credentials at all.**
It appears twice above, before and after the restart, for this reason.

---

## Symptom playbook

| Symptom | Means | Do |
|---|---|---|
| `redis-cli PING` returns `PONG` unauthenticated | the `nopass` trap | roll back step 1 immediately; the broker is open |
| redis-server will not start, `Aborting Redis startup because of ACL errors` | a bad rule; Redis refuses the **whole file** | comment out `aclfile`, restart, fix, re-dry-run |
| A service logs `NOPERM ... no permissions to run the '<cmd>' command` | missing command grant | `ACL SETUSER <user> +<cmd>` live, then commit it |
| A service logs `NOPERM ... no permissions to access one of the keys` | missing `~pattern` - the **quieter** mistake | `ACL SETUSER <user> ~<topic>` live, then commit it |
| `check_redis_floor.sh` says `could not read redis_version` | missing `+info`; warn-only, so nothing else reports it | `ACL SETUSER <user> +info` |
| `AuthenticationError` on connect | the service's own credential is wrong | check the URL's username half, not just the password |
| `ERR DB index is out of range` from a test suite | `databases 1` working as intended | fix the test's URL; do not widen `databases` |
| `redis-cli -u .../15` prints that error **and then `PONG`** | redis-cli falls back to db0 and carries on; redis-py raises instead | not a fault - but never verify `databases 1` with a URL suffix, use `SELECT 15` as a command, or you will read the trailing `PONG` as success |
| Redis starts but binds only loopback | the tailnet wait did not fire | R1 / observo#473; do not proceed, check `journalctl -u redis-server` |
| Streams come back far shorter than they went in | **a historical `FLUSHDB` replayed against db0** - see step 1a-bis | roll back `databases 1`, restart. If 1a-bis was skipped, the AOF still holds the history and replays correctly once the database exists again; if it ran, there is no history to replay - restore the snapshot shipped just before it (`docs/RECOVERY.md`) |
| `DB index is out of range` in `/var/log/redis/redis-server.log` **at startup** | the AOF holds commands for a database `databases 1` removed | every one is a command that just executed against db0 instead. Stop and audit |

---

## What not to do

- **Do not `CONFIG REWRITE`.** It works and it does not destroy this repo's
  delimited block (checked, not assumed), but it normalises unrelated directives
  - `dir` and `logfile` get rewritten - which the live-config test then reads as
  drift, and it splits the ACL's source of truth across a file this repo only
  partly tracks.
- **Do not add `+client|setinfo`.** It does not exist before Redis 7.2 and this
  broker is 7.0.15; the grant is rejected and takes every user down with it. It
  becomes a required step of any upgrade to >= 7.2.
- **Do not grant `archiver` `~content.blobs`** to make an error go away. That
  omission is the entire reason this file exists.
- **Do not do step 4 before steps 2 and 3.** `user default off` with any client
  still on `default:` locks that client out. `CLIENT LIST` grouped by `user=`
  is the check, and it showed zero `default` connections before the flip.
- **Do not start a restart window with `default` still `off`.** Nothing else
  can `CONFIG SET save ""` when a restart comes up wrong, and the on-disk copies
  are being overwritten by the minute while someone finds `acladmin`'s
  password. Re-enable before the restart, disable after the verification.
- **Do not schedule this window alongside any service's own VM move.** R6
  generalised: one moving part at a time.

---

## Incident, 2026-09-10: `databases 1` wiped db0

Recorded because the fix above is only credible with the failure beside it.

**What happened.** `databases 1` and `aclfile` were added together and
`redis-server` restarted at 00:46:21. The AOF replayed, `SELECT 15` failed, and
the `FLUSHDB` this epic ran against db15 on 2026-09-08 executed against **db0**.
The broker came up holding only what had been written since that flush.

**Duration: 2 minutes 23 seconds.** 00:46:22 to 00:48:45, from restart to the
rollback of `databases 1` being live.

**The remnant matched the theory exactly**, which is what identified it:

| Stream | before | during the incident | added since the 09-08 flush |
|---|---|---|---|
| `content.fetch` | 948 | **30** | 30 |
| `content.blobs` | 948 | **30** | 30 |
| `info.watch-status` | 29,076 | **1,304** | 1,304 |
| `content.fetch-policy` | 28,750 | **978** | 978 |

**Recovery was complete**, because the AOF is append-only and the history was
never rewritten - removing `databases 1` and restarting replayed it correctly.
All ten streams, all five consumer groups at their real positions, `lag 0` on
every group, 27 `replicator:cmd:fetch:*` keys with TTLs intact, `db0: 37 keys /
27 expires` - identical to the pre-incident state.

**Three things nearly made it worse, and are worth carrying:**

- **A background save clobbered the shutdown RDB.** `dump.rdb` was 5.1 MB (the
  full pre-restart dataset) when first listed and 206 KB roughly a minute later,
  because the degraded server hit a `save` point and overwrote it with the small
  dataset. If the AOF had *also* been damaged, that minute was the whole recovery
  window. **`CONFIG SET save ""` and `CONFIG SET auto-aof-rewrite-percentage 0`
  are the first commands to run when a restart comes up wrong** - before
  diagnosing anything - because both of the on-disk copies are being actively
  overwritten while you think. Since step 4 only `default` can run them, which
  is why a window is entered with `default` already re-enabled and left that
  way until the verification is done.
- **An AOF rewrite would have been unrecoverable.** `auto-aof-rewrite-min-size`
  is 64 MB and the incr was 41 MB, so it did not fire. It was margin, not design.
- **The probe reported nothing wrong.** Every threshold is an upper bound -
  length caps, memory, DLQ depth - so a broker that has lost 96% of its entries
  is, to this probe, a very healthy broker. See below.

**A follow-up the probe should carry:** there is no check for a stream getting
*shorter*. `XLEN` collapsing between ticks is not something a producer-capped
stream does, and the state file already persists per-tick numbers, so the
comparison is nearly free. Filed as its own issue.
