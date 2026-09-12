# The ACL cutover

The CannObserv/broker#2 work either side of the cohort restart window
([RESTART-WINDOW.md](RESTART-WINDOW.md)): before it - minting the passwords, the
dry run, where each service lives - and after it - each service onto its own
credential, the probe onto `brokeradmin`, the shared password retired - plus the
`nopass` trap the dry run exists to catch. The window itself (including step 1a,
installing the ACL file), the symptom playbook, what not to do and the
2026-09-10 incident stay in RESTART-WINDOW.md.

This is the record of the cutover, and the order a new cluster repeats. To change
a grant on this one, see [deploy/README.md](../deploy/README.md), *Changing a grant*.

Steps 2 to 4 below continue from Step 1 (1a to 1d) in RESTART-WINDOW.md; the
numbered items under *Before the window* are preparation, not steps. Commands use
the `pw` helper and `$A`, `$B` and `$U`, all defined at the top of
RESTART-WINDOW.md; `$U` (`default:`) authenticates only while a window is open.

Moved out of RESTART-WINDOW.md on 2026-09-11, when the runbook ran past the
per-doc context budget.

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
It appears twice for this reason: in the dry run above, and after the restart in [RESTART-WINDOW.md](RESTART-WINDOW.md) step 1d.
