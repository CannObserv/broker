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
the `pw` and `rcli` helpers defined at the top of RESTART-WINDOW.md; `rcli` takes
the ACL user as its first argument, and `rcli default` authenticates only while a
window is open.

Moved out of RESTART-WINDOW.md on 2026-09-11, when the runbook ran past the
per-doc context budget.

## Before the window - no downtime, do this first

### 1. Mint the ACL passwords

Six, one per placeholder, plus `default`. **On a migrating cluster**
`__DEFAULT_PW__` is the current `requirepass` value, not a new one - that is what
makes the first restart a no-op for every service, and it stays on the line after
`default` is retired so that the rollback can never land on `nopass`. On this
cluster that was true from 2026-09-10 until CannObserv/broker#46 rotated it;
`__DEFAULT_PW__` is now a value that was never a live `requirepass` anywhere, and
the section below is how it got there and how it is done again.

```bash
# In a script that sets pipefail, `head` closing the pipe SIGPIPEs `tr` and the
# subshell exits 141 - so the mint disables it for itself and asserts the length.
mint() { set +o pipefail; LC_ALL=C tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 40; }
sudo install -m 0400 -o root -g root /dev/null /etc/redis/broker-acl-passwords
for p in ARCHIVER WATCHER REPLICATOR BROKERADMIN ACLADMIN CITEST; do
    echo "__${p}_PW__=$(mint)"
done | sudo tee -a /etc/redis/broker-acl-passwords >/dev/null
echo "__DEFAULT_PW__=$(sudo cat /etc/redis/broker-password)" \
    | sudo tee -a /etc/redis/broker-acl-passwords >/dev/null
sudo chmod 0400 /etc/redis/broker-acl-passwords
awk -F= '{ printf "%-16s %s chars\n", $1, length($2) }' \
    <(sudo cat /etc/redis/broker-acl-passwords)     # -> 40 each, and CHECK IT
```

`tr -dc 'A-Za-z0-9'` is not fussiness: these end up in `redis://user:pass@host`
URLs in three `.env` files, and a `/`, `+`, `@` or `#` in a password is a URL
that parses as something else. `default:` already cost this cohort one silent
outage (CannObserv/archiver#195); do not spend another on percent-encoding.

**Draw from a stream, not from `openssl rand -base64 32`.** This line read
`openssl rand -base64 32 | tr -dc 'A-Za-z0-9' | head -c 40` until broker#46, and
that spelling cannot keep its promise: base64 of 32 bytes is 44 characters, `tr`
drops every `+`, `/` and `=`, and `head` has no shortfall to make up from. It
mints under 40 about one time in twenty and says nothing - `P(X>=4)` for
`X ~ Bin(43, 2/64)` is 0.045 - and it produced 39 on broker#46's first attempt. The six minted in 2026-09-10's run are all 40, so
nothing here is short; the recipe was lucky six times. The length check on the
last line is the point, whichever source you draw from.

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
# Spelled out, NOT `rcli`: that helper is pinned to 6379, and reaching for it
# here would talk to production instead of the throwaway server.
REDISCLI_AUTH="$(pw ACLADMIN)" redis-cli --user acladmin -h 127.0.0.1 -p 6399 ACL LIST
sudo kill "$(sudo cat /root/aclcheck.pid)"              # no user holds +shutdown, by design
sudo shred -u /root/users.acl.check /root/aclcheck.log
```

**`PING` must return `NOAUTH`.** If it returns `PONG`, stop - see *The `nopass`
trap* below. `ACL LIST` must show seven users: `archiver`, `watcher`,
`replicator`, `brokeradmin`, `acladmin`, `citest`, and `default` - **`off`**
since step 4, still carrying its password hash.

### 3. Note where each service lives

The rolling steps happen on each service's host, not here. **Current hosts are
in [STREAMS.md](STREAMS.md), *Participants, hosts and paths*** - that table is
checked against `CLIENT LIST` on the live broker, so it cannot drift the way
this section's own table did. The env files are each service's:

| Service | Env file |
|---|---|
| archiver | `/etc/archiver/.env` |
| watcher | `/etc/watcher/.env` |
| replicator | `/etc/replicator/.env` |

**As of the 2026-09-10 cutover, and no longer true:** watcher ran in `lax` on its
own VM, and replicator shared that VM with no tailnet node of its own - inferred
from there being no `replicator` peer in `tailscale status`, and from broker#1
Phase 3's Gap 1 finding replicator's unit pulling the shared VM's local
`redis-server` back up after the cutover. Both moved to their own `pdx` VMs by
2026-09-15 (CannObserv/broker#8). This section still placed them in `lax` days
afterwards, and nothing noticed, because nothing compared it with anything - the
reason the host table now lives where a test reads it.

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
rcli acladmin ACL SETUSER <service> +<command>
rcli acladmin ACL SAVE                            # persists to /etc/redis/users.acl
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
rcli brokeradmin CLIENT LIST | grep -oE 'user=[^ ]+' | sort | uniq -c
#   4 user=archiver  1 user=brokeradmin  3 user=replicator  3 user=watcher  - and no user=default
rcli brokeradmin ACL LOG 5                                # quiet: nothing newer than the last fix
rcli acladmin ACL LIST | grep '^user acladmin'            # precondition, not optional
```

```bash
rcli acladmin ACL SETUSER default off
rcli acladmin ACL SAVE
```

Then verify every axis, not only the one that changed:

```bash
redis-cli PING                                             # -> NOAUTH Authentication required.
rcli default PING                                          # -> WRONGPASS ... or user is disabled
for u in archiver watcher replicator brokeradmin acladmin citest; do
    if [ -z "$(pw "${u^^}")" ]; then
        echo "$u: no plaintext on this node (rotated by digest) - verify from its own host"
        continue
    fi
    printf '%-13s %s\n' "$u" "$(rcli "$u" PING)"           # -> PONG, each one it can reach
done
rcli brokeradmin CLIENT LIST | grep -c 'flags=b'           # same count as before the flip
sudo grep '^user default' /etc/redis/users.acl             # -> user default off #<hash> ~* &* +@all
```

**The skip in that loop is the general case, not an archiver exception.** Any
verification that authenticates *as* a service stops working the moment that
service's credential is rotated by a hash-only handoff: this node then holds a
digest and no plaintext, `pw` returns empty, and what used to print `PONG`
prints `WRONGPASS` - which also writes an `AUTH` / `reason: auth` entry into
`ACL LOG` naming the service whose credential was just rotated, the most
alarming thing that log can say about a node where nothing is wrong. That is
what `archiver` did on 2026-09-23 (CannObserv/archiver#251), and it is what the
next rotated user will do. The replacements are verification **from the
service's own host**, or an assertion about the ACL rather than about
authentication - `rcli brokeradmin ACL GETUSER <user>` showing exactly one
password hash, which `brokeradmin` can already do.

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
rcli acladmin ACL SETUSER default on      # the password survives 'off'
rcli acladmin ACL SAVE
# ... the window ...
rcli acladmin ACL SETUSER default off
rcli acladmin ACL SAVE
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

## Rotating `__DEFAULT_PW__` - a supported operation, and four writes

`default` being `off` is not the end of its exposure, which is why this section
exists (CannObserv/broker#46). One secret wears three hats here by construction:
step 1 mints `__DEFAULT_PW__` **as** the current `requirepass`, so
`/etc/redis/broker-acl-passwords`, `/etc/redis/broker-password` and
`redis.conf`'s `requirepass` line all carry the same value, and the live
`default` password is that value's hash. Three documented paths turn it back
into a live credential, and all three are what you reach for when something is
already wrong: opening **any** restart window (`ACL SETUSER default on` - no
other user holds `BGREWRITEAOF`, `CONFIG SET` or `SHUTDOWN`), step 4's rollback,
and the last-resort restart with `aclfile` commented out, which does not even
need `default` to be `on`. So the break-glass for every window, and the
break-glass behind it, are one string; if it leaks, it is rotated, and `off`
buys nothing.

It leaked on 2026-09-23: CannObserv/archiver#251 found `ARCHIVER_REDIS_URL`
logged unredacted at every archiver publisher start, which put **the credential
every service used before the 2026-09-10 cutover** into another host's journald
in cleartext, back to the start of retention there. Rotated the same day. Not
vacuuming that journal is deliberate - it is the journal that answered
CannObserv/archiver#247, and deletion costs evidence this cohort has already had
to use once.

**Four writes, or a restart silently reverts part of it** - and the ACL is
touched twice, at both ends, so that no crash in between leaves this node
without a credential it knows. None of it puts a secret on a command line
(CannObserv/broker#47). **Run it as a script, not pasted line by line:** the
guard below exits, and `set +o pipefail` only means something where a script set
it.

```bash
pw() { sudo sed -n "s/^__${1}_PW__=//p" /etc/redis/broker-acl-passwords; }
rcli() { local u=$1; shift
         REDISCLI_AUTH="$(pw "${u^^}")" redis-cli --user "$u" -h localhost -p 6379 "$@"; }

OLD="$(pw DEFAULT)"; OLDH="$(printf %s "$OLD" | sha256sum | cut -d' ' -f1)"
# `set +o pipefail`: `head` closing the pipe SIGPIPEs `tr`, and under pipefail
# that is exit 141 for a command that did exactly its job. It is what this
# rotation hit on the day, inside a `set -euo pipefail` script.
NEW="$(set +o pipefail; LC_ALL=C tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 40)"
[ "${#NEW}" -eq 40 ] || { echo "minted ${#NEW} chars, want 40"; exit 1; }
NEWH="$(printf %s "$NEW" | sha256sum | cut -d' ' -f1)"

# 1a. ADD the new password to the live `default`, keeping the old one. Both
#     authenticate from here until 1b - verified on a scratch 7.0.15, as is
#     `!<hash>` keeping the `off` flag. deploy/README.md, "Changing a grant".
rcli acladmin ACL SETUSER default "#$NEWH"
rcli acladmin ACL SAVE

# 2. The placeholders file - `pw DEFAULT`, and any future re-render.
# 3. /etc/redis/broker-password - the `aclfile`-commented-out recovery path.
# 4. /etc/redis/redis.conf's `requirepass` line - the same path's directive.
#    Values travel on a pipe, never on a `sudo sed -i` command line:
#      sudo cat <file> | awk ... | sudo tee <file>.new  &&  sudo mv

# 1b. Only once 2, 3 and 4 are verified below: RETIRE the old password.
rcli acladmin ACL SETUSER default "!$OLDH"
rcli acladmin ACL SAVE
sudo grep -q "^user default off #$NEWH ~\* &\* +@all$" /etc/redis/users.acl   # or STOP
```

**Why the ACL is written at both ends.** A single `"#$NEWH" "!$OLDH"` leaves a
window in either order: crash after it and `NEW` is lost with `OLD` already
gone, so the break-glass for every restart window is a string nobody holds;
crash before it, with the files already written, and the same is true the other
way round. Adding first costs nothing - `ACL SETUSER` ADDS, which is the same
property `deploy/README.md` warns about for rules - and between 1a and 1b the
user simply carries two passwords, both live. **The live suite is legitimately
red in that interval**: `test_every_tracked_user_still_carries_a_password`
compares password *counts*, and two-against-one is what it is there to notice.
Finish 1b before reading anything into it.

**`[ "${#NEW}" -eq 40 ]` is not belt and braces.** An empty `NEW` hashes to
`e3b0c442...`, a perfectly valid 64-hex digest that `ACL SETUSER` accepts, and
the verification below would then compare an empty `pw DEFAULT` against an empty
file against an empty directive and report four agreeing digests. It is the one
failure in this procedure that ends with every check green and the instance's
break-glass set to the hash of the empty string.

**Not `CONFIG SET requirepass`.** Verified on a scratch 7.0.15: it *replaces*
`default`'s password rather than adding one, and does not clear `off` - so it is
a fifth way to set the credential, wearing the name of the directive. Leave the
running value alone. It then reports the **retired** secret until the next
restart, which is correct and worth knowing: see *What `CONFIG GET requirepass`
does not tell you* below.

Verify by digest, printing no cleartext - all four must agree, and the live user
must still be `off` with exactly one password:

```bash
d() { tr -d '\n' | sha256sum | cut -c1-16; }
sudo sed -n 's/^user default off #\([0-9a-f]*\) .*/\1/p' /etc/redis/users.acl | cut -c1-16
pw DEFAULT | d
sudo cat /etc/redis/broker-password | d
sudo cat /etc/redis/redis.conf | sed -n 's/^requirepass //p' | d
rcli brokeradmin ACL GETUSER default        # -> off, one hash, the same one
```

**Nothing is committed.** `deploy/redis.conf.broker` holds `__REQUIREPASS__` and
`deploy/redis-acl.conf` holds `>__DEFAULT_PW__`, and the two live-comparison
tests exclude the value by name - `NOT_COMPARED` in
`test_live_broker_matches_tracked_config.py` and in
`test_live_acl_matches_tracked_acl.py`. The suite is the confirmation, not the
assumption: run `uv run pytest tests/deploy` after, and `git status` should be
clean.

**Done 2026-09-23**, as `acladmin`, `default` never enabled, retired digest
`9c4ea97f...`. No service reconnected, no window opened, 191 deploy tests green
after.

### What `CONFIG GET requirepass` does not tell you

Once an aclfile declares `default`, **the aclfile's line wins and `requirepass`
is not authoritative for authentication.** Verified on a scratch 7.0.15: a server
started with `requirepass fromredisconf` and an aclfile saying
`user default on >fromaclfile` authenticates `fromaclfile` and answers
`WRONGPASS` to `fromredisconf` - while `CONFIG GET requirepass` still reports
`fromredisconf`. **It reports a value that does not authenticate.**

That is why the directive's only job here is the last-resort path where the
`aclfile` line is commented out, and why the rotation writes the file rather
than the running config. `tests/deploy/test_live_broker_matches_tracked_config.py`
still asserts the live `requirepass` is neither empty nor the placeholder - an
empty one is `nopass` by another door, on the one path where the directive does
govern - and its docstring says which guarantee that is and which it is not.

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
