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
the `cred` and `rcli` helpers defined at the top of RESTART-WINDOW.md; `rcli` takes
the ACL user as its first argument, and works for the two users this node holds
a credential for, `acladmin` and `brokeradmin` ([NODE-CREDENTIALS.md](NODE-CREDENTIALS.md)). The
steps are recorded as they ran on 2026-09-10, when those helpers read plaintext
from the passwords file; since CannObserv/broker#52 none is held there.

Moved out of RESTART-WINDOW.md on 2026-09-11, when the runbook ran past the
per-doc context budget.

## Before the window - no downtime, do this first

### 1. Mint the ACL passwords

One per service minted on this node. A service minted since
CannObserv/broker#72 is not in the loop - see the hash-only handoff below. These
are the plaintexts
that leave the node: each is handed to its service and then replaced here by its
digest (below). The two users that stay on the node, `acladmin` and
`brokeradmin`, are minted straight into encrypted credentials and never touch
this file in plaintext - [NODE-CREDENTIALS.md](NODE-CREDENTIALS.md).

`default` depends on the cluster. **On a migrating cluster** whose services still
say `default:`, `__DEFAULT_PW__` is the current `requirepass` value, not a new
one, and the tracked `default` line has to read `on ~* &* +@all` for the first
load - that is what makes the first restart a no-op for every service. Step 4
makes it the tombstone. **On a new cluster**, or once step 4 has run, it is the
digest of a value minted and discarded, and the line is the tombstone from the
start. This cluster ran the first shape on 2026-09-10 and has been in the second
since CannObserv/broker#52.

```bash
# In a script that sets pipefail, `head` closing the pipe SIGPIPEs `tr` and the
# subshell exits 141 - so the mint disables it for itself and asserts the length.
mint() { set +o pipefail; LC_ALL=C tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 40; }
sudo install -m 0400 -o root -g root /dev/null /etc/redis/broker-acl-passwords
for p in ARCHIVER WATCHER REPLICATOR; do
    echo "__${p}_PW__=$(mint)"
done | sudo tee -a /etc/redis/broker-acl-passwords >/dev/null
# default, on a new cluster: the digest of a value nobody keeps.
echo "__DEFAULT_PW_SHA256__=$(mint | sha256sum | cut -d' ' -f1)" \
    | sudo tee -a /etc/redis/broker-acl-passwords >/dev/null
sudo chmod 0400 /etc/redis/broker-acl-passwords
awk -F= '{ printf "%-24s %s chars\n", $1, length($2) }' \
    <(sudo cat /etc/redis/broker-acl-passwords)     # -> 40 each, 64 for a digest; CHECK IT
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

**Then take the service plaintexts off this node.** The mint writes each in
plaintext because each has to be handed to its service. Once `archiver`,
`watcher` and `replicator` hold theirs - *Step 2 - each service onto its own
credential*, after the window, not preparation item 2 below - replace each of
those three lines with
`__<USER>_PW_SHA256__=<its digest>` - deploy/README.md, *Changing a grant*.
Nothing here authenticates as them, and
`test_the_node_holds_no_plaintext_for_any_user` fails until it is done
(CannObserv/broker#49, #52). `observo`'s went through that interval on 2026-09-24,
minted ahead of its consumer (CannObserv/broker#62): read out of the passwords
file into the operator's password manager, verified by a `PING` from
`observo-primary` as `observo`, written to `/etc/observo/.env`, and only then
replaced here by its digest. (The user was deleted by CannObserv/broker#75,
never having connected.)

**A service minted now never passes through that interval: its handoff is
hash-only.** Since CannObserv/broker#72 the hourly backup refuses a plaintext
line in the passwords file, so #62's order would fail every run until the
service took its credential. `processor` (CannObserv/broker#75) is the first
minted this way, in CannObserv/archiver#251's shape:

1. **On the service's host**, the mint above into its env file - 40
   alphanumerics, length asserted - and to the password manager. The service
   sends this node its digest alone, `printf %s "$pw" | sha256sum | cut -d' ' -f1`.
2. **Here**, `rcli acladmin ACL SETUSER <user> on "#<digest>" <the tracked
   rules>`, `ACL SAVE`, and `__<USER>_PW_SHA256__=<digest>` appended to the
   passwords file. A digest may sit on a command line; it is not the credential.
3. **From the service's host**, a `PING` as the user (`REDISCLI_AUTH` plus
   `--user`), which is the only verification there is: this node holds no
   plaintext to test with, before or after.

### 2. Dry-run the real file against a throwaway server

`tests/deploy/test_redis_acl.py` already does this with throwaway passwords on
every test run. This repeats it with the **real** passwords file, which is the
only thing that catches a malformed passwords file - and since
CannObserv/broker#49 the suite repeats that too, on the node, as
`test_the_nodes_passwords_file_renders_the_credentials_that_are_live`. The
rendered file is `#<sha256>` for every user, plaintext or digest line alike, so
the check file below holds no secret; it is shredded anyway.

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
P="$(cred acladmin)" && [ -n "$P" ] && REDISCLI_AUTH="$P" redis-cli --user acladmin -h 127.0.0.1 -p 6399 ACL LIST
sudo kill "$(sudo cat /root/aclcheck.pid)"              # no user holds +shutdown, by design
sudo shred -u /root/users.acl.check /root/aclcheck.log
```

**`PING` must return `NOAUTH`.** If it returns `PONG`, stop - see *The `nopass`
trap* below. `ACL LIST` must show seven users: `archiver`, `watcher`,
`replicator`, `processor`, `brokeradmin`, `acladmin`, and `default` - **`off`**,
`-@all`, still carrying a password hash. No test credential: `citest` was
deleted by CannObserv/broker#53.

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
| processor | `/etc/processor/.env`, on `co-processor`, as `CO_PROCESSOR_BUS_URL` - read by the `processor` unit since 2026-10-02 (CannObserv/broker#75) |

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

For each of archiver, watcher, replicator and processor, on its own host:

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
# /etc/broker/.env: BROKER_REDIS_URL -> redis://brokeradmin@localhost:6379/0
#   - the user alone; the password is the unit's encrypted credential
#   (NODE-CREDENTIALS.md; CannObserv/broker#52)
sudo systemctl start broker-bus-health.service
journalctl -u broker-bus-health -n 5 -o cat --no-pager   # -> finding_count: 0
```

`brokeradmin` is the probe's tick and nothing else since CannObserv/broker#52:
`INFO`, `SCAN`, `EXISTS`, `XINFO`, `XRANGE`. On 2026-09-10 it was also the
operator's read identity and the DLQ backstop; both moved to `acladmin`. If the
probe reports `broker unreachable or probe failed: NoPermissionError`, it is a
missing grant, not an outage.

### Step 4 - retire the shared password

**Done 2026-09-10 14:18 UTC**, as `acladmin`. Recorded as it was run, so the
next cluster - or this one after broker#4 rebuilds it - has the shape.

**Last, and only once steps 2 and 3 are confirmed for all four clients.** The
check is not "the services look fine"; it is that no connection is
authenticated as `default`:

```bash
rcli acladmin CLIENT LIST | grep -oE 'user=[^ ]+' | sort | uniq -c
#   4 user=archiver  1 user=brokeradmin  3 user=replicator  3 user=watcher  - and no user=default
rcli acladmin ACL LOG 5                                   # explained: redis-acl.conf's grant provenance (header or stanza) or its not-a-fault list names each entry
rcli acladmin ACL LIST | grep '^user acladmin'            # precondition, not optional
```

```bash
# As run on 2026-09-10: `off` alone. A new cluster goes straight to the
# tombstone, as CannObserv/broker#52 did here on 2026-09-29 - no grants, and a
# password that is the digest of a value nobody keeps:
DH="$(mint | sha256sum | cut -d' ' -f1)"
rcli acladmin ACL SETUSER default off resetpass "#$DH" resetkeys resetchannels -@all
rcli acladmin ACL SAVE
```

Then verify every axis, not only the one that changed:

```bash
redis-cli PING                                             # -> NOAUTH Authentication required.
rcli acladmin ACL GETUSER default                          # -> flags off, one hash, commands -@all
rcli brokeradmin INFO server | grep redis_version          # the probe's credential still answers
for u in archiver watcher replicator processor; do
    echo "$u: held by digest on this node - verify from its own host"
done
rcli acladmin CLIENT LIST | grep -c 'flags=b'              # same count as before the flip
sudo grep '^user default' /etc/redis/users.acl             # -> user default off #<hash> resetchannels -@all
```

**The skip in that loop is the general case, not an archiver exception.** Any
verification that authenticates *as* a service stops working the moment that
service's credential is rotated by a hash-only handoff: this node then holds a
digest and no plaintext, the old `pw` helper returned empty, and what used to print `PONG`
prints `WRONGPASS` - which also writes an `AUTH` / `reason: auth` entry into
`ACL LOG` naming the service whose credential was just rotated, the most
alarming thing that log can say about a node where nothing is wrong. That is
what `archiver` did on 2026-09-23 (CannObserv/archiver#251), and since
CannObserv/broker#49 converted the rest the same day, it is what **every**
service user does: `archiver`, `watcher` and `replicator` are all
held by digest alone, and `processor` has been since its hash-only mint
(CannObserv/broker#75). The replacements are verification **from the
service's own host**, or an assertion about the ACL rather than about
authentication - `rcli acladmin ACL GETUSER <user>` showing exactly one
password hash.

`ACL SETUSER default off` does **not** disconnect clients already authenticated
as `default` on this Redis (7.0): they keep working until they reconnect, and
then fail. That is why the `CLIENT LIST` check comes first - a straggler would
break at its next restart rather than now, and look healthy in between.

Then update `deploy/redis-acl.conf` to `user default off >__DEFAULT_PW__
resetchannels -@all`, and the passwords file's `default` line to
`__DEFAULT_PW_SHA256__=$DH`, and commit, so the tracked file matches what
`ACL SAVE` wrote. `tests/deploy/test_redis_acl.py` pins the line as declared,
`off`, carrying a password, and granting nothing.

**There is no reversing it, and nothing needs to.** Until CannObserv/broker#52
the reversal - `ACL SETUSER default on` - was also how every restart window
opened, because no other user could `BGREWRITEAOF`, `CONFIG SET` or `SAVE`.
`acladmin` holds those now, and an enabled tombstone grants nothing. A
migrating cluster that needs `default` back for a straggler restores the grants
and a known password in one `ACL SETUSER`, as `acladmin`, and should ask why
first.

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
On 2026-09-10 `acladmin` was refused `XADD`, `XLEN`, `INFO`, `CONFIG SET` and
`FLUSHALL`; since CannObserv/broker#52 it holds the operator's reads and the
window's commands by name, and is still refused `XADD` and `FLUSHALL`. The split
that survives is the other direction: the probe's credential changes nothing.

**Keep a shell on the node** through step 4 regardless. The last-resort recovery
is still `redis-server`'s own config - restart with the `aclfile` line commented
out, restoring `requirepass` behaviour - and that is a cohort-wide event, which
is exactly why `acladmin` is the path you want to reach for first.

---

## Node credentials, `requirepass`, and `default`'s retired rotation

Moved to [NODE-CREDENTIALS.md](NODE-CREDENTIALS.md) when CannObserv/broker#52
pushed this file past its context budget: minting and rotating `acladmin` and
`brokeradmin`, why `requirepass` belongs to no user, and the record of #46's
four-write rotation of `default`, which #52 retired.

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

**Who can read it is who holds `+config|get`**, and Redis 7.0 cannot narrow that
grant to a parameter (CannObserv/broker#50). Until CannObserv/broker#52 the
running value was `default`'s break-glass password, so each holder was a reader
of it. All three services lost the grant on 2026-09-23: watcher and replicator
first - neither ever issued `CONFIG` - then archiver, once
CannObserv/archiver#257 moved its floor check's cap read to `INFO memory`. The
probe lost it in #52, when the config mirror test moved to `acladmin`; and the
value it would read is nobody's password now anyway.

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
including `co-processor` (`tag:processor`, admitted to this node's port by a
policy rule on 2026-09-29 for CannObserv/broker#75, and authenticating as its
own user) - arriving as a *side
effect of enabling the mechanism meant to prevent it*.

`deploy/redis-acl.conf` therefore always declares `default`, and
`test_default_is_declared_disabled_and_still_carries_a_password` fails if the
line is ever removed - or if it loses its password, which would turn the
rollback `ACL SETUSER default on` into the same trap by another door. **The
one-line check is `redis-cli PING` with no credentials at all.**
It appears twice for this reason: in the dry run above, and after the restart in [RESTART-WINDOW.md](RESTART-WINDOW.md) step 1d.
