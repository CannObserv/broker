# Recovery: losing `co-broker`

Runbook for CannObserv/broker#4. What is backed up, how, what it costs, and how
to rebuild the node from it. The restore is rehearsed by
`tests/deploy/test_backup_restore_rehearsal.py` against a real `redis-server`
on every test run, and by hand against a real object - the record is at the
bottom.

---

## What is exposed

`/var/lib/redis` is the only copy of:

- every stream's entries;
- **every consumer group's `last-delivered-id`** and PEL - the messages
  delivered and not yet acked;
- replicator's `replicator:cmd:fetch:*` expiring state.

A group's position is recoverable from nowhere else. Losing it means every
group re-provisions at `ensure_group`'s default `$`, which is silent and skips
everything between the lost position and the tail - the failure broker#1 went
to some length to avoid during a *planned* migration, arriving unplanned. And
a single-purpose node's loss presents as an idle cluster: participants connect,
find nothing, and log nothing interesting.

`appendonly yes` protects against a process restart, not against the disk.
The AOF history is also shorter-lived than it looks: the base is 89 bytes and
the incr file holds every write since the AOF was enabled, growing at a few MB
a day, and `auto-aof-rewrite-min-size 64mb` will collapse it into a fresh base
on its own within a couple of weeks of writing this. The 2026-09-10 incident
was recovered by replaying that history; that was margin, not design.

---

## The backup

| | |
|---|---|
| **What** | the server's own `dump.rdb` - rewritten atomically at its `save` points (`3600 1`, `300 100`, `60 10000`), so a copy at any moment is a consistent point in time |
| **When** | hourly - `broker-backup.timer`, `OnCalendar=hourly`, five minutes of jitter, `Persistent=true` so a reboot's missed tick still runs |
| **Where** | `gs://co-gcs-broker-backup/co-broker/<snapshot time>.rdb.gz` - the prefix is the hostname |
| **Named by** | the snapshot's own `ctime`, the aux field the save writes into the file, as `20260910T153511Z`. A listing reads as a timeline and the newest name is the newest data |
| **Verified** | `redis-check-rdb` on the private copy before anything is uploaded - the format, every record, the CRC64 trailer. A file that fails is refused |
| **Described** | object metadata: `snapshot_at`, `sha256`, `size_bytes`, `redis_version`, `rdb_version`, `keys`, `source_host` - readable without downloading |
| **Retained** | 30 days, by the **bucket's lifecycle rule**. The node's identity cannot delete |
| **RPO** | the backup interval plus the save interval: under two hours worst case, about an hour typically |
| **Credential** | `/etc/broker/co-broker-backup.json` (0400 root:root), `roles/storage.objectCreator` + `roles/storage.objectViewer` on that one bucket. **No Redis credential** - the job reads a file |
| **Watched by** | the probe: `backup` findings (never completed, last attempt failed, last success too old) and `persistence` findings (`rdb_last_bgsave_status` and friends, and changes left unsaved for over three hours - the case where every hourly backup is the same stale file, judged from the server because only it can tell idle from broken), both reaching notifier |

It holds no opinion on the AOF (never shipped - point in time is the contract),
on configuration (this repo is the copy), or on secrets (below).

**Why the file, and not `BGSAVE` or `redis-cli --rdb`.** Both would need a
grant on the broker and both fork the server; the file needs neither, is
consistent by construction (the server renames a complete temp file into
place, and a copy holding the old inode reads it to the end), and its freshness
is bounded by the `save` points and reported by the probe when they stop. If
the RPO ever needs tightening, `redis-cli --rdb` under a user holding
`+sync +replconf` is the upgrade path; nothing else changes.

**Why create-only.** Overwriting an object needs `storage.objects.delete`, and
the identity does not hold it, so the history is append-only at IAM: a node
that is compromised can add snapshots and cannot destroy them.
`if_generation_match=0` says the same thing in code, and shipping the same
snapshot twice - an hour with no `save` point - is a 412 the job reports as
`unchanged`, a success. Retention lives in the bucket's lifecycle rule for the
same reason it does for replicator's temp store: the process that writes must
not be the process that can erase. Same property as replicator's replicate
writer, for the same reason.

**Why root.** `/var/lib/redis` is 0750 `redis:redis` and `dump.rdb` is 0660,
so the reader is root or a member of the `redis` group - and the group holds
*write* on the live snapshot. Root confined by the unit's sandbox is the safer
of the two: `ProtectSystem=strict` makes the filesystem read-only except the
state directory and a private `/tmp`, and the only capability left is the one
that reads. Verified before the credential existed: the unit reads the
snapshot, runs the checker, compresses, reaches GCS, and can write nothing of
Redis's.

---

## The trap: a restored RDB is ignored under `appendonly yes`

broker#4 said a restored snapshot "needs the AOF directory removed for it to
be read at all". It is the other way round, and the difference is an empty
broker that looks like a successful restore.

**Redis 7 with `appendonly yes` loads the AOF manifest and nothing else.** A
`dump.rdb` beside an absent `appendonlydir` is not read: the server starts
empty, logs `Creating AOF base file ... on server start`, and writes a fresh
base over the silence. `tests/deploy/test_backup_restore_rehearsal.py` pins
that behaviour so a Redis upgrade that changes it is noticed rather than
assumed.

So the snapshot has to *become* the base of a multi-part AOF. That is all
`src/broker/restore.py` does:

```
/var/lib/redis/appendonlydir/
    appendonly.aof.1.base.rdb    <- the snapshot, byte for byte
    appendonly.aof.1.incr.aof    <- empty
    appendonly.aof.manifest      <- file appendonly.aof.1.base.rdb seq 1 type b
                                    file appendonly.aof.1.incr.aof seq 1 type i
```

Redis recognises the base by its `REDIS` magic, loads it, opens the incr file,
and the next write appends there. Every stream and every group's position and
PEL come back exactly, and every TTL comes back as its absolute expiry - a key
whose expiry passed during the outage is gone, correctly. The rehearsal asserts
each one.

`restore.py` refuses to touch an existing `appendonlydir`. Moving one aside is
the operator's decision, made by hand, because the directory it would replace
may be the better copy.

---

## Rebuilding the node

### 0. What must exist outside the node

None of these are in the backup, and the runbook stops without them. Keep
them in the operator's password manager; nothing in this repo ships them
anywhere.

| Secret | Path on the node | Mode | Why the value matters |
|---|---|---|---|
| the Redis `requirepass` | `/etc/redis/broker-password` | 0400 root | also `default`'s ACL password; the window-only identity |
| the six ACL passwords | `/etc/redis/broker-acl-passwords` | 0400 root | **the services carry these in their own env files** - a rebuild must reuse the same values or update three services on two hosts |
| the probe's env | `/etc/broker/.env` | 0640 root:exedev | `BROKER_REDIS_URL` (`brokeradmin`), `GOOGLE_APPLICATION_CREDENTIALS` (the wheelhouse reader) |
| the wheelhouse reader | `/etc/broker/co-pypi-reader.json` | 0640 root:exedev | `uv sync` needs it |
| the notifier check-in | `/etc/broker/notifier.env` | 0400 root | the monitor id is node-agnostic; the same monitor continues |
| the backup writer | `/etc/broker/backup.env` + `/etc/broker/co-broker-backup.json` | 0400 root | also what the **restore** reads with - `objectViewer` lists and downloads |

Plus the tailnet: the services connect to `broker` by name, so the new node
has to join as `broker` under `tag:broker`, and the old one has to be removed
so the name is free. That is done in the Tailscale admin console, not here.

### 1. The VM

Same distro family, **Redis 7.0.x or newer** - a newer server loads an older
RDB; an older one refuses a newer (the object's `rdb_version` metadata says
which it is; 10 is 7.0). `apt install redis-server` starts a default-config
server; **stop it** before touching `/var/lib/redis`.

```bash
sudo systemctl stop redis-server
sudo ls -la /var/lib/redis        # a default-config dump.rdb is fine; no appendonlydir yet
```

### 2. The repo, the config, the secrets

Everything in `deploy/README.md`'s *Install* section, in order: restore the
secrets at the paths and modes above, append `redis.conf.broker` with the
password substituted, render and install the ACL file, the drop-in and the
wait script, the units. Then:

```bash
set -a; . /etc/broker/.env; set +a
uv run --no-project --with 'google-cloud-storage>=2,<4' python scripts/sync_wheelhouse.py
uv sync --group dev
```

Do not start `redis-server` yet. `appendonly yes` is now in its config, and
started empty it would create the fresh base the trap above describes.

### 3. The data

```bash
sudo sh -c 'set -a; . /etc/broker/backup.env; set +a
  /home/exedev/broker/.venv/bin/python -m src.broker.restore --list'          # newest first
sudo sh -c 'set -a; . /etc/broker/backup.env; set +a
  /home/exedev/broker/.venv/bin/python -m src.broker.restore --latest --into /var/lib/redis'
```

`--object <name>` for a specific snapshot rather than the newest; `--file
<path>` for one already on disk (`.rdb` or `.rdb.gz`). The command prints what
it staged and the three lines to run next:

```bash
sudo chown -R redis:redis /var/lib/redis/appendonlydir
sudo chmod 0750 /var/lib/redis/appendonlydir && sudo chmod 0640 /var/lib/redis/appendonlydir/*
sudo systemctl start redis-server
```

### 4. Verify the positions, not the key count

```bash
pw() { sudo sed -n "s/^__${1}_PW__=//p" /etc/redis/broker-acl-passwords; }
B="redis://brokeradmin:$(pw BROKERADMIN)@localhost:6379/0"

journalctl -u redis-server -n 20 -o cat --no-pager | grep -E 'loaded from base file|DB index'
redis-cli -u "$B" --no-auth-warning INFO keyspace   # db0:keys=N,expires=M - the rule is below

for s in info.changes info.registry info.watch-status content.fetch content.fetch-policy \
         content.blobs content.revisions content.artifacts content.replicate; do
    printf '%-22s %s\n' "$s" "$(redis-cli -u "$B" --no-auth-warning XLEN "$s")"
done
for s in content.fetch content.revisions content.artifacts content.replicate content.blobs; do
    echo "== $s"; redis-cli -u "$B" --no-auth-warning XINFO GROUPS "$s"    # last-delivered-id, pending, lag
done
```

`DB loaded from base file appendonly.aof.1.base.rdb` in the journal is the
line that says the snapshot was read. `Creating AOF base file` is the line
that says it was not - stop, the directory is wrong.

The key count is a bound, not an equality. `keys` is at most the object's
`keys` metadata (`restore --list` prints it), short by exactly the
`replicator:cmd:fetch:*` guards whose expiry passed between the snapshot and
now - and the base loads those before the expiry cycle removes them, so for a
second after start the count can read higher than it will settle at. After an
outage longer than a guard's TTL, `expires` is 0, and that is correct:
replicator refetches those commands unguarded, which is what a guard expiring
means. The checks that must be exact are the `XLEN` and `XINFO GROUPS` lines.
(`DBSIZE` is not granted to `brokeradmin`, and is not needed.)

### 5. What the participants see

They reconnect on their own; all three classify a broker outage as transient.
Then:

- **Entries published after the snapshot are gone from the bus.** Archiver's
  outbox has already marked them published, so nothing re-emits them; the
  facts still exist in archiver's database, not on the bus. That is the RPO,
  stated honestly.
- **Group positions roll back consistently** with the entries, so nothing is
  skipped and nothing is double-delivered beyond the PEL.
- **PEL entries are redelivered** through each consumer's `XAUTOCLAIM`, which
  is what the PEL is for.
- **Watcher's LWW streams** republish their full set within five minutes.
- **Replicator's `cmd:fetch:*` guards** come back with their absolute
  expiries, so any that lapsed during the outage are already gone and
  replicator refetches those commands unguarded - which is what a guard
  expiring means.

### 6. Close out

```bash
sudo systemctl enable --now broker-bus-health.timer broker-backup.timer
sudo systemctl start broker-backup.service && journalctl -u broker-backup -n 3 -o cat --no-pager
set -a; . /etc/broker/.env; set +a
uv run pytest tests/deploy -q          # 0 skipped on a node that is fully installed
```

The first probe tick after the timer starts should be `finding_count: 0`; the
`stream-reset` check will not fire, because `entries-added` on a restored
stream is whatever the snapshot recorded and the probe's own state file was
lost with the node.

---

## Same node, lost data

When the node is fine and the data is not - a wipe, a bad restart - the AOF
may be the better copy, and the incident in `docs/RESTART-WINDOW.md` was
recovered exactly that way. Check it first:

```bash
sudo systemctl stop redis-server
sudo redis-check-aof /var/lib/redis/appendonlydir/appendonly.aof.manifest
```

If it is intact and holds what was lost, a restart replays it and this runbook
is not needed. If not, move it aside - **never delete it** - and continue from
step 3:

```bash
sudo mv /var/lib/redis/appendonlydir "/var/lib/redis/appendonlydir.$(date -u +%Y%m%dT%H%M%SZ)"
```

---

## Verifying a backup by hand

```bash
sudo sh -c 'set -a; . /etc/broker/backup.env; set +a
  /home/exedev/broker/.venv/bin/python -m src.broker.restore --list'       # newest first, with keys and sha256
sudo sh -c 'set -a; . /etc/broker/backup.env; set +a
  /home/exedev/broker/.venv/bin/python -m src.broker.restore --latest --into /tmp/rehearsal'
sudo chown -R "$(id -u)" /tmp/rehearsal       # staged as root; the throwaway server runs as you
redis-check-rdb /tmp/rehearsal/appendonlydir/appendonly.aof.1.base.rdb   # \o/ RDB looks OK! \o/  N keys read, M expires
sha256sum /tmp/rehearsal/appendonlydir/appendonly.aof.1.base.rdb         # == the sha256 `--list` printed for it
redis-server --port 6399 --bind 127.0.0.1 --dir /tmp/rehearsal --appendonly yes --save '' \
    --daemonize yes --pidfile /tmp/rehearsal/pid --logfile /tmp/rehearsal/log
sleep 1; grep -E 'loaded from base file|Creating AOF base' /tmp/rehearsal/log   # the first line, never the second
redis-cli -p 6399 INFO keyspace
for s in content.fetch content.revisions content.artifacts content.replicate content.blobs; do
    echo "== $s"; redis-cli -p 6399 XINFO GROUPS "$s"
done
redis-cli -p 6399 SHUTDOWN NOSAVE; rm -rf /tmp/rehearsal   # nothing left listening, nothing left on disk
```

The throwaway server holds a copy of production data, unauthenticated, on
loopback; the last line is not optional.

The state file the job writes is `/var/lib/broker-backup/state.json`; the
probe reads it every ten minutes and its `backup` finding is what turns a
silent failure into a notifier alert.

---

## Provisioning the bucket and the writer

**Done 2026-09-10.** The writer is the account the block below creates,
`co-broker-backup` in project `co-gcs`; the block is kept as it was run, for the
next cluster. Run it from a workstation with `roles/storage.admin`: the node's
identity cannot read the bucket's own metadata, so the location and the
lifecycle rule are verifiable only from there (the same blindness replicator's
writer has). The one value this record does not carry is the location - it was
created to match `co-gcs-blobs`, and the node cannot read it back; the
`describe` at the end of the block is where to confirm both it and the rule.

```bash
PROJECT=co-gcs
BUCKET=co-gcs-broker-backup
SA=co-broker-backup

gcloud storage buckets create "gs://$BUCKET" --project="$PROJECT" --location=<same as co-gcs-blobs> \
    --uniform-bucket-level-access --public-access-prevention
printf '{"rule":[{"action":{"type":"Delete"},"condition":{"age":30}}]}\n' > /tmp/lifecycle.json
gcloud storage buckets update "gs://$BUCKET" --lifecycle-file=/tmp/lifecycle.json
gcloud storage buckets describe "gs://$BUCKET" --format="yaml(location, lifecycle_config)"
#   those are `gcloud storage`'s key names; the API's camelCase spellings print nothing rather than erroring

gcloud iam service-accounts create "$SA" --project="$PROJECT" --display-name="co-broker RDB backup writer"
for role in roles/storage.objectCreator roles/storage.objectViewer; do
    gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" \
        --member="serviceAccount:$SA@$PROJECT.iam.gserviceaccount.com" --role="$role"
done
gcloud iam service-accounts keys create co-broker-backup.json \
    --iam-account="$SA@$PROJECT.iam.gserviceaccount.com"
```

Two predefined roles rather than replicator's custom one, because the backup
needs no `update`: `objectCreator` is `create` alone, `objectViewer` is `get`
and `list`, and nothing in either deletes or overwrites. Leave the bucket's
soft-delete default (seven days) in place - on a backup bucket it is a second
safety net against an administrator's mistake, not litter. Versioning off.

On the node:

```bash
sudo install -m 0400 -o root -g root co-broker-backup.json /etc/broker/co-broker-backup.json
sudo systemctl enable --now broker-backup.timer
sudo systemctl start broker-backup.service && journalctl -u broker-backup -n 3 -o cat --no-pager
```

`/etc/broker/backup.env` already names that path and the bucket.

---

## Rehearsal record

- **Every test run**: `tests/deploy/test_backup_restore_rehearsal.py` seeds a
  real `redis-server` with a stream, a group two entries in with one acked, and
  a TTL key; `SAVE`s; ships the file through `run_backup` into a fake bucket;
  restores it through `restore.newest_object` / `download` / `gunzip_file` /
  `stage_appendonlydir`; starts a second server under `appendonly yes` and
  asserts the length, `last-delivered-id`, `entries-read`, the PEL and the TTL.
  The control case, the same snapshot with no staging, comes up with `DBSIZE 0`.
- **2026-09-10, by hand** on `co-broker`, throwaway servers, Redis 7.0.15:
  positions intact; the control confirmed the trap.
- **2026-09-10 16:03 UTC, before the key existed.** The unit was started once
  to prove the sandbox: it loaded its env, failed at the absent key file, and
  recorded `last_failure_at` under `ProtectSystem=strict`. The state directory
  was then removed by hand so the probe would read "not installed" until the
  timer was enabled - which is why the first success below has no failure
  beside it.
- **16:32 UTC, the first run with the key.** Created
  `co-broker/20260910T153511Z.rdb.gz`: 498,485 bytes of RDB, 144,810 gzipped,
  37 keys. Every field the object's metadata carries matched the state file.
  The 27 keys with TTLs are `redis-check-rdb`'s `expires` line, which neither
  carries.
- **16:34 UTC, a second run.** `unchanged`: the 412 that *Why create-only*
  predicts, observed - not a 403 from the missing `delete`.
- **16:35 UTC, the restore.** `restore --latest` staged that object on a
  scratch directory; `redis-check-rdb` passed and the base's sha256 matched the
  metadata. A throwaway server under `appendonly yes` logged `DB loaded from
  base file` and came up with 36 of the 37 keys (the 37th a
  `replicator:cmd:fetch:*` guard whose expiry had passed in the hour since the
  snapshot) and every group at exactly its snapshot position:
  `replicator.fetch` and `watcher.blobs` at `entries-read 993` against the live
  broker's 994 an hour on, the other three identical to live. The server was
  shut down and the directory removed.
- **Minutes later, the probe.** Its next tick with the state file present
  raised no `backup` finding: `finding_count: 0`.

---

## What this does not cover

- Anything written after the newest snapshot: the RPO above.
- The AOF's history. Point in time is the contract.
- The secrets, the tailnet identity, and the policy that admits the node -
  the operator's, listed in step 0 so they are a checklist rather than a
  discovery.
- The participants' own state. Their databases and stores are theirs.
