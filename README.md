# broker

Operational code for the Cannabis Observer **change-bus broker** - the Redis
Streams instance the cluster's services publish to and consume from: four,
Processor the newest, consuming since 2026-10-02 (broker#62, #75), and a fifth,
Provisioner, declared ahead of its go-live (broker#78).

This repo owns the broker's *tuning*, its *monitoring*, and the *cluster stream
inventory*. It owns no application logic and no data model. Nothing here is
imported by any service; the services reach the broker over the network, by URL.

| Path | What it is |
|---|---|
| [`deploy/redis.conf.broker`](deploy/redis.conf.broker) | The broker's tuning as deployed - bind, auth, AOF persistence, `noeviction`, an explicit `maxmemory` cap. Appended to `/etc/redis/redis.conf`, with the credential templated |
| [`deploy/redis-acl.conf`](deploy/redis-acl.conf) + [`deploy/render-acl.sh`](deploy/render-acl.sh) | The per-service ACL users - what actually scopes each service to its own streams, rather than documenting it. Since broker#14 `+xadd`/`+xtrim`/`+set` ride **selectors** naming only what each service produces, so a consumer cannot publish to the stream it reads; since broker#43 the group commands ride a selector naming only what it consumes, so a producer cannot take delivery in its consumer's group. Rendered to `/etc/redis/users.acl`; changed live as `acladmin` and mirrored back |
| [`deploy/redis-server.service.d/broker.conf`](deploy/redis-server.service.d/broker.conf) + [`deploy/wait-for-tailnet-addr.sh`](deploy/wait-for-tailnet-addr.sh) | Unit ordering only: `After=tailscaled` plus the `/proc/net/fib_trie` wait that R1's boot race exists for |
| [`deploy/broker-bus-health.service`](deploy/broker-bus-health.service) / [`.timer`](deploy/broker-bus-health.timer) | The periodic WARN-only health probe, every 10 minutes |
| [`src/broker/bus_health.py`](src/broker/bus_health.py) | The probe: memory headroom **and eviction policy**, per-stream length against retention caps, last-entry age on groupless streams, per-group `pending`, **the age of what a consumer group has not been delivered** - the check a stopped reader is otherwise invisible to, since pending counts only what was delivered; both, and whether the group exists at all, come out of one `XINFO GROUPS` per stream (#29) - **DLQ depth and continuity** - a queue drained between ticks is reported from its monotonic `entries-added` even though depth never saw it - disk, persistence status, and the backup's freshness |
| [`deploy/broker-backup.service`](deploy/broker-backup.service) / [`.timer`](deploy/broker-backup.timer) | The hourly RDB backup - root confined to read-only everything but its own state directory, holding no Redis credential (broker#4) |
| [`deploy/sysctl.d/`](deploy/sysctl.d/60-broker-memory.conf), [`system.slice.d/`](deploy/system.slice.d/broker-memory.conf), the two `memory.conf` drop-ins, [`earlyoom.default`](deploy/earlyoom.default) | The node's defence against dev tooling's memory pressure (broker#21): a reserve for atomic allocations, the memory-overcommit mode redis asks for, `MemoryLow=` and `OOMScoreAdjust=` for the bus and its network path, and earlyoom's config, installed and disabled (broker#58; kept off after broker#71 made sessions killable, since the kernel takes them before the bus). [`deploy/README.md`](deploy/README.md#memory-protection-broker21) |
| [`src/broker/backup.py`](src/broker/backup.py) / [`restore.py`](src/broker/restore.py) | The job: verify `dump.rdb`, gzip, create-only upload named by the snapshot's time. And the restore: stage a snapshot as the AOF base, which is the only way Redis 7 will load it under `appendonly yes` |
| [`docs/STREAMS.md`](docs/STREAMS.md) | The cluster stream inventory - who produces, who consumes, which health primitive applies, and who drains each DLQ. The producer column is what the ACL's publish selectors are derived from |
| [`docs/DLQ-DRAINING.md`](docs/DLQ-DRAINING.md) | Who writes, triages and backstops each dead-letter queue, the probe's evidence capture, and the `XTRIM MINID` drain procedure - split from STREAMS.md on 2026-09-29 |
| [`docs/NETWORK-PATHS.md`](docs/NETWORK-PATHS.md) | Where each participant runs (the table `CLIENT LIST` is checked against), the measured latency from each, the path beside every number, and the accepted DERP risk |
| [`docs/NON-STREAM-KEYS.md`](docs/NON-STREAM-KEYS.md) | Every key on the instance that is not a stream - Replicator's volatile dedupe keys - and what losing them costs; split from STREAMS.md on 2026-10-07 |
| [`docs/CONSUMER-REGISTRATIONS.md`](docs/CONSUMER-REGISTRATIONS.md) | The one-time reap of orphaned consumer registrations, and why it cannot recur |
| [`docs/BUS-HEALTH.md`](docs/BUS-HEALTH.md) | What the probe watches and why: the per-stream monitoring contracts, its stream, memory, DLQ and disk checks, loss detection, and the constants it mirrors (its `backup` and `persistence` findings are in `docs/RECOVERY.md`) |
| [`docs/LWW-CAP.md`](docs/LWW-CAP.md) | The one threshold read off the stream rather than mirrored, because a corpus size is not a number anyone can mirror - split from BUS-HEALTH.md on 2026-10-07 |
| [`docs/MEMORY-PROTECTION.md`](docs/MEMORY-PROTECTION.md) | The `maxmemory` cap: that all three producers survive `OOM command not allowed` without dropping or dead-lettering, which commands stay admitted at the cap, and why `noeviction` is load-bearing beyond refusing writes |
| [`docs/UNDELIVERED-CONSUMERS.md`](docs/UNDELIVERED-CONSUMERS.md) | A consumer that stopped reading: why `pending`, `lag` and `idle` are each blind to it, and the positions the probe compares instead |
| [`docs/RECOVERY.md`](docs/RECOVERY.md) | Losing the node: what is exposed, the backup's design and its grant, the ACL digests shipped with it (broker#72), the rebuild runbook, the rehearsal record |
| [`docs/RESTART-WINDOW.md`](docs/RESTART-WINDOW.md) | The cohort restart window: the identities and the steps as run |
| [`docs/INCIDENT-2026-09-10.md`](docs/INCIDENT-2026-09-10.md) | What `databases 1` did to db0, and why `BGREWRITEAOF` comes before it |
| [`docs/ACL-CUTOVER.md`](docs/ACL-CUTOVER.md) | The per-service credential cutover around that window: the passwords, the dry run, each service onto its own user, retiring `default`, the `nopass` trap, and why `CONFIG GET requirepass` reports a value that does not authenticate (broker#46) |
| [`docs/NODE-CREDENTIALS.md`](docs/NODE-CREDENTIALS.md) | `acladmin` and `brokeradmin` as encrypted credentials: minting, rotating, what that does not protect; `requirepass` belonging to no user; `default`'s retired rotation (broker#52) |

## Provenance

Every file here moved out of **CannObserv/archiver** under
[archiver#193](https://github.com/CannObserv/archiver/issues/193) D6, tracked by
[broker#1](https://github.com/CannObserv/broker/issues/1) Phase 1.

Archiver operated the broker from the same VM it ran on (archiver#109). Once
the broker moved to a neutral node, two of these artifacts began measuring the
wrong machine: the config parity test asserted a path under
`/etc/systemd/system/` on *archiver's* host, and the probe's disk check - which
exists for **AOF headroom** - reported archiver's disk. Splitting the repo is
what makes them true again.

The epic's close, and what landed after it on the grants and reads this repo
owns:

- CannObserv/broker#1 - the relocation epic. Closed 2026-09-15, every sub-issue
  with it. #14 - confining each service's `+xadd`/`+xtrim` to the streams it
  produces - landed 2026-09-18 and is live on the node.
  #29 - one `XINFO GROUPS` per grouped stream for the position, the pending
  count and the group's existence - landed 2026-09-18.
  #34 - archiver's `+xtrim` narrowed to `~info.changes` and its two DLQs, live
  2026-09-22: nothing on the instance can `XTRIM` `info.registry` now, and
  its row in `docs/STREAMS.md` says why (CannObserv/archiver#234 answered).
  #59 then cut the two DLQs: archiver's `+xtrim` is `~info.changes` alone,
  and it disposes of dead letters by `+xdel` (live 2026-09-24).
  #41 took `+xtrim` off watcher and replicator, which trim nothing by
  decision (CannObserv/watcher#327, CannObserv/replicator#119), and folded
  each service's publish into one selector (live 2026-10-06): archiver's
  `info.changes` is the one stream outside `*.dlq` any identity can trim.
  #43 then moved the group commands off each root into a consume selector
  naming only the streams that service consumes in a group, and every read
  into a selector naming the streams its owner answered for
  (CannObserv/archiver#321, CannObserv/watcher#344, CannObserv/replicator#129),
  live 2026-10-06: no producer can take delivery in its consumer's group.

Onboarding and credential history since, moved from AGENTS.md's *Related*
(2026-09-29):

- CannObserv/broker#62 - Observo onboarded ahead of its consumer, 2026-09-24:
  ACL user `observo`, watcher's grants on the pair, the `content.process` /
  `content.derived` rows, two probed groups cannobserv v0.19.4 marks *pending
  broker#62*, and the co-core pin at `>=0.19.4`. The tailnet rule and the
  credential handoff (`CO_OBSERVO_BROKER_TOKEN` in `/etc/observo/.env`, the
  node's line a digest) were done the same day. Superseded by #75.
- CannObserv/broker#75 - the `content.process` consumer moved to a service of
  its own, Processor (`co-processor`), 2026-09-30: ACL user `processor` with
  #62's line unchanged, minted hash-only because #72's backup refuses a
  plaintext line; `observo` deleted, never having connected; the probe on
  `processor.process`; co-core renamed it upstream in cannobserv#503
  (`9dcc1c75`, unreleased as of 2026-10-05). Processor consumed from
  2026-10-02 and Watcher issued in shadow from 2026-10-03 (watcher#325): both
  groups live. NOPERM shown on this broker by a planned rehearsal, 2026-10-04:
  a refused publish left the command pending and the reclaim delivered it, no
  strike. The `tag:observo-primary` tailnet rule came out 2026-10-05; closed
  that day.
- CannObserv/broker#78 - Provisioner (`co-provisioner`), the cohort's
  infrastructure service, a read-only consumer of `content.revisions` in
  `provisioner.revisions`, 2026-10-07: ACL user `provisioner` on #43's shape,
  read off its source, live on the digest of a discarded value until its
  hash-only handoff; no DLQ by its owner's choice. The probe grew several groups
  per stream (`GroupCheck`), and this group's pending rule is an entry age, not
  the two-tick rule; the group is dormant until first seen. Go-live is the
  operator's list on the issue.
- CannObserv/broker#64 - `content.persist` (archiver -> replicator, cannobserv
  #493), provisioned 2026-09-26 ahead of both ends: archiver's `+xadd` in a
  selector only, replicator's third worker pool, `replicator.persist` probed,
  co-core `>=0.19.6`. The canonical stream set in tests is now read off
  co-core (`tests/canonical.py`), because the hand list let this stream past
  a green suite. Replicator's loop is live (2026-09-26T21:38Z). Archiver's
  issuance, the second half of the go-live order, shipped off under
  archiver#276 and went on under archiver#283 (2026-10-01T15:35:09Z; first
  command on the stream 16:01:01Z). #76 then moved the undelivered threshold
  onto Replicator's own timing of the handler, and confirmed the probe clean
  over the first three commands.
- CannObserv/archiver#251 - a broker credential in archiver's journald, from
  the application's start log and from one `sudo` command line. Both halves
  landed here on 2026-09-23: #47 (no runbook puts a credential in `argv`,
  guarded by `tests/deploy/test_runbook_credentials.py`) and #46 (the `default`
  credential rotated). Neither is a tracked-file change; both live files are
  templated and the value is `NOT_COMPARED` in `tests/deploy/test_live_*.py`.
- CannObserv/broker#72 - the service users' ACL digests ship with every
  backup, 2026-09-29, so a rebuilt node re-admits each service without a new
  password. A plaintext line fails the run, the node users are minted fresh
  and never ship, and `restore --digests` writes the newest set. This retired
  the password-manager copy that went stale when `observo` was minted.
- CannObserv/broker#53 - `citest` deleted live 2026-10-06T21:38:34Z, its
  passwords line with it. R4's test credential (#2) had never authenticated
  and had no holder after #49. No test credential exists here now: the tracked
  ACL declares participants and node users only, and `databases 1` stays. The
  siblings' suites default to loopback; CannObserv/replicator#132 asks
  replicator's to refuse any other host.

## What stayed in archiver

The split is not clean, and the seam is worth knowing:

- **`collect_outbox_findings`** queries `information.changes_outbox`. Nothing
  broker-side about it; archiver keeps it and keeps a reduced bus-health timer
  to run it.
- **`collect_group_lag`** feeds archiver's dashboard bus panel. `XPENDING`
  against a remote broker is an ordinary client call.
- **`scripts/check_redis_floor.sh`** is a client-side assertion that the
  broker a service is about to talk to is Redis >= 7.0. It stays with each
  client.

## The `OOM` seam, in both directions

`deploy/redis.conf.broker` sets `maxmemory-policy noeviction` with an
explicit cap (CannObserv/archiver#196 repoints archiver's half at the new
filename). That converts memory pressure into bounded, instance-wide
`OOM command not allowed` errors instead of a kernel OOM-kill of the whole
broker. It is only safe because archiver's outbox publisher classifies that
error as **transient** and retries through it, rather than dead-lettering valid
events - `_TRANSIENT_PUBLISH_ERRORS` in
`CannObserv/archiver:src/core/changes/publisher.py`.

**The cap and that classification are one decision, and they now live in two
repositories with no test spanning them.** Each side names the other in a
comment. Do not change either alone.

Whether Watcher and Replicator have an equivalent durable retry is **their own
property, and this repo does not assert it** - see the `Producer durability
under OOM` column in [`docs/STREAMS.md`](docs/STREAMS.md).

## Development

Python >= 3.12, uv, pytest, ruff.

`co-core` resolves from a local wheelhouse (`./.wheelhouse`, gitignored), not
PyPI. Populate it before `uv sync`/`uv run` or resolution fails:

```bash
set -a; . /etc/broker/.env; set +a   # GOOGLE_APPLICATION_CREDENTIALS=co-pypi-reader key
uv run --no-project --with 'google-cloud-storage>=2,<4' python scripts/sync_wheelhouse.py
uv sync --group dev
uv run pytest
uv run ruff check .
```

CI authenticates keyless via Workload Identity Federation instead - the
read-scoped `github-ci` provider impersonating the `objectViewer`-only
`co-pypi-reader` service account.
