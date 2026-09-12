# broker

Operational code for the Cannabis Observer **change-bus broker** - the Redis
Streams instance the cluster's three services publish to and consume from.

This repo owns the broker's *tuning*, its *monitoring*, and the *cluster stream
inventory*. It owns no application logic and no data model. Nothing here is
imported by any service; the services reach the broker over the network, by URL.

| Path | What it is |
|---|---|
| [`deploy/redis.conf.broker`](deploy/redis.conf.broker) | The broker's tuning as deployed - bind, auth, AOF persistence, `noeviction`, an explicit `maxmemory` cap. Appended to `/etc/redis/redis.conf`, with the credential templated |
| [`deploy/redis-acl.conf`](deploy/redis-acl.conf) + [`deploy/render-acl.sh`](deploy/render-acl.sh) | The per-service ACL users - what actually scopes each service to its own streams, rather than documenting it. Rendered to `/etc/redis/users.acl`; changed live as `acladmin` and mirrored back |
| [`deploy/redis-server.service.d/broker.conf`](deploy/redis-server.service.d/broker.conf) + [`deploy/wait-for-tailnet-addr.sh`](deploy/wait-for-tailnet-addr.sh) | Unit ordering only: `After=tailscaled` plus the `/proc/net/fib_trie` wait that R1's boot race exists for |
| [`deploy/broker-bus-health.service`](deploy/broker-bus-health.service) / [`.timer`](deploy/broker-bus-health.timer) | The periodic WARN-only health probe, every 10 minutes |
| [`src/broker/bus_health.py`](src/broker/bus_health.py) | The probe: memory headroom **and eviction policy**, per-stream `XLEN` against retention caps, last-entry age on groupless streams, `XPENDING`, **DLQ depth and continuity** - a queue drained between ticks is reported from its monotonic `entries-added` even though depth never saw it - disk, persistence status, and the backup's freshness |
| [`deploy/broker-backup.service`](deploy/broker-backup.service) / [`.timer`](deploy/broker-backup.timer) | The hourly RDB backup - root confined to read-only everything but its own state directory, holding no Redis credential (broker#4) |
| [`src/broker/backup.py`](src/broker/backup.py) / [`restore.py`](src/broker/restore.py) | The job: verify `dump.rdb`, gzip, create-only upload named by the snapshot's time. And the restore: stage a snapshot as the AOF base, which is the only way Redis 7 will load it under `appendonly yes` |
| [`docs/STREAMS.md`](docs/STREAMS.md) | The cluster stream inventory - who produces, who consumes, which health primitive applies, and who drains each DLQ |
| [`docs/BUS-HEALTH.md`](docs/BUS-HEALTH.md) | What the probe watches and why: the per-stream monitoring contracts, its stream, memory, DLQ and disk checks, the `noeviction` contract, loss detection, and the constants it mirrors (its `backup` and `persistence` findings are in `docs/RECOVERY.md`) |
| [`docs/RECOVERY.md`](docs/RECOVERY.md) | Losing the node: what is exposed, the backup's design and its grant, the rebuild runbook, the rehearsal record |
| [`docs/RESTART-WINDOW.md`](docs/RESTART-WINDOW.md) | The cohort restart window: the identities, the steps as run, and the 2026-09-10 `databases 1` incident |
| [`docs/ACL-CUTOVER.md`](docs/ACL-CUTOVER.md) | The per-service credential cutover around that window: the passwords, the dry run, each service onto its own user, retiring `default`, and the `nopass` trap |

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
