# broker - Agent Guidelines

Be terse. Prefer fragments over full sentences. Skip filler and preamble.
Sacrifice grammar for density. Lead with the answer or action.

## Project Overview

Operational code for the Cannabis Observer **change-bus broker** - the Redis
Streams instance archiver, watcher, and replicator publish to and consume from.
Redis tuning, a WARN-only health probe, and the cluster stream inventory.

**No application logic, no data model, no HTTP surface.** Nothing here is
imported by any service; services reach the broker over the network, by URL.

Every file moved out of CannObserv/archiver under archiver#193 D6
(broker#1 Phase 1). Provenance and the seam it created: [README.md](README.md).

## Development Methodology

TDD required. Red -> Green -> Refactor.

## Environment & Tooling

Python >=3.12, uv, pytest, ruff. **`co-core` resolves from a local wheelhouse**
(`./.wheelhouse`, gitignored), not PyPI. Populate it before `uv sync`/`uv run`
or resolution fails:

```bash
set -a; . /etc/broker/.env; set +a   # GOOGLE_APPLICATION_CREDENTIALS
uv run --no-project --with 'google-cloud-storage>=2,<4' python scripts/sync_wheelhouse.py
uv sync --group dev
uv run pytest
uv run ruff check . && uv run ruff format --check .
```

Source env files as `set -a; . <file>; set +a` - `export $(cat ... | xargs)`
silently corrupts values.

## Rules

- **No retention opinion on `content.blobs`.** No length or age row for that
  stream: this repo owns no cap for it, and a threshold with no owner cries
  wolf. Its group *is* probed - the old unqualified "never `content.blobs`" was
  archiver's role boundary, and broker#1 Phase 5 retired it on the grounds that
  a neutral node has no role to be out of bounds of.
- **DLQs: the broker detects, captures and escalates; the consumer triages.**
  Splitting the old single "drainer" role is broker#1 Phase 5. Anything
  mechanical and suffix-keyed belongs here; reading a payload to tell residue
  from a real failure, and the `XTRIM` after it, belongs to the stream's own
  consumer (`DLQ_DRAINERS`). Do not add payload semantics to this repo to close
  that gap - that is the boundary, not an omission. An unclaimed `*.dlq` is
  reported as unassigned, never skipped.
- **The probe joins no consumer group and holds no database credential.** Both
  are pinned by `tests/deploy/test_bus_health_units.py`. `XPENDING` is
  read-only introspection; joining a group would silently swallow another
  service's messages.
- **The backup holds no Redis credential, and its identity cannot delete.**
  `broker-backup.service` reads `dump.rdb` - the server's own atomic snapshot -
  and creates objects under `objectCreator` + `objectViewer`; retention is the
  bucket's lifecycle rule. Do not add a Redis URL to that unit or `delete` to
  that grant for convenience; `tests/deploy/test_backup_units.py` and
  `tests/test_backup.py` pin both. A restored snapshot is **ignored** under
  `appendonly yes` unless staged as the AOF base: `src/broker/restore.py`,
  `docs/RECOVERY.md`.
- **The `OOM` seam spans two repos.** `deploy/redis.conf.broker`'s cap and
  archiver's `_TRANSIENT_PUBLISH_ERRORS` are one decision. Each names the other.
  Do not change either alone (archiver#193 R5). The cap moved out of
  `deploy/redis-server.dropin.conf` in Phase 5, so **archiver's half still
  points at the old path** until it is updated - CannObserv/archiver#196.
- **A non-stream key pattern is inventoried before it is written.**
  `docs/STREAMS.md`, *Non-stream keys on `db0`*. There is exactly one today,
  Replicator's `replicator:cmd:*` dedupe keys, and they are the **only volatile
  keys on the instance** - which is what makes `noeviction` load-bearing beyond
  refusing writes, since any `volatile-*` policy would make that one namespace
  the whole eviction candidate set. Do not change the policy without reading
  that section; two tests pin it, one on the config and one on the live
  keyspace.
- **Mirrored constants.** The three retention caps in `src/broker/bus_health.py`
  are copies of numbers owned elsewhere, each with its source named. Group
  names are **derived** via co-core's `group_name()`, never spelled - that is
  the point of cannobserv#384 and the reason this repo depends on co-core at
  all. See `docs/STREAMS.md`, "Mirrored constants".
- **No em dashes.** ASCII `-`.
- **No inline module imports.** Ruff `PLC0415`.
- All UTC, ISO 8601.

## Commit Messages

```
#<number> <type>: <description>      # with issue
<type>: <description>                # without issue
```

Types: feat, fix, refactor, docs, test, chore.

## Layout

```
deploy/          the artifacts the node deploys + the bus-health and backup units;
                 see deploy/README.md
docs/STREAMS.md  the cluster stream inventory - who produces, consumes, drains
docs/RECOVERY.md node loss: the backup, the restore, the rehearsal record
docs/RESTART-WINDOW.md
                 the cohort restart window, its identities, the 2026-09-10 incident
docs/SKILLS.md   vendored agent skills: inventory, selection, refresh
scripts/         wheelhouse sync (runs before `uv sync`, must not import the project)
src/broker/      bus_health.py (the probe), backup.py, restore.py,
                 logging.py (service-local, not a mirror)
tests/           mirrors src/; tests/deploy/ asserts installed artifacts match deploy/
                 and rehearses the restore against a real redis-server
```

## Agent Skills

Vendored from `gregoryfoster/skills` into `skills/` (agentskills.io) and
`.claude/skills/` (Claude Code). Symlinks dangle until the submodule is
initialised: `bash .skills/doctor.sh`. A `SessionStart` hook refreshes the
submodule daily on `main` and commits the bump itself. Review/ship are the
`-python-fastapi` variants - right gate, wrong deploy step: broker has no service
to restart after a merge. [docs/SKILLS.md](docs/SKILLS.md).

## Related

- CannObserv/broker#1 - the relocation epic. Phases 1-4 done; remaining work is
  its sub-issues: #2 ACL users, #3 alerting, #4 backup, #5 restart window,
  #6 OOM contract, #7 exercise two streams, #8 latency matrix.
- CannObserv/archiver#193 - D6 (why this repo exists), R5 (the OOM seam)
- CannObserv/archiver#196 - archiver's half of the OOM seam, stale since the
  cap moved to `deploy/redis.conf.broker`
