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
or resolution fails. No unit syncs it: on the node that is by hand, and due
again after any `co-core` pin change (broker#37):

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

- **No retention opinion on `content.blobs`.** No *retention* length or age row
  for that stream: this repo owns no cap for it, and a threshold with no owner
  cries wolf. Its group *is* probed - the old unqualified "never
  `content.blobs`" was archiver's role boundary, and broker#1 Phase 5 retired it
  on the grounds that a neutral node has no role to be out of bounds of. The
  group's `warn_undelivered_age_seconds` is not the exception it looks like: an
  undelivered age is a statement about `watcher.blobs`'s read loop, which this
  node measures directly and therefore owns, not about how long entries are
  kept. Retention is the axis with no owner here (broker#20).
- **DLQs: the broker detects, captures and escalates; the consumer triages.**
  Splitting the old single "drainer" role is broker#1 Phase 5. Anything
  mechanical and suffix-keyed belongs here; reading a payload to tell residue
  from a real failure, and the `XTRIM` after it, belongs to the stream's own
  consumer (`DLQ_DRAINERS`). Do not add payload semantics to this repo to close
  that gap - that is the boundary, not an omission. An unclaimed `*.dlq` is
  reported as unassigned, never skipped.
- **The probe joins no consumer group and holds no database credential.** Both
  are pinned by `tests/deploy/test_bus_health_units.py`. `XINFO GROUPS` is
  read-only introspection; joining a group would silently swallow another
  service's messages.
- **A `brokeradmin` grant nothing issues names its caller.** That credential is
  three callers - the probe, an operator at a `redis-cli`, and the deploy tests
  - so a command `src/broker/` never issues is not residue by default: `+xlen`
  (broker#13) and `+xpending` (broker#32, after #29) are both kept deliberately.
  The stanza above the rule in `deploy/redis-acl.conf` has to say which caller,
  and `tests/deploy/test_redis_acl.py` fails when one does not. Record it or cut
  it; do not leave it to read as residue. Archiver's unused DLQ disposals are
  held the same way (CannObserv/archiver#238). The inverse is withheld on
  purpose: `brokeradmin`'s `+xtrim` stops at `~*.dlq`, the only thing keeping an
  operator off a **Never XTRIMmed** stream (broker#34). Do not widen it in an
  incident.
- **The backup holds no Redis credential, and its identity cannot delete.**
  `broker-backup.service` reads `dump.rdb` - the server's own atomic snapshot -
  and creates objects under `objectCreator` + `objectViewer`; retention is the
  bucket's lifecycle rule. Do not add a Redis URL to that unit or `delete` to
  that grant for convenience; `tests/deploy/test_backup_units.py` and
  `tests/test_backup.py` pin both. A restored snapshot is **ignored** under
  `appendonly yes` unless staged as the AOF base: `src/broker/restore.py`,
  `docs/RECOVERY.md`.
- **The `OOM` seam spans this repo and archiver.** `deploy/redis.conf.broker`'s cap and
  archiver's `_TRANSIENT_PUBLISH_ERRORS` are one decision. Each names the other.
  Do not change either alone (archiver#193 R5). The cap moved out of
  `deploy/redis-server.dropin.conf` in Phase 5, and archiver's half was
  repointed at the new path by CannObserv/archiver#196.
- **A non-stream key pattern is inventoried before it is written.**
  `docs/STREAMS.md`, *Non-stream keys on `db0`*. There is exactly one today,
  Replicator's `replicator:cmd:*` dedupe keys, and they are the **only volatile
  keys on the instance** - which is what makes `noeviction` load-bearing beyond
  refusing writes, since any `volatile-*` policy would make that one namespace
  the whole eviction candidate set. Do not change the policy without reading
  `docs/MEMORY-PROTECTION.md`, *`noeviction` is load-bearing beyond refusing writes*;
  tests pin it on the config and on the live keyspace.
- **Mirrored constants.** The retention caps in `src/broker/bus_health.py`
  are copies of numbers owned elsewhere, each with its source named. A mirrored
  default is not always the whole rule: the LWW cap is `max(500, 10 x the set
  watcher republishes)`, and the set size is **read off the stream's own span**
  each tick rather than mirrored, because a corpus size changes with no edit
  anywhere - the one failure a mirror cannot cover (broker#44). Group names are
  **derived** via co-core's `group_name()`, never spelled - that is the point of
  cannobserv#384 and the reason this repo depends on co-core at all. See
  `docs/BUS-HEALTH.md`, "Mirrored constants" and "The one cap that is read, not
  mirrored".
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
docs/BUS-HEALTH.md
                 the probe: per-stream contracts, stream and DLQ checks, loss detection
docs/MEMORY-PROTECTION.md
                 the maxmemory cap: what it does to the producers, why `noeviction`
docs/UNDELIVERED-CONSUMERS.md
                 the group-undelivered check: positions, not pending or lag
docs/RECOVERY.md node loss: the backup, the restore, the rehearsal record
docs/RESTART-WINDOW.md
                 the cohort restart window and its identities
docs/INCIDENT-2026-09-10.md
                 what `databases 1` did to db0, and the order that avoids it
docs/NETWORK-PATHS.md
                 measured latency per participant, its path, the DERP risk
docs/CONSUMER-REGISTRATIONS.md
                 the one-time orphan reap, and why it cannot recur
docs/ACL-CUTOVER.md
                 the per-service credential cutover around that window
docs/SKILLS.md   vendored agent skills: inventory, selection, refresh
docs/SOCRATICODE.md
                 semantic search: tools, prefetch, graph health, index scope
scripts/         wheelhouse sync (runs before `uv sync`, must not import the project)
src/broker/      bus_health.py (the probe), backup.py, restore.py,
                 logging.py (service-local, not a mirror)
tests/           mirrors src/; tests/deploy/ asserts installed artifacts match deploy/,
                 rehearses the restore against a real redis-server, and guards the
                 SocratiCode config (#17) and ruff's reach into the docs (#38)
```

## Agent Skills

Vendored from `gregoryfoster/skills` into `skills/` (agentskills.io) and
`.claude/skills/` (Claude Code). Symlinks dangle until the submodule is
initialised: `bash .skills/doctor.sh`. Unprompted commits land on `main`:
a `SessionStart` hook bumping the submodule daily, and a Thursday workflow
appending a context measurement - pull before pushing. Review/ship are the
`-python-fastapi` variants - right gate, wrong deploy step: broker has no service
to restart after a merge. [docs/SKILLS.md](docs/SKILLS.md).

## Related

- CannObserv/broker#1 - the relocation epic. Closed 2026-09-15, every sub-issue
  with it. #14 - confining each service's `+xadd`/`+xtrim` to the streams it
  produces - landed 2026-09-18 and is live on the node.
  #29 - one `XINFO GROUPS` per grouped stream for the position, the pending
  count and the group's existence - landed 2026-09-18.
  #34 - archiver's `+xtrim` narrowed to `~info.changes` and its two DLQs, live
  2026-09-22: nothing on the instance can `XTRIM` `info.registry` now, and
  its row in `docs/STREAMS.md` says why (CannObserv/archiver#234 answered).
- Open follow-on: CannObserv/replicator#98 (recovery re-claiming its own
  failing retry starves `XREADGROUP` - a live consumer the undelivered check
  cannot tell from a gone one). #43 (each grouped stream's producer holds the
  group commands on its root, so it can `XACK` its consumer's work away -
  #14's hole the other way round). #45 (the LWW set reading absorbs one missed
  republish and not two, and `stream-age` waits for three - a ten-minute window
  where the length check warns with nothing naming the cause).
- CannObserv/watcher#319 - the notice for the other end of #44's mirror:
  `RETAINED_FULL_SETS` and the `*/5` republish period are copied into
  `src/broker/bus_health.py`, and the period moves from watcher's *environment*
  (`WATCHER_WATCH_STATUS_REPUBLISH_CRON`) with no commit anywhere.
- CannObserv/archiver#193 - D6 (why this repo exists), R5 (the OOM seam)
- CannObserv/archiver#196 - archiver's half of the OOM seam, repointed after the
  cap moved to `deploy/redis.conf.broker`

## Detail Docs

- [docs/STREAMS.md](docs/STREAMS.md) - which streams exist; who produces, consumes and drains each; non-stream keys; where each participant runs
- [docs/NETWORK-PATHS.md](docs/NETWORK-PATHS.md) - the measured latency from each participant, the path beside every number, and the accepted DERP risk
- [docs/CONSUMER-REGISTRATIONS.md](docs/CONSUMER-REGISTRATIONS.md) - the one-time reap of orphaned consumer registrations, and why it cannot recur
- [docs/BUS-HEALTH.md](docs/BUS-HEALTH.md) - changing the probe or reading a finding: its stream, memory, DLQ, loss and disk checks, and why
- [docs/MEMORY-PROTECTION.md](docs/MEMORY-PROTECTION.md) - the `maxmemory` cap: that all three producers survive it, and why `noeviction` is load-bearing beyond refusing writes
- [docs/UNDELIVERED-CONSUMERS.md](docs/UNDELIVERED-CONSUMERS.md) - a consumer that stopped reading: why `pending`, `lag` and `idle` are each blind to it, and what the probe compares instead
- [docs/RECOVERY.md](docs/RECOVERY.md) - losing the node or its data: the backup and its findings, the restore, the rehearsal record
- [docs/RESTART-WINDOW.md](docs/RESTART-WINDOW.md) - restarting `redis-server`: the runbook and its symptom playbook
- [docs/INCIDENT-2026-09-10.md](docs/INCIDENT-2026-09-10.md) - `databases 1` wiping db0, and why `BGREWRITEAOF` comes first
- [docs/ACL-CUTOVER.md](docs/ACL-CUTOVER.md) - how the cluster moved onto per-service ACL users, and the order a new one repeats; to change a grant, [deploy/README.md](deploy/README.md)
- [docs/SKILLS.md](docs/SKILLS.md) - the vendored agent skills, their refresh hook, the context cadence
- [docs/SOCRATICODE.md](docs/SOCRATICODE.md) - semantic search over this repo and its four siblings: the tool table, the prefetch, graph health, index scope

<!-- BEGIN socraticode-policy -->
## Code Exploration Policy

SocratiCode is the preferred semantic-search tool here once indexed (the
cohort's shared Qdrant on `co-index` + on-disk graph; manifest
`.socraticodecontextartifacts.json`). Its MCP tools are **deferred** - schemas
load only after the `ToolSearch` prefetch that
`.claude/hooks/socraticode-reminder.sh` prints each session.

**Negative rule.** Use SocratiCode MCP tools first for semantic questions
("where is X", "how does Y work", "what depends on Z"). Reach for `grep`/`rg`
only on exact strings (error messages, log lines, known symbols). Reserve the
Explore subagent for path-pattern walks (`*.py` under `src/broker/`), not
semantic search.

| Goal | Tool |
|------|------|
| Where is X defined / how does Y work / what touches Z | `codebase_search` |
| Exact string or regex (errors, log lines, known symbols) | `grep` / `rg` |
| Imports/dependents of a file - blast radius of a change | `codebase_graph_query` / `codebase_impact` |

Full tool table, prefetch query, per-tool guidance: [`docs/SOCRATICODE.md`](docs/SOCRATICODE.md).
<!-- END socraticode-policy -->
