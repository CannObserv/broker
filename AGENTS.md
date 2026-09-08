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

- **Never `content.blobs` beyond its DLQ.** The role boundary archiver carries
  came across with the inventory: no length or age row for that stream. The
  `*.dlq` sweep is keyed on the suffix and covers it, which is intended.
- **The probe joins no consumer group and holds no database credential.** Both
  are pinned by `tests/deploy/test_bus_health_units.py`. `XPENDING` is
  read-only introspection; joining a group would silently swallow another
  service's messages.
- **The `OOM` seam spans two repos.** `deploy/redis-server.dropin.conf`'s cap
  and archiver's `_TRANSIENT_PUBLISH_ERRORS` are one decision. Each names the
  other. Do not change either alone (archiver#193 R5).
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
deploy/          redis-server drop-in + the bus-health units; see deploy/README.md
docs/STREAMS.md  the cluster stream inventory - who produces, consumes, drains
scripts/         wheelhouse sync (runs before `uv sync`, must not import the project)
src/broker/      bus_health.py (the probe), logging.py (service-local, not a mirror)
tests/           mirrors src/; tests/deploy/ asserts installed artifacts match deploy/
```

## Related

- CannObserv/broker#1 - the relocation epic and this repo's bootstrap
- CannObserv/archiver#193 - D6 (why this repo exists), R5 (the OOM seam)
