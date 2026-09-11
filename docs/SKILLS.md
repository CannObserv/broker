# Agent Skills

Vendored from [`gregoryfoster/skills`](https://github.com/gregoryfoster/skills)
by that repo's `managing-skills` pattern: one git submodule, one symlink per
skill. Follows the [agentskills.io](https://agentskills.io) spec.

## Layout

| Path | What it is |
|---|---|
| `skills-vendor/gregoryfoster-skills/` | The submodule. Read-only here - changes go upstream |
| `skills/<name>` | agentskills.io discovery: a symlink into the submodule, or a committed override that shadows it |
| `.claude/skills/<name>` | Claude Code discovery: a symlink to `../../skills/<name>`, so an override shadows the vendor copy in both systems |
| `.skills/doctor.sh` | A real file, not a symlink: it repairs dangling vendor symlinks by initialising the submodule, and would dangle itself if it were one. `reviewing-*` / `shipping-*` run it as their preflight |

Every skill symlink dangles until the submodule is initialised - a clone
without `--recurse-submodules`, a fresh `git worktree add`. Run
`bash .skills/doctor.sh` (it runs `git submodule update --init --recursive`).

Adding a skill means both entries, `skills/<name>` and `.claude/skills/<name>`.

## Refresh

**No auto-refresh hook is installed.** The submodule pointer is frozen at the
commit it was vendored at until bumped by hand:

```bash
git submodule update --init --remote --merge -- skills-vendor/
git add skills-vendor/ && git commit -m "chore: update skill submodules"
```

`--init` is load-bearing: without it an unregistered submodule is skipped
silently and git still exits 0. The once-per-UTC-day `SessionStart` hook that
automates this (on `main` only, commits the bump itself):
`bash skills-vendor/gregoryfoster-skills/skills/managing-skills/scripts/install-refresh.sh`.

## Selection

Ten of upstream's nineteen skills: the `gregoryfoster/skills` set archiver,
notifier, replicator and watcher vendor, less what does not apply here.

**Review and ship are the `-python-fastapi` variants, though broker is not a
FastAPI service.** Their gate is this repo's gate - `pre-ship.sh` runs
`ruff check`, `ruff format --check` and `pytest -m "not integration"`, and
passed unmodified at vendoring. Their Alembic, OpenAPI and route steps key on
paths broker does not have, and the review's `deploy/` checks (unit ordering,
`OnFailure=`, bounded restarts) fit it well. The alternatives are worse: the
stack-neutral `shipping-work`'s `pre-ship.sh` is a stub that exits 1 until
forked, and Click is another stack.

**One step does not transfer.** The ship skill's post-merge table says a code
change needs `systemctl restart <project>`. There is no `broker` unit. The probe
and backup are timers that run from this checkout, so a `src/` change is live on
their next tick; a `deploy/` change is installed by hand
([deploy/README.md](../deploy/README.md)). The only long-running service is
`redis-server`, whose restart is a cohort-wide event -
[RESTART-WINDOW.md](RESTART-WINDOW.md), never a ship step.

Left out:

| Skill | Why |
|---|---|
| `init-project-fastapi` | Scaffolds a new FastAPI service |
| `vendoring-openapi-client` | Broker calls no HTTP API |
| `auditing-ci-cost` | One CI workflow; no sibling vendors it either |
| `reviewing-code`, `shipping-work`, `-php`, `-python-click` | Other stack variants of the two above |

**No `obra/superpowers`.** Every sibling also vendors it (`brainstorming`,
`test-driven-development`, `systematic-debugging`, ...). None of the ten below
depends on it; adding it is a second submodule by the same procedure.

## Inventory

All plain symlinks - no local overrides.

| Skill | Triggers |
|---|---|
| `curating-context` | curate context, context budget, trim AGENTS.md |
| `enforcing-architecture` | add a fitness function, enforce this contract, lock this rule |
| `init-socraticode` | init socraticode, set up code search, index this project |
| `managing-skills` | add skill repo, update vendor skills, enable auto-refresh |
| `orchestrating-issue-backlog` | orchestrate backlog, prioritize issues, clear backlog |
| `reviewing-architecture` | AR, architecture review |
| `reviewing-code-python-fastapi` | CR, code review, perform a review |
| `shipping-work-python-fastapi` | ship it, push GH, close GH, wrap up |
| `using-git-worktrees` | create worktree, destroy worktree, merge worktree, wt |
| `writing-plans` | write a plan, plan this |
