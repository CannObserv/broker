# Agent Skills

Vendored from [`gregoryfoster/skills`](https://github.com/gregoryfoster/skills)
by that repo's `managing-skills` pattern: one git submodule, one symlink per
skill. Follows the [agentskills.io](https://agentskills.io) spec.

## Layout

| Path | What it is |
|---|---|
| `skills-vendor/gregoryfoster-skills/` | The submodule. Read-only here - changes go upstream. Ruff skips it (`pyproject.toml`), as every sibling does: the refresh hook moves it unreviewed |
| `skills/<name>` | agentskills.io discovery: a symlink into the submodule, or a committed override that shadows it |
| `.claude/skills/<name>` | Claude Code discovery: a symlink to `../../skills/<name>`, so an override shadows the vendor copy in both systems |
| `.skills/doctor.sh` | A real file, not a symlink: it repairs dangling vendor symlinks by initialising the submodule, and would dangle itself if it were one. `reviewing-*` / `shipping-*` run it as their preflight |

Every skill symlink dangles until the submodule is initialised - a clone
without `--recurse-submodules`, a fresh `git worktree add`. Run
`bash .skills/doctor.sh` (it runs `git submodule update --init --recursive`).

Adding a skill means both entries, `skills/<name>` and `.claude/skills/<name>`.

## Refresh

A `SessionStart` hook advances the submodule pointer. At most once per UTC day,
on `main` only, it fetches upstream, **commits the bump itself** - staging only
`skills-vendor/` and `.skills/doctor.sh` - and **pushes it**
(gregoryfoster/skills#293). A rejected push is rolled back rather than left
waiting, so this checkout never sits ahead of `origin/main` on the hook's
account: an unpushed bump is what stranded replicator's service (replicator#94).
It never pulls `main`, never force-pushes, never pushes a commit it did not
write, and never blocks a session. Log: `.git/skills-update.log`.

The install is two artifacts, and only the second makes it run:
`.claude/hooks/skills-submodule-update.sh` (a symlink into the vendored
`managing-skills`) and its entry in `.claude/settings.json`. The symlink alone
looks installed and refreshes nothing.

| To | Run |
|---|---|
| Check both halves | `bash skills/managing-skills/scripts/install-refresh.sh --check` |
| Remove both | `bash skills/managing-skills/scripts/install-refresh.sh --uninstall` |
| Refresh by hand | `git submodule update --init --remote --merge -- skills-vendor/` |
| Hold at a commit | one `<submodule-path> <commit-ish>` line in `.skills/skills-pin` |

`--init` in the manual refresh is load-bearing: without it an unregistered
submodule is skipped silently and git still exits 0.

The first session in a fresh clone or new worktree fails the hook with exit 127:
it is a vendor symlink, and Claude Code runs hooks in parallel, so nothing can
initialise the submodule before it. `bash .skills/doctor.sh` once fixes it.

## Context cadence

`.github/workflows/context-cadence.yml` measures the agent-context surface
(`AGENTS.md` and the docs it links) every Thursday at 15:51 UTC, a slot derived
from the repo name to stagger the cohort. It appends one `baseline:scheduled`
row to `.skills/context-metrics.jsonl` and **pushes it to `main` itself**, so a
local `main` can fall behind by a `chore: weekly context measurement` commit.
It measures and never curates; a budget warning in its run means someone runs
`curate context` here.

- **It needs the `ANTHROPIC_API_KEY` repository secret.** It checks the
  credential first and fails the run without it. An estimated count can't be
  appended to a ledger of exact counts, so without a key nothing is recorded.
- **The merge drivers are per clone.** `.gitattributes` names two drivers git
  does not have built in, and git config isn't versioned. Without them the
  calibration files conflict on merge as if unprotected. Every new checkout runs
  `bash skills/curating-context/scripts/install-cadence.sh` once;
  `--check` reports all seven guarantees.
- **A row's deltas are derived, and a merge can stale them.** `delta_tokens`
  and `delta_days` are computed against whatever row precedes it at append
  time; a merge that lands a row in between leaves them describing the wrong
  predecessor (CannObserv/broker#56, gregoryfoster/skills#325). The run warns
  on it (`--repair --dry-run`). Fix on a branch cut from current `main`, merged
  before `main` moves on: `record-telemetry.sh --repair`, committed on its own.
  After merging or rebasing `main` into a curation branch, run it there too.
  Rewrite a run's own row with `--amend`, never by hand.
- **The workflow is generated.** Re-run the installer rather than editing it.
  That includes the em dashes in upstream's text, which the no-em-dash rule
  doesn't cover here.

## Selection

Eleven of upstream's twenty skills: the `gregoryfoster/skills` set archiver,
notifier, replicator and watcher vendor, less what does not apply here, plus
`using-mayfly-chat` (#63), which the cohort adopts together
(gregoryfoster/skills#302).

**`using-mayfly-chat` needs Node.js 18+** on the host running the agent; its
wrapper exits 4 without it. **A channel URL never reaches a durable store** -
not an issue, commit, doc or plan: it is read, write and delete access to the
channel. `tests/deploy/test_skills_inventory.py` scans every committable file
(tracked, plus untracked and unignored - what `git add -A` takes) with
upstream's pattern, so a live URL fails the suite rather than relying on
discipline. The same pattern by hand, before committing a session's output, is
in the skill's `references/security.md`. Run an exchange from the scratchpad,
not this checkout: the skill's recipes write `read.json`, `post.json`,
`listen.json` and the message body into the working directory, and a
decrypted transcript left there is one `git add -A` from a commit.

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
`test-driven-development`, `systematic-debugging`, ...). None of the eleven below
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
| `using-mayfly-chat` | mayfly, open a channel, join the channel, chat with <repo>, agent chat |
| `writing-plans` | write a plan, plan this |
