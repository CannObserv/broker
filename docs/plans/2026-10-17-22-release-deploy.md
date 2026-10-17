---
title: "#22: broker's units run a root-owned release, never the dev checkout"
date: 2026-10-17
status: draft
---

# #22: broker's pilot - run releases, not `~/broker`

## Problem

Both broker units run the checkout every agent works in
(`WorkingDirectory=/home/exedev/broker`, read from `deploy/` at `7e659f7`):

| | `broker-bus-health` (timer, 10 min) | `broker-backup` (timer, hourly) |
|---|---|---|
| `User=` | `exedev` | **root**, `CapabilityBoundingSet=CAP_DAC_READ_SEARCH` |
| Runs | `/usr/local/bin/uv run python -m src.broker.bus_health` (syncs the venv every tick) | `/home/exedev/broker/.venv/bin/python -m src.broker.backup` |

So a branch switch, an uncommitted edit, a hook commit or a `uv sync` in
`~/broker` is production at the next tick. That happened on 2026-09-15/16
(#17) and 2026-10-01 (#76). And hourly, root runs Python that `exedev` can edit,
with a capability that reads every file on the host and a key that writes to
the backup bucket. That is #22's sharpest case, and goal 5.

Since #22 was filed, three repos have shipped the pattern:

- status#9 and status#14: releases, then root-owned releases;
- processor#2: no HTTP, a CI gate, a drift check through co-status;
- observo#673: root-owned releases, a dedicated user, root-only secrets.

This doc applies them to broker and answers #22's Q1-14 plus the brief's
broker-specific questions (B1-B8). The decisions for approval are D1-D11, at
the end.

**Measured on the node (2026-10-17):**

- `/srv` is `root:root 0755` and empty. It is on the same filesystem as
  `/home`: `/dev/root`, 20 GB, 4.6 GB free (76% used).
- The interpreter is `/usr/bin/python3.12`; the venv's `pyvenv.cfg` has
  `home = /usr/bin`. uv is 0.12.7, systemd 255.
- `/var/lib/redis` is `redis:redis 0750` and `dump.rdb` is `0660`. **`/etc/redis`
  is `redis:redis 2770`**, so the `redis` group can write the directory that
  holds `redis.conf` and `users.acl`. `broker-acl-passwords` is `root 0400`.
- `/etc/broker/.env` and `co-pypi-reader.json` are `root:exedev 0640`.
  `backup.env`, `status.env` and `co-broker-backup.json` are `root 0400`.
- `/var/lib/broker-backup` is `root 0755`, and `state.json` is `0644`. The
  probe reads it as `exedev`.
- The only file either unit writes under its `WorkingDirectory` is
  `__pycache__`, plus `uv run`'s venv sync. Everything else goes to a
  `StateDirectory=` or a private `/tmp`.

## Approach

Follow observo#673's root-owned release, with processor's no-HTTP deploy
(CI gate, unit install, drift through co-status):

```
/srv/broker/                  root:root 0755 (the deploy refuses anything else)
  releases/<sha12>/           git archive of a commit on origin/main, its locked
                              wheels, its own .venv; REVISION written last by root;
                              root's, read-only
  live -> releases/<sha12>    both units; replaced only by a sudo rename
```

`scripts/deploy.sh`, adapted from processor's, does the whole deploy:

1. **Check:** runs as `exedev` and refuses root; takes a `flock`; asks the CI gate.
2. **Build:** builds the release at its final path.
3. **Switch:** renames `live` to the new release.
4. **Install units:** installs any `deploy/broker-*.{service,timer}` from the
   release that differs from its installed copy, then one `daemon-reload`.
5. **Verify,** and switch back on failure.

It restarts nothing. A timer picks up `live` at its next start, and Redis is
never touched.

The backup moves onto the release as root first. A later child drops root
entirely, to a static `broker-backup` user that receives `dump.rdb` as a
copy made by the one privileged step.

### #22's questions

| Q | Recommendation |
|---|---|
| **1 Location, ownership** | `/srv/broker`, as drawn above, owned by root (status#14, observo). A read-only release that `exedev` owns stops accidents but not its owner (`chmod +w`). Root ownership makes every change take `sudo`, which is journaled. On exe.dev, the design assumes only that the VM's disk persists, which the checkout already assumes: `/srv` and `/home` are the same filesystem |
| **2 Atomicity, rollback** | A venv embeds its absolute path, so each release is built at its final path. `REVISION` is the commit marker; a directory without one is an interrupted build and is rebuilt. The swap is a `rename(2)`. Rollback is `scripts/deploy.sh <old sha12>`, CI-gated like any deploy (`--skip-ci` in an emergency, logged). Keep 5 releases plus `live`. A release linked by `live` is never rebuilt in place. Prune removes `REVISION` first, and a failed prune is only a note |
| **3 Git source** | The local object store after `git fetch --prune origin` in `~/broker`. The commit must be an ancestor of `origin/main` as fetched. The repo is public, so no credential is involved, and no token goes in a URL (the store-helper trap stays shut). "The pushed default branch" means `origin/main` at deploy time, and its push run of `ci.yml` must be green |
| **4 Who deploys** | `scripts/deploy.sh`, run by an agent or the operator as `exedev`. It uses `sudo` for every write under `/srv/broker`, for the unit files and for `systemctl`. There is no deploy oneshot unit. `exedev`'s blanket sudo is exe.dev's VM default, so this is not an adversarial boundary. It turns a silent edit into a journaled one (status#14) |
| **5 Dependencies, private wheels** | In the release: `UV_PYTHON_DOWNLOADS=never uv sync --locked --no-dev --compile-bytecode --link-mode copy --python /usr/bin/python3.12`. Copy mode, because hardlinks would share inodes with `~/.cache/uv` and every dev venv (status#14). The wheelhouse is synced **at deploy time** (B3). The editable install is kept: its `.pth` points into the read-only release itself, and the units run `-m src.broker.*` from the release as their working directory. Nothing syncs at start |
| **6 Env and state** | Inventory below. No unit reads the repo `.env` today (`test_the_unit_loads_environment_only_from_etc_broker`), and a release has no `.env`. Every write already lands in `StateDirectory=` or a private `/tmp`, so no state moves. `__pycache__` goes: bytecode is compiled at build, and both units set `PYTHONDONTWRITEBYTECODE=1` |
| **7 Restart semantics** | Nothing is long-running. systemd resolves `WorkingDirectory=/srv/broker/live` at each start, and Python's working directory is the resolved release, so a tick already running finishes on its old release. The deploy restarts nothing, and never `redis-server`. Neither the restart window nor replicator#94's reconnect work applies |
| **8 Guards** | **At deploy time:** not root, a root-owned deploy root, the lock, ancestry, the CI gate, an interpreter not under `/home`, and a venv that imports `src.broker.bus_health`, `src.broker.backup` and co-core. **At start time: none.** The `tests/deploy` parity tests stay post-deploy evidence, never a start condition |
| **9 Build id, drift** | `REVISION` (D5). The drift check is a timer of its own that reports to co-status (D6) |
| **10 `OnFailure=`** | **Beside this design, and not needed here.** A failed probe tick is a missed check-in, which co-status reports as `missing`. A failed backup is the probe's `backup` finding on its next tick. A failed drift run leaves its own monitor `missing`. A failed deploy is its exit status: 1 means switched back and the old build answers; 4 means nothing answers |
| **11 Dev services** | Broker has none, so there is no exception to name and no `dev` target. The inner loop stays pytest in a worktree, against a throwaway `redis-server`. A hand-run probe from a checkout is a dev run and takes its own `--state-file`. While the probe's `User=` is `exedev` (D3), nothing but convention stops it writing `/var/lib/broker-bus-health` |
| **12 Tests** | A new `tests/deploy/test_deploy.py`, ported from status's and processor's stub harness: stubs for `uv`, `systemctl`, `stat` and `curl` (for the CI gate), a `sudo` stub with a ledger playing root, and a temp deploy root, with any write under the root that skips `sudo` failing the test. Unit tests: no unit names `/home/exedev`; `WorkingDirectory=/srv/broker/live`; `ExecStart=` is the release venv's interpreter, never `uv`. The existing installed-matches-repo tests stay. A node test: `live` resolves to a root-owned release whose `REVISION` is on `origin/main`. This extends the `tests/deploy` parity pattern |
| **13 Packaging** | Broker adopts gregoryfoster/skills#345's `.skills/deploy_command` (D10). Changes to `init-project-fastapi`, the `reviewing-code` inversion and `using-git-worktrees` are #22's step 5, upstream, after this pilot. They go to gregoryfoster/skills with per-post approval and are not a pilot child. Broker has no `worktree_venv` knob, so skills#201's knob needs nothing here |
| **14 Rollout** | status, processor and observo are done. Broker is the timer-only, no-HTTP pilot, with the root case. After it closes: one issue per remaining repo (archiver, watcher, replicator, notifier), filed with approval, **replicator first**, since its start-time guard is #22's documented outage and the one guard to retire |

**Q6 inventory:**

| Unit | Reads | Writes |
|---|---|---|
| `broker-bus-health` | `/etc/broker/.env` (`BROKER_REDIS_URL`; `GOOGLE_APPLICATION_CREDENTIALS` unset), `-/etc/broker/status.env`, credential `broker-brokeradmin`, `/var/lib/broker-backup/state.json` | `/var/lib/broker-bus-health/{state.json,dlq-evidence/}`; today also `__pycache__`, `.venv` (the sync) and `~/.cache/uv` in the checkout, all gone after C3 |
| `broker-backup` | `/etc/broker/backup.env`, `co-broker-backup.json`, `/etc/redis/broker-acl-passwords`, `/var/lib/redis/dump.rdb` | `/var/lib/broker-backup/state.json` (0644, for the probe), a private `/tmp` |
| `restore` (by hand, as root) | the bucket | `--into` |

### Broker-specific questions (the brief)

| B | Recommendation |
|---|---|
| **B1 The backup runs dev-tree code as root** | Two steps, one cause each (D2). **C4** moves the unit onto the release, still as root. That meets goal 5 as written: root runs only code that root owns. **C5** drops root for a static `broker-backup` system user: no home, no login, **not in `redis`**. `ExecStartPre=+/usr/bin/install -p -m 0400 -o broker-backup ... /var/lib/redis/dump.rdb ${RUNTIME_DIRECTORY}/dump.rdb` is the only privileged step: root runs coreutils, never Python. `LoadCredential=` hands over the passwords file and the writer key, and `CapabilityBoundingSet=` is empty. **The `redis` group is rejected:** it holds write on `dump.rdb` and on `/etc/redis` itself. **`DynamicUser=` is rejected:** it moves `StateDirectory=` under `/var/lib/private` (`0700`), so the probe could no longer read `state.json` |
| **B2 `uv run` syncs every tick** | The probe's `ExecStart=` becomes `/srv/broker/live/.venv/bin/python -m src.broker.bus_health ...`, as the backup already does (C3). uv leaves the runtime path entirely. The venv is frozen at build |
| **B3 The co-core wheelhouse** | `deploy.sh` runs `scripts/sync_wheelhouse.py` in `~/broker` as `exedev`, which can read the `co-pypi-reader` key through `/etc/broker/.env`. Then it copies into the release's `.wheelhouse/` only the files `uv.lock` names under `registry = ".wheelhouse"`, as `cp` and never a link, and fails the build if one is missing. The checkout's 652 files (89 MB) never enter a release. For production, this ends #37's manual step after a pin change. The dev tree still syncs by hand for its own tests |
| **B4 `/etc/broker/.env`** (`root:exedev 640`) | **Unchanged.** Since #52 it holds no secret: a URL that names only the user, and a key *path*. Its readers: the probe unit (systemd reads it as root), the deploy's wheelhouse sync (as `exedev`), and the operator and live tests (as `exedev`). `co-pypi-reader.json` stays `root:exedev 640`. It is build tooling, observo's call too. The probe keeps `UnsetEnvironment=GOOGLE_APPLICATION_CREDENTIALS` |
| **B5 Build id, drift** | D5 and D6 |
| **B6 The live-probe convention ends** | Each place is rewritten in the child that changes it. C7 sweeps the rest. The list: AGENTS.md (Environment & Tooling's "no unit syncs it", Agent Skills' "wrong deploy step", a Deploy line, `scripts/` in Layout); `docs/SKILLS.md`, "One step does not transfer"; `docs/CI.md`'s "`~/broker` is the ... units' `WorkingDirectory`"; `deploy/README.md` (the unit rows, Install, Environment's "No unit syncs the wheelhouse", the check-in payload); `docs/RECOVERY.md` (five `restore` lines from `/home/exedev/broker/.venv`, and the rebuild order: `deploy.sh` before the timers); `scripts/sync_wheelhouse.py`'s docstring; `deploy/broker-bus-health.service`'s comment that "`uv run` in the dev tree makes a separate change"; `tests/deploy/test_backup_units.py:109`; the memory notes `broker-checkout-is-live-probe` and `burn-down-epic-80`; #22's interim convention; and #80's "How a check-in verifies a child", where "Deployed means `~/broker`'s working tree" becomes "`live`'s `REVISION` is the merged sha12". The #80 change is handed to the coordinator, who edits through `epic-edit.sh`. `docs/RESTART-WINDOW.md:92` stays: that line runs the tests from the dev tree, which is still right |
| **B7 The patching knob** | Units keep their names, so `.skills/patching-hosts` is unchanged: the `backup broker-backup.service redis-server.service` line, both `not-restarter` exceptions, and `quiet *:00-*:07`. The reboot chain starts `broker-backup.service` by name, and C5's user is invisible to it. The `health` line greps the probe's `Bus health summary` for `"finding_count": 0`; C1 adds a field to that line and must keep that match |
| **B8 Nothing restarts Redis** | Confirmed: there is no window, and both timers switch between ticks. The probe's commands don't change, so no new `ACL LOG` entry is expected. Each child that switches a unit reads `ACL LOG` as `acladmin` (never resetting it) and names any new entry, per #48. The backup and the drift check hold no Redis credential |

### D5: the build id

`REVISION`, read by the code (status R10). It is `dev` when absent, so C1
ships before any release exists.

- **The probe** adds `revision` to its `Bus health summary` line and to the
  check-in's **`variables`**. **Not** to `metadata`: `metadata.fault` is the
  value co-status compares (#84), and a revision there would report every deploy
  as a new fault.
- **The backup** writes `revision` into `state.json` and logs it.

### D6: the drift signal

`broker-drift.timer` runs hourly, from the release, as `DynamicUser=` with
`LoadCredential=` for its key. It ports processor's `drift.py` rules:

- **Alert:** live behind `origin/main` *in runtime paths* (`src/`,
  `deploy/broker-*`, `scripts/deploy.sh`, `pyproject.toml`, `uv.lock`) for
  more than 8 h after the push.
- **Also alert:** off `main`, or unstamped.
- **Report:** one check-in per run to a co-status monitor of its own,
  `co-broker-drift`.

co-status raises the alarm through notifier, like every other monitor.

**Why a unit of its own, not a probe finding:** a lag you mean to keep is
silenced with `systemctl stop broker-drift.timer` without silencing the bus,
and a deploy concern stays out of the bus fault set.

**Budget:** unauthenticated GitHub calls (60 an hour per address), 1-10 per
run, shared with the CI gate.

**Prerequisite:** status's operator mints the monitor and key. That is a
CannObserv/status request, filed with approval.

## Tradeoffs / alternatives

- **Releases owned by `exedev` (status before #14, processor):** rejected. An
  agent can `chmod +w` and edit what root runs.
- **A deploy checkout (a `git worktree` at `/srv/broker`) instead of
  archives:** rejected. A checkout carries `.git`, hooks and submodules, and
  can be switched. An archive has nothing to drift.
- **The backup reads `dump.rdb` through the `redis` group:** rejected. That
  group holds write on the live snapshot and on `/etc/redis`.
- **A POSIX default ACL on `/var/lib/redis` for a backup user:** rejected. It
  edits Redis's own data directory, every AOF file inherits it, and a restore
  or package upgrade can drop it silently.
- **The backup as root on the release, as the end state:** rejected. It
  satisfies goal 5, but root plus `CAP_DAC_READ_SEARCH` can still read every
  file on the host, for Python that needs one file. It is kept as C4, the
  interim.
- **Forcing a probe tick to verify a deploy:** rejected. The two-tick
  `pending` rule (`docs/BUS-HEALTH.md`) would compare two ticks seconds apart,
  so an in-flight entry reads as a stuck one. The deploy waits for the next
  natural tick instead (at most 10 min). The backup *is* forced: a repeat
  snapshot is `unchanged` (a 412 on create), and a run adds one small digests
  object.
- **Units run `uv run --frozen --no-sync` (status R5):** rejected for the
  venv's own interpreter. That needs no uv binary or uv cache at runtime, and
  the backup already works that way.
- **`--no-editable`:** no gain inside an immutable release.
- **One cutover child:** rejected. One more child is cheaper than a failure
  with two possible causes (observo's reasoning).
- **Drift as a `bus_health` finding:** rejected, as in D6.

## Steps (children of #22, each switching at most one unit)

1. **C1, `revision` (code only, no unit change).** Add `REVISION` reading
   (`dev` when absent) to the probe's summary line, the probe's check-in
   `variables`, and the backup's `state.json` and log. It ships the way it
   does today, merged into `~/broker`.
   **Verify:**
   - the next tick's summary line has `"revision": "dev"` and still matches
     the knob's `health` grep;
   - the check-in lands;
   - the next backup's `state.json` has `revision`;
   - `ACL LOG` has no new entry.
2. **C2, `scripts/deploy.sh` and the first release (no unit switched).**
   Includes the CI gate, the wheelhouse copy, unit install (a no-op here, since
   the release's units still match the installed ones), the stub harness, and a
   one-time `sudo install -d -m 755 /srv/broker`.
   **Verify:**
   - `/srv/broker`, `releases/` and the release are root's; the release is
     `555` and `REVISION` is `444`;
   - `REVISION` equals `origin/main`;
   - a venv file has `stat -c %h` of 1;
   - the release venv imports both modules and co-core 0.36.0;
   - as `exedev`, `touch` in the release and `ln -sfn` over `live` are both
     refused;
   - the journal shows `CI passed`;
   - both units still read `WorkingDirectory=/home/exedev/broker`.
3. **C3, switch `broker-bus-health`.** `WorkingDirectory=/srv/broker/live`,
   `ExecStart=` the release venv, `PYTHONDONTWRITEBYTECODE=1`. Installed by
   `deploy.sh`.
   **Verify:**
   - the next natural tick logs `revision` equal to the sha12, with 0 findings,
     and its check-in lands;
   - `systemctl show -p WorkingDirectory,ExecStart` names the release;
   - an uncommitted edit under `~/broker/src/` doesn't reach a tick;
   - a rollback drill (deploy the previous sha12, then forward again) shows
     both revisions on ticks;
   - `ACL LOG` has no new entry;
   - `tests/deploy` passes on the node.
4. **C4, switch `broker-backup` (still root).** `WorkingDirectory=` and
   `ExecStart=` on the release; `ProtectHome=yes`, since it no longer reads
   `/home`. The deploy forces one run.
   **Verify:**
   - the forced run says `uploaded` or `unchanged`, and `state.json`'s
     `revision` is the sha12;
   - the next timer run succeeds;
   - the probe reports no `backup` finding;
   - the knob's lines are unchanged.
5. **C5, `broker-backup` drops root.** `useradd --system broker-backup`, then
   B1's unit shape. `backup.env`'s `GOOGLE_APPLICATION_CREDENTIALS` points at
   the credential path.
   **Verify:**
   - the run's journal `_UID` is `broker-backup`'s;
   - the upload and the digests both ship;
   - `id broker-backup` lists no `redis`;
   - `sudo -u broker-backup test -r /var/lib/redis/dump.rdb` fails;
   - `/run/broker-backup` is gone after the run;
   - `StateDirectory` is the new user's and `state.json` is still `0644`;
   - the probe reports no `backup` finding;
   - the next hourly run succeeds.
6. **C6, the drift check** (one new unit). D6. Needs the co-status monitor
   first.
   **Verify:**
   - a forced run logs `drift check` with `kind: ok` and `checkin: 202`;
   - `--test-alert` reaches notifier;
   - `co-broker-drift` reads `ok`.
7. **C7, retire the live-tree convention** (no unit). Finish B6's list.
   Write `.skills/deploy_command` if skills#345 has merged, or name
   `scripts/deploy.sh` in AGENTS.md if not. Hand #80's recipe change to the
   coordinator.
   **Verify:**
   - `grep -rn '/home/exedev/broker' deploy/ docs/ scripts/ src/` returns
     nothing but history;
   - CI is green;
   - `tests/deploy` passes on the node.

**Order:**

- C1 goes first, so the first release reports its own revision.
- C3 comes before C4: the probe is WARN-only and ticks every 10 minutes, and
  it is the alarm if C4 breaks the backup.
- C6 needs C2 and an answer from co-status. It blocks nothing else.

## Decisions (for the operator)

- **D1:** root-owned releases under `/srv/broker`, built and switched by
  `scripts/deploy.sh` (run as `exedev` with `sudo`) from a CI-green commit on
  `origin/main`. Q1-4, Q8.
- **D2:** the backup drops root in two steps: onto the release as root (C4),
  then to a static `broker-backup` user with a privileged `install` copy and
  `LoadCredential=` (C5). Not through the `redis` group, and not as
  `DynamicUser=`. B1.
- **D3:** the probe stays `User=exedev` in this pilot. A dedicated probe user
  is a follow-up issue, filed after C7. It would close the window in which
  `exedev` can read the decrypted `brokeradmin` credential, which the unit's
  own comment names. Operators would then need `sudo` to delete DLQ evidence.
- **D4:** the deploy syncs the wheelhouse and copies only the wheels the lock
  names into the release. `/etc/broker/.env` and `co-pypi-reader.json` stay as
  they are. B3, B4.
- **D5:** the build id is `REVISION`, carried in the probe's summary line and
  the check-in's `variables` (never `metadata`) and in the backup's
  `state.json`. Q9.
- **D6:** drift is checked by the `broker-drift` timer, reporting to its own
  co-status monitor `co-broker-drift`, with an 8 h grace. Filing the status
  request needs your per-post approval. Q9, goal 4.
- **D7:** the CI gate is the `push` run of `ci.yml` on `main`, every job
  `success`, checked unauthenticated, with a logged `--skip-ci`. The `audit`
  workflow is report-only and is not gated.
- **D8:** verification is the next natural probe tick (no forced tick) plus
  one forced backup run. Exit 1 means switched back; exit 4 means nothing
  answers.
- **D9:** the deploy installs only `deploy/broker-*.{service,timer}`. Host
  configs (`redis.conf.broker`, the drop-ins, `redis-acl.conf`) are compared,
  and a difference is a note, never an install. The deploy never restarts
  `redis-server`.
- **D10:** adopt skills#345's `.skills/deploy_command` = `scripts/deploy.sh`
  when it ships. Until then, AGENTS.md names the script. No vendored skill is
  edited.
- **D11:** C1-C7 are filed as sub-issues of #22, and #22's body becomes their
  epic (as observo did). #80 keeps one line pointing at it.

## Open questions / risks

- **Disk:** 4.6 GB is free. A no-dev, copy-mode venv is about 70-100 MB (the
  dev venv is 91 MB), so 5 releases plus `live` is at most about 0.6 GB. C2
  measures the real figure.
- **C5's mechanisms are new on this node:** `ExecStartPre=+`, `LoadCredential=`
  into a static user, and `RuntimeDirectory=`. They are standard systemd 255,
  but no broker unit uses them yet. If C5 fails, its fallback is C4's state,
  which already meets goal 5.
- **A rebuilt node now needs GitHub and GCS before its timers run**: `deploy.sh`
  comes before the units in `docs/RECOVERY.md`'s order. With GitHub down,
  `--skip-ci`. `restore` runs as root from `/srv/broker/live`, which is
  root-owned code: an improvement on today.
- **gregoryfoster/skills#345** is still open, and **D6** waits on co-status.
  Neither blocks C1-C5.
