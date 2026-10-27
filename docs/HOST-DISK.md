# Host disk - what else fills `/`

The probe's `disk` finding (WARN at 90% used or under 2 GiB free,
[BUS-HEALTH.md](BUS-HEALTH.md)) is about Redis's AOF headroom, but `/` is a
20 GB root that Redis shares with the operator's editor and the test suite. On
2026-10-24 the editor's builds pushed it over the line (CannObserv/broker#126).
This is what grows there, what prunes itself, and the disk GC that prunes the
rest (#127).

## What grows

Measured 2026-10-25, after the 2026-10-24 cleanup (`/` at 71%, 5.6 GB free):

| Path | Size | Bound |
|---|---|---|
| `~/.vscode-server/cli/servers/` | 1.3 GB, two builds | the editor's: **none that holds**; the GC's `vscode-servers` - below |
| `~/.vscode-server/code-<hash>` | 35 MB per CLI binary, three | the editor's: none; the GC's `vscode-clis` |
| `~/.vscode-server/extensions/` | 910 MB: three Claude Code builds, one Redis | the editor's restart, and the GC's `claude-extensions` - below |
| `/tmp/pytest-of-exedev` | 611 MB | `tmp_path_retention_policy = "failed"`, count 1 (#126, #127) - below |
| `/srv/broker/releases` | 441 MB | `scripts/deploy.sh` keeps five |
| `~/.npm`, `~/.cache` (uv) | 1.2 GB, 913 MB | the GC's `npx`, `npm-cache` and `uv` (`uv cache prune` only) |
| `/tmp/claude-1000` | 676 MB | agent scratchpads; a reboot clears `/tmp` |
| `/var/lib/redis` | 38 MB | `maxmemory` ([MEMORY-PROTECTION.md](MEMORY-PROTECTION.md)) |

## What the editor prunes itself

Read against VS Code 1.140.0's source (`07f806f`, the build running here) and
this node's files, 2026-10-25. Nothing was reloaded or restarted to find out.

- **Extension builds: on a restart.** A superseded build is listed in
  `extensions/.obsolete` and removed when the server next starts.
- **The VSIX cache: on a restart.** `CachedExtensionVSIXs` keeps the newest
  package per extension, at most 20 (`extensionDownloader.ts`, `cleanUp`).
- **Server builds: an LRU of five, which leaks.** Two long-lived processes
  install into `cli/servers/`: the CLI's *agent host* (`code-<hash> agent host`,
  up since boot), which checks Stable every 6 hours and pre-downloads each
  release whether or not a client uses it (seven between 2026-10-04 and 10-23),
  and the remote-SSH `command-shell`, which installs the client's build. Both
  share one cache, `download_cache.rs`'s `KEEP_LRU = 5` over `lru.json`. Each
  process reads that list once and then overwrites it (`state.rs` keeps it in
  memory), so a build one adds falls out of the file when the other writes, and
  nothing ever deletes a build that is not in the list. Today the running build
  is absent from `lru.json`, and four of its five entries name directories
  removed by hand on 10-24. The eviction does not check for a running server,
  either. At 2026-10-22 04:13Z the CLI found the running server's logfile
  missing and downloaded its build again, the day after the agent host's
  1.143.0 download: consistent with the LRU having deleted a running build
  (inferred, not observed).
- **`code tunnel prune` - supported, and not usable here.** It deletes every
  server directory without a live pid. On 1.140.0 it deletes nothing and says
  it succeeded: it parses `Stable-<hash>` with a lowercase-only quality match
  (read in `options.rs` and microsoft/vscode#337944, open; not run here). 1.143.1's source has the fix, and from then it
  would also delete the agent host's staged newer build. No dry run, no record.

So the extensions are bounded by the operator's own restarts, and the server
builds and CLI binaries by nothing the editor does. The GC bounds both.

## The disk GC

Provisioner's canonical GC and headroom sensor (CannObserv/provisioner#61),
adopted through the vendored `maintaining-host-disk` skill (#127). It replaced
#126's by-hand `scripts/prune_editor_builds.sh`. The skill's `scripts/_pin.sh`
pins the release, and `VERSION` 2 is installed under
`/usr/local/lib/cannobserv/disk-gc/`. This host's half is
`/etc/cannobserv/disk-gc/disk-gc.conf`, which no script writes. A change to it
is a host change, and needs the operator's approval. A change to the GC's rules
is Provisioner's, raised through the operator: an installed copy that no longer
matches its MANIFEST is refused at the next update.

- **`cannobserv-disk-gc.timer`:** daily, 05:15 UTC plus up to 30 min. Tiers:
  `vscode-servers vscode-clis claude-extensions vsix-cache plugin-cache npx
  npm-cache pytest-garbage uv worktrees`, over `/home/exedev`. No `docker`:
  nothing builds images here, and `docker.socket` stays disabled. `worktrees`
  covers `~/broker` (`worktree_repos`), so only directories git no longer
  registers go, once untouched for 60 min. `uv_clean_above_mb = 0`: prune,
  never clean. Don't start `scripts/deploy.sh` around 05:15: its `uv sync` and
  the GC's `uv cache prune` share `~/.cache/uv`.
- **The SocratiCode pin:** `npx_spec_files` names this repo's
  `.claude/settings.json`, so the `SOCRATICODE_SPEC` tree is kept. An npx
  reinstall is what starved this node's RAM on 2026-09-16. A re-pin needs no
  config edit. An unreadable settings file keeps every npx tree.
- **`cannobserv-disk-headroom.timer`:** every 10 min. It checks in to
  `co-broker-disk-headroom` (CannObserv/status#70), on the `co-broker` tenant,
  with the tenant's key copied root to root from
  `/etc/broker/status-checkin.key`. It reports `low-headroom`, plus `gc-failed`
  and `gc-stale` (no GC run in 72 h), which the probe can't see.
- **The lines: alert at 85% used, clear at 80%.** `df`'s used plus available
  is 18.57 GiB. On 2026-10-27, `/` was at 69% (5.77 GiB free). One node run's
  basetemp, about 200 MB, is 1%, so 70% at worst. The probe's `disk` warning
  bites first at its 2 GiB-free bound, which is 89.2%. 85% leaves 2.78 GiB
  free and pages 4.2 points before it. 80% is 10 points above the baseline, so
  a page clears once the GC has run. patching-hosts reads `alert_at_percent`
  as a floor: a patch step that would cross it refuses (gregoryfoster/skills#390).

**Where it differs from the retired script**, each accepted in #127
(the skill's `references/editor-builds.md`):

- It removes a **staged newer server build** once it's past the grace window.
  The CLI's agent host pre-downloads each Stable release, so the likely cost is
  a re-download per release.
- With nothing running, it **keeps no build**.
- It prunes the **VSIX cache** (`vsix-cache`). The editor prunes it too, but
  only on a restart.

Read it with `sudo`. Without it, `journalctl` prints `-- No entries --`:

```bash
systemctl list-timers 'cannobserv-disk-*'
sudo journalctl -u cannobserv-disk-gc.service --since -1d
sudo journalctl -u cannobserv-disk-headroom.service -p warning --since -1d
```

## pytest's share

`tmp_path_retention_policy = "failed"` and `tmp_path_retention_count = 1` in
`pyproject.toml`: a passing session keeps no `tmp_path`, and only the newest
session's tree is kept (#127), so the GC's line is the baseline plus one
run's. A session with a failure, or one cut short, keeps the failed tests' and
its session-scoped `mktemp` dirs (the throwaway engines'
`acl*`, with their ACL files). The default kept every test's for three
sessions, about 200 MB each on a node run. The deploy tests' `555` releases
outlast the per-test cleanup (`rmtree(..., ignore_errors=True)` cannot enter
them) and go with the rotation at the next session, which makes them writable first.
A by-hand `rm -rf` of the base temp needs `chmod -R u+w` before it.
`tests/deploy/test_pytest_tmp_retention.py` pins both halves.
