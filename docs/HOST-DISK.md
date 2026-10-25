# Host disk - what else fills `/`

The probe's `disk` finding (WARN at 90% used or under 2 GiB free,
[BUS-HEALTH.md](BUS-HEALTH.md)) is about Redis's AOF headroom, but `/` is a
20 GB root that Redis shares with the operator's editor and the test suite. On
2026-10-24 the editor's builds pushed it over the line (CannObserv/broker#126).
This is what grows there, what prunes itself, and the step that prunes the rest.

## What grows

Measured 2026-10-25, after the 2026-10-24 cleanup (`/` at 71%, 5.6 GB free):

| Path | Size | Bound |
|---|---|---|
| `~/.vscode-server/cli/servers/` | 1.3 GB, two builds | **none that holds** - below |
| `~/.vscode-server/code-<hash>` | 35 MB per CLI binary, three | none: one per client update |
| `~/.vscode-server/extensions/` | 910 MB: three Claude Code builds, one Redis | the editor's restart - below |
| `/tmp/pytest-of-exedev` | 611 MB | `tmp_path_retention_policy = "failed"` (#126) - below |
| `/srv/broker/releases` | 441 MB | `scripts/deploy.sh` keeps five |
| `~/.npm`, `~/.cache` (uv) | 1.2 GB, 913 MB | unchanged since 2026-10-24; not pruned |
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
builds and CLI binaries by nothing.

## The by-hand prune

`scripts/prune_editor_builds.sh`, until patching-hosts prunes editor builds
itself (#126 chose that: the attended monthly run, not a timer in this repo's
units; the ask is drafted on #126). It keeps what `ps` shows running, anything
newer, and what it cannot read, and touches nothing the editor prunes itself.
The rules are in its header and `tests/deploy/test_prune_editor_builds.py`.

In the monthly patching run, before its dry-run cost reads free disk, as the
editor's user:

```bash
bash scripts/prune_editor_builds.sh           # dry run: read what it would remove
bash scripts/prune_editor_builds.sh --apply   # only after the operator approves it
```

`--apply` writes `~/.local/state/editor-prune/<UTC>.log`: each removal and its
bytes, the total, and `df /` before and after. Paste it on the run's record.
Never run it blind mid-session: the editor is the operator's working surface.

## pytest's share

`tmp_path_retention_policy = "failed"` in `pyproject.toml`: a passing session
keeps no `tmp_path`. A session with a failure, or one cut short, keeps the
failed tests' and its session-scoped `mktemp` dirs (the throwaway engines'
`acl*`, with their ACL files). The default kept every test's for three
sessions, about 200 MB each on a node run. The deploy tests' `555` releases
outlast the per-test cleanup (`rmtree(..., ignore_errors=True)` cannot enter
them) and go with the three-session rotation, which makes them writable first.
A by-hand `rm -rf` of the base temp needs `chmod -R u+w` before it.
`tests/deploy/test_pytest_tmp_retention.py` pins both halves.
