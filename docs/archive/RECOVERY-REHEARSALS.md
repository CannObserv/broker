# Recovery rehearsals

The record of each restore and backup rehearsal behind
[RECOVERY.md](../RECOVERY.md), moved out of it in CannObserv/broker#111 to keep
the runbook inside its context budget. History: nothing here is a step to run.

## Rehearsal record

- **Every test run**: `tests/deploy/test_backup_restore_rehearsal.py` seeds
  the node's engine (`engine_binary`, #102) with a stream, a group two entries
  in with one acked, and a TTL key; `SAVE`s; ships the file through
  `run_backup` into a fake bucket; restores it through `restore.newest_object`
  / `download` / `gunzip_file` / `stage_appendonlydir`; starts a second server
  under `appendonly yes` and asserts the length, `last-delivered-id`,
  `entries-read`, the PEL and the TTL.
  The control case, the same snapshot with no staging, comes up with `DBSIZE 0`.
  broker#72: the shipped digests are restored onto a "rebuilt" node, fresh node
  users are appended, and the result is rendered and loaded. Every service
  authenticates; `acladmin`'s old password is refused.
- **2026-09-10, by hand** on `co-broker`, throwaway servers, Redis 7.0.15:
  positions intact; the control confirmed the trap.
- **2026-09-10 16:03 UTC, before the key existed.** The unit was started once
  to prove the sandbox: it loaded its env, failed at the absent key file, and
  recorded `last_failure_at` under `ProtectSystem=strict`. The state directory
  was then removed by hand so the probe would read "not installed" until the
  timer was enabled - which is why the first success below has no failure
  beside it.
- **16:32 UTC, the first run with the key.** Created
  `co-broker/20260910T153511Z.rdb.gz`: 498,485 bytes of RDB, 144,810 gzipped,
  37 keys. Every field the object's metadata carries matched the state file.
  The 27 keys with TTLs are `redis-check-rdb`'s `expires` line, which neither
  carries.
- **16:34 UTC, a second run.** `unchanged`: the 412 that RECOVERY's *Why
  create-only* predicts, observed - not a 403 from the missing `delete`.
- **16:35 UTC, the restore.** `restore --latest` staged that object on a
  scratch directory; `redis-check-rdb` passed and the base's sha256 matched the
  metadata. A throwaway server under `appendonly yes` logged `DB loaded from
  base file` and came up with 36 of the 37 keys (the 37th a
  `replicator:cmd:fetch:*` guard whose expiry had passed in the hour since the
  snapshot) and every group at exactly its snapshot position:
  `replicator.fetch` and `watcher.blobs` at `entries-read 993` against the live
  broker's 994 an hour on, the other three identical to live. The server was
  shut down and the directory removed.
- **Minutes later, the probe.** Its next tick with the state file present
  raised no `backup` finding: `finding_count: 0`.
- **2026-09-29, the ACL digests (broker#72), by hand on `co-broker`.**
  `restore --digests` ran twice to a scratch path, first on the pre-CR-1 name
  and then on `co-broker/20260929T210440Z.digests`. Each came out 0400 root
  with the five service users, and its sha256 matched the node file's service
  lines and the state file. A second write to the first path was refused. The unit's namespace under `/etc/broker` showed
  `co-broker-backup.json` alone.
- **2026-10-16, Valkey 7.2.13 (broker#102), by hand on `co-broker`.** The
  `valkey-server` and `valkey-tools` debs extracted with `dpkg-deb -x` into a
  scratch directory, never installed. Run with `BROKER_TEST_ENGINE` naming the
  extracted `valkey-server` and a `PATH` putting `valkey-check-rdb` first:
  this rehearsal, `test_aof_replay.py`, `test_bus_health_group_positions.py`,
  `test_redis_acl.py`, `test_throwaway_engine.py` and `tests/test_backup.py`,
  240 passed, the four DLQ skips. The snapshot was RDB v11, `valkey-ver`
  `7.2.13`, recorded as `redis_version`. Valkey's checker also passed a 7.0.15
  file and reported its `redis-ver`; 7.0's rejected Valkey's (exit 1).
- **2026-10-22, the unit without root (broker#109), by hand on `co-broker`.**
  The new unit, renamed and run from `/run/systemd/system/`, with its state
  and runtime directories renamed and `ExecStart=` swapped for a script that
  uploads nothing. As uid 997 in its own group alone: `/etc/broker` empty, both
  credentials their sources' sizes, the live `dump.rdb` unreadable, the copy
  `0400` in a `0700` runtime directory, the checker passing 48 keys, the
  digests projected, one listing as the writer. So systemd 255 reads a
  `LoadCredential=` source past `TemporaryFileSystem=`, #110's open question.
  The #108 unit's shape then wrote `state.json` as root into the directory the
  first left `broker-backup`'s: the chown a switch back needs. Both runtime
  directories were gone after the runs.
