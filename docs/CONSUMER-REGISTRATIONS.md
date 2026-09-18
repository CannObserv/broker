# Consumer registrations: the one-time orphan reap

A procedure that ran once and cannot recur, kept because the reasoning in it
is what makes that true. Which streams carry which groups is
[STREAMS.md](STREAMS.md); what the probe watches on them is
[BUS-HEALTH.md](BUS-HEALTH.md). Moved out of STREAMS.md when it ran past the
per-doc context budget (CannObserv/broker#14 review, finding 8).

## Orphaned consumer registrations, one time only

Provenance: CannObserv/archiver#156.

Archiver's group consumers used to name themselves `{hostname}:{pid}`, so every
restart that received a message left a registration behind and nothing reaped it.
Seven had accumulated on `archiver.revisions` by 2026-08-27, six of them dead.
The consumers are now named for their group (`archiver-revisions-1`,
`archiver-artifacts-1`), stable across restarts, so **this cannot recur** - which
is why the cleanup is a one-time procedure here rather than a startup reaper.

Order matters: **deploy, restart, then reap.** Reaping before the restart leaves
the running process's own registration behind.

⚠️ **`XGROUP DELCONSUMER` destroys that consumer's pending entries.** Never reap
a consumer with a non-zero PEL - that is a stranded message needing `XAUTOCLAIM`,
not an orphan. The loop below therefore re-reads `pending` per consumer and skips
any that is non-zero, rather than trusting a check the operator ran beforehand;
`XGROUP DELCONSUMER` returns how many entries it destroyed, and by the time you
can read that number they are already gone.

It also skips the **current** consumer by name rather than matching the old
`{hostname}:{pid}` shape. A `watcher:` filter would be specific to this VM's
hostname and would silently match nothing anywhere else, which reads identical to
"already clean".

```bash
reap_orphans() {   # stream group live-consumer-name
  redis-cli XINFO CONSUMERS "$1" "$2" \
    | awk '/^name$/{getline n} /^pending$/{getline p; print n, p}' \
    | while read -r name pending; do
        if   [ "$name" = "$3" ];  then echo "keep $name (current consumer)"
        elif [ "$pending" != 0 ]; then echo "SKIP $name - pending=$pending, stranded not orphan"
        else redis-cli XGROUP DELCONSUMER "$1" "$2" "$name" >/dev/null && echo "reaped $name"
        fi
      done
}

reap_orphans content.revisions archiver.revisions archiver-revisions-1
reap_orphans content.artifacts archiver.artifacts archiver-artifacts-1
```

`archiver.artifacts` carried 0 registrations as of 2026-08-27 - its stream has
never delivered an entry, and registration happens on delivery - so that second
call is expected to print nothing. It is in the procedure anyway because the
group used the same pre-fix naming and would have leaked identically once
replication traffic started.

**Do not verify by looking for `archiver-revisions-1`.** Registration happens on
*delivery*: an `XREADGROUP` that returns zero entries does not register the
consumer, so on a quiet stream the new name is correctly absent and appears when
traffic next arrives. Verify from the journal instead:

```bash
sudo journalctl -u archiver -n 200 | grep 'Bus consumer starting'
```
