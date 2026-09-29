# DLQ draining

Who writes, who triages and who backstops each dead-letter queue, and the
procedure that empties one. Split out of [STREAMS.md](STREAMS.md), whose table
names each queue's writer and drainer - the assignment this doc explains; its
**Never XTRIMmed** rows are the streams the drain below is refused on.

## Who drains a DLQ

Provenance: CannObserv/archiver#162.

Three roles, and no two of them are reliably the same service:

- **Writer** - whichever service's consumer calls `AsyncBusConsumer.dead_letter()`
  on that topic; the `DLQ` column of [STREAMS.md](STREAMS.md) names it per stream. Archiver writes
  `content.revisions.dlq` and `content.artifacts.dlq`, Replicator writes
  `content.fetch.dlq` and `content.replicate.dlq`, and `content.persist.dlq`
  once its persist loop is enabled (CannObserv/broker#64), Observo will write
  `content.process.dlq` and Watcher `content.derived.dlq` once the processing
  pair's consumers ship (CannObserv/broker#62), and the groupless config/state
  streams can write none at all.
- **Drainer - the stream's own consumer, per stream.** Named in the `DLQ`
  column of [STREAMS.md](STREAMS.md), so an unowned queue reads as a blank cell rather than something
  to infer. **Settled by CannObserv/broker#1 Phase 5**, replacing "Archiver, for
  every DLQ on this broker" - a claim that was a corollary of operating the
  instance (CannObserv/archiver#109, CannObserv/archiver#162) and lost its
  premise when CannObserv/archiver#193 D6 moved the broker to a neutral node.

  Two things forced it rather than tidiness. **D3's per-service ACL users make
  the old assignment unimplementable**: draining `content.fetch.dlq` would need
  Archiver granted `~content.fetch.dlq` plus `+xrange +xtrim`, and finding a
  queue nobody told it about would need instance-wide `SCAN` - a cluster-wide
  hole through the one model whose payoff is that Archiver cannot name
  `content.blobs`. The consumer-drains rule needs a far smaller grant: every
  service already holds `~<its own topic>.dlq`. **It is not "no extra grant",
  which is what this said until CannObserv/broker#12** - a key pattern is not a
  deletion grant, and for five of the six queues below the named drainer could
  write its queue and not empty it. Each drainer now holds a **selector**,
  `(+xdel ~<its own>.dlq)`, which is the only ACL grammar that scopes a command
  to a pattern; the root permission set never holds `+xdel`, so the grant cannot
  reach the stream the queue is a copy of. And **triage is not mechanical**
  - "residue, or a real permanent failure?" is a question about the payload, and
  the consumer is the party that can read it. The #162 drain settles that: those
  110 were Replicator's writes, of Watcher's commands, caused by Archiver's test
  suite, and nothing about operating the instance would have told you so.

- **Broker - detection, evidence, escalation, and the backstop.** The mechanical
  half of the old drainer role stays cluster-wide, because it is suffix-keyed
  and needs no payload semantics, and because the broker is the only party that
  can `SCAN` for a queue nobody claims. `src/broker/bus_health.py` warns on any
  non-zero `*.dlq` depth, names the drainer from `DLQ_DRAINERS` (a mirror of the
  column in [STREAMS.md](STREAMS.md)), and **dumps the entries to `dlq-evidence/` under the unit's
  `StateDirectory` on first sight**. A `*.dlq` key with no named drainer is
  reported as unassigned rather than skipped - a DLQ with nobody named is a DLQ
  nobody empties, which is exactly how `content.fetch.dlq` reached 110.
- **Polluter** - whoever put junk in it, which is automatically none of the
  above. The 110 were Replicator's writes, of Watcher's commands, caused by
  Archiver's test suite (CannObserv/archiver#157).

**Resting state is depth 0 on every `*.dlq` key.** That is the invariant worth
holding: a non-zero depth then means a real dead-letter awaiting triage, rather
than a number an operator has to know the backstory of before ignoring it. This
repo's bus-health probe scans every `*.dlq` key each tick and WARNs on any
non-zero depth; a dashboard rendering of the same numbers is archiver's
(CannObserv/archiver#147), alongside the other streams that cannot use group
lag.

Draining is never in-band cleanup. Audit, back up, trim, verify - in that order,
because reversing it destroys the evidence you needed to justify the trim.

**The back-up step is already done.** It is the one an operator under time
pressure skips, so the probe does it on the tick that first sees the depth: the
entries are at
`/var/lib/broker-bus-health/dlq-evidence/<topic>/<last-id>.json` on the broker
node, and the finding names the exact path. Capture is incremental by stream id,
so a queue that keeps growing accumulates one dump per growth rather than
re-dumping itself every ten minutes. Read those before the `XRANGE` below;
re-dump only if you want the entries in `redis-cli`'s own framing.

```bash
# `cred` and `rcli` as defined at the top of RESTART-WINDOW.md - the operator,
# acladmin; bare redis-cli is NOAUTH, and a password on its argv is the wrong fix.
rcli acladmin XLEN content.fetch.dlq                # what you are about to delete
rcli acladmin XINFO STREAM content.fetch.dlq | grep -A1 last-generated-id   # the boundary, FIRST
rcli acladmin --no-raw XRANGE content.fetch.dlq - <last-generated-id> > /var/tmp/fetch-dlq-$(date +%F).txt
# read it: every payload residue, or is a real permanent failure hiding in there?
rcli acladmin XTRIM content.fetch.dlq MINID <last-generated-id, +1ms>
rcli acladmin XLEN content.fetch.dlq                # -> 0, or what landed since the boundary
```

`XTRIM MINID`, not `DEL`: the boundary confines the deletion to the entries you
actually audited, and the key plus any consumer groups stay in place. **The
boundary is taken before the read, and the dump stops at it.** Taken after -
this runbook's order until CannObserv/broker#59 - it lands above anything
dead-lettered while you were reading, and the trim removes those unread. Taken
first, a later entry carries a higher id and survives, and everything the trim
removes is in the dump. The same hazard is why archiver's triage disposes by
`XDEL` of named ids (CannObserv/archiver#238).

**For `*.dlq` keys only.** Four rows of [STREAMS.md](STREAMS.md) say **Never XTRIMmed** -
`content.replicate`, `content.process`, `content.persist` and `info.registry` -
and this procedure pointed at any of them is refused: `acladmin`, the credential it runs as since CannObserv/broker#52,
holds `+xtrim` on `~*.dlq` and nothing else (CannObserv/broker#34; it was
`brokeradmin`'s until #52). The refusal is the backstop against the incident
reflex - `acladmin` could lift it with its own `+acl`, and must not; the reasons
are in the rows.

Worked example, the CannObserv/archiver#162 drain (2026-08-19): 110 entries, every one a
`content_fetch` command against `example.com`, all inside one 18-minute window on
2026-08-13, zero non-residue payloads, zero consumer groups on the key.
`XTRIM content.fetch.dlq MINID 1786635782730-0` removed exactly those 110 and
left the key at depth 0.
