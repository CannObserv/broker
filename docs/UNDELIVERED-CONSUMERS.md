# Consumers that stopped reading

The `group-undelivered` check: the 2026-09-16 event that asked for it, why
`pending`, `lag` and `idle` are each blind to it, the positions it compares
instead, and what no duration can size.

The probe that runs it, and every other check on this node, is
[BUS-HEALTH.md](BUS-HEALTH.md); which stream carries which group is
[STREAMS.md](STREAMS.md).

Moved out of BUS-HEALTH.md on 2026-09-22, when it ran past the per-doc
context budget. The one-line contract stayed there, in *The bus-health
probe*; this is the reasoning behind it.

## A consumer that stopped reading

Provenance: CannObserv/broker#20.

**A group whose consumer is gone was invisible to every other check here**, and
the probe said so out loud: after the node's reboot on 2026-09-16
(redis-server up 15:26:34Z, AOF intact) replicator never reconnected,
`replicator.fetch` held one undelivered command from 15:27:00Z onwards, and the
tick at 15:38 and every tick after it reported **0 findings**. It was found by
hand.

Each existing signal is blind to a consumer that has stopped *reading*:

- **`pending` counts what was delivered and not acked.** A consumer that never
  calls `XREADGROUP` is delivered nothing, so its pending count sits at `0` -
  which is the healthy value. The two-tick rule above catches a consumer wedged
  *after* delivery; this is the half before it.
- **Last-entry age catches a stopped producer**, and exists only for the
  permanently-groupless streams.
- **Consumer `idle` is useless right after a restart** - exactly when it is most
  wanted. It read 869 s for every consumer on every group at 15:41: the time
  since the AOF load, not since each consumer's last read.

## Positions, not counters - and why not `lag`

`XINFO GROUPS` reports a `lag` per group, which looks like the answer and is
not. Measured that morning:

| Group | `lag` | Where it actually stood |
|---|---|---|
| `watcher.blobs` | 152 | at the stream's `last-generated-id` - caught up |
| `archiver.revisions` | 147 | at the stream's `last-generated-id` - caught up |
| `replicator.fetch` | 153 | behind by **one** entry - the real fault |

A lag-based check would have raised three findings, two of them false, and
misstated the third. The cause is structural rather than a quirk: `lag` is
`entries-added` minus the group's `entries-read`, and **`entries-read` does not
survive a reload** - `test_a_reload_keeps_the_position_and_loses_lags_input`
restarts a server to show it. `last-delivered-id` does survive, because it is
what the consumer's next read resumes from.

So the check compares positions and dates one entry:

1. `XINFO STREAM <stream>` -> `last-generated-id`, which the length and
   continuity checks already read;
2. `XINFO GROUPS <stream>` -> that group's `last-delivered-id`, plus the
   `pending` count and the group names the checks above read out of the same
   reply - one round trip, one observation (CannObserv/broker#29);
3. equal - or the group *ahead*, which happens when an `XADD` lands between the
   two replies - and the group is caught up, whatever `lag` says. Nothing
   further is read;
4. behind, and `XRANGE <stream> (<last-delivered-id> + COUNT 1` gives the oldest
   entry it has not been offered. The timestamp in that id is its age.

**The threshold is 5 minutes on every group, and it is not a mirrored
constant.** Every other threshold in this probe copies a retention cap owned in
another repo; this one is owned here, because it describes the consumer's read
loop as this node can observe it. All five groups are blocking `XREADGROUP`
readers, so delivery is immediate - `replicator.fetch` answered the 14:18:00Z
command at 14:18:01Z - and five minutes is two orders of magnitude of slack over
that, and ~55x the slowest handler: `content.replicate`'s, 5.4 s at the 64 MiB
blob ceiling (CannObserv/replicator#96, broker#30). A consumer that ever moves
to a schedule rather than a blocking read needs its own value on its row: that
schedule's period plus margin, with the source named the way a mirrored constant
names its owner. `test_every_probed_group_carries_an_undelivered_threshold`
fails if a sixth group arrives without one, and `StreamCheck` refuses the other
direction - a threshold on a row with no group - at import.

**What no duration sizes: a consumer alive and not reading**, since a reader
inside a handler is not reading - one stalled-provider attempt, or recovery
re-claiming its own failing entry every cycle so `XREADGROUP` never runs
(CannObserv/replicator#98). Both hold a delivered entry, so the finding words
itself by the group's `pending` count: at 0, a stopped reader (for a consumer
that acks after handling, as replicator's does); above 0, run `XPENDING <stream>
<group> - + 10` twice. A delivery count that climbs is a consumer alive and
retrying; one that stands still is one long attempt or a dead holder, which
`CLIENT LIST` separates.

**A stream trimmed past its group's position is its own finding**
(`group-undelivered-lost`), not an age. The group is behind and the entries it
is behind by are gone - trimmed or deleted before delivery - so there is nothing
to date and nothing that will ever arrive. Reported as an age it would read as
either healthy or as a stopped consumer, and the remedy is neither: it is the
hazard `content.replicate` is carved out of every trim path for, happening on a
stream that is not carved out.

**It costs no grant, no round trip, and joins nothing.** `XINFO STREAM`,
`XINFO GROUPS` and `XRANGE` are all read-only introspection `brokeradmin`
already held, so the check shipped without touching `deploy/redis-acl.conf`;
since CannObserv/broker#29 that `XINFO GROUPS` is the one the pending count
comes from; `test_the_probe_can_read_a_groups_position_without_joining_it`
asserts both halves - that the reads are permitted, and that `XREADGROUP` and
`XGROUP CREATE` are still refused. A probe that joined a group would take delivery of another
service's messages, which is the rule it exists on the other side of.

**Corroboration, not contract.** Zero `user=replicator` connections in
`CLIENT LIST` was the first visible sign on the day, and it is the right thing
to check next when this finding fires on a group holding nothing. It is not the
check itself: a connection count is not what a consumer promises, and a
connected process that has stopped reading looks identical to a healthy one.
