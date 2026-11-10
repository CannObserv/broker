# The LWW cap

How the probe sets the length threshold for `content.fetch-policy` and
`info.watch-status`, whose cap is partly a corpus size - and, since
CannObserv/broker#139, `info.registry`'s, under the same rule
([below](#inforegistry-the-same-rule-hourly)). The rest of the probe,
and the caps that are mirrored whole, are [BUS-HEALTH.md](BUS-HEALTH.md).

Moved out of BUS-HEALTH.md on 2026-10-07, when it ran past the per-doc
context budget (CannObserv/broker#42).

## The one cap that is read, not mirrored

`LWW_PRODUCER_MAXLEN` is a **default, not the whole rule**. Each producer
(watcher's `resolve_stream_maxlen`, archiver's `resolve_fetch_policy_maxlen`)
floors each LWW cap at `RETAINED_FULL_SETS` (10) copies of the set it is about
to republish, so the cap in force is `max(500, 10 x set)`. The sets were 3
(`content.fetch-policy`) and 4 (`info.watch-status`) on 2026-09-22 and the
default governs; from about 54 entries per set a threshold of 550 would have
said *the retention cap for this stream is not being applied* while the cap was
being applied correctly, just higher - and a standing WARN with the wrong cause
trains an operator to ignore the LWW rows, which is the blindness
CannObserv/broker#40 closed (CannObserv/broker#44).

**The third term cannot be mirrored.** It is the size of the producer's corpus,
it changes with no edit anywhere, and that is precisely the failure a mirror
cannot cover. So the probe reads it off the stream. The first reading,
`republished_set_size`, needs one `XINFO STREAM` reply - the one the length and
age checks already make - and is `length / the republishes the span holds`.
Three properties carry it:

- it is entries per republish **whether or not the cap is applied**, because an
  untrimmed stream grows its span in step with its length, so a broken cap still
  climbs through the threshold instead of carrying it along;
- it **rounds up twice** - the oldest retained entries are a partial set, and
  the division charges that fragment to the whole ones before the remainder is
  ceilinged. Up delays a real breach by a tick or two; down would invent one;
- it **never lowers the threshold**: `max(default, floor)` is the producers' rule
  and the probe's.

**What it reads on this node.** Measured 2026-09-22, against the live broker as
`brokeradmin` - `XINFO STREAM` is the read the length and age checks already
make, so this needs no grant nobody holds and no round trip nobody pays:

| Stream | `XLEN` | span | set read | cap in force |
|---|---|---|---|---|
| `content.fetch-policy` | 500 | 830 min (166 republishes) | 4, for a 3-host set | the mirrored 500 |
| `info.watch-status` | 504 | 625 min (125 republishes) | 5, for a 4-item set | the mirrored 500 |

One over the true set in both rows, which is the rounding working: the reading
is an upper bound on the set and therefore on the cap, and at these sizes it
changes nothing at all - `10 x 5` is far under 500.

The producer's end of the mirror is on notice:

- CannObserv/watcher#319 - the notice for the other end of #44's mirror:
  `RETAINED_FULL_SETS` and the `*/5` republish period are copied into
  `src/broker/bus_health.py`, and the period moves from watcher's *environment*
  (`WATCHER_WATCH_STATUS_REPUBLISH_CRON`) with no commit anywhere.

### A window that is not uniform

The span reading has two unknowns - the set size and the republishes per
period - and one equation, closed by assuming the second is 1. Three things
break that:

- **a gap** - republishes that did not happen, counted as if they had: reads
  low, warns early (CannObserv/broker#45);
- **a set that steps up** - the window averages old sets and new: low (#45).
  A bulk import or a restore does it; growth an item at a time does not;
- **republishing more often than the period** - watcher's mutation-deferred
  republishes on top of the `*/5` cron, `R` per period: reads `R` times high,
  so a cap that stops being applied is silent until the stream is `R` times
  over (CannObserv/broker#51). `R` was 1.00 when #51 measured it, but its
  trigger - a registry filling past 50 items - is also what makes the floor
  govern.

`read_set_size` takes the largest of three readings, and refuses the span where
it cannot be read:

| Reading | Read off | Wrong for |
|---|---|---|
| **span** | one reply | a gap, a step up (low, for most of a window); `R > 1` (high) |
| **since the last tick** | `entries-added` and the newest id, this tick and last, on the ids' clock | the tick straddling a producer's return (low); `R > 1` (high); after a mid-burst tick (up to half a set high, one tick) |
| **remembered** | a span in the state file, anchored to its window's newest entry | nothing it outlives: it expires when its anchor trims out, which after a gap is when the gap leaves |

The anchor moves on every tick whose window is no wider than
`RETAINED_FULL_SETS` periods, and otherwise only for a larger reading - moving
it only for a larger one pinned it to the fencepost's high reading until it
expired before the tick it was kept for. The same refresh stops a remembered set
outliving a shrink.

**A narrow window is refused, not read.** A stream that has been trimmed
(`entries-added` past its length) is at its cap, and at one republish a period
ten full sets cannot fit in fewer than nine periods - the fencepost, not a
chosen margin. At its cap the length is a second equation, so a since-last-tick
reading still stands if ten of it fit in the length: a shrink passes, `R > 1`
fails by `R`. With nothing left, the first tick is withheld (a shrink past one
republish cuts every anchor in one `XADD`) and from the second the mirrored
default governs, the finding naming the faster republish. A broken cap loses
nothing to that tick: narrow means under nine periods, and a threshold only
reports it past eleven. A stream still filling towards its cap is never refused.

**Replayed** in `tests/test_bus_health.py`: watcher's producer against a model
of `MAXLEN ~`, the probe ticking every 10 minutes with its state carried. The
model's macro node is 13 entries, as measured 2026-09-24 (`content.fetch-policy`
506 entries in 39 nodes, `info.watch-status` 500 in 63, `radix-tree-keys`): the
4096-byte limit binds, not the 100-entry one. A node smaller than one set keeps
the overshoot inside the 10% margin. *Before* is #44's span alone.

| Scenario | Before | Now |
|---|---|---|
| 1 missed republish | silent | silent |
| 2, 3, 6 or 24 missed | 5 ticks of `stream-length`, **after** the producer is back | silent |
| set steps up 1.1x, 1.25x | silent | silent |
| steps up 1.5x / 2x / 3x, 10x | 2 / 3 / 4 ticks | silent |
| set shrinks 2x, 3x, 10x | silent | silent |
| cap stops being applied | reported 2.5 periods later | the same tick |
| cap lost 2 periods after a 9x shrink | reported 10.5 periods later | 0.5 |
| `R` = 2, 3, 5, 10 | silent, the threshold `R` x the cap | a standing, named finding from 8.5, 6.5, 4.5, 4.5 periods |

The *before* gap row is worse than #45 recorded: `stream-age` clears once the
producer is back, and the length finding then stood alone until the gap trimmed
out, whatever its length. A period *lengthened* at home and not here reads the
set low permanently, the same mirror failure as any other.

## Two writers on one stream

`content.fetch-policy` had two producers from CannObserv/broker#93 until
Watcher's grant was withdrawn (CannObserv/broker#95): Watcher's, and Archiver's
(CannObserv/archiver#366), which built dark and took over at F1
(2026-10-26T21:42:27Z) with a full-set republish and the same cap and floor.
**Neither check changed while one published at a time**, which was the flip's
rule (the design's D8, one hard flip, no dual-run). The set read off the stream
followed whichever producer was writing, and the flip itself (a gap, then a
full set) was the gap and the step the readings above already survive: its
only finding was `stream-age` over the 2 h 47 min gap (#95's read-back). The
three mirrored numbers (the 500, the 10-set floor and the five-minute period)
were Watcher's until F1 and are Archiver's after, equal on both sides, so no
threshold moved at the flip, and since CannObserv/broker#135 they cite Archiver
alone for this stream. The 500's mover is `ARCHIVER_FETCH_POLICY_STREAM_MAXLEN`,
and the other two have none on Archiver's side. If either owner moves one,
`LWW_FULL_SET_FLOOR` splits per stream (CannObserv/broker#94).

Since #95 the ACL refuses Watcher a second write, but not a producer running
twice - two copies of Archiver's republish, or one doubled by a regression -
and that reads the same here as two producers did.

**Both at once is invisible while the sets are small.** Replayed as above,
each writer on its own `*/5`, offset a minute
(`test_replay_two_producers_below_the_floor_are_silent`,
`test_replay_two_producers_above_the_floor_are_named`):

| Sets (first + second writer) | `stream-length` after dual-run starts |
|---|---|
| 3 + 4, 4 + 4, 30 + 30 | never |
| 60 + 60, 100 + 100 | standing from 8.5 periods, naming a faster republish or a producer publishing twice |

Below the floor each producer caps at its own `MAXLEN ~500`, and the
threshold never falls under 550, so the length cannot breach: the summed set
read off the stream can only raise the threshold. Once each floor governs, the
stream sits at a cap holding too few periods for ten summed sets, the window is
refused, and the mirrored default governs - the `R > 1` path. At today's
sizes F1's dual-run would have shown in Replicator's journal instead: one
`ignoring a fetch policy without a generation` line per Watcher frame once a
host held an Archiver frame (CannObserv/replicator#135, stated on
CannObserv/archiver#366).

## `info.registry`, the same rule hourly

CannObserv/archiver#373 item 4 moved `info.registry` off a flat 50k onto this
rule (CannObserv/broker#139): `max(1_000, 10 x set)`, the floor computed by the
hourly snapshot alone, deltas carrying the last snapshot's cap. At 50k
Watcher's groupless `0-0` boot replay read about 100 days of history; at 1,000,
about two. Its three numbers are mirrored under their own names -
`REGISTRY_PRODUCER_MAXLEN`, `REGISTRY_RETAINED_FULL_SETS`,
`REGISTRY_REPUBLISH_PERIOD_SECONDS` - into `REGISTRY_FULL_SET_FLOOR`, not
`LWW_FULL_SET_FLOOR`: equal values today, separate constants in Archiver.
`ARCHIVER_REGISTRY_STREAM_MAXLEN` survives under the floor, raising the cap only.

**Read high, which is the safe side for the ceiling.** The deltas and each
deploy's republished set land between snapshots, so the span counts more than
one set a period - `R > 1` above, small: the 24 h to 2026-11-10T13:51Z held 507
entries for a 21-item set, 21.1 an hour, which the span rounds up to 22. A high set raises the threshold
only once `10 x set` passes 1,000, from about 100 items.

**`entries-removed` stays scoped by the default.** The cap in force is never
under 1,000, so a removal leaving fewer is a fault whatever the set. Scoped by
the floored cap instead, the high reading would put every hourly trim under it
once the floor governs. Between 1,000 and the floored cap a removal goes
unreported; the ceiling still catches a cap that stops being applied.

**The narrow-window refusal, once the floor governs.** Ten snapshots plus the
deltas and deploy sets between them have to span nine periods, or the window is
refused and the mirrored 1,100 governs - a false length finding. That needs
deltas past about a ninth of the set every hour; on 2026-11-10 they were about
3 a day against a 21-item set.

**The one-time trim.** Read 2026-11-10T13:51Z: `XLEN` 7,596, `entries-added`
10,201, first entry 2026-09-08T22:35:16Z. Broker's mirror deploys first, so
until Archiver's first publish under 1,000 the probe reports `stream-length`
every tick (7,596 over 1,100; the span reads a set of 6, 7,596 over 1,503
periods rounded up). The trim leaves
1,000 to about 1,100 (`stream-node-max-entries` 100) and no `entries-removed`,
since the stream ends at or over the scope. Archiver first would have reported
`entries-removed` every hour against the 50k mirror instead.
