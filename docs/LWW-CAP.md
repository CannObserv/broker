# The LWW cap

How the probe sets the length threshold for `content.fetch-policy` and
`info.watch-status`, whose cap is partly a corpus size. The rest of the probe,
and the caps that are mirrored whole, are [BUS-HEALTH.md](BUS-HEALTH.md).

Moved out of BUS-HEALTH.md on 2026-10-07, when it ran past the per-doc
context budget (CannObserv/broker#42).

## The one cap that is read, not mirrored

`LWW_PRODUCER_MAXLEN` is a **default, not the whole rule**. Watcher's
`resolve_stream_maxlen` floors each LWW cap at `RETAINED_FULL_SETS` (10) copies
of the set it is about to republish, so the cap in force is
`max(500, 10 x set)`. The sets were 3 (`content.fetch-policy`) and 4
(`info.watch-status`) on 2026-09-22 and the default governs; from about 54
entries per set a threshold of 550 would have said *the retention cap for this
stream is not being applied* while the cap was being applied correctly, just
higher - and a standing WARN with the wrong cause trains an operator to ignore
the LWW rows, which is the blindness CannObserv/broker#40 closed
(CannObserv/broker#44).

**The third term cannot be mirrored.** It is the size of Watcher's corpus, it
changes with no edit anywhere, and that is precisely the failure a mirror cannot
cover. So the probe reads it off the stream. The first reading,
`republished_set_size`, needs one `XINFO STREAM` reply - the one the length and
age checks already make - and is `length / the republishes the span holds`.
Three properties carry it:

- it is entries per republish **whether or not the cap is applied**, because an
  untrimmed stream grows its span in step with its length, so a broken cap still
  climbs through the threshold instead of carrying it along;
- it **rounds up twice** - the oldest retained entries are a partial set, and
  the division charges that fragment to the whole ones before the remainder is
  ceilinged. Up delays a real breach by a tick or two; down would invent one;
- it **never lowers the threshold**: `max(default, floor)` is Watcher's rule and
  the probe's.

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
