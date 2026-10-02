# How each participant reaches this broker

The measured latency between every participant and this node, the path each
number was taken on, and the one risk that path carries. Who produces and
consumes each stream, and where each participant runs, is
[STREAMS.md](STREAMS.md), *Participants, hosts and paths* - this is the
measurement half of that section, moved out when STREAMS.md ran past the
per-doc context budget (CannObserv/broker#14 review, finding 8).

## Latency, with the path beside every number

Record the path, not only the milliseconds. A relayed path has no warm case to
converge on - it does not improve with traffic - so a number without its path
reads as a tuning question when it is a topology one.

Client-side, from each participant's own host, as its own ACL user:

| Participant -> broker | Cold: fresh TCP + `AUTH` + `PING`, p50 | Warm `PING`, p50 | Path | Measured |
|---|---|---|---|---|
| `replicator` -> broker | **4.05 ms** (n 6, 3.96-10.48) | **0.47 ms** (n 30, 0.44-0.56) | direct | 2026-09-11, `co-replicator` (CannObserv/replicator#88) |
| `watcher` -> broker | **7.31 ms** (n 6, 5.17-9.94) | **1.55 ms** (n 30, 0.57-2.09) | direct | 2026-09-15, `co-watcher` (CannObserv/watcher#296) |
| `archiver` -> broker | not taken | not taken | direct | - |
| `processor` -> broker | not taken | not taken | **direct under traffic, DERP (sea) at idle** - a NAT hairpin to this node's public endpoint, 1 ms, lapsing after ~150 s idle (CannObserv/processor#15, 2026-10-02) | - (a `PING` as `processor` succeeded at the handoff, by 2026-10-02 (broker#75); timings are CannObserv/processor#1's to take) |

Network-side, one vantage point and one method for all three, so the rows are
comparable with each other rather than only with themselves - `tailscale ping`
x20 from `co-broker`, 2026-09-15 21:01Z:

| From `co-broker` to | min | p50 | max | Path |
|---|---|---|---|---|
| to `archiver` | 1 ms | **1 ms** | 4 ms | direct, 20 of 20 |
| to `replicator` | 1 ms | **1 ms** | 1 ms | direct, 20 of 20 |
| to `watcher` | 1 ms | **1 ms** | 1 ms | direct, 20 of 20 |
| to `co-processor` | 16 ms | **17 ms** | 18 ms | **DERP (sea), 20 of 20** - 2026-10-02 17:41Z, as at 2026-10-01 22:04Z and 2026-09-30 00:09Z; never direct from this side |

Archiver's client-side cold and warm cells were never taken with the method the
other two used; its network path is confirmed direct above, and a Redis `PING`
adds microseconds to it. Only archiver's host can fill those cells.

**`co-processor` is direct one way, lazily, through a shared NAT**
(CannObserv/broker#75, CannObserv/processor#15). The two VMs sit behind the
same exe.dev public address: `tailscale netcheck` reports `16.145.19.221` on
both, and this node advertises `16.145.19.221:13487` [measured 2026-10-02].
From `co-processor` the path goes direct within seconds of traffic - `via
16.145.19.221:13487 in 1ms`, a NAT hairpin onto this node's own endpoint - and
lapses back to DERP (sea) after about 150 s idle (processor#15). From this side
it has never gone direct: 20 of 20 relayed at each of three runs, the last at
2026-10-02T17:41Z, with the peer showing no current address where `watcher`
shows its public one.

What that means for the bus: the consumer's blocking `XREADGROUP` keeps its own
session warm, so the path should stay direct while the service runs; an idle
service, or the first command after one, pays ~16 ms through the relay.
Whether it holds under the real service is processor#15's to confirm once
CannObserv/processor#1 runs, and a fall back to DERP under load comes back to
broker#75. It depends on exe.dev's NAT hairpinning, which no participant
controls. Before 2026-10-02 it never formed at all (2026-09-30, both ends
relayed); what changed is not known - processor#8's re-up is one candidate.
The client-side cells are processor#1's to fill, as `processor`.
The row it replaces was `observo-primary`'s, the host #62 declared for the
role: direct at 1 ms, 17 of 20, on 2026-09-24.

**History, kept for the comparison.** Before 2026-09-12 watcher and replicator
shared a VM in `lax`, and that path went through DERP (sea) at 36-40 ms and never
went direct - across 24 pings in three runs, despite `UDP: true` on that side.
The same replicator worker's `content.fetch-policy` replay took 333 ms there and
39 ms from `pdx`. D1 in broker#1 ("the broker must be in the same region as its
highest-frequency participants") reads like a tuning preference; measured, it
was the difference between a 1 ms direct hop and a 40 ms round trip through a
third party's relay.

## Risk: the tailnet relay (DERP) - accepted

A path through DERP is a second single point of failure beside this node
(broker#1 R7), operated by a third party, and the epic's risk list did not carry
it. Measured, it is **a boot-time transient, not a steady-state dependency**:

- **Steady state is direct for all three**, confirmed from both ends - the
  three live participants. `co-processor`, declared ahead of its consumer, is
  the exception: direct from its side only under traffic, through a NAT hairpin,
  and relayed at idle and from this side (above). It is revisited when its
  consumer runs (CannObserv/processor#15).
- **DERP appears only in the first seconds after a participant boots.**
  Replicator's first `tailscale ping` after a boot went through DERP (sea) at
  17-18 ms before the direct path formed at 1 ms, and the path was direct again
  after each of four reboots. Watcher after its cold reboot at
  2026-09-15 16:46Z: home DERP at 16:46:32, direct at 16:46:37 - the same second
  as its first `PING` of the boot, which took 52 ms against 28 ms on an
  established path. The first packets rode the relay, then the switch.

So a relay outage does nothing to a path that is already direct - direct packets
do not touch it. What it can touch is a participant **booting during** the
outage, whose first connection may be delayed while the path forms.

**Accepted**, because a delayed first connection is a failure all three
participants already absorb at every broker restart: the 2026-09-10 `databases 1`
window took this broker away for about two seconds and every group resumed at its
exact position with nothing pending ([RESTART-WINDOW.md](RESTART-WINDOW.md)). The
cost is a slower start, not a lost message. Revisit if a participant ever
settles on a relayed path again - the table above would show it as a path that
stops saying `direct`.

