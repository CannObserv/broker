# How each participant reaches this broker

Where each participant runs, the measured latency between it and this node,
the path each number was taken on, and the one risk that path carries. Who
produces and consumes each stream is [STREAMS.md](STREAMS.md). Both halves
moved out of it when it ran past the per-doc context budget: the measurements
first (CannObserv/broker#14 review, finding 8), the participant table on
2026-10-07.

## Participants, hosts and paths

Where each participant runs, and how its packets reach this broker. The four
live nodes are in `pdx` since 2026-09-15, which closes the cross-region interval
broker#1 R6 made unavoidable (CannObserv/broker#8). Processor's VM is a
fifth, consuming since 2026-10-02 (CannObserv/broker#75, which re-homed the
role #62 had declared for Observo): `co-processor`, on the tailnet as
`tag:processor` since 2026-09-29, admitted to `tag:broker` on 6379 by a policy
rule the same day. It runs Tailscale with `--accept-dns=true` since
CannObserv/processor#8, so its bus URL names the broker as `broker`, like the
other three.

Provisioner's is a sixth, **declared, not yet connected** (CannObserv/broker#78):
`co-provisioner`, on the tailnet as `provisioner`, `tag:provisioner`, since
2026-10-07, per its owner's VM record. **Not yet a peer from here** - on
2026-10-07 `tailscale status` on `co-broker` did not list it, since no policy
rule admits `tag:provisioner` to `tag:broker` yet. That rule is the first of its
go-live steps; its address and path are verified, and its latency rows added,
once it connects.

| Service | Tailnet node | VM | Region | Tailnet address | Path to broker |
|---|---|---|---|---|---|
| `archiver` | `archiver` | `co-registrar` | pdx | `100.109.138.101` | direct |
| `watcher` | `watcher` | `co-watcher` | pdx | `100.66.24.24` | direct |
| `replicator` | `replicator` | `co-replicator` | pdx | `100.114.136.20` | direct |
| `processor` | `co-processor` *(consuming since 2026-10-02, CannObserv/processor#1)* | `co-processor` | not recorded | `100.110.22.56` | direct - a hairpin through the exe.dev NAT both VMs share, both ways since the service runs (CannObserv/processor#15, *Latency, with the path beside every number* below) |
| `provisioner` | `provisioner` *(declared, not connected - CannObserv/broker#78)* | `co-provisioner` | pdx | `100.105.197.95` | not yet measured - no policy rule admits it yet |
| broker | `broker` | `co-broker` | pdx | `100.97.91.19` | - |

**This table is checked against the live broker.** `CLIENT LIST` reports each
connection's peer address and `user=`, so a participant that moves reconnects
from an address this table does not name and
`test_every_connected_participant_is_where_the_docs_say` goes red on the node
until the row follows. A host table here has rotted silently before: the one in
[ACL-CUTOVER.md](ACL-CUTOVER.md) kept watcher in `lax` and replicator on
watcher's VM for days after both had moved.

The measured latency from each participant, the path beside every number, and
the accepted DERP risk: the two sections below.

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
| `processor` -> broker | **4.5 ms** (n 5, 4.1-9.5) | **0.51 ms** (n 20, p95 0.66) | direct - a NAT hairpin, `via 16.145.19.221:13487` | 2026-10-03 18:45Z, `co-processor`, redis-py in the service's venv (CannObserv/broker#75); the service's own `XADD content.derived` took 2.0 ms (n 1, the first command's `publish_ms`) |

Network-side, one vantage point and one method for all three, so the rows are
comparable with each other rather than only with themselves - `tailscale ping`
x20 from `co-broker`, 2026-09-15 21:01Z:

| From `co-broker` to | min | p50 | max | Path |
|---|---|---|---|---|
| to `archiver` | 1 ms | **1 ms** | 4 ms | direct, 20 of 20 |
| to `replicator` | 1 ms | **1 ms** | 1 ms | direct, 20 of 20 |
| to `watcher` | 1 ms | **1 ms** | 1 ms | direct, 20 of 20 |
| to `co-processor` | 1 ms | **1 ms** | 3 ms | direct, 20 of 20 - 2026-10-04 02:21Z, with the service running; DERP (sea) 20 of 20 at each of three runs before it, 2026-09-30 to 2026-10-02 |

Archiver's client-side cold and warm cells were never taken with the method the
other two used; its network path is confirmed direct above, and a Redis `PING`
adds microseconds to it. Only archiver's host can fill those cells.

**`co-processor` is direct through a shared NAT, once its service runs**
(CannObserv/broker#75, CannObserv/processor#15). The two VMs sit behind the
same exe.dev public address: `tailscale netcheck` reports `16.145.19.221` on
both, and this node advertises `16.145.19.221:13487` [measured 2026-10-02].
From `co-processor` the path goes direct within seconds of traffic - `via
16.145.19.221:13487 in 1ms`, a NAT hairpin onto this node's own endpoint - and
lapses back to DERP (sea) after about 150 s idle (processor#15). From this side
it has never gone direct: 20 of 20 relayed at each of three runs, the last at
2026-10-02T17:41Z, with the peer showing no current address where `watcher`
shows its public one.

**Settled once the service ran.** The consumer's blocking `XREADGROUP` keeps
the session warm. From `co-processor` the path stayed direct under the service
(installed 2026-10-02T23:27Z), including 5 minutes with no pings, and it
re-formed direct on the first ping after a graceful reboot of `co-processor`
(2026-10-03 16:53Z; processor#15, closed). From this side it went direct too:
20 of 20 at 1 ms via `16.145.19.221:41641`, `co-processor`'s endpoint behind the
same NAT (2026-10-04T02:21Z), where three runs before the service all relayed.
Two things stay unmeasured: a reboot of **this** node, and a fall back to DERP
under load, which would come back to broker#75. The path depends on exe.dev's
NAT hairpinning, which no participant controls. Before 2026-10-02 it never
formed at all (2026-09-30, both ends relayed); what changed is not known -
processor#8's re-up is one candidate.
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

- **Steady state is direct for all four**, confirmed from both ends.
  `co-processor`'s is a NAT hairpin, and it was relayed until its service ran
  (above, CannObserv/processor#15).
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

