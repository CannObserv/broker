# Non-stream keys

Every key on this broker that is not a stream: who writes it, how long it
lives, and what losing it costs. The streams are [STREAMS.md](STREAMS.md); why
the one volatile namespace makes `noeviction` load-bearing is
[MEMORY-PROTECTION.md](MEMORY-PROTECTION.md).

Moved out of STREAMS.md on 2026-10-07, when it ran past the per-doc context
budget.

## Non-stream keys on `db0`

Provenance: CannObserv/broker#9, CannObserv/replicator#80.

This file had no row for anything that is not a stream, which is how an audit
came to find these by scanning the keyspace rather than by reading.

| Pattern | Owner | Kind | Lifetime | Commands used | What it is |
|---|---|---|---|---|---|
| `replicator:cmd:<stream suffix>:<command_id>` | Replicator | string, **volatile** | `REPLICATOR_DEDUPE_TTL_SECONDS`, default 86400 | `SET .. NX EX`, `EXISTS` | De-duplication of `content.fetch` / `content.replicate` / `content.persist` commands. Written **after** the handler completes; read by an `EXISTS` **before** the next one runs. Reasoning: [`CannObserv/replicator:docs/CONVENTIONS.md#the-replicatorcmd-keys`](https://github.com/CannObserv/replicator/blob/main/docs/CONVENTIONS.md#the-replicatorcmd-keys) |

**This is the only non-stream key pattern any service writes here.** A new one
belongs in this table before it belongs on the broker. Processor adds none
(CannObserv/broker#62, #75): it keeps no dedupe keys, because a redelivered
command re-runs a deterministic extraction whose output is written
content-addressed and if-absent - and its ACL user holds neither `+set` nor
`+exists`, which is what keeps that a property of the broker rather than of
Processor's source.

**One namespace per command stream**, and the suffix is the same one co-core's
`group_name` puts after the service - so `content.fetch` gives both the group
`replicator.fetch` and the keys `replicator:cmd:fetch:<id>`. Today that means
two namespaces, `fetch` and `replicate`.

*Losing them costs re-work, never correctness.* Set-after-success means a key
can only short-circuit work already known to have finished, so an empty
namespace costs a re-fetch, a content-addressed re-store that is a no-op, and a
duplicate fact the issuer contract already requires consumers to tolerate. The
framing that matters: a `db0` that has lost these has lost the streams and the
groups' **PELs** with them, and the PEL is Replicator's only durable record of
intent - it has no database and no outbox. These keys are the cheapest thing in
that blast radius.

> **The plural is load-bearing, and getting it wrong is silent.** The ACL granted
> `~replicator:cmd:fetch:*` until broker#9 - one segment of the namespace rather
> than the namespace. Nothing could have observed the gap: the replicate loop
> completes no commands while no alias table is provisioned, so its namespace is
> **empty rather than absent**, and a grant derived from what was seen on the
> wire cannot see a namespace with no traffic. The moment that loop completes
> one - which is what broker#7 exists to make happen - the `EXISTS` before the
> handler is denied, replicator#82 classifies `NOPERM` transient, and the loop
> backs off and retries forever without ever running a handler. Nothing lost,
> nothing progressing. Fixed live 2026-09-10 to `~replicator:cmd:*`;
> `tests/deploy/test_redis_acl.py` now derives the namespaces from co-core's
> command taxonomy, so a third command stream cannot arrive without a grant or
> a red test. Same lesson as the `+exists` omission that wedged the fetch loop
> the same day: **an observed inventory is only as good as its attribution**,
> and a namespace with no traffic yet is the blind spot.

Measured on this broker 2026-09-10: **37 keys on `db0`, 27 of them dedupe keys
with TTLs and 10 streams without**, average TTL remaining ~13.5 h; every dedupe
key under the `fetch` segment, the `replicate` segment empty. Replicator's own
audit the day before found the same 27 with TTLs spanning 534 s to 84,713 s.
The counts come from `INFO keyspace` and `SCAN MATCH`, which is all the probe's
`brokeradmin` and the operator's `acladmin` hold for this - neither has `+ttl`
or `+type`, deliberately, and the average is the one `INFO` reports.
