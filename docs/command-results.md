# Getting a result back from `Apply`

`Propose` returns a `Future`, and `Future.Wait` answers exactly one question: *did this entry commit?*
That is the whole of what the library can promise, because the library does not know what the entry
means. For `Set` and `Delete` that is enough — the command either took effect or it didn't.

It is not enough for compare-and-swap. "Committed" and "the value matched" are different facts, and the
second one only exists at the instant `Apply` evaluated it. A read afterwards is a different point in
time and can report a different answer. So something has to carry a per-command result from inside
`Apply` back out to the handler that proposed it.

Three shapes were on the table.

## Rejected: `Apply` returns `[]any`, routed to each entry's `Future`

This is hashicorp's shape, and it is the tidiest: one wait, one answer, the library does the routing
because it already knows which index belongs to which waiter.

It is also a breaking change to `StateMachine` and to `Future`, for the benefit of one embedding. The
library's job is consensus; teaching it to carry opaque per-command payloads back to callers widens
that contract permanently. Worth revisiting if a second embedding ever wants the same thing — until
then the cost lands in the wrong place.

## Rejected: key the results by log index

Superficially the best of both. The state machine stashes results in a map keyed by index; `Apply`
already receives `[]LogEntry` so it has `log.Index` for free, even for an entry whose payload it cannot
parse; `Future.Index()` already exposes the index to the proposer. Nothing in the library changes.

**It loses a race that is certain on a single-node cluster.** The index is assigned by `Propose`, so
the handler can only register its waiter *after* `Propose` returns. On a single node the majority is
one, so the entry is committed the moment it is appended — the apply loop can run, find no waiter, drop
the result, and the handler then blocks until its context expires.

What makes this worse than an ordinary bug is how it fails. On three nodes the commit needs a network
round trip, so registration wins essentially every time. The bug is invisible in the cluster you demo
and reproducible only in the configuration you develop against — or the other way round, depending on
which you happened to try first. A race whose visibility is inversely correlated with the seriousness
of the deployment is the worst kind to ship.

Any fix inverts the ordering: registration has to happen before the append, which means the key cannot
be something the append assigns.

## Chosen: key by a caller-chosen command id, registered before `Propose`

The id lives inside the command payload, chosen by the handler. So the waiter can exist before the
entry does:

```go
id := newCommandID()
sm.Register(ctx, id)
defer sm.Forget(id)

future, err := node.Propose(ctx, raft.EntryType_Command, data)
if err := future.Wait(ctx); err != nil { return err }   // did it commit?
return sm.WaitForResult(id)                             // what did applying it produce?
```

There is no window. The library is untouched. The cost is two waits instead of one, and the discipline
that they are asked in that order — a future that fails must not fall through to `WaitForResult`,
because nothing will ever complete that waiter.

### What this costs

**A command whose payload will not unmarshal is unaddressable.** The id is inside the bytes that failed
to parse, so there is nothing to look up and the waiter is never notified. That is acceptable: this node
marshalled those bytes itself before proposing them, so a parse failure means log or disk corruption,
not a malformed request. The right handling is a bounded wait, not a lookup — which is why
`CommandResult` carries the request's context and `WaitForResult` selects on it.

**Registration must be paired with `Forget`, including on the path where `Propose` itself fails.**
`defer` immediately after `Register`. Because the entry carries its request context, an entry whose
caller has gone away is provably garbage, so `Sweep` can reap a missed `Forget` — a backstop, not a
substitute.

**Results are delivered only after the batch is durable.** `Apply` collects waiters into two slices as
it stages commands and releases them after `DB.Apply` returns. Notifying inside the loop would hand a
success receipt to clients whose writes are still sitting in a batch that a later entry may cause the
whole call to discard.

### Not solved

Exactly-once. Ids are server-generated, so a client retry proposes a second entry and both apply.
Making the *client* choose the id would let a retry resolve to the same waiter, but only alongside a
dedup table consulted in `Apply` — otherwise the command still applies twice. The wire format already
carries the id, so this is additive whenever it becomes worth doing.
