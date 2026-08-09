# Building Raft — The Journey

This document tells the story behind the code. Not what it does, but what went wrong, why it went
wrong, and what each fix taught me. If the README is the map, this is the travel log.

---

## Starting Point

I wanted to understand Raft from first principles — not by reading a library, but by building one.
The paper is deceptively readable. Three pages into the implementation I hit the first wall: the paper
describes *what* the algorithm must guarantee, but says almost nothing about *how* to structure the
goroutines that run it.

The first working version was a `Peer` struct that baked gRPC clients directly into the Raft logic.
Peers knew about URLs. The storage layer returned proto types. Tests imported both `db/` and `raft/`
creating a circular dependency. It worked as a binary. It couldn't be imported as a library.

That was fine for a first pass. The goal was to make it correct first, then clean.

---

## Bug 1: The `wg.Wait()` Goroutine Leak

### In the election loop

The first version of `election()` used a `sync.WaitGroup` to collect `RequestVote` responses:

```go
for _, peerID := range peers {
    wg.Add(1)
    go sendRequestVote(peerID, &wg, responseCh)
}
wg.Wait()
// now drain responseCh
```

This seemed natural — wait for all RPCs, then count votes. The bug appeared under load.

When an election timed out, `startElection` cancelled the context and spawned a *new* election
goroutine. But the old goroutine was still blocked at `wg.Wait()`. When the RPCs eventually
returned, the old goroutine unblocked, counted votes, and tried to send on `electionResChan` — which
was already full from the newer election. Permanent block. The goroutine never exited.

Each election timeout cycle leaked one goroutine. Each leaked goroutine incremented `currentTerm`
via `SetCurrentTerm`. After a few minutes of election churn the term had inflated into the hundreds
of thousands. Eventually the process OOM'd.

**The fix:** replace `wg.Wait()` with a buffered channel sized to the number of peers. Every
`sendRequestVote` goroutine sends its result and exits immediately, regardless of whether anyone is
still listening. The caller reads from the channel until it has a majority or has processed all
responses — whichever comes first.

```go
responseCh := make(chan responseRequestVote, len(peers))
for _, peerID := range peers {
    go sendRequestVote(ctx, peerID, responseCh)
}
// read up to len(peers) responses; return as soon as majority reached
```

No `wg.Wait()`. No synchronization point that ignores context cancellation.

### In the heartbeat loop

The same structural mistake appeared in `sendLogs`. It used `wg.Wait()` to wait for all four peer
RPCs before returning:

```go
for _, peerID := range peers {
    wg.Add(1)
    go func(id string) {
        defer wg.Done()
        sendAppendLogs(id, ...)
    }(peerID)
}
wg.Wait()
```

On a loaded machine, `db reads + RPC timeout + response processing + goroutine scheduling` routinely
exceeded the 100ms heartbeat interval. Every tick spawned a new goroutine while the previous one was
still inside `wg.Wait()`. Goroutines accumulated. All of them hammered the same followers
concurrently. RPC latency spiked. More goroutines hit their timeouts. A feedback loop that ended with
the leader losing authority and the cluster electing a new one — which then did the same thing.

**The fix:** one independent goroutine per peer (`sendLogsPerPeer`). Each runs its own ticker and its
own `inFlight` guard. A slow follower blocks only its own pipeline; the other three run normally.
There is no `wg.Wait()` anywhere in the heartbeat path.

```go
for _, peerID := range cfg.Peers {
    go sendLogsPerPeer(ctx, peerID, stepDownCh)
}
```

This is also why per-peer goroutines are the correct design, not a performance optimization. `wg.Wait()`
couples all peers together: one slow follower blocks heartbeats to everyone. Independent loops
isolate the failure. This is how etcd, TiKV, and CockroachDB implement replication.

---

## Bug 2: Term Inflation from Immediate Re-election

After fixing the goroutine leak, a subtler inflation appeared in a specific scenario: two nodes with
stale logs rejoined a running cluster simultaneously.

Both nodes kept losing elections — the log-up-to-date check rejected their votes on every peer.
Each failed election took ~50ms (one RPC timeout round-trip). The original code handled a
`Candidate` result from `election()` by immediately calling `becomeCandidate()` again:

```go
case ServerRole_Candidate:
    n.becomeCandidate() // jumps straight back to startElection
```

This bypassed `startElectionOut` and its randomized timeout entirely. Two nodes each attempting ~20
elections per second inflated the term by ~40/second. Within 12 seconds the cluster was at term 783
with no stable leader.

The Raft paper is explicit: a candidate that fails to win should wait for a new randomized timeout
before trying again. The randomized wait is the mechanism that breaks symmetry between competing
candidates. Without it, two nodes with identical stale logs will collide on every attempt forever.

**The fix:** on a `Candidate` result, call `becomeFollower()` instead. The node re-enters the
randomized timer in `startElectionOut`. If a leader's heartbeat arrives, the timer resets and the
node stays follower. If no heartbeat arrives, the timeout fires and it tries again — at a random
time, reducing the chance of collision.

```go
case ServerRole_Candidate:
    n.becomeFollower() // back to randomized wait, not immediate retry
```

The deeper principle: **follower is the safe default**. Any role that fails to fulfill its
responsibility retreats to follower, not to an immediate retry:

- Candidate fails to win → follower.
- Leader can't initialize state → follower.
- Leader sees a higher term → follower.

A zombie leader sends conflicting `AppendEntries`. A runaway candidate inflates terms. A follower
just waits — the only role that can't cause harm.

---

## Bug 3: Zombie Leaders from Child Goroutines Driving Role Transitions

After fixing the heartbeat loop, a third bug surfaced. `sendLogs` was calling `becomeFollower()`
directly when it observed a higher term in an `AppendEntries` response:

```go
if resp.Term > currentTerm {
    n.becomeFollower() // ← wrong place to do this
    return
}
```

This created a three-part failure chain:

1. `sendLogs` called `becomeFollower()`, which started a new election timer — but `heartbeatCtx` was
   never cancelled, so all `sendLogsPerPeer` goroutines kept running. The node was simultaneously
   follower and leader.

2. When the node won the next election and called `becomeLeader()`, it spawned a *second* set of
   heartbeat goroutines on top of the existing zombie set. Every leadership cycle added another layer.

3. The zombie goroutines kept sending `AppendEntries` with stale leader state, corrupting follower
   logs and causing more spurious step-downs in an unbounded loop.

**The fix:** child goroutines never drive role transitions. `sendLogs` signals a channel and returns.
The orchestrator (`startSendLogs`) reads the signal, cancels the heartbeat context to stop all
`sendLogsPerPeer` goroutines, then calls `becomeFollower()` exactly once.

```go
// in sendLogs — signal only, never transition directly
if resp.Term > currentTerm {
    stepDownCh <- struct{}{}
    return
}

// in startSendLogs — the single owner of role transitions
case <-stepDownCh:
    cancel()            // stops all sendLogsPerPeer goroutines
    n.becomeFollower()  // one call, clean shutdown
    return
```

The `stepDownCh` buffer is sized to `len(peers)`. Context cancellation doesn't preempt running
code — it fires at the next `select`. Between when `startSendLogs` reads the first signal and when
the other goroutines notice the cancellation, any of them might also reach the step-down line.
Buffering all of them ensures no goroutine ever blocks on send.

The rule: **only the goroutine that owns a lifecycle can end it**. `startSendLogs` owns the
heartbeat context; it's the only one allowed to cancel it and trigger a step-down.

---

## Bug 4: `HandleAppendEntries` truncated more than it was allowed to

The follower's append path used to be blunt: after the `prevLogIndex`/`prevLogTerm` check passed it
ran `DeleteLogs(prevLogIndex+1, 0)` — delete everything after `prevLogIndex` — and then re-appended
whatever the leader sent. Simple, and it *looks* safe because the consistency check upstream
guarantees the log matches up to `prevLogIndex`.

It isn't safe. AppendEntries RPCs can arrive delayed or duplicated. Consider a follower whose log is
already ahead of a particular (stale) heartbeat: an old AppendEntries with a small `Entries` set (or
an empty one, a bare heartbeat) still passes the `prevLogIndex` check, and the unconditional
`DeleteLogs` then throws away perfectly good entries after `prevLogIndex` — entries the leader still
considers committed. The next real heartbeat re-replicates them, so it usually self-heals, but for a
window the follower's log has *regressed*, which is exactly what Raft §5.3 forbids: *"If an existing
entry conflicts with a new one … delete the existing entry and all that follow it"* — the operative
word being **conflicts**.

The fix is to truncate only on an actual term conflict. Walk the incoming entries against what we
already hold:
- an entry we already have at the same index **and term** is a no-op — skip it (idempotent),
- the first index we don't have yet is where the genuinely-new suffix begins,
- the first index where our term differs from the leader's is the *only* place we truncate, and we
  truncate from there.

The lesson generalizes past this function: a correctness rule that names a precondition
("if it *conflicts*") is not satisfied by a stronger action that ignores the precondition
("always truncate"). "Stronger" here means "throws away more," and throwing away committed log is the
one thing a follower may never do.

### The configuration rollback that rides along

This is also where cluster membership tracking hooks in. Membership now lives in a `configurations`
struct with two views (see `raft_config.go`): `latest` — the most recent config seen in the log,
committed or not, which is the live operating set that replaced the old single `cfg.Peers` map — and
`committed`, the last config we know actually committed, each tagged with the log index that produced
it.

Why two views, and why the follower's truncation cares: a config change replicates as a normal log
entry, so `latest` can advance to an entry that has **not** committed yet. If that very entry sits in
a suffix we just truncated as a conflict, `latest` is now pointing at a configuration that no longer
exists in anyone's log. It has to roll back — and the only safe thing to roll back *to* is the last
configuration we know committed. Hence `rollbackLatestIfTruncated(fromIndex)`: if the truncation
started at or below `latestIndex`, revert `latest` (and `latestIndex`) to `committed`. The index tags
are what make "did this truncation invalidate the current config?" answerable at all.

The forward direction is live too: a config entry carries the **whole** configuration as a JSON
`map[string]Peer` (deciding to ship the full set rather than a per-member delta makes applying it a
straight replace, and sidesteps having to replay deltas in order), so
`processConfigurationLogEntry` just decodes it into `latest`. What's still missing is the third leg:
advancing `committed` when a config entry commits. That has since landed
(`advanceCommittedConfiguration`, driven by the commit-index updater), and getting it right needed a
piece that wasn't obvious: the *leader* also has to record which log index produced `latest`. Only the
follower did, so on a leader `latestIndex` sat at 0 forever and every uncommitted configuration looked
committed.

---

## Bug 5: Catching a new member up, without the snapshot loop deleting the logs it needs

Adding a member has a race between two subsystems. `AddMember` sends the new server a snapshot, then
replicates the log tail after it. Meanwhile the periodic snapshot loop keeps compacting — and if it
compacts past where the member has caught up to, the logs the catch-up still needs are gone, and (worse
than a wasted retry) the next round can append entries with a gap in the follower's log. Two lessons
came out of getting this right.

**Delay, don't bound — but with the right primitive.** The fix is a retain floor: the member publishes
the lowest index it still needs in `catchingUpIdx`, and the compactor refuses to delete past it. The
first attempt did that with a bare spin — `for floor <= target {}` with an empty body. That is a
busy-wait: it pegs a CPU core re-reading an atomic millions of times a second while nothing changes,
and it never checks `ctx.Done()`, so it hangs forever on shutdown. The lesson is the standard one, but
it's easy to reach for the spin first: *waiting on a predicate that another goroutine changes is a
condition-variable/channel problem, not a loop problem.* The rewrite parks in a `select` on a buffered
signal channel (or `ctx.Done()`), and — because the floor is **level** state, not a one-shot event —
re-Loads the floor after every wake rather than assuming one signal means "safe now." The signal is a
"something changed, look again" tap, not a "you may proceed" grant. Intermediate taps (the floor
advancing but still below the compaction target) must keep it waiting.

**An atomic's zero value is a value, not "unset."** `catchingUpIdx` is an `atomic.Int64`; its zero
value is `0` — which is a perfectly valid log index. The inactive sentinel is a separate constant
(`DefaultCatchingUpIdx`), so forgetting to initialise the field in `NewNode` doesn't leave it "empty,"
it leaves it claiming a retain floor at index 0 — and the compactor then blocks the *first* snapshot
forever, waiting for a catch-up that will never release a floor nobody set. This was masked only
because the snapshot loop isn't wired into a running node yet. The general rule: when a zero value is a
legal domain value, the "none" state needs its own sentinel *and* explicit initialisation — the type's
default can't carry that meaning for you.

---

## Bug 6: A `select` that was only ever meant to end

Replication used to fix its peer set once, when a leadership term began. A member promoted by
`AddMember` therefore counted toward quorum immediately but received no log entries until the next
election — half-promoted. Closing that meant telling the running orchestrator about a membership
change, so `startSendLogs` grew a fourth case:

```go
select {
case <-stepDownCh:          cancel(); n.becomeFollower(); return
case <-n.electionTimeoutCh: cancel(); n.becomeFollower(); return
case <-ctx.Done():          cancel(); return
case id := <-n.memberAddedCh:
    go n.sendLogsPerPeer(heartbeatCtx, id, ...)   // ← no return
}
```

Every case in that select had been **terminal**. Each one ended the leadership term, so there was no
loop around it and no need for one — the function was written to make exactly one decision and exit.
`memberAddedCh` was the first case that wasn't terminal, and without a loop the consequence isn't
"the case runs and we wait for the next event", it's: control falls out of the select, the function
*returns*, and its `defer cancel()` tears down `heartbeatCtx` — stopping every `sendLogsPerPeer`
goroutine.

The failure is silent, which is what makes it nasty. The role never changes, nothing is logged,
`GetRole()` still says `Leader`. The node simply stops replicating. Followers time out and elect
someone else, and the old leader carries on believing it leads a cluster that has moved on. Adding a
member — a routine administrative action — disabled the leader.

Two smaller bugs came in the same change and are worth recording because they're the same *kind* of
mistake, an assumption that quietly stopped holding:

- `becomeLeader` created the per-peer channel map **inside** the peer loop, so every iteration threw
  away the previous peer's channel and only the last peer ended up stoppable. Fine when the loop body
  had nothing to accumulate; wrong the moment it did.
- `AddMember` and `RemoveMember` sent directly on those channels. They only exist during a leadership
  term, so any membership change after a step-down blocked its caller forever on a nil channel. This
  is what hung the entire test suite — `go test` stopped terminating.

**The lesson:** *a control structure encodes an assumption about its cases, and adding a case can
violate it without any type error.* A `select` with no loop is a statement that every branch is
terminal. That statement was true and unwritten, so nothing pushed back when it stopped being true.
The fix is one `for`, but the durable fix was writing the asymmetry down in `INVARIANTS.md` next to
the code, because the next non-terminal case will look just as harmless.

A footnote on the nil-channel fix, because I got the reasoning wrong first and a mutation test caught
it. I "fixed" the hang with an `if ch == nil { return }` guard *and* a non-blocking select, then
verified by deleting the guard — and the test still passed. The guard isn't what prevents the hang: a
send to a nil channel inside a `select` with a `default:` just falls through. The `default:` is
load-bearing; the nil check only suppresses a spurious "notification dropped" warning when we aren't
leading. Worth knowing that a passing mutation is information too — it told me which line was actually
doing the work.

---

## The Refactor: From Binary to Library

With the core bugs fixed, the implementation worked as a 5-node Docker cluster. But the code was
tightly coupled to its deployment:

- `Peer` had `map[string]RaftRpcClient` — gRPC baked in, not injectable
- Storage methods returned `*types.LogEntry` (proto types) — leaked the serialization format
- Tests imported `db/` which imported `raft/` — circular dependency
- No way to use the Raft logic without also using PebbleDB and gRPC

The goal became: make `raft/` an importable library. A caller should be able to bring their own
networking, their own storage, and their own state machine.

### Three interfaces

```go
type Transport interface {
    RequestVote(peerID string, args RequestVoteArgs) (RequestVoteResponse, error)
    AppendEntries(peerID string, args AppendEntriesArgs) (AppendEntriesResponse, error)
}

type Storage interface {
    GetCurrentTerm(ctx context.Context) (uint, error)
    SetCurrentTerm(ctx context.Context, term uint) error
    // ... log CRUD, metadata
}

type StateMachine interface {
    Apply(entry LogEntry) error
}
```

`Node` holds these interfaces. The library calls out through them; it never creates connections or
opens files.

### Plain Go structs

Proto types stayed inside `db/` and `server/` as a serialization detail. The `raft/` package uses
`LogEntry{Index, Term, Data}` — no `ClientRequestId`, no `EntryType`, no `proto.Message`. The
`server/` package converts at the gRPC boundary. The `db/` package converts at the storage
boundary. The library knows nothing about either.

### Breaking the import cycle

Tests in `raft/` previously imported `db/` for a real storage implementation. But `db/` imports
`raft/` to implement `raft.Storage`. Circular.

The fix: `raft/db_mock.go` ships both a testify `MockStorage` (for unit tests that need exact call
expectations) and a `MemStorage` (for integration-style tests that need real state across calls).
Tests in `raft/` use these; they never import `db/`.

### Config

`RaftConfig` became `Config`. `Peers []string` instead of `ServerIDRpcUrlMap map[string]string`.
The library only knows peer IDs; the `Transport` implementation knows the addresses.

---

## The Apply Loop: Three Mechanisms Solving Each Other's Problems

With the library refactor done, committed entries still never reached a state machine. The first
design used a bridge goroutine to translate `commitCond` broadcasts into a buffered channel, and a
separate consumer goroutine with an `inFlight` flag to prevent concurrent applies.

Each piece created work for the next. The buffered channel needed drain logic so it wouldn't fill.
The `inFlight` flag meant a commit arriving mid-apply would be dropped, so completion had to
re-signal. The re-signal could deadlock against the held lock, so the send had to be non-blocking.
Three mechanisms, each fixing a problem the previous one introduced.

**The fix:** delete all of it and use `sync.Cond` directly in one goroutine. An inner loop guards
spurious wakeups; an outer loop catches commits that land during slow work (DB reads, `sm.Apply`),
because the condition is re-checked on reacquire without needing a signal at all. `lastApplied` is
tracked locally to keep the DB out of the hot path.

The lesson that generalizes: when three mechanisms are load-bearing for each other, the problem is
usually the first one. Removing the bridge channel removed the need for the other two.

The `sync.Cond` was later replaced by a plain buffered channel, `commitCh`, so that the loop could
select over the wake-up and `ctx.Done()` together instead of needing a broadcast on every cancellation
path. That swap cost more than it looked like it would — see Bug 8.

### Why `commitMu` is not `mu`

The original reason was specific to `sync.Cond`: `Wait()` holds its lock while sleeping, so a
`commitCond` built on `mu` would have had the apply loop holding the node's general state lock for as
long as there was nothing to apply — blocking the election timer, the heartbeat loop and every role
transition. The cluster would have seized up while idle.

That reason is gone. A channel receive holds nothing, and the loop releases `commitMu` before it waits.
Worth being honest that the justification changed rather than pretending the original still applies.

The lock stays split for two reasons that survive. `commitMu` now guards `futureList` as well as
`commitIndex`, a concern genuinely separate from role and peer state. And the apply loop still holds it
across every condition check, so merging would put the RPC surface behind a lock the loop touches on
every iteration. Neither is ever acquired while holding the other, in either direction — that is the
property to preserve if these are ever revisited.

Once the apply loop existed, `Propose` could wait on the same condition variable — it appended, then
blocked until `commitIndex` reached its entry's index. That is no longer how `Propose` works either;
see Bug 7.

---

## Bug 7: `select` has no notion of priority, and the refactor that discovered it

`Propose` used to block. `waitForCommit` did the waiting, and it had a shape worth looking at before
the bug makes sense:

```go
for n.commitIndex < index && ctx.Err() == nil && !leadershipEnded(leaderCh) {
    n.commitCond.Wait()
}

if n.commitIndex >= index { return nil }   // committed wins
if ctx.Err() != nil       { return ctx.Err() }
return ErrLeadershipLost
```

Two separate questions, answered in two separate places. The loop asks *should I still be asleep?*
The `if`s below ask *what is true now?* It never asks who woke it — the wake-up reason is thrown away
and the verdict re-derived from state. That is why the tie between "committed" and "stepped down"
always resolved the same way: committed, because the entry is in the log for good and reporting failure
invites a retry that appends it twice.

Turning `Propose` into a future fused those two questions into one `select`:

```go
select {
case <-f.doneCh:      return nil
case <-f.leaderClose: return ErrLeadershipLost
...
}
```

Now the case that fires *is* the verdict. That coincides with the truth only when exactly one thing is
true. When the entry committed **and** the term ended, both channels are ready — and Go's `select`
chooses among ready cases uniformly at random. That is a spec guarantee, not an implementation quirk;
it exists to prevent starvation. So no ordering of the cases and no restructuring inside a single
`select` can express a preference. **Priority has to live somewhere a `select` isn't.**

The test that caught it loops 100 times, because a single attempt passes about half the time.

Three wrong turns on the way to the fix, each instructive:

1. **`if _, ok := <-f.leaderClose; !ok`** inside the `doneCh` case. On a close-only channel a blocking
   receive is a *wait*, not a *question*: still leading → blocks forever; stepped down → `ok` is false.
   `ok` can never be true, so the `return nil` beneath it was unreachable. Asking a question of a
   channel needs `select` + `default`.
2. **Right idiom, wrong branch.** Adding the non-blocking check to the `doneCh` case reads as *"the
   entry committed… and we stepped down → tell the caller it failed"* — the invariant negated. Reaching
   that case already settles the question; the tie only hurts where you are about to report failure.
3. **Guarding the producer instead.** `processFutures` grew an `!n.IsLeader()` check, which stopped the
   drain from completing committed futures exactly when leadership was ending — reintroducing the same
   loss from the other end, and inverting `commitMu -> mu` while it was at it.

The fix is the original shape: let the `select` mean only *stop sleeping*, then re-derive the verdict
below it in priority order via a non-blocking `committed()`. Which is to say the old code was right and
the refactor's job was to keep its structure, not just its inputs.

The corollary showed up later. A future is keyed by index alone, so the follower commit path
deliberately does **not** drain: a new leader may have truncated our entry and put its own at that
index, and completing on index alone would report success for a proposal that was discarded.
`ErrLeadershipLost` is wrong-but-safe there; `nil` would be wrong-and-unsafe.

---

## Bug 8: four hangs from replacing a condition variable with a channel

`sync.Cond` cannot be selected on, so shutting the apply loop down meant broadcasting on every
cancellation path. Replacing `commitCond` with a buffered `commitCh` fixed that — the loop selects over
the wake-up and `ctx.Done()` together — and produced four distinct hangs on the way, in one function.

**`Wait()` is lock-neutral. A channel receive is not.** That single sentence is the whole bug. `Wait()`
releases the lock, sleeps, and reacquires before returning, so a caller that held the lock going in
still holds it coming out, and the condition is always re-evaluated under it. Every broken version was
an attempt to re-create that property by hand:

```go
select { case <-ctx.Done(): ...; case <-n.commitCh: ; default: }
```

*A spin.* The `default:` means the select never blocks, and `commitMu` — taken above the outer loop —
is never released. The loop burns a core while holding the one lock `SetCommitIndex` needs, so the
condition can never change. Livelock, and it survived the test suite because every test advanced the
commit index *before* the goroutine was scheduled.

*Unlock with no reacquire.* `fatal error: sync: unlock of unlocked mutex` on the first wake-up, plus a
data race on `commitIndex` at the condition check.

*Reacquire after the inner loop.* Correct for the path that waited, fatal for the path that did not:
skipping the inner loop reaches a `Lock()` on a mutex already held, and Go mutexes are not reentrant.
Three tests hung. The tell was that the top of the inner loop had **two predecessors that disagreed
about the lock** — from the outer loop it was held, from a wake-up it was not — so no placement outside
the loop could be right for both.

The fix is *unlock, receive, lock*, all inside the inner loop.

**And then the fourth, on the sender side.** Every sender did `Lock(commitMu)` → `commitCh <- struct{}{}`
→ `Unlock`. The loop only receives when it has nothing to apply, so during `applyEntries` there is no
consumer at all. A burst fills the buffer, the next sender blocks *mid-send while holding `commitMu`*,
and the loop then blocks acquiring `commitMu` on its way back to the receive that would have released
that sender. A cycle, not a race — deterministic, and no scheduling can break it.

Two independent fixes, and the difference between them is the lesson. Draining the buffer before
sending works, but only because *every* sender holds `commitMu` — a rule nothing enforces, and which
two of the three call sites broke, so the deadlock survived the "fix". Sending after releasing the lock
also works, and is local: you can read `SetCommitIndex` alone and see it. What landed is a non-blocking
send into a size-1 buffer, which needs no rule at all — the send cannot block whoever calls it, and the
channel's own capacity provides the coalescing the drain loop was doing by hand.

One more, found while cleaning up: removing the drain also removed the `defer`, and the early return in
`SetCommitIndex` for a lower index started leaking `commitMu` outright. No test took that branch —
they all advance monotonically — while `HandleAppendEntries` reaches it routinely.

Both scenarios are now pinned by tests. Both need a gate proving the loop is parked before the burst
starts: the first version of the deadlock test passed in 14ms because the burst finished before the
goroutine ever ran.

---

## What Remains

Snapshotting, membership changes (both directions), pre-vote and leadership transfer have all landed
since this section was last written. What is left:

- **The gRPC surface has not kept up with the library.** `PreVote`, `TimeoutNow` and
  `InstallSnapshot` are stubs in `grpcTransport` because `proto/rpc.proto` has no such RPCs. That was
  cosmetic until elections went through the pre-vote gate — now every pre-vote against a real cluster
  errors out, which the round counts as a withheld vote, so **no node can win an election in a real
  deployment**. Every library test passes, because they all mock the transport. A good reminder that
  "all green" is scoped to what the tests actually exercise.
- **Linearizable reads**: reads currently go through the log; read leases or heartbeat-based reads
  would allow bypassing the log for non-mutating queries.
- **Futures carrying `(index, term)`**: a `Future` is keyed by log index alone, so a proposal whose
  entry a new leader truncated cannot be distinguished from one that committed. Today that is handled
  conservatively — the follower commit path does not drain, so such a waiter gets `ErrLeadershipLost`
  rather than a false success. Carrying the term would let it be precise.
- **`Future.errCh`**: the field exists and nothing ever sends on it, so that case in `Wait` is
  unreachable. Either wire it (an entry truncated out from under a waiter is the obvious candidate) or
  drop it.

**Done since this section was last written**: `Propose` returns a `Future` instead of blocking. The
part that took longest to get right was not the waiter list — a slice under `commitMu`, because a
channel can neither be peeked nor cleared — but the tie-break the old blocking version got for free.
See Bug 7.
