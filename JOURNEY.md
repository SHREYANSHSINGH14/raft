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
advancing `committed` when a config entry commits, which belongs in the apply loop. Until that lands,
`committed` only holds the bootstrap config, so a rollback reverts to bootstrap rather than to the true
last-committed configuration. See `STATE.md`.

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

### Why `commitMu` is not `mu`

`sync.Cond.Wait()` holds its lock while sleeping. If `commitCond` had been built on `mu` — the
node's general state lock — then the apply loop sleeping in `Wait()` would hold `mu` for as long as
there was nothing to apply, blocking the election timer, the heartbeat loop, and every role
transition. The cluster would deadlock while idle.

So `commitMu` is a second, independent mutex whose only job is to be `commitCond`'s lock.
`SetCommitIndex` writes `commitIndex` under `mu`, then broadcasts *without* holding it.

This is also why the lock is required around `Wait()` at all, which is worth stating because it
looks like ceremony: `sync.Cond` keeps a list of sleeping goroutines, and a goroutine must register
itself on that list before it sleeps. The lock makes "register on the list" and "release the lock and
sleep" atomic. Without it, a `Broadcast()` firing in between would walk the list, not find the
goroutine that had already decided to wait, and leave it asleep forever with nothing left to wake it.

Once the apply loop existed, `Propose` could wait on the same condition variable — it appends, then
blocks until `commitIndex` reaches its entry's index. That's what makes `Propose` return only after
the entry is genuinely committed rather than merely appended locally.

---

## What Remains

**In progress (uncommitted):** snapshotting and log compaction. The on-disk format and the follower's
`HandleInstallSnapshot` exist; neither snapshot creation nor snapshot *sending* is wired to a running
node yet. See `STATE.md` for the precise state and the next steps.

- **Membership changes**: the cluster topology is fixed at startup. Design is decided and written up
  in `docs/membership-change.md`; it depends on snapshotting landing first.
- **Linearizable reads**: reads currently go through the log; read leases or heartbeat-based reads
  would allow bypassing the log for non-mutating queries.
- **Propose as a future**: `Propose` blocks until commit. Returning a future would let the caller do
  other work while waiting — and would fix the fact that a blocked `Propose` currently hangs
  indefinitely if the leader steps down.
