# raft

A Raft consensus implementation built from the paper, structured as an importable Go library.
`raft/` is the library; `db/` (PebbleDB), `server/` (gRPC), and `cmd/` are one concrete embedding of it.

- `README.md` — what it is and how to use it (the map)
- `JOURNEY.md` — bugs hit, why they happened, what each fix taught (the travel log)
- `STATE.md` — what is in flight right now (read this first when resuming)
- `docs/architecture.mmd` — one connected Mermaid flowchart of a whole node (read when the details stop fitting in your head); `docs/architecture.md` is the per-concern breakdown with prose
- `docs/` — design decisions made before building

## Architecture

`raft/` never touches the network, the disk, or the application state directly. It calls out through
three interfaces the caller implements ([interfaces.go](raft/interfaces.go)):

- `Transport` — sends RPCs to peers. Owns addresses, pooling, retries, timeouts.
- `Storage` — persists term, votedFor, lastApplied, and the log.
- `StateMachine` — applies committed entries; snapshots and restores.

Proto types stay inside `db/` and `server/` as a serialization detail. The library speaks plain Go
structs (`LogEntry{Index, Term, Type, Data}`). `server/` converts at the gRPC boundary, `db/` at the
storage boundary. **Do not leak proto types into `raft/`.**

`raft/db_mock.go` ships both a testify `MockStorage` and a real in-memory `MemStorage`, because
tests in `raft/` cannot import `db/` — `db/` imports `raft/` to implement `raft.Storage`, so it
would be a cycle.

## Invariants — the things that are easy to break

### The three mutexes are deliberately separate. Do not merge them.

- **`mu`** — general node state (role, leaderID, peer NextIndex/MatchIndex).
- **`commitMu`** — guards `commitIndex`. It is the lock behind `commitCond`.
- **`clientMu`** — serializes the caller-facing entry points (`Propose`, `HandleAppendEntries`,
  `HandleRequestVote`). Lives in the library so callers like `server/rpc.go` don't need their own lock.

`commitMu` is separate from `mu` because **`sync.Cond.Wait()` holds its lock while sleeping**. If
`commitCond` used `mu`, the apply loop sleeping in `Wait()` would block every internal goroutine that
needs `mu` — election, heartbeat, role transitions. `SetCommitIndex` updates `commitIndex` under `mu`,
then broadcasts on `commitCond` *without* holding `mu`. The long-form explanation lives at
[node.go:35-57](raft/node.go#L35-L57).

`commitCond` is what makes `Propose` block until commit and what wakes the apply loop.
`Wait()` must be called with `commitMu` held; `Broadcast()` needs no lock.

### Child goroutines never drive role transitions.

Only the goroutine that owns a lifecycle may end it. `sendLogs` observing a higher term does **not**
call `becomeFollower()` — it signals `stepDownCh` and returns. `startSendLogs` (the sole owner of the
heartbeat context) reads the signal, cancels the context to stop every `sendLogsPerPeer`, then calls
`becomeFollower()` exactly once. Violating this produced zombie leaders — see JOURNEY.md Bug 3.

`stepDownCh` is buffered to `len(peers)`. Context cancellation doesn't preempt running code; between
the first signal and the others noticing cancellation, any peer goroutine may also reach the step-down
line. Buffering all of them guarantees no goroutine blocks on send.

### Follower is the safe default.

Any role that fails its responsibility retreats to follower, never to an immediate retry:
candidate loses an election → follower (re-enters the *randomized* timer, which is what breaks
symmetry between competing candidates); leader can't initialize → follower; leader sees a higher
term → follower. An immediate `becomeCandidate()` retry bypasses the randomized wait and inflates
terms without bound — see JOURNEY.md Bug 2.

### No `wg.Wait()` in the election or heartbeat paths.

`wg.Wait()` is a synchronization point that ignores context cancellation, and it couples all peers
together — one slow follower blocks everyone. Elections use a buffered channel sized to `len(peers)`;
replication uses one independent `sendLogsPerPeer` goroutine per peer, each with its own ticker and
`inFlight` guard. See JOURNEY.md Bug 1.

### The apply loop parks during snapshots.

`snapShotInProgress` (atomic) is checked in `shouldWaitForApply` ([apply_loop.go:64](raft/apply_loop.go#L64)).
While the state machine is being captured, the apply loop must not advance `lastApplied` past the
index the snapshot is being taken at.

### The live cluster configuration is `configurations.latest`, not `cfg.Peers`.

Membership is tracked in the `configurations` struct ([raft_config.go](raft/raft_config.go)) as two
views: `latest` (most recent config in the log, committed or not — the **operating** set every peer
helper, election, and heartbeat reads) and `committed` (last config known committed), each tagged with
its producing log index. `cfg.Peers` is only the **bootstrap seed**, copied into both views once in
`NewNode`/`NewNodeMock`; nothing may read it at runtime. Any raw `&Node{}` built in a test must seed
`configurations`, not just `cfg.Peers`, or the node behaves as if it has no peers.

`configurations` is guarded by `mu` (same lock as the peer maps). Mutators deep-copy via `clonePeers`
so `latest` and `committed` never alias. When `HandleAppendEntries` truncates a conflicting suffix, it
calls `rollbackLatestIfTruncated`: if the truncation removes the entry that produced `latest`, `latest`
reverts to `committed` — an uncommitted config that just left the log must not stay live. See
JOURNEY.md Bug 4.

Config entries carry the **whole** configuration as a JSON `map[string]Peer` (AddMember marshals
`n.peersSnapshot()`), so `processConfigurationLogEntry` on the follower is a straight replace of
`latest`. `committed` still only advances on commit (`setCommittedConfiguration`), which is not yet
wired into the apply loop — see STATE.md.

### Compaction is *delayed* for a catching-up member, never bounded — via a channel, not a spin.

While `AddMember` catches up a new member it publishes a retain floor in `catchingUpIdx` (atomic): the
lowest log index that member still needs. `runSnapshotOnce` does not compact past it — it **parks** in
`waitForCatchUpFloor` until the floor clears (`floor == DefaultCatchingUpIdx` or `floor > compact
target`), waking on the `catchUpSignal` channel or `ctx.Done()`. Always change the floor through
`setCatchingUpIdx` (store **and** signal) — a bare `catchingUpIdx.Store` leaves the snapshot goroutine
asleep forever. The wait re-Loads the floor after every wake because the floor is level state, not a
one-shot event.

Two footguns: (1) `catchingUpIdx` **must be initialised to `DefaultCatchingUpIdx`** in `NewNode`/
`NewNodeMock` — the `atomic.Int64` zero value is `0`, a valid index, which would make the compactor
think a floor at 0 is permanently held and block the first snapshot forever. (2) `AddMember`'s `defer`
must release the floor through `setCatchingUpIdx(DefaultCatchingUpIdx)` so the signal fires on every
exit path (success, abort, panic-unwind).

## Conventions

- Errors: `ErrNotFound` is defined in `raft/` so the library doesn't depend on Pebble's sentinel.
- Logging: `zerolog.Ctx(ctx)` everywhere; the logger rides on the context.
- Tests use testify mocks; method-name constants (e.g. `methodApply`) are used for `.On(...)` setup.

## Working on this repo

Read `STATE.md` before starting — it records what is half-finished and why.

When a session produces a non-obvious decision (a design chosen over an alternative, a signature
changed for a reason, a bug whose root cause was subtle), write it down before the session ends:
- A bug and what it taught → `JOURNEY.md`
- A design decision, with the options rejected and why → `docs/`
- An invariant future-you could break by accident → here
