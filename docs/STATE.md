# Where I am

**Last updated: 2026-08-15.** Short-lived by design — rewrite it, don't append.
If I sat down right now, what would I need to know?

## Just finished

- **`example/statemachine` exists** — a Pebble `StateMachine` implementing `Apply`, `Snapshot` and
  `Restore`. The interface had only ever been mocked, so this is the first real implementation and the
  first check on whether its shape was a good guess. It mostly was; see "What the implementation
  taught the interface" below.
- **The server is wired.** `NewNode` was being handed `nil` as the state machine, and the snapshot
  config was never copied into `raft.Config` — so `Start` would have panicked in `NewTicker(0)` before
  the apply loop got as far as its own nil deref. Both fixed, plus a supervisor that shuts the server
  down when `Node.Fatal()` fires and a sweeper for abandoned command waiters.
- **`InstallSnapshot` is on the wire, end to end.** Client-streaming RPC in the proto, chunking sender
  in `grpcTransport`, receiving handler in `example/server/rpc.go` that feeds an `io.Pipe` so the
  library streams the snapshot to disk as it arrives rather than buffering it.
- **KV endpoints on the debug server** — `/kv/set`, `/kv/delete`, `/kv/cas`, `/kv/get`. No new proto
  service; replication is the point, not the API surface.
- **Command results reach the client.** `Apply` collects waiters and releases them only after the
  batch is durable. Keyed by a caller-chosen command id, registered *before* `Propose` — see
  [command-results.md](command-results.md) for why the obvious alternative (keying on log index) is
  racy on exactly the cluster size you develop against.

- **`applyEntries` no longer calls `sm.Apply` with an empty slice**, so a batch of only config/no-op
  entries doesn't hand the state machine nothing.

`go build`, `go vet` and `go test ./...` are green.

## Blockers, in order

**1. None of it has been run.** No cluster has served a `/kv/set` or completed an `InstallSnapshot`
transfer. Everything above is compile-and-reason confidence, and the last session found four separate
bugs in the send path alone by reading it. Stand up three nodes before trusting any of this.

**2. `example/statemachine` has no tests** — the only package in the repo without any. The cheapest
high-value one is a `Persist` → `Restore` round trip: it pins the framing, the `sm:` prefix handling,
and replace-not-merge in a single test. Truncating the stream at a few offsets covers the
`io.EOF` / `io.ErrUnexpectedEOF` split, which is the part most likely to rot.

## What the implementation taught the interface

Three things only showed up once there was a real `StateMachine`, and all three are now invariants:

- **The state machine and the log share a Pebble instance**, so state machine keys need a namespace of
  their own (`sm:`). Without one a client `SET` can overwrite `current_term`, and `Snapshot` captures
  the Raft log and ships it to another node. STATE.md previously suggested a separate Pebble instance;
  a prefix plus bounded iterators is cheaper and keeps one fsync domain.
- **`Apply`'s error return is not the place for a rejected command.** A CAS that did not match is a
  deterministic outcome every replica reaches identically; returning it stops the apply loop on one
  node and calls that divergence. The split is now explicit — `ErrCommandFailed` wraps command-level
  errors, everything else is node-level and fatal.
- **`Restore` is a replace, and it can be non-atomic.** Bounded batches are safe because the snapshot
  file is already on disk and `SetLastApplied` has not run, so startup replays the same file.

## Crash recovery

Write orderings are all safe (see INVARIANTS.md — `currentTerm` before `votedFor`, `Apply` before
`SetLastApplied`, snapshot durable before compaction). `Node.restore` covers the read side:
`commitIndex` from the durable `lastApplied`, `snapshotLatest` from the snapshot meta, and both
configuration views from the snapshot plus a replay of every config entry the log holds above it.

**The library assumes a durable state machine, and `restore` deliberately does not call `sm.Restore`.**
State below the snapshot index is already on disk, and so is everything appended and applied after it.
Restoring at startup would roll the state machine back to the snapshot index while the apply loop
resumed from `lastApplied`, silently dropping everything applied since — and `lastApplied` is always
>= the snapshot index, because our own snapshots are taken at the applied index. A volatile state
machine would need the opposite design (restore, then set `lastApplied` to the snapshot index and
replay), which is why hashicorp does not persist `lastApplied` at all.

The one exception is `lastApplied < meta.Index`, which can only mean a leader-pushed
`InstallSnapshot` was interrupted. `restore` finishes it. Where in the window it died does not need
distinguishing, because `Restore` is a wholesale replace and re-running it on an already-current state
machine is inert.

Two things still open here:

- **`replayConfigurations` scans the whole log above the snapshot index on every boot.** Linear in log
  length, so a node that has not snapshotted in a while pays for it at startup. Narrowing it needs a
  get-entries-by-type on `Storage`, which does not exist.
- **None of `restore` is tested.** The library tests mock everything and never exercise `Start`, so the
  interrupted-install branch — the one most worth pinning — has no coverage.

## Who does what

Split by learning density, not by the library/`example` boundary.

**Mine:** the Pebble `StateMachine`, startup recovery, and everything in the library.

**Benchmarking is deferred** until a cluster actually commits. First pass when it's time:
`processFutures` drain (O(n) in pending proposals) and the `MemStorage` append path, `-count=10
-benchmem` through benchstat; then mutex/block profiles, the only real check on whether the three-lock
split earns itself. Cluster-level needs an open-loop generator or the tail latencies are fiction.

## Decided but not built

- **`(index, term)` keying for futures.** Keyed by index alone, so a truncated proposal is
  indistinguishable from a committed one. Conservative today: the follower commit path doesn't drain, so
  such a waiter gets `ErrLeadershipLost`.
- **`Future.errCh`** — allocated on every `Propose`, never sent on, so that `Wait` case is unreachable.
  Wire it or delete it.
- **Linearizable reads.** `/kv/get` reads the local Pebble directly and is documented as stale. A
  correct read needs either a log entry per read or a ReadIndex round trip.
- **Exactly-once commands.** Command ids are server-generated, so a client retry proposes a second
  entry. Client-supplied ids plus a dedup table in `Apply` would fix it; the id already travels in the
  command payload, so the wire format does not have to change.
- **Nothing in the library reacts to `Fatal` — deliberate.** The obvious reactions (step down, refuse to
  campaign, reject `TimeoutNow`, stop serving reads) are all reachable by the caller cancelling the
  context it passed to `Start`, so the library would only be picking one policy on the caller's behalf.
  The finer policy worth knowing about but not built: a node with a stuck state machine still has a
  correct, durable log, so it remains a valid log replica and can keep counting toward quorum. Keeping
  it alive and only refusing reads and leadership preserves fault tolerance that a full stop throws
  away — the cost is a sticky in-memory flag checked in `election()` and `HandleTimeoutNow`, which is
  the one campaign path that bypasses the election timer. In-memory, not persisted: `SetLastApplied`
  never ran, so a restart replays the failed entry and a transient fault should not survive it.

## Open questions

- `SnapshotMeta.PrevIndex/PrevTerm` is vestigial now that catch-up anchors at `meta.Index/meta.Term`.
- Why streaming `InstallSnapshot` rather than the paper's chunked/offset form?
- Confirm `db.Store.DeleteLogs` really deletes the prefix in the production store (the mocks do). A
  no-op there would mask the whole retain-floor design.
- `CommandResultBuffer.Sweep` runs on a 30s ticker in `example/server`. Whether that is the right home
  for it, or whether the buffer should reap on insert, is untested either way.

## Known TODOs in code

- `write_logs.go` — `clientMu` held across the append's disk write, blocking every caller-facing entry
  point. Fixing it means handing out indexes without the lock, still ordered with `futureList`.
- `snapshot.go` — collapse apply loop + snapshot into one goroutine with channels.
- `add_member.go` / `heartbeat.go` — `> 5` snapshot-retry and `maxCatchUpRounds = 10` should be config.
- `append_entries.go` — leader-side replication fallback is still decrement-by-one.

## Housekeeping

- Debug print at `heartbeat.go:257` sprays `CurrentTerm / Res Term` into every test run.
- `refs/original/*` from the history rewrite are still around locally (3).
