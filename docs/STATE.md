# Where I am

**Last updated: 2026-08-13.** Short-lived by design — rewrite it, don't append.
If I sat down right now, what would I need to know?

## Just finished

- **`PreVote` and `TimeoutNow` are on the wire** — proto, `grpcTransport` (send), `Server` (receive).
  Multi-node elections work now; before this, every pre-vote errored against a real cluster and no node
  could win.
- **One `LogEntry` converter** in [example/types/convert.go](../example/types/convert.go), replacing three
  hand-written copies.
- **`LogEntry.Type` survives serialization**, so membership changes finally reach followers. The two
  enums are offset by one (proto reserves 0 for `UNSPECIFIED`, `EntryType_Command` is `iota`), so the
  converter maps them with an explicit switch — a cast would turn every `Config` into a `NoOp`
  silently. `UNSPECIFIED` decodes to `Command`, because entries already on disk have no Type field.
- **`applyEntries` filters to `EntryType_Command`** before calling `sm.Apply`, so the library's own
  no-op and config entries never reach the state machine and no embedding has to remember to skip them.
- **`lastApplied` is persisted** in `example/db`. Was a stub returning 0.
- **`Node.restore` rebuilds derived state at startup** — `commitIndex`, `snapshotLatest`, and both
  configuration views. Membership now survives a restart. See "Crash recovery" below for why it does
  not restore the state machine.
- **`HandleInstallSnapshot` reordered** so nothing destructive happens before the new snapshot is
  durable, and `ErrNoSnapshot` distinguishes "no snapshot" from "cannot read the snapshot directory".

The enum was deliberately *not* renumbered and not split into internal/user categories — the explicit
mapping plus the apply filter get the same result without touching the library's public API.
`EntryType_Barrier` remains declared and unused by anything.

Regenerated with protoc 32.0 (the version the checked-in stubs used; local apt protoc is 3.21.12 and
would downgrade the header). `protoc-gen-go` also moved 1.31.0 → 1.36.11 — that's the churn in
`log.pb.go`. protoc 32.0 is installed at `~/.local/bin/protoc`.

`go build`, `go vet`, `go test ./...` and `-race` all green.

## Blockers, in order

**1. `InstallSnapshot` transport is still a stub**, so a lagging follower can never be caught up.
`HandleInstallSnapshot` is complete; what's missing is that `InstallSnapshotArgs.Reader` is an
`io.Reader` and gRPC has no such thing. Send side chunks the reader onto a stream; receive side wraps
`stream.Recv()` in an `io.Reader`, buffering leftovers.

The trap: [install_snapshot.go:21](../install_snapshot.go#L21) drains the reader on **every** exit,
including its early rejections. The sender must stream to completion even when the answer is already
`Success: false`, or the stream hangs instead of failing.

**2. `example/server/server.go:75` passes `nil` as the `StateMachine`.** Now that `lastApplied` is real,
`shouldTriggerSnapshot` can fire, so this reaches a nil deref instead of silently no-opping.

**3. Nit in `applyEntries`:** `sm.Apply` is called even when the filtered slice is empty, so a batch of
only config/no-op entries hands the state machine nothing. Harmless for a real store, noisy for mocks.
Guard it with `len(commandEntries) > 0`.

## Crash recovery

Write orderings are all safe (see INVARIANTS.md — `currentTerm` before `votedFor`, `Apply` before
`SetLastApplied`, snapshot durable before compaction). `Node.restore` now covers the read side:
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

`HandleInstallSnapshot` was reordered to make that recoverable: everything destructive now runs after
`writeSnapshotToDisk`, whose atomic rename is the commit point. It used to delete the old snapshots and
the whole log *first*, so a crash left no snapshot, no log, and nothing on disk to signal an install
had been in progress. The blanket log delete is gone rather than moved — the block further down
already truncates correctly by comparing the entry at `LastIncludedIndex` against the snapshot's term.

Two things still open here:

- **`replayConfigurations` scans the whole log above the snapshot index on every boot.** Linear in log
  length, so a node that has not snapshotted in a while pays for it at startup. Narrowing it needs a
  get-entries-by-type on `Storage`, which does not exist.
- **None of `restore` is tested.** The library tests mock everything and never exercise `Start`, so the
  interrupted-install branch — the one most worth pinning — has no coverage.

## Who does what

Split by learning density, not by the library/`example` boundary.

**Mine:** the Pebble `StateMachine` (`Apply`, `Snapshot`, `Restore`), startup recovery, and everything
in the library. `StateMachine` has only ever been mocked, so its shape is an unvalidated guess —
`Storage` got a sharp contract precisely because it has two implementations.

Two things that decide the `StateMachine` design: `Apply` must be idempotent per entry (`Set(k,v)` is
free, `Increment(k)` isn't), and `Restore` is a **replace**, not a load — the caller is usually a
lagging follower with stale state, so keys absent from the snapshot must be deleted, not left behind.
Use a Pebble instance separate from `Store`, or clearing the keyspace wipes the raft log.

**Delegable:** `InstallSnapshot` transport, codegen and plumbing.

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

## Open questions

- `SnapshotMeta.PrevIndex/PrevTerm` is vestigial now that catch-up anchors at `meta.Index/meta.Term`.
- Why streaming `InstallSnapshot` rather than the paper's chunked/offset form?
- Confirm `db.Store.DeleteLogs` really deletes the prefix in the production store (the mocks do). A
  no-op there would mask the whole retain-floor design.

## Known TODOs in code

- `write_logs.go` — `clientMu` held across the append's disk write, blocking every caller-facing entry
  point. Fixing it means handing out indexes without the lock, still ordered with `futureList`.
- `snapshot.go` — collapse apply loop + snapshot into one goroutine with channels.
- `add_member.go` / `heartbeat.go` — `> 5` snapshot-retry and `maxCatchUpRounds = 10` should be config.
- `append_entries.go` — leader-side replication fallback is still decrement-by-one.

## Housekeeping

- Debug print at `heartbeat.go:257` sprays `CurrentTerm / Res Term` into every test run.
- `refs/original/*` from the history rewrite are still around locally (3).
