# Where I am

**Last updated: 2026-08-12.** Short-lived by design — rewrite it, don't append.
If I sat down right now, what would I need to know?

## Just finished

On branch `feat/wire-prevote-timeoutnow-lastapplied`.

- **`PreVote` and `TimeoutNow` are on the wire** — proto, `grpcTransport` (send), `Server` (receive).
  Multi-node elections work now; before this, every pre-vote errored against a real cluster and no node
  could win.
- **One `LogEntry` converter** in [example/types/convert.go](../example/types/convert.go), replacing three
  hand-written copies. Behaviour preserved exactly — `Type` is still dropped.
- **`lastApplied` is persisted** in `example/db`. Was a stub returning 0.

Regenerated with protoc 32.0 (the version the checked-in stubs used; local apt protoc is 3.21.12 and
would downgrade the header). `protoc-gen-go` also moved 1.31.0 → 1.36.11 — that's the churn in
`log.pb.go`. protoc 32.0 is installed at `~/.local/bin/protoc`.

`go build`, `go vet`, `go test ./...` and `-race` all green.

## Blockers, in order

**1. `LogEntry.Type` is dropped in `convert.go`.** Config entries arrive as commands, so
`processConfigurationLogEntry` never fires and **membership changes never reach followers**. Not a proto
limitation — `log.proto` has the field. Invisible to tests: `MemStorage` and the mock transport pass the
struct through intact.

Fix: one field each direction, plus an explicit enum mapping — proto reserves 0 for `UNSPECIFIED` and
`EntryType_Command` is `iota`, so the two are offset by one. Don't cast. Add a round-trip test so the
next new field can't go missing the same way.

**2. `InstallSnapshot` transport is still a stub**, so a lagging follower can never be caught up.
`HandleInstallSnapshot` is complete; what's missing is that `InstallSnapshotArgs.Reader` is an
`io.Reader` and gRPC has no such thing. Send side chunks the reader onto a stream; receive side wraps
`stream.Recv()` in an `io.Reader`, buffering leftovers.

The trap: [install_snapshot.go:21](../install_snapshot.go#L21) drains the reader on **every** exit,
including its early rejections. The sender must stream to completion even when the answer is already
`Success: false`, or the stream hangs instead of failing.

**3. `example/server/server.go:75` passes `nil` as the `StateMachine`.** Now that `lastApplied` is real,
`shouldTriggerSnapshot` can fire, so this reaches a nil deref instead of silently no-opping.

## No crash-recovery path

Write orderings are all safe already (see INVARIANTS.md — `currentTerm` before `votedFor`, `Apply`
before `SetLastApplied`, snapshot durable before compaction). The **read** side is missing:
[node.go:178-213](../node.go#L178-L213) loads `currentTerm` and `votedFor` and nothing else.

- The state machine is never restored at startup — `sm.Restore` is only called when a leader pushes a
  snapshot at us.
- `configurations` is never rebuilt from the log, so restart reverts to the `cfg.Peers` seed and loses
  every membership change.
- `snapshotLatest` isn't restored, so after restart-following-compaction `logTermAt` can't validate a
  `prevLogIndex` at the snapshot boundary.

**A durable state machine is now load-bearing.** `GetLastApplied` used to always return 0, so the apply
loop replayed the whole log and rebuilt the FSM by accident. It now resumes from the real index — a
volatile FSM would start empty and never receive entries at or below `lastApplied`.

## Who does what

Split by learning density, not by the library/`example` boundary.

**Mine:** the Pebble `StateMachine` (`Apply`, `Snapshot`, `Restore`), startup recovery, and everything
in the library. `StateMachine` has only ever been mocked, so its shape is an unvalidated guess —
`Storage` got a sharp contract precisely because it has two implementations.

Two things that decide the `StateMachine` design: `Apply` must be idempotent per entry (`Set(k,v)` is
free, `Increment(k)` isn't), and `Restore` is a **replace**, not a load — the caller is usually a
lagging follower with stale state, so keys absent from the snapshot must be deleted, not left behind.
Use a Pebble instance separate from `Store`, or clearing the keyspace wipes the raft log.

**Delegable:** the `Type` fix, `InstallSnapshot` transport, codegen and plumbing.

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
