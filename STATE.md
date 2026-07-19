# Where I am

**Last updated: 2026-07-14.** This file is short-lived by design — rewrite it, don't append to it.
It answers one question: if I sat down right now, what would I need to know?

## In flight: snapshotting / log compaction (uncommitted)

The whole working tree is one milestone: snapshots + log compaction, which `docs/membership-change.md`
correctly identifies as the prerequisite for membership changes.

**It is half-wired. Neither end is connected to a running node.** The disk format and the follower's
receive handler exist and are partly tested, but nothing triggers a snapshot and nothing sends one.

### What landed and works

- `raft/snapshot.go` — on-disk format (`<dir>/<index>-<term>-<nanos>/{snapshot,meta.json}`), atomic
  write via tmp-dir + fsync + rename. Well tested (`raft/snapshot_test.go`, 13 cases).
- `raft/install_snapshot.go` — `HandleInstallSnapshot`, the follower's receive path.
- `StateMachine` gains `Snapshot`/`Restore`; `Storage` gains `CompactLogs`; `Apply` now takes a `ctx`.
- `LogEntry` gains an `EntryType` (Command / NoOp / Config / Barrier).
- `Config.Peers` went from `[]string` to `map[string]Peer`, folding in the leader-only `nodeIdxs` map
  and adding `PeerState` (Unknown/Staging/Voter/NonVoter) to prep for membership changes.
- Locking moved out of `server/rpc.go` into the library as `Node.clientMu`.
- Apply loop parks on `snapShotInProgress` so `lastApplied` can't outrun the captured snapshot index.

### Blocking — do these first

1. **`go test ./raft/...` panics.** `raft/apply_loop_test.go` was never updated for the new
   `Apply(ctx, entries)` signature — it still stubs `sm.On(methodApply, entries)` with one argument
   while the mock now calls `m.Called(ctx, entries)`. Fix: `sm.On(methodApply, mock.Anything, entries)`
   at ~7 call sites. Purely mechanical. `go build ./...` is clean, so this hides until you run tests.
2. **`db.Store.CompactLogs` ([db/db.go:360](db/db.go#L360)) is a stub returning `nil`.** The mocks
   (`MemStorage`, `MockKVStore`) have real implementations; the production store does not. Compaction
   is a no-op in the real system.
3. **Path bug: [raft/install_snapshot.go:70](raft/install_snapshot.go#L70)** passes the bare directory
   *name* to `writeSnapshotToDisk` where a full path is expected, so the follower writes relative to
   the process CWD — then reopens from `cfg.SnapshotDir + "/" + dir` at line 84 and always fails.
   The leader path gets this right (`snapshot.go:82` joins with `cfg.SnapshotDir`).
4. **Unsigned underflow: [raft/snapshot.go:143](raft/snapshot.go#L143)** — `lastApplied - latestSnapshotIndex`
   on `uint`. If a snapshot on disk is *ahead* of `lastApplied` (just installed from a leader, or after
   a restart before `lastApplied` is re-read), this wraps to a huge number and every tick snapshots.

### Not wired at all — this is the actual next milestone

- **Snapshot creation never runs.** `startSnapshotLoop` ([raft/snapshot.go:30](raft/snapshot.go#L30))
  is called from nowhere. `Node.Start` starts `waitForQuorum`, `startElectionOut`, `startApplyLoop` —
  and not this. Even if started it would no-op: `server/server.go` never sets `SnapshotDir`,
  `SnapshotInterval`, or `SnapshotThreshold`, and nothing `MkdirAll`s the snapshot dir.
- **InstallSnapshot is never sent.** There is no `InstallSnapshot` method on the `Transport` interface,
  and no such RPC in `proto/rpc.proto` — so `HandleInstallSnapshot` is unreachable from the network
  even as a receiver. The replication fallback doesn't exist either: on a failed `AppendEntries`,
  [heartbeat.go:212](raft/heartbeat.go#L212) just decrements `nextIndex` by one and retries forever.
  There is no "the entry at nextIndex has been compacted, send a snapshot instead" branch.
- `EntryType_NoOp` / `_Config` / `_Barrier` are declared but never produced or consumed. In particular
  `becomeLeader` does not append the no-op entry.
- `HandleInstallSnapshot` has **zero test coverage**.
- `server/server.go:75` still passes `nil` as the `StateMachine` to `NewNode`.

### Suggested order

Fix (1) so tests run at all → fix (3) and (4), the two real bugs → implement `db.Store.CompactLogs` →
add `InstallSnapshot` to the proto + `Transport` + `server/rpc.go` → add the leader-side send fallback
in the `AppendEntries` failure path → wire `startSnapshotLoop` into `Node.Start` and set the snapshot
config in `server/server.go` → test `HandleInstallSnapshot`.

## Decisions I made while building this and never wrote down

Recover these while they're still recoverable — none of the reasoning exists in the repo:

- Why streaming (`io.Reader` on `InstallSnapshotRequest`) rather than the paper's chunked/offset
  InstallSnapshot?
- Why `clientMu` inside the library rather than leaving the lock in `server/rpc.go`?
- Why gate the apply loop with an atomic `snapShotInProgress` flag rather than another mechanism?
  (There's a TODO at `snapshot.go:29` saying you'd rather do apply+snapshot in one goroutine with
  channels — is the flag a known-temporary shim, or the intended design?)
- Why implement the §7 log-retain optimization ([install_snapshot.go:118-146](raft/install_snapshot.go#L118-L146))
  after `docs/membership-change.md:40` said you were leaning toward always-discard?

## Known open TODOs in code

- `write_logs.go:12` — return a future from `Propose` instead of blocking.
- `write_logs.go:36` — a blocked `Propose` can hang forever if the leader steps down; needs a `leaderCloseCh`.
- `write_logs.go:38` — append before taking the lock, to shorten the critical section.
- `snapshot.go:29` — collapse apply loop + snapshot into one goroutine with channels.

## Housekeeping

`raftd` (a 30MB build artifact) is untracked and should be gitignored. `docs/` is untracked and
should be committed.
