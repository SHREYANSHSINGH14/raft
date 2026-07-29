# Where I am

**Last updated: 2026-07-19.** This file is short-lived by design — rewrite it, don't append to it.
It answers one question: if I sat down right now, what would I need to know?

## Just committed

The snapshot milestone is no longer sitting uncommitted. Three commits on `main`:

- `639e2ae` chore — gitignore the 29MB `raftd` binary.
- `4c12347` feat — the snapshot/compaction milestone (25 source files) plus test repairs.
- `66a2720` docs — `CLAUDE.md`, `STATE.md`, refreshed `JOURNEY.md` and `docs/membership-change.md`.

Working tree is clean. `go build ./...` and `go test ./...` are green.
**`go test ./... -race` is not** — see the top item below.

## In flight: membership configuration tracking (this session, uncommitted)

Groundwork for membership changes landed alongside the snapshot work below:

- **`configurations` struct** ([raft_config.go](raft/raft_config.go)) with `latest`/`committed` views
  + their producing indexes. `latest` is now the single live operating config and **replaced
  `cfg.Peers`** everywhere (peer helpers, election, heartbeat, `becomeLeader`, `waitForQuorum`).
  `cfg.Peers` is bootstrap-seed only. `NewNode`/`NewNodeMock` seed both views from it.
- **`HandleAppendEntries` rewritten** ([append_entries.go](raft/append_entries.go)) from unconditional
  truncate-and-append to §5.3 conflict-only truncation (skip matching prefix, truncate only on a term
  conflict). Fixed a latent bug where a delayed/duplicate AppendEntries could truncate committed
  entries — see JOURNEY.md Bug 4. The commit-index block below it is unchanged (no staged-commit yet).
- **`rollbackLatestIfTruncated`** is live: truncating the suffix that produced `latest` reverts it to
  `committed`. Tested in `append_entries_test.go` (tests 22–23).

- **Config entries carry the whole config.** `AddMember` marshals `n.peersSnapshot()` (a full
  `map[string]Peer`) as the `EntryType_Config` payload, and `processConfigurationLogEntry`
  ([configuration.go](raft/configuration.go)) decodes it and replaces `latest` on the follower path.
  Tested in `append_entries_test.go` (test 24). `MemberAdditionInfo` was removed.

**Still stubbed / not wired:**
- `setCommittedConfiguration` exists but nothing calls it — advancing `committed` when a config entry
  commits belongs in the apply loop and isn't wired. So `committed` currently only ever holds the
  bootstrap config, which means `rollbackLatestIfTruncated` reverts to bootstrap, not to the true last
  committed config. Wiring the apply loop to advance `committed` is the next step.
- `AddMember` mutates `latest` on the leader directly (`addPeer`) rather than through
  `processConfigurationLogEntry`; only the follower path decodes the entry. Removal has no payload/flow.
- `AddMember` ([add_member.go](raft/add_member.go)) still stops after append+commit-wait (steps 3–5 —
  snapshot/catch-up/promote — are TODO).
- Quorum math still counts by `PeerState_Voter` in election but `getMajorityMatchIndex` does not filter
  — unchanged by this session, still the gap `docs/membership-change.md` flags.

## In flight: snapshotting / log compaction

Prerequisite for membership changes, per `docs/membership-change.md`.

**Still half-wired. Neither end is connected to a running node.** The disk format and the follower's
receive handler exist and are tested; nothing triggers a snapshot and nothing sends one.

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

1. **Race: the `Persist` goroutine is never joined.** `runSnapshotOnce`
   ([snapshot.go:73-79](raft/snapshot.go#L73-L79)) spawns `go snap.Persist(ctx, pw)` and returns
   without waiting for it. On the error path `writeSnapshotToDisk` fails, `runSnapshotOnce` returns,
   the deferred `pr.Close()` unblocks `Persist` — and that goroutine is *still running*, still calling
   into the caller's `Snapshot` object after the function that owns it has returned.
   `go test ./... -race` fails on `TestRunSnapshotOnce_PersistError_TmpDirCleaned` because the
   goroutine records a mock call while `AssertExpectations` reads the call list. That's the detector
   surfacing a real ownership bug, not test noise.
   Fix: capture the error in a buffered channel, `pr.Close()` first (so a blocked `Persist` write
   returns `io.ErrClosedPipe` rather than deadlocking), then receive before returning.
2. **`db.Store.CompactLogs` ([db/db.go:360](db/db.go#L360)) is a stub returning `nil`.** The mocks
   (`MemStorage`, `MockKVStore`) have real implementations; the production store does not. Compaction
   is a no-op in the real system.
3. **Unsigned underflow: [snapshot.go:143](raft/snapshot.go#L143)** — `lastApplied - latestSnapshotIndex`
   on `uint`. If a snapshot on disk is *ahead* of `lastApplied` (just installed from a leader, or after
   a restart before `lastApplied` is re-read), this wraps to a huge number and every tick snapshots.

Fixed since the last revision of this file: the apply-loop test panic (mock arity after `Apply` gained
a `ctx`, plus five `AssertNotCalled` checks that were passing vacuously for the same reason), a
concurrent map iterate/write in the `runSendLogs` test helper, and the `install_snapshot.go` path bug
that wrote relative to the process CWD.

### Not wired at all — this is the actual next milestone

- **Snapshot creation never runs.** `startSnapshotLoop` ([snapshot.go:30](raft/snapshot.go#L30))
  is called from nowhere; `Node.Start` ([node.go:94](raft/node.go#L94)) starts `waitForQuorum`,
  `startElectionOut`, and `startApplyLoop` only. Even if started it would no-op: `server/server.go`
  never sets `SnapshotDir`, `SnapshotInterval`, or `SnapshotThreshold`, and nothing `MkdirAll`s the
  snapshot dir.
- **InstallSnapshot is never sent.** `Transport` ([interfaces.go:11](raft/interfaces.go#L11)) has only
  `AppendEntries` and `RequestVote`, and there is no such RPC in `proto/rpc.proto` — so
  `HandleInstallSnapshot` is unreachable from the network even as a receiver. The replication fallback
  doesn't exist either: on a failed `AppendEntries`, [heartbeat.go:216](raft/heartbeat.go#L216) just
  decrements `nextIndex` by one and retries forever. There is no "the entry at `nextIndex` has been
  compacted, send a snapshot instead" branch.
- `EntryType_NoOp` / `_Config` / `_Barrier` are declared but never produced or consumed. In particular
  `becomeLeader` does not append the no-op entry.
- [server/server.go:75](server/server.go#L75) still passes `nil` as the `StateMachine` to `NewNode`.

### Task: test `HandleInstallSnapshot`

`install_snapshot_test.go` is empty — the handler is the largest untested function in the library, and
it touches disk, the state machine, and the log store. Enumerate the cases yourself.

### Task: test the leader-side InstallSnapshot fallback (`heartbeat_test.go`)

Separate from the handler above: once the "AppendEntries failed because the entry at `nextIndex` was
compacted → send InstallSnapshot instead" branch exists in `sendLogs` ([heartbeat.go:216](raft/heartbeat.go#L216)),
cover it in `heartbeat_test.go`. Enumerate the cases yourself.

### Suggested order

Fix (1) so `-race` is green → fix (3) → implement `db.Store.CompactLogs` → add `InstallSnapshot` to
the proto + `Transport` + `server/rpc.go` → add the leader-side send fallback in the `AppendEntries`
failure path → wire `startSnapshotLoop` into `Node.Start` and set the snapshot config in
`server/server.go` → test `HandleInstallSnapshot`.

## Decisions I made while building this and never wrote down

Recover these while they're still recoverable — none of the reasoning exists in the repo:

- Why streaming (`io.Reader` on `InstallSnapshotRequest`) rather than the paper's chunked/offset
  InstallSnapshot?
- Why `clientMu` inside the library rather than leaving the lock in `server/rpc.go`?
- Why gate the apply loop with an atomic `snapShotInProgress` flag rather than another mechanism?
  (There's a TODO at `snapshot.go:29` saying you'd rather do apply+snapshot in one goroutine with
  channels — is the flag a known-temporary shim, or the intended design?)
- Why implement the §7 log-retain optimization ([install_snapshot.go:118-146](raft/install_snapshot.go#L118-L146))
  after `docs/membership-change.md` said you were leaning toward always-discard?

## Known open TODOs in code

- `write_logs.go:12` — return a future from `Propose` instead of blocking.
- `write_logs.go:45` — a blocked `Propose` can hang forever if the leader steps down; needs a `leaderCloseCh`.
- `write_logs.go:47` — append before taking the lock, to shorten the critical section.
- `snapshot.go:29` — collapse apply loop + snapshot into one goroutine with channels.

## Housekeeping

- Leftover debug print: [heartbeat.go:191](raft/heartbeat.go#L191) `fmt.Printf` sprays
  `CurrentTerm: N / Res Term: N` into every test run. Same in `server/rpc_concurrency_test.go:185,235`.
