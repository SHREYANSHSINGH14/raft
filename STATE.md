# Where I am

**Last updated: 2026-07-31.** This file is short-lived by design — rewrite it, don't append to it.
It answers one question: if I sat down right now, what would I need to know?

## Just committed

The membership-change milestone. `AddMember` is now an end-to-end flow, and the InstallSnapshot
**send** path exists (it was receive-only before). Recent commits on `main`:

- membership config tracking (`configurations` latest/committed, §5.3 conflict-only append).
- exclude non-voters from replication + majority math.
- **this commit** — `AddMember` + InstallSnapshot send path + catch-up + deadline scaling.

`go build ./...`, `go vet ./...`, and `go test ./...` are green.
**`go test ./... -race` is not** — the pre-existing `Persist`-goroutine race (top blocking item).

## In flight: membership changes (`AddMember`)

### What landed this milestone

- **`AddMember` full flow** ([add_member.go](raft/add_member.go)): add peer as `Staging` → append
  full-config `EntryType_Config` entry → wait commit → InstallSnapshot → **catch-up rounds** → promote
  to Voter/NonVoter → wait commit. On any failure it **rolls back** (removes the staging peer, replicates
  the removal). Single outstanding change enforced by `hasStagingPeer`.
- **Catch-up loop** — sends `nextIndex..end` whole each round (per the paper), bounded by
  `maxCatchUpRounds`. Caught-up decision is Ongaro §4.2.1: a round finishing within an election timeout
  ⇒ keeping pace ⇒ promote; still slow after the last round ⇒ abort.
- **InstallSnapshot send path**: `Transport.InstallSnapshot`, `callInstallSnapshot` (streams the file,
  size-scaled deadline, returns full `SnapshotMeta`), `sendInstallSnapshot` (heartbeat fallback).
- **Delay-compaction retain floor**: `catchingUpIdx` (atomic) is the lowest index a catching-up member
  still needs; `runSnapshotOnce` **parks** in `waitForCatchUpFloor` (channel `catchUpSignal`, not a
  busy-wait) until the floor clears, then compacts. `setCatchingUpIdx` stores + signals.
- **AppendEntries deadline scaling** by entry count (`AppendEntriesDeadlineScaleCount/TimeMs`), shared by
  heartbeat and catch-up. Entries aren't serialized at this layer, so it's count-based not byte-based.
- Staging peers skipped in the heartbeat fan-out (`startSendLogs`); `getMajorityMatchIndex`/election/
  `waitForQuorum` count Voters only (`majoritySize`, `voterPeerIDs`).

### Blocking / broken — do these first

1. **`-race`: the `Persist` goroutine is never joined.** `runSnapshotOnce` spawns `go snap.Persist(...)`
   and returns without waiting. `TestRunSnapshotOnce_PersistError_TmpDirCleaned` fails under `-race`
   because the goroutine records a mock call while `AssertExpectations` reads it. Fix: capture the error
   in a buffered channel, `pr.Close()` first, receive before returning.
2. **The catch-up's first AppendEntries will be rejected by the follower.** It sends `prevLogIndex =
   meta.Index` (the snapshot anchor), but `HandleAppendEntries` still does `GetLogByIndex(prevLogIndex)`
   → `ErrNotFound` (that index is inside the snapshot, compacted) → reply false. The follower needs to
   accept the snapshot boundary as a valid prevLog anchor — i.e. a `logTermAt` helper that validates
   `prevLogIndex == snapshotLatestIndex` against a cached `snapshotLatestTerm`. **Neither exists yet.**
   Until this lands, catch-up cannot actually succeed against a real follower.
3. **`db.Store.CompactLogs`/`DeleteLogs` prefix path** — confirm the production store actually deletes
   the prefix (the mocks do). Compaction being a no-op in prod would mask the whole retain-floor design.

### Not wired at all

- **Snapshot creation never runs.** `startSnapshotLoop` ([snapshot.go:34](raft/snapshot.go#L34)) is
  called from nowhere; `Node.Start` doesn't start it, and `server/server.go` sets no `SnapshotDir`/
  interval/threshold and passes `nil` as the `StateMachine`.
- **`AddMember` is untested** — no `add_member_test.go`. It's now the largest untested path (locks,
  commit waits, InstallSnapshot, catch-up rounds, rollback, promotion).
- **Dynamic fan-out gap**: `startSendLogs` snapshots its peer set at `becomeLeader`, so a freshly
  promoted Voter gets no `sendLogsPerPeer` goroutine until the next leadership term — yet it now counts
  toward quorum. Close this or promotion is only half-real.
- **`RemoveMember` doesn't exist**; the config payload only models add/promote.
- `setCommittedConfiguration` is defined but never called — `committed` only ever holds the bootstrap
  config, so `rollbackLatestIfTruncated` reverts to bootstrap, not the true last-committed config.

## Open questions worth resolving

- `SnapshotMeta.PrevIndex/PrevTerm` = `meta.Index-1`'s entry. The catch-up anchor should really be
  `meta.Index/meta.Term` (the snapshot's own last-included). Revisit whether `PrevIndex/PrevTerm` is the
  right field or an off-by-one.
- Why streaming InstallSnapshot rather than the paper's chunked/offset form?
- Why `clientMu` in the library rather than `server/rpc.go`? (Answer reconstructed in chat: the
  check-then-act on term/votedFor/log isn't transactional; the lock must not depend on every caller
  remembering it. Worth writing into JOURNEY.md.)

## Known open TODOs in code

- `write_logs.go` — return a future from `Propose` instead of blocking; a blocked `Propose` can hang if
  the leader steps down (needs `leaderCloseCh`).
- `snapshot.go` — collapse apply loop + snapshot into one goroutine with channels.
- `add_member.go` / `heartbeat.go` — the `> 5` snapshot-retry and `maxCatchUpRounds = 10` should be
  configurable.

## Housekeeping

- Leftover debug print: [heartbeat.go:210](raft/heartbeat.go#L210) `fmt.Printf` sprays
  `CurrentTerm / Res Term` into every test run. Same in `server/rpc_concurrency_test.go`.
