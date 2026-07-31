# Where I am

**Last updated: 2026-07-31.** This file is short-lived by design — rewrite it, don't append to it.
It answers one question: if I sat down right now, what would I need to know?

## Just committed

The membership-change milestone. `AddMember` is now an end-to-end flow, and the InstallSnapshot
**send** path exists (it was receive-only before). Recent commits on `main`:

- membership config tracking (`configurations` latest/committed, §5.3 conflict-only append).
- exclude non-voters from replication + majority math.
- `AddMember` + InstallSnapshot send path + catch-up + deadline scaling.
- **latest (`5991a3d`)** — anchor the prevLog check on the snapshot boundary, both sides
  (`logTermAt` + cached `snapshotLatestTerm`), which unblocks catch-up against a real follower.

`go build ./...`, `go vet ./...`, `go test ./...`, **and `go test ./... -race`** are all green — the
long-standing `Persist`-goroutine race is fixed (`runSnapshotOnce` now joins the goroutine).

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
- **Snapshot-boundary prevLog anchor** (commit `5991a3d`): `logTermAt` treats `prevLogIndex ==
  snapshotLatestIndex` as a valid anchor (validated against a cached `snapshotLatestTerm`, set on both
  create and install), so replication survives compaction. Used on **both** sides — the follower
  (`HandleAppendEntries`) and the leader (`sendLogs` + AddMember catch-up). This closed the "catch-up
  rejected by a real follower" blocker.

### Blocking / broken — do these first

1. **`db.Store.CompactLogs`/`DeleteLogs` prefix path** — confirm the production store actually deletes
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

- The catch-up now anchors at `meta.Index/meta.Term` (the snapshot's own last-included) and the backoff
  uses `logTermAt`, so the boundary is handled correctly. `SnapshotMeta.PrevIndex/PrevTerm`
  (= `meta.Index-1`'s entry) is now unused by the catch-up — likely vestigial; remove it or find it a
  purpose.
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
