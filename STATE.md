# Where I am

**Last updated: 2026-08-03.** This file is short-lived by design — rewrite it, don't append to it.
It answers one question: if I sat down right now, what would I need to know?

## Just committed

The **RPC-surface milestone**: pre-vote, leadership transfer, and `RemoveMember`. Membership changes
are now symmetric (add *and* remove), and elections are gated behind a round that costs nothing to
lose. On `main`:

- `HandlePreVote` + `Transport.PreVote` — the receiving half of §9.6.
- `timeoutNowCh` + `leaderCloseCh` — the two channels the rest of the milestone needed.
- `HandleTimeoutNow` + `Transport.TimeoutNow` — leadership transfer, receiving half.
- **elections gated behind the pre-vote round** — `election()` now probes before it bumps the term.
- **this node lives inside its own configuration** — the refactor that made self-removal expressible.
- `RemoveMember` with self-removal handoff.
- docs: `CLAUDE.md` → `INVARIANTS.md`.

On branch `feat/dynamic-fanout` (not merged): replication to members added or removed mid-term.

`go build ./...`, `go vet ./...`, `go test ./...` and `go test ./... -race` are all green.

## The one thing that will bite you first

**Three `Transport` methods are stubs in `grpcTransport`** ([server/server.go](server/server.go)) —
`PreVote`, `TimeoutNow`, `InstallSnapshot` — because `proto/rpc.proto` has no such RPCs. They return
`"not implemented"`.

This used to be harmless. It is not any more: since elections went through the pre-vote gate, every
pre-vote against a real cluster errors out, which the round counts as a withheld vote — so **no node
in a multi-node deployment can win an election**. (A single-node cluster still works: with one voter
the round needs no RPCs and passes on its own vote.) The library tests all pass because they mock the
transport. Adding the three RPCs to the proto plus the `server/rpc.go` conversions is the single
highest-value next task.

Second-order effects of the same gap: the `RemoveMember` leadership handoff always fails and falls
through to the bare step-down, and a lagging follower can never be caught up by snapshot.

## What landed this milestone

- **Pre-vote (Ongaro §9.6)**, both halves. `HandlePreVote` is side-effect free by design — no term
  persisted, no vote spent, no election timer reset — and `election()` runs the round *before*
  `SetCurrentTerm`, so losing it costs nothing. A higher-term response stops the round but is never
  adopted.
- **Leadership transfer (§3.10)**, receiving half. `HandleTimeoutNow` does not transition; it signals
  `timeoutNowCh` and the election-timer goroutine campaigns, keeping the "only the goroutine that owns
  a lifecycle may end it" rule intact.
- **`leaderCloseCh`** — a `Propose` parked in `waitForCommit` now fails with `ErrLeadershipLost` on
  step-down instead of blocking until its caller's context expires. Closing it must be paired with a
  `commitCond.Broadcast`; see INVARIANTS.md.
- **`configurations.latest` holds the whole membership, self included.** This replaced a
  peers-only map plus a hardcoded `+1` in `majoritySize`. It is what makes "the leader is no longer a
  member" expressible at all, and it fixed a liveness bug where a removed leader kept counting itself
  in majorities (§4.2.2). Helpers now split three ways — see INVARIANTS.md before touching them.
- **`RemoveMember`** with the same shape as `AddMember`, plus a self-removal branch: hand off via
  `TimeoutNow` to the most caught-up voter, then step down **whether or not that worked** (§4.2.2
  prescribes the step-down; the handoff is only an optimisation on top).
- **Dynamic fan-out** (on the branch) — closes the long-standing gap where a freshly promoted Voter
  counted toward quorum but received no replication until the next leadership term.

## Not wired at all

- **The three proto RPCs**, above. Everything else is downstream of this.
- **`server/server.go` still passes `nil` as the `StateMachine`** and sets no `SnapshotDir` /
  interval / threshold, so although `Node.Start` now calls `startSnapshotLoop`, a real node still
  never snapshots.

## Decided but not built

**`Propose` as a future.** Design worked out in full; nothing written yet. The shape:

```go
type future struct {
    idx        uint
    done       chan struct{}   // CLOSED on commit — never sent to
    leaderLost <-chan struct{} // node's leaderCloseCh, captured at creation
}
```

The waiter selects on `done` / `leaderLost` / `ctx.Done()`. Points that took a while to reach:

- **Capture `leaderCloseCh` in the future**, don't have step-down walk a queue. Otherwise a `Propose`
  enqueuing between the drain and the role flip is never woken.
- **`done` must be closed, not sent to**, so an abandoned waiter can't block the updater.
- **The waiter list is a `[]*future` under `commitMu`, not a `chan *future`.** A channel can't be
  peeked (you'd need a pending-head slot) and, worse, can't be cleared — a full buffer would block
  `Propose` while it holds `clientMu`, which is a deadlock.
- **Enqueue inside `clientMu`**, next to `appendEntry`, or append order stops matching index order and
  the prefix drain silently strands a waiter.
- **Cap the list** (`MaxPendingProposals`, ~1000) as admission control — reject with a sentinel, never
  evict. Evicting either hangs the victim or reports a false success. The cap exists for the
  lost-quorum case, where `commitIndex` freezes and nothing drains.

Note this only removes one of `commitCond`'s two clients — the apply loop, `install_snapshot.go` and
`snapshot.go` still use it.

## Open questions worth resolving

- `SnapshotMeta.PrevIndex/PrevTerm` is unused by the catch-up now that it anchors at
  `meta.Index/meta.Term` — vestigial. Remove it or find it a purpose.
- Why streaming `InstallSnapshot` rather than the paper's chunked/offset form?
- Confirm `db.Store.DeleteLogs` actually deletes the prefix in the production store (the mocks do).
  Compaction being a no-op in prod would mask the whole retain-floor design.

## Known open TODOs in code

- `snapshot.go` — collapse apply loop + snapshot into one goroutine with channels.
- `add_member.go` / `heartbeat.go` — the `> 5` snapshot-retry and `maxCatchUpRounds = 10` should be
  configurable.

## Housekeeping

- Leftover debug print in `heartbeat.go` sprays `CurrentTerm / Res Term` into every test run. Same in
  `server/rpc_concurrency_test.go`.
- `refs/original/*` from the commit-history rewrite are still around locally; delete them once you are
  happy with the history.
