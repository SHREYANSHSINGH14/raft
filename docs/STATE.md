# Where I am

**Last updated: 2026-08-09.** This file is short-lived by design — rewrite it, don't append to it.
It answers one question: if I sat down right now, what would I need to know?

## Just finished

**Repo restructured: the library is now the repo root.** On branch `refactor/library-at-root`.
`raft/*.go` moved to `.` (still `package raft`), so the import is `github.com/SHREYANSHSINGH14/raft`
instead of the old `.../raft/raft` stutter. Everything else — `cmd`, `config`, `db`, `server`,
`types`, `proto`, `scripts`, `main.go`, `peers.yaml`, `config.dev.env`, `Dockerfile`,
`docker-compose.yaml` — moved under `example/`. Imports, `option go_package` in both `.proto` files,
the Dockerfile and the compose build context (`context: ..`, since `COPY . .` has to reach the library
at the root) are all rewired; `PEER_INFO` in `config.dev.env` is now `example/peers.yaml`, resolved
relative to the working directory, so the dev binary must be run from the repo root.

Also dropped in the same pass: the **Makefile** (two `go build` lines didn't earn a file — the README
spells them out now) and the checked-out `raftd` binary. And **every doc except the README moved into
`docs/`** — this file, `INVARIANTS.md`, `JOURNEY.md`. Relative links inside them gained a `../`.
`CLAUDE.md` at the root is a symlink and now points at `docs/INVARIANTS.md`; it stays untracked.

Purely mechanical — no library code changed.

**`Propose` returns a `Future` instead of blocking.** This was the last item in the "decided but not
built" section of the previous STATE.md, and it is now built:

- `Propose` appends and returns; `Future.Wait(ctx)` is the commit-wait. `AddMember`, `RemoveMember`
  and the debug HTTP endpoint all propose-then-wait, so their behaviour is unchanged.
- `futureList` is a `[]*Future` under **`commitMu`**, drained as a sorted prefix by `processFutures`,
  which the commit-index updater calls after `SetCommitIndex` and `advanceCommittedConfiguration`.
- `newFuture` captures `leaderCloseCh`, so a step-down releases every waiter with one `close()`.
- `Future.Wait` re-derives its verdict below the `select` in priority order — committed, context,
  leadership — because a `select` picks among ready cases at random. That tie-break was free in the old
  blocking version and had to be rebuilt; see JOURNEY.md Bug 7 for the three wrong turns.

**And `commitCond` is gone.** The apply loop now waits on `commitCh` so it can select over the wake-up
and `ctx.Done()` together, instead of needing a broadcast on every cancellation path. `sync.Cond` is no
longer used anywhere in the package.

Two things about it are worth keeping in mind before touching that code, because getting either wrong
hangs the node rather than failing a test:

- The wait is **lock-neutral** — unlock, receive, lock, all inside the inner loop — because a channel
  receive does not restore the lock the way `Cond.Wait` did.
- `commitCh` is **buffered 1** and written only through `signalCommit`, which is **non-blocking**. A
  blocking send under `commitMu` deadlocks against the loop.

Four separate hangs came out of that swap; JOURNEY.md Bug 8 has them. Two are now pinned by tests
(`TestApplyLoop_CommitBurstDuringSlowApply_DoesNotWedge`,
`TestSetCommitIndex_LowerIndex_IgnoredAndReleasesLock`), both of which need a gate proving the loop is
parked before the burst starts — without it they pass in milliseconds without testing anything.

Alongside it, in the same pass:

- `becomeLeader` reordered so the role flips **last**, after `initLeaderTermState` and the appends.
  A staging peer left behind by a dead leader's `AddMember` is now aborted through the log (a config
  entry without it), not by mutating `latest` in memory.
- `becomeLeader` appends a **no-op entry** on election win.
- All sends to `electionTimeoutCh` go through `signalElectionTimeout` (non-blocking), matching
  `signalTimeoutNow` and `notifyMember*`.
- Dead code removed: `waitForCommit`, `leadershipEnded`, `getLatestConfigurationIndex`,
  `getCommittedConfigurationIndex`.

`go build ./...`, `go vet ./...`, `go test ./...` and `go test ./... -race` are all green.

## The one thing that will bite you first

**Three `Transport` methods are still stubs in `grpcTransport`** ([server/server.go](../example/server/server.go))
— `PreVote`, `TimeoutNow`, `InstallSnapshot` — because `proto/rpc.proto` has no such RPCs. They return
`"not implemented"`.

Since elections went through the pre-vote gate, every pre-vote against a real cluster errors out, which
the round counts as a withheld vote, so **no node in a multi-node deployment can win an election**. A
single-node cluster still works: with one voter the round needs no RPCs and passes on its own vote. The
library tests all pass because they mock the transport.

Adding the three RPCs to the proto plus the `example/server/rpc.go` conversions is the highest-value next task,
and it is the prerequisite for running any real application against this.

Second-order effects of the same gap: the `RemoveMember` leadership handoff always fails and falls
through to the bare step-down, and a lagging follower can never be caught up by snapshot.

**And right behind it: `LogEntry.Type` is dropped at every conversion boundary.** All three converters
copy `Index`, `Term`, `Data` and silently omit `Type` — [example/db/db.go:39-52](../example/db/db.go#L39-L52)
(`toProto`/`fromProto`), [example/server/server.go:173](../example/server/server.go#L173) (leader→wire),
[example/server/rpc.go:29](../example/server/rpc.go#L29) (wire→follower). It is not a proto limitation:
`log.proto` defines `EntryType type = 3` and the full enum.

`EntryType_Command` is `iota`, so the zero value is a *valid* type — nothing errors, config entries just
become commands. The leader appends a config entry, stores it (`Type` lost), reads it back at
[heartbeat.go:241](../heartbeat.go#L241) to replicate it, ships it (lost again), and the follower's
`entry.Type == EntryType_Config` test at [append_entries.go:151](../append_entries.go#L151) never fires —
so `processConfigurationLogEntry` never runs and **membership changes never reach followers**. A restart
loses the leader's own config the same way. Invisible to the test suite: `MemStorage` and the mock
transport pass the struct through intact.

Fix is a field at each of the three sites plus a named enum mapping (proto reserves 0 for
`UNSPECIFIED`, so the two enums are offset by one — map it explicitly, don't cast).

## Not wired at all

- **The three proto RPCs**, above. Everything else is downstream of it.
- **`example/server/server.go:75` passes `nil` as the `StateMachine`** and sets no `SnapshotDir` / interval /
  threshold, so although `Node.Start` calls `startSnapshotLoop`, a real node still never snapshots.
- **`Future.errCh`** — the field is allocated on every `Propose` and nothing ever sends on it, so that
  case in `Wait` is unreachable. Decide: wire it (an entry truncated out from under a waiter is the
  obvious candidate) or delete the field.

## Decided but not built

**Admission control on `futureList`.** The list is created with capacity 1024 but has no cap. If quorum
is lost, `commitIndex` freezes, nothing drains, and the list grows without bound while every waiter
sits there. The decision from the original design still stands: cap it (`MaxPendingProposals`, ~1000)
and reject new proposals with a sentinel — **never evict**, since evicting either hangs the victim or
reports a false success.

**`(index, term)` keying for futures.** A `Future` is keyed by index alone, so a proposal whose entry a
new leader truncated is indistinguishable from one that committed. Handled conservatively today: the
follower commit path deliberately does not drain, so such a waiter gets `ErrLeadershipLost` rather than
a false success. Carrying the term would make it precise. See INVARIANTS.md.

## Open questions worth resolving

- `SnapshotMeta.PrevIndex/PrevTerm` is unused by the catch-up now that it anchors at
  `meta.Index/meta.Term` — vestigial. Remove it or find it a purpose.
- Why streaming `InstallSnapshot` rather than the paper's chunked/offset form?
- Confirm `db.Store.DeleteLogs` actually deletes the prefix in the production store (the mocks do).
  Compaction being a no-op in prod would mask the whole retain-floor design.

## Known open TODOs in code

- `write_logs.go` — `clientMu` is held across the append's disk write, so a slow store blocks every
  caller-facing entry point. Fixing it means handing out log indexes without holding the lock, and the
  index handed out must stay in order with `futureList`.
- `snapshot.go` — collapse apply loop + snapshot into one goroutine with channels.
- `add_member.go` / `heartbeat.go` — the `> 5` snapshot-retry and `maxCatchUpRounds = 10` should be
  configurable.
- `append_entries.go` — the leader-side replication fallback is still the naive decrement-by-one.

## Housekeeping

- Leftover debug print at `heartbeat.go:257` sprays `CurrentTerm / Res Term` into every test run.
- `refs/original/*` from the commit-history rewrite are still around locally (3 of them); delete them
  once you are happy with the history.
