# Where I am

**Last updated: 2026-08-12.** This file is short-lived by design — rewrite it, don't append to it.
It answers one question: if I sat down right now, what would I need to know?

## Just finished

**Pre-vote and leadership transfer are on the wire.** `PreVote` and `TimeoutNow` now exist in
`example/proto/rpc.proto` with their four messages, and both directions are wired:
`grpcTransport.PreVote`/`TimeoutNow` ([example/server/server.go](../example/server/server.go)) send,
`Server.PreVote`/`TimeoutNow` ([example/server/rpc.go](../example/server/rpc.go)) receive and route to
the library handlers. **This unblocks multi-node elections** — the pre-vote round no longer errors out
against a real cluster, so the "no node can win an election" problem below is gone.

Regenerated with protoc 32.0 (reports `libprotoc 6.32.0`, matching what the checked-in stubs were
built with). The local apt protoc was 3.21.12 and would have silently downgraded the generated header;
protoc 32.0 is now installed at `~/.local/bin/protoc` with the well-known protos in `~/.local/include`.
`protoc-gen-go` also moved 1.31.0 → 1.36.11, which is the only remaining churn in `log.pb.go`.

**One converter, not three.** The `raft.LogEntry ⇄ types.LogEntry` mapping was hand-written at three
call sites, which is why `Type` went missing from all three independently. It now lives once in
[example/types/convert.go](../example/types/convert.go) — `LogEntryToRaft` / `LogEntryFromRaft` plus
slice forms — and all ten call sites in `db`, `db_mock` and `server` go through it. That file is
hand-written; everything else in `example/types` is generated.

**`lastApplied` is persisted.** [example/db/db.go](../example/db/db.go) `Set/GetLastApplied` were
`return nil` / `return 0, nil` stubs; they now read and write `LastAppliedKey` with `pebble.Sync`.
`GetLastApplied` deliberately returns `0, nil` on a missing key rather than `ErrNotFound` — both
callers treat any error as fatal and `startApplyLoop` *returns*, so a sentinel would stop a fresh node
from ever applying anything. Three tests in `example/db/db_test.go`.

Read the "durable state machine" item under **Not wired at all** before running a node — persisting
`lastApplied` and restoring the FSM at startup are now a package deal, and only half of it exists.

## Previously

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

**`LogEntry.Type` is dropped at the conversion boundary.** This is now the top blocker — `PreVote` and
`TimeoutNow` are wired, so elections work, but membership changes still do not.

The converter is a single place now ([example/types/convert.go](../example/types/convert.go)), and it
copies `Index`, `Term`, `Data` — not `Type`. That was preserved deliberately when the three call sites
were collapsed, so the refactor stayed behaviour-preserving and the fix stays a separate commit.

It is not a proto limitation: `log.proto` defines `EntryType type = 3` and the full enum.
`EntryType_Command` is `iota`, so the zero value is a *valid* type — nothing errors, config entries
just become commands. The leader appends a config entry, stores it (`Type` lost), reads it back at
[heartbeat.go:241](../heartbeat.go#L241) to replicate it, ships it (lost again), and the follower's
`entry.Type == EntryType_Config` test at [append_entries.go:151](../append_entries.go#L151) never fires
— so `processConfigurationLogEntry` never runs and **membership changes never reach followers**. A
restart loses the leader's own config the same way. Invisible to the test suite: `MemStorage` and the
mock transport pass the struct through intact.

Fix is one field in each direction in `convert.go` plus a named enum mapping (proto reserves 0 for
`UNSPECIFIED`, so the two enums are offset by one — map it explicitly, don't cast), and a round-trip
test asserting `in == LogEntryToRaft(LogEntryFromRaft(in))` so the next added field cannot go missing.

**`InstallSnapshot` is still a stub in `grpcTransport`** ([example/server/server.go](../example/server/server.go)),
so a lagging follower can never be caught up by snapshot. The receiving half
(`Node.HandleInstallSnapshot`) is complete. What is missing is transport adaptation in both directions,
because `InstallSnapshotArgs.Reader` is an `io.Reader` and gRPC has no such thing:

- **send**: read `args.Reader`, chunk it onto a client-stream; first message carries `Term`,
  `LeaderID`, `SnapshotMetadata`, `SnapshotSize`, the rest carry bytes.
- **receive**: wrap `stream.Recv()` in an `io.Reader`, buffering leftovers across calls since a chunk
  is usually larger than the `p []byte` handed to `Read`.

The trap: [install_snapshot.go:21](../install_snapshot.go#L21) drains the reader on **every** exit path,
including the two early rejections at lines 28 and 50. So the sender must stream to completion even
when the answer is already `Success: false`, and the adapter must drain cleanly rather than erroring.
Get it wrong and the stream hangs instead of failing.

## Not wired at all

**There is no crash-recovery path.** This is the biggest structural gap and it is not about
transactions — every *write* ordering in the library is already safe in the sense that a crash leaves
you conservative rather than corrupt:

- `currentTerm` before `votedFor` on every term bump ([request_vote.go:40-52](../request_vote.go#L40-L52),
  [append_entries.go:37-44](../append_entries.go#L37-L44), [election.go:158-169](../election.go#L158-L169),
  [install_snapshot.go:181-188](../install_snapshot.go#L181-L188)). A crash between them leaves a stale
  `votedFor` under a newer term, so we refuse a vote we could have granted — one wasted election round.
  Reverse the order and a crash clears `votedFor` while the term stands, which permits **two votes in
  one term**. That is the real safety violation, and the ordering is the only thing preventing it.
- `sm.Apply` before `SetLastApplied` ([apply_loop.go:89-93](../apply_loop.go#L89-L93)) — a crash between
  re-applies, never skips. Idempotent `Apply` absorbs it, which is why no shared batch is needed.
- snapshot file written before `DeleteLogs` compacts ([snapshot.go:132-144](../snapshot.go#L132-L144)).
- truncate-then-append in `HandleAppendEntries` — a crash leaves a shorter log, the leader re-replicates.

Note all of the above assume `pebble.Sync`. Switching to `NoSync` for write throughput silently trades
away election safety; if a benchmark ever tempts that, it is not a tuning knob.

What is missing is the **read** side. [node.go:178-213](../node.go#L178-L213) — `Start` loads
`currentTerm` and `votedFor` and nothing else:

- **The state machine is never restored at startup.** `sm.Restore` is called only from
  `HandleInstallSnapshot` ([install_snapshot.go:127](../install_snapshot.go#L127)) — i.e. when a leader
  pushes a snapshot at us. A restarting node never reads its own `SnapshotDir`.
- **`configurations` is never rebuilt from the log.** On restart `latest` and `committed` revert to the
  `cfg.Peers` bootstrap seed, so every membership change is lost. Masked today by the `Type` bug.
- **`snapshotLatest` is not restored**, so after a restart following compaction, `logTermAt` cannot
  validate a `prevLogIndex` sitting at the snapshot boundary.

**Persisting `lastApplied` and a durable state machine are now a package deal.** Before this session
`GetLastApplied` always returned 0, so the apply loop replayed the whole log and rebuilt the state
machine by accident. It now resumes from the real index — correct **if and only if** the state machine
is itself durable. A volatile FSM would start empty and never receive entries at or below
`lastApplied`. Do not ship a state machine that keeps state in memory without also adding the startup
restore above.

Also still unwired:

- **`example/server/server.go:75` passes `nil` as the `StateMachine`** and sets no `SnapshotDir` /
  interval / threshold. Now that `lastApplied` is real, `shouldTriggerSnapshot` can actually fire, so
  this path reaches a nil dereference rather than silently no-opping. Wire a state machine before
  running a node.
- **`Future.errCh`** — the field is allocated on every `Propose` and nothing ever sends on it, so that
  case in `Wait` is unreachable. Decide: wire it (an entry truncated out from under a waiter is the
  obvious candidate) or delete the field.

## What I implement myself

The split is by learning density, not by the library/`example` boundary. Mechanical plumbing can be
delegated; anything where the design is still a guess is mine, because writing it is what tells me
whether the guess was right.

**Mine — do not delegate:**

- **The Pebble `StateMachine`** — `Apply`, `Snapshot`, `Restore`. This is the one that matters.
  `StateMachine` has *never* been implemented for real, only mocked (`state_machine_mock.go`), so the
  shape of the interface — `Snapshot()` returning a `Snapshot` with `Persist`/`Release` rather than a
  plain `io.Reader` — is currently an unvalidated guess. `Storage` got its sharp contract precisely
  because it has two implementations (`MemStorage` and `example/db`); `StateMachine` has none.
  Concretely: `Apply` must be idempotent per entry (`Set(k,v)` is for free, `Increment(k)` is not —
  that needs the entry index stored alongside the value), and `Snapshot`/`Release` is a point-in-time
  consistent read while commits continue, which is what `pebble.NewSnapshot()` is for and why
  `Release` exists at all.
- **Startup recovery** — restoring the FSM from `SnapshotDir`, rebuilding `configurations` from the
  log, restoring `snapshotLatest`. Same reason: it may show that `Storage` and `StateMachine` need to
  share a handle, and that is a library decision, not an embedding one.
- **Everything in the library itself**, including the items under "Decided but not built" below.

**Delegable — mechanical, no design left in it:**

- The `LogEntry.Type` fix plus round-trip test (the diagnosis is already written down above; what
  remains is typing).
- `InstallSnapshot` transport adaptation. Genuinely non-trivial, but it is *streaming-transport*
  design, not consensus design — the Raft half is already complete in `HandleInstallSnapshot`.
- Proto/codegen plumbing, converters, config wiring, test scaffolding.

**Benchmarking is deliberately deferred.** No `Benchmark*` exists in the repo and none should until a
multi-node cluster actually commits entries — a throughput number against a cluster that cannot elect
or replicate measures nothing. When it is time, the first pass is small: microbenchmarks on the
`processFutures` prefix drain (it is O(n) in outstanding proposals per commit) and the `MemStorage`
append path, run with `-count=10 -benchmem` through `benchstat`; then mutex/block profiles, which are
the only empirical check on whether the three-lock split in INVARIANTS.md actually buys anything —
`clientMu` serializing `Propose` + `HandleAppendEntries` + `HandleRequestVote` is the likeliest real
bottleneck. Anything cluster-level needs an open-loop load generator, or the tail latencies will be a
polite fiction (coordinated omission).

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
