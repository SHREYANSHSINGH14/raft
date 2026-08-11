# Membership Change Design — Decided Flow

## Approach
- **Single-server changes** (Ongaro dissertation), NOT joint consensus.
  - Reason: matches production implementations (hashicorp/raft, etc.), smaller bug surface than joint consensus's overlapping-majority complexity.
- **Model B: logged staging.** Membership changes go through the normal log/commit pipeline like any other entry — not a special out-of-band mechanism.

## Configuration States
Three states per server in the cluster configuration:
- **Voter** — full member, counts toward election/commit majorities.
- **NonVoter** — replicates log entries, does NOT count toward majorities. Used for a server that's caught up but not yet promoted.
- **Staging** — freshly added server, catching up via replication (including InstallSnapshot if far behind). Does NOT count toward majorities yet.

## Add Member Flow
1. Client calls library-exposed `AddMember` (or similar) — **not** a raw log-append the client constructs themselves.
2. Internally, `AddMember`:
   - Adds the new server as **Staging**.
   - Triggers normal replication (AppendEntries) to catch it up.
   - If the new server is too far behind (leader has already compacted logs it needs), replication internally falls back to **InstallSnapshot** — same trigger path as a lagging voter, but purely a liveness/replication concern, not a membership concern (these two subsystems are deliberately kept separate — see Key Learnings below).
3. Once the Staging server is sufficiently caught up (some replication-lag threshold), the leader appends a **configuration change log entry** promoting it from Staging → Voter.
4. That config entry replicates and commits like any normal log entry (single-server change = simple majority under the *new* configuration is sufficient, no joint quorum needed).
5. Once committed, the server is a full Voter.

## Remove Member Flow
1. Client calls library-exposed `RemoveMember`.
2. Leader appends a configuration change log entry removing the server from the voter set.
3. Commits under simple majority rules (single-server change).
4. **§6 disruption rule**: a removed server, if not explicitly notified/shut down, could still time out and start elections, disrupting the cluster even though it's no longer a member. Mitigation: **TimeoutNow RPC** — leader can directly trigger the *replacement* leadership transfer target to start an election immediately (used more generally for leadership transfer, but relevant here for graceful handling around membership churn).
5. Client notification of completion is **polling-based**, keyed on server ID — client polls to check whether the removal has actually committed, rather than a push/callback mechanism.

### Decided: what happens when the leader removes *itself* and the handoff fails

A leader removing itself is the interesting case, because by the time the entry commits the leader is
no longer a member — it cannot simply "stay leader and try again", since the committed configuration
says it is not in the cluster. Three options were on the table:

- **A — retry `TimeoutNow` against the next-most-caught-up voter**, with a finite budget, then give up.
- **B — step down anyway** if the handoff fails, accepting a brief leaderless gap until the ordinary
  randomized election timer fires somewhere.
- **C — refuse self-removal entirely**, requiring a separate, successful leadership transfer first, so
  the "committed self-removal, now stuck" state never arises.

**Chosen: B, and only B — no retry ladder.** The deciding argument is that B is not a fallback at all;
Ongaro §4.2.2 *prescribes* it — the removed leader steps down once C_new commits, full stop. That makes
`TimeoutNow` an optimisation layered on top (it shortens the leaderless gap from a full election
timeout to one round-trip), not a precondition for correctness. Once you see it that way, A is extra
machinery guarding a case the baseline already handles, and C trades a real capability for a problem
that doesn't exist. B also matches the repo's standing principle that follower is the safe default:
retreat from a role you can no longer fulfil rather than cling to it.

The cost is honest and bounded: a gap of up to one election timeout when the chosen successor is
unreachable. Safety is never at risk — stepping down cannot violate anything.

### Why this node lives inside its own configuration

`configurations.latest` originally held **peers only** — the node itself was never a key, and
`majoritySize` compensated with a hardcoded `+1`. Self-removal is inexpressible in that model: there
is nothing to remove, and a config entry marshalled from a self-excluding map is byte-identical
whether or not the author is being removed.

Two options: keep the representation and add a `selfRemoved` flag consulted by the quorum helpers, or
put this node in the map like any other member. The flag is smaller, but it only patches the local
quorum math — the *cluster* still can't be told the leader left, because the wire format still can't
say it. Making membership a property of the map fixes the representation once: self-removal is a
delete, the quorum helpers stop counting us for free (§4.2.2), and a config entry means the same thing
on the leader that wrote it and every follower that applies it. It also fixed a pre-existing bug where
a follower applying a config entry adopted a map that listed *itself* as a peer and omitted the leader.

The cost is that the peer helpers split three ways — RPC fan-out excludes self, majority math includes
it — which is now the first thing INVARIANTS.md (this directory) warns about.

## Key Constraint (now satisfied — was the prerequisite for all of the above)
- **InstallSnapshot must exist first** — Staging servers that are far behind rely on it to catch up before they can even be considered for promotion.
- **Status as of 2026-07-31: the send path now exists.** `Transport.InstallSnapshot`, `callInstallSnapshot` (streams the file with a size-scaled deadline, returns the full `SnapshotMeta`), and `sendInstallSnapshot` (the heartbeat fallback: when a peer's `nextIndex` is below the latest snapshot index, send a snapshot instead of decrementing forever). `AddMember` also drives InstallSnapshot directly for a fresh member. The follower-side `HandleInstallSnapshot` and on-disk format were already there. See `docs/STATE.md` for the one remaining wiring gap (the catch-up's first AppendEntries anchor still needs the follower to accept the snapshot boundary as its prevLog).

## Key Learnings Baked Into This Design
- **Replication/liveness (InstallSnapshot) is a different subsystem from membership changes**, even though they interact. Don't conflate "follower is lagging, catch it up" with "cluster membership is changing" — a temporarily lagging *existing voter* uses InstallSnapshot too, but that's not a membership event. Staging exists specifically for *new* servers being onboarded, not for punishing/demoting slow existing voters.
- **Single outstanding config-change slot** — only one membership change in flight/uncommitted at a time (standard Raft constraint to avoid overlapping-majority ambiguity).
- **Commit ≠ apply** — a config change being committed (replicated to majority) doesn't mean every node has applied it yet; matters for reasoning about when a removed server is *actually* safe to fully disregard.

## Gap Between This Doc and the Code (as of 2026-08-03)
Both directions have landed. What is left is plumbing, not design.
- **`AddMember` is implemented** (`add_member.go`): add as Staging → commit → InstallSnapshot →
  catch-up rounds → promote → commit, with rollback on failure, and tested.
- **`RemoveMember` is implemented** (`remove_member.go`): guards (leader, one change at a time,
  peer exists, not the last voter) → remove → replicate the whole membership → commit, restoring the
  peer on any failure. Self-removal hands off via `TimeoutNow` and steps down regardless, per the
  decision recorded above.
- **`TimeoutNow` exists**, receiving half (`HandleTimeoutNow` + `Transport.TimeoutNow`). The handler
  signals the election-timer goroutine rather than transitioning itself.
- **Quorum math filters by `PeerState`** and now counts self out of the configuration map rather than
  via a hardcoded `+1`, so a removed leader stops counting itself (§4.2.2).
- **Compaction vs. catch-up is coordinated** by a retain floor (`catchingUpIdx`) that *delays* (parks,
  does not skip) compaction while a member still needs the logs — see JOURNEY.md Bug 5.
- **The dynamic fan-out gap is closed** (branch `feat/dynamic-fanout`): a member promoted mid-term gets
  its own replication goroutine, and a removed member's goroutine exits — see JOURNEY.md Bug 6.
- **`committed` now advances on commit** (`advanceCommittedConfiguration`, from the commit-index
  updater), so a truncation rolls `latest` back to the last configuration that actually committed
  rather than to the bootstrap set.
- Still missing: the **proto RPCs** for `PreVote`/`TimeoutNow`/`InstallSnapshot` — all three are stubs
  in `grpcTransport`, so none of this works against a real cluster. See STATE.md (this directory).

## Still Open / Not Yet Decided
- ~~Whether to implement the paper's optional §7 step-6 log-retain optimization for InstallSnapshot~~ —
  **the code went the other way and already implements retain** (`install_snapshot.go`: it compares
  the local entry at `LastIncludedIndex` against `LastIncludedTerm` and only discards the whole log on
  mismatch). The reason for changing course was never recorded. Either write it down or revisit it.
- ~~Exact replication-lag threshold for Staging → Voter promotion.~~ — **decided (Ongaro §4.2.1):**
  replicate for up to `maxCatchUpRounds`; a round that completes within an election timeout means the
  member is keeping pace → promote. Still slow after the last round → abort.
- Whether InstallSnapshot streaming needs resumability, or always-restart-from-scratch on transfer
  failure — affects whether the leader needs per-follower snapshot progress tracking at all. Note the
  request type already streams (`InstallSnapshotRequest` carries an `io.Reader`), so the "streaming vs
  chunked" question is settled in code; only resumability is open.