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

## Key Constraint (NOT yet satisfied — prerequisite for all of the above)
- **InstallSnapshot must exist first** — Staging servers that are far behind rely on it to catch up before they can even be considered for promotion.
- **Status as of 2026-07-14: it does not.** An earlier version of this doc claimed InstallSnapshot was "already implemented." That was wrong and cost re-entry time. What actually exists is the follower-side `HandleInstallSnapshot` handler and the on-disk snapshot format. There is no `InstallSnapshot` method on `Transport`, no such RPC in `proto/rpc.proto`, and no leader-side send path — so §Add Member step 2's "replication internally falls back to InstallSnapshot" does not exist. `heartbeat.go` still decrements `nextIndex` by one on a failed `AppendEntries` and retries forever. See `STATE.md`.

## Key Learnings Baked Into This Design
- **Replication/liveness (InstallSnapshot) is a different subsystem from membership changes**, even though they interact. Don't conflate "follower is lagging, catch it up" with "cluster membership is changing" — a temporarily lagging *existing voter* uses InstallSnapshot too, but that's not a membership event. Staging exists specifically for *new* servers being onboarded, not for punishing/demoting slow existing voters.
- **Single outstanding config-change slot** — only one membership change in flight/uncommitted at a time (standard Raft constraint to avoid overlapping-majority ambiguity).
- **Commit ≠ apply** — a config change being committed (replicated to majority) doesn't mean every node has applied it yet; matters for reasoning about when a removed server is *actually* safe to fully disregard.

## Gap Between This Doc and the Code (as of 2026-07-14)
Only the *data model* for membership has landed — `PeerState_Staging/Voter/NonVoter` (`raft/raft_config.go`),
`SetPeerState` (`raft/node_helpers.go`, called only from `HandleInstallSnapshot`), and `EntryType_Config`
(`raft/types.go`, never produced or consumed). None of the behaviour has:
- No library-exposed `AddMember` / `RemoveMember`.
- **The quorum math ignores `PeerState` entirely.** `getMajorityMatchIndex` (`raft/heartbeat.go`) counts
  every entry in the peers map, and `election` computes majority from `len(peerIDs)`. Today
  `server/server.go` marks everyone `Voter`, so this is latent rather than live — but both must filter
  by `PeerState` before Staging is usable, or a catching-up server would count toward majorities.
- No `TimeoutNow` RPC (the §6 disruption-rule mitigation above).

## Still Open / Not Yet Decided
- ~~Whether to implement the paper's optional §7 step-6 log-retain optimization for InstallSnapshot~~ —
  **the code went the other way and already implements retain** (`raft/install_snapshot.go`: it compares
  the local entry at `LastIncludedIndex` against `LastIncludedTerm` and only discards the whole log on
  mismatch). The reason for changing course was never recorded. Either write it down or revisit it.
- Exact replication-lag threshold for Staging → Voter promotion.
- Whether InstallSnapshot streaming needs resumability, or always-restart-from-scratch on transfer
  failure — affects whether the leader needs per-follower snapshot progress tracking at all. Note the
  request type already streams (`InstallSnapshotRequest` carries an `io.Reader`), so the "streaming vs
  chunked" question is settled in code; only resumability is open.