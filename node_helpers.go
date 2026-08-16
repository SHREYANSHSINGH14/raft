package raft

import (
	"context"

	"github.com/rs/zerolog"
)

// node_helpers.go — the thread-safe accessors for Node state.
//
// Everything in this file takes the relevant lock itself, so a caller must NOT
// already hold that lock. Nothing here does I/O or drives a role transition; it
// is all read-modify-write on in-memory state, plus the small signal helpers that
// hand work to the goroutine that owns it.
//
// Sections, in order:
//
//	1. Role, identity and leader
//	2. Leadership term — leaderCloseCh
//	3. Cluster configuration — reading
//	4. Cluster configuration — quorum math
//	5. Cluster configuration — mutation
//	6. Per-peer replication indexes
//	7. Dynamic replication fan-out
//	8. Commit index
//	9. Snapshot bookkeeping
//	10. Election signals
//	11. Persistent state passthrough

// =============================================================================
// 1. Role, identity and leader
//
// All guarded by mu.
// =============================================================================

func (n *Node) SetRole(role ServerRole) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Role = role
	return
}

func (n *Node) GetRole() ServerRole {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.Role
}

func (n *Node) GetID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.ID
}

func (n *Node) SetLeaderID(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.LeaderID = id
}

func (n *Node) GetLeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.LeaderID
}

// =============================================================================
// 2. Leadership term — leaderCloseCh
//
// The channel is open for exactly as long as this node leads. Every Future
// registered during the term captures it, so a step-down fails those waiters with
// ErrLeadershipLost rather than leaving them blocked on entries the next leader may
// never commit.
// =============================================================================

// setLeaderCloseCh opens a fresh leadership channel. Called by becomeLeader, so
// each leadership term gets its own — a channel closed by a previous step-down
// must never be reused.
func (n *Node) setLeaderCloseCh() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.leaderCloseCh = make(chan struct{})
}

// clearLeaderCloseCh ends the current leadership term: it closes the channel and
// clears the field. The close is what releases the Futures — each one selects on
// the channel it captured, so one close answers every waiter in flight.
//
// Taking the channel out of the field under mu before closing is what makes the
// close happen exactly once — two concurrent step-downs cannot both see non-nil.
// The commit signal is sent without mu held, matching SetCommitIndex.
func (n *Node) clearLeaderCloseCh() {
	n.mu.Lock()
	ch := n.leaderCloseCh
	n.leaderCloseCh = nil
	n.mu.Unlock()

	if ch == nil {
		return // not leader; nothing to close and nobody to wake
	}
	close(ch)

	// Nudge the apply loop so it re-evaluates rather than sleeping through a term
	// change. Nothing it waits on actually depends on leadership, so this is a
	// no-op wake in practice — kept because a step-down is exactly the moment not
	// to assume that. Sent with mu released, and commitMu never taken.
	n.signalCommit()
}

func (n *Node) getLeaderCloseCh() chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderCloseCh
}

func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.Role == ServerRole_Leader
}

// Fatal returns a channel that is closed when this node can no longer keep its
// state machine in step with the log — a failed Apply, or a store that cannot be
// read at startup. Committed entries cannot be retracted and Raft never
// re-delivers an entry it has already handed to Apply, so once this fires the
// local state machine is behind the log for good and will not catch up on its own.
//
// The library does not act on this beyond stopping the apply loop, because the
// useful responses are all the caller's to choose: cancel the context passed to
// Start and take the node down, alert an operator, or wipe the state machine and
// let the leader rebuild it through InstallSnapshot. What the caller must not do
// is ignore it. Everything else here keeps running — the node still answers
// AppendEntries, still grants votes, and can still win an election — so an
// unwatched Fatal is a node serving reads from a state machine frozen in the past.
//
// Note the log itself is fine. Only the state machine is stuck, so this node
// remains a valid log replica and continues to count toward quorum. Callers that
// want to keep that fault tolerance can stop serving reads and leave the node
// running; callers that want the simple thing cancel the context.
//
// Use FatalErr for the cause. Reading a closed channel never blocks, so this is
// safe to select on alongside ctx.Done().
func (n *Node) Fatal() <-chan struct{} {
	return n.fatalCh
}

// FatalErr returns the failure that closed Fatal, or nil if it has not fired.
func (n *Node) FatalErr() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.fatalErr
}

// setFatal records err and closes fatalCh, once. Callers must not hold mu.
//
// The error is stored before the close so that any goroutine woken by the close
// is guaranteed to see it — the close is what publishes the write.
func (n *Node) setFatal(ctx context.Context, err error) {
	n.fatalOnce.Do(func() {
		n.mu.Lock()
		n.fatalErr = err
		n.mu.Unlock()

		close(n.fatalCh)
		zerolog.Ctx(ctx).Error().Err(err).Msg("fatal: state machine can no longer follow the log, node must stop serving reads")
	})
}

// =============================================================================
// 3. Cluster configuration — reading
//
// These all read n.configurations.latest, the live operating configuration.
// cfg.Peers is only the bootstrap seed (copied into configurations in NewNode) and
// must not be read at runtime.
//
// configurations.latest holds EVERY member of the cluster, this node included.
// That is what lets a leader remove itself: membership becomes a property of the
// map rather than something inferred from the map's silence. It also makes a
// config log entry — which carries the whole map — mean the same thing on the
// leader that wrote it and on every follower that applies it.
//
// The consequence is that the readers split three ways, and reaching for the
// wrong one is the easy mistake here:
//
//   - peerIDs / voterPeerIDs — everyone EXCEPT self. Use for anything that puts
//     an RPC on the wire; we never send to ourselves.
//   - voterCount / isVoter (§4) — INCLUDING self. Use for majority math.
//   - peersSnapshot — the whole map, self included. Use when the configuration
//     itself is the subject: marshalling a config entry, match-index bookkeeping.
// =============================================================================

func (n *Node) GetPeerIndex(id string) Peer {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.configurations.latest[id]
}

// peersSnapshot returns a copy of the latest peers map, safe to read without
// racing concurrent NextIndex/MatchIndex updates to n.configurations.latest.
func (n *Node) peersSnapshot() map[string]Peer {
	n.mu.Lock()
	defer n.mu.Unlock()
	return clonePeers(n.configurations.latest)
}

// peerIDs returns a snapshot of peer IDs — every member EXCEPT this node — safe
// to range over without racing concurrent NextIndex/MatchIndex updates to
// n.configurations.latest.
func (n *Node) peerIDs() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	ids := make([]string, 0, len(n.configurations.latest))
	for id := range n.configurations.latest {
		if id == n.ID {
			continue
		}
		ids = append(ids, id)
	}
	return ids
}

// voterPeerIDs returns the IDs of the OTHER voters — the peers an election or a
// quorum probe sends RPCs to. Only Voter members qualify: Staging (still catching
// up) and NonVoter (replica-only) members are excluded, so they neither raise the
// majority threshold nor get counted in it. Self is excluded because we never RPC
// ourselves; use voterCount when you need the number a majority is taken over.
func (n *Node) voterPeerIDs() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	ids := make([]string, 0, len(n.configurations.latest))
	for id, peer := range n.configurations.latest {
		if id == n.ID || peer.PeerState != PeerState_Voter {
			continue
		}
		ids = append(ids, id)
	}
	return ids
}

// lookupPeer returns the entry for id in the live operating configuration,
// whether id is in it at all, and how many peers the configuration holds. All
// three are read under one acquisition of mu so a caller making a sequence of
// membership decisions (HandlePreVote) never sees two different configurations.
func (n *Node) lookupPeer(id string) (peer Peer, ok bool, configSize int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	peer, ok = n.configurations.latest[id]
	return peer, ok, len(n.configurations.latest)
}

// hasStagingPeer reports whether a membership change is already in flight — Raft
// allows only one at a time, and AddMember parks its new server in Staging for the
// duration.
func (n *Node) hasStagingPeer() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, peer := range n.configurations.latest {
		if peer.PeerState == PeerState_Staging {
			return true
		}
	}
	return false
}

// =============================================================================
// 4. Cluster configuration — quorum math
//
// These count self, unlike the RPC-fan-out readers in §3.
// =============================================================================

// voterCount returns how many members vote, this node included if it is still one.
// This is the number majoritySize is taken over. It reads self out of the map like
// any other member, which is the whole point of keeping self in there: once a
// configuration removes us, we stop counting ourselves without anyone having to
// remember to special-case it.
func (n *Node) voterCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	count := 0
	for _, peer := range n.configurations.latest {
		if peer.PeerState == PeerState_Voter {
			count++
		}
	}
	return count
}

// isVoter reports whether this node is itself a voting member of the live
// configuration. False once a configuration that removed us takes effect —
// Ongaro §4.2.2: a leader that has been removed keeps replicating until C_new
// commits, but must not count itself in majorities while doing so.
func (n *Node) isVoter() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.configurations.latest[n.ID].PeerState == PeerState_Voter
}

// majoritySize returns how many votes carry a cluster of voterCount voters.
// voterCount includes this node when it is a voter — unlike the old signature,
// which took a peer count and added self unconditionally, and so kept counting a
// node the cluster had already removed.
func majoritySize(voterCount int) int {
	return voterCount/2 + 1
}

// =============================================================================
// 5. Cluster configuration — mutation
//
// AddMember and RemoveMember drive these under clientMu, then replicate the
// resulting membership as one EntryType_Config entry. Note appendEntry is what
// records WHICH log index produced the configuration (configurations.latestIndex);
// mutating the map here does not.
// =============================================================================

func (n *Node) addPeer(id string, peer Peer) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.configurations.latest[id] = peer
	return
}

func (n *Node) removePeer(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.configurations.latest, id)
}

func (n *Node) SetPeerState(id string, state PeerState) {
	n.mu.Lock()
	defer n.mu.Unlock()

	peer := n.configurations.latest[id]
	peer.PeerState = state
	n.configurations.latest[id] = peer
}

// =============================================================================
// 6. Per-peer replication indexes
//
// Leader-only bookkeeping, stored on each peer's entry in the configuration.
// This node's own entry has no meaningful NextIndex/MatchIndex — becomeLeader
// skips seeding it, and getMajorityMatchIndex substitutes our real last log index.
// =============================================================================

func (n *Node) SetNextPeerIndex(id string, idx uint) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// map returns a copy of value so if we do
	// n.configurations.latest[id].NextIndex = idx
	// it won't work coz we change value of copy
	// not the original thing so to change the
	// actual value assign a new struct
	// Better to use pointers if frequent change
	// but for learning we keep it like this
	peer := n.configurations.latest[id] // copy
	peer.NextIndex = idx                // modify
	n.configurations.latest[id] = peer  // write back
}

func (n *Node) SetMatchPeerIndex(id string, idx uint) {
	n.mu.Lock()
	defer n.mu.Unlock()

	peer := n.configurations.latest[id]
	peer.MatchIndex = idx
	n.configurations.latest[id] = peer
}

// =============================================================================
// 7. Dynamic replication fan-out
//
// startSendLogs decides its peer set once, when a leadership term begins. These
// channels are how membership changes reach it mid-term: memberAddedCh asks the
// orchestrator to start replicating to a newly promoted member, and each peer's
// entry in memberRemovedCh tells that peer's goroutine to stop.
//
// Both only exist for the duration of a leadership term — becomeLeader creates
// them, becomeFollower clears them. Every accessor below therefore has to tolerate
// their absence: a membership change racing a step-down must not block on a
// channel nobody is reading, and must not panic on a map nobody has created. All
// of them take mu, because the orchestrator mutates the same map from its own
// goroutine.
// =============================================================================

// ensureMemberRemovedCh returns the peer's stop channel, creating it if absent.
//
// This is the only accessor, used for both the peers that existed when the term
// began (becomeLeader has already made their channels, so this just reads them)
// and for a member promoted mid-term (which has none yet). A separate read-only
// variant existed briefly; nothing in production needed one, since a peer being
// replicated to must always be stoppable.
func (n *Node) ensureMemberRemovedCh(id string) <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.memberRemovedCh == nil {
		n.memberRemovedCh = make(map[string]chan struct{})
	}
	if _, ok := n.memberRemovedCh[id]; !ok {
		n.memberRemovedCh[id] = make(chan struct{}, 1)
	}
	return n.memberRemovedCh[id]
}

// notifyMemberAdded asks the heartbeat orchestrator to start replicating to a
// newly promoted member. It is a no-op when we are not leading, and never blocks:
// a bare send would deadlock AddMember forever against a nil channel (we stepped
// down) or an orchestrator that has already exited.
//
// It is the `default:` clause that makes this safe, not the nil check — a send to
// a nil channel inside a select simply falls through. The nil check only skips the
// warning below, which would otherwise fire on every membership change made while
// not leading.
func (n *Node) notifyMemberAdded(ctx context.Context, id string) {
	n.mu.Lock()
	ch := n.memberAddedCh
	n.mu.Unlock()

	if ch == nil {
		return // not leading; there is no fan-out to update
	}
	select {
	case ch <- id:
	default:
		// The buffer is sized 1 and AddMember is serialised by hasStagingPeer, so
		// this should not happen. Log rather than drop silently: the consequence is
		// a member that gets no replication until the next leadership term.
		zerolog.Ctx(ctx).Warn().Msgf("member-added notification for %s dropped; it will not be replicated to until the next term", id)
	}
}

// notifyMemberRemoved tells a peer's heartbeat goroutine to stop. Same no-op and
// non-blocking rules as notifyMemberAdded — and note the goroutine may already
// have exited on its own (step-down cancels heartbeatCtx), in which case nobody
// reads this and the buffered slot is simply discarded with the term.
func (n *Node) notifyMemberRemoved(id string) {
	n.mu.Lock()
	ch := n.memberRemovedCh[id]
	n.mu.Unlock()

	if ch == nil {
		return
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}

// clearMemberChannels ends the fan-out bookkeeping for a leadership term. Called
// by becomeFollower; becomeLeader builds a fresh set.
func (n *Node) clearMemberChannels() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.memberAddedCh = nil
	n.memberRemovedCh = nil
}

// =============================================================================
// 8. Commit index
//
// Guarded by commitMu, NOT mu. The apply loop parks on commitCh whenever it has
// nothing to apply, and it holds commitMu while evaluating that condition — a
// shared lock would freeze every goroutine that needs mu for as long as the loop
// had nothing to do. See node.go.
// =============================================================================

// signalCommit wakes the apply loop to re-check its condition. It is the only
// correct way to write to commitCh.
//
// The send is non-blocking, and that is load-bearing twice over.
//
// It cannot deadlock. A blocking send made while holding commitMu wedges the node
// permanently: the loop is outside its receive for the whole of applyEntries, a busy
// leader commits more than a bufferful in that time, and the sender then sits on the
// very lock the loop needs to get back to the receive that would release it. Not
// blocking removes the cycle rather than relying on every caller to release the lock
// first — which is the kind of rule that gets broken at two call sites out of three.
//
// It cannot pile up. commitCh is buffered 1 and the signal is a level, not a queue:
// it says "state moved, go look", and the loop re-reads commitIndex and
// snapShotInProgress when it wakes. A full buffer means a wake-up is already pending,
// so the dropped signal costs nothing — and the loop wakes once per burst instead of
// once per sender.
func (n *Node) signalCommit() {
	select {
	case n.commitCh <- struct{}{}:
	default:
	}
}

// SetCommitIndex advances commitIndex and wakes the apply loop. A lower index is
// ignored — commitIndex only moves forward.
//
// No defer: the signal must go out with commitMu released (see signalCommit), so
// every path unlocks explicitly. The early return is the one that is easy to get
// wrong, and leaking the lock there wedges the node for good.
func (n *Node) SetCommitIndex(idx uint) {
	n.commitMu.Lock()
	if idx < n.commitIndex {
		n.commitMu.Unlock()
		return
	}
	n.commitIndex = idx
	n.commitMu.Unlock()

	n.signalCommit()
}

func (n *Node) GetCommitIndex() uint {
	n.commitMu.Lock()
	defer n.commitMu.Unlock()

	return n.commitIndex
}

// =============================================================================
// 9. Snapshot bookkeeping
//
// The last-included index and term of the most recent snapshot, set on both create
// and install. logTermAt reads them to accept the snapshot boundary as a valid
// prevLog anchor even though that entry has been compacted away.
// =============================================================================

// lastIndex returns the index of the last entry the node holds, falling back to the
// snapshot when the log itself is empty.
//
// The store cannot answer this on its own. A node that has snapshotted and compacted
// has an empty log and a real last index — the store reports 0, which is
// indistinguishable from a node that has never held anything. Only the snapshot
// metadata, which lives here and not in Storage, separates the two.
//
// 0 means genuinely fresh: no entries, no snapshot.
func (n *Node) lastIndex(ctx context.Context) (uint, error) {
	idx, err := n.store.GetLastIndex(ctx)
	if err != nil {
		return 0, err
	}
	if idx != 0 {
		return idx, nil
	}
	if snapIdx := n.GetSnapshotLatestIndex(); snapIdx != 0 {
		return snapIdx, nil
	}
	return 0, nil
}

// firstIndex returns the lowest index the node can still serve from its log, with the
// same snapshot fallback as lastIndex.
//
// With an empty log and a snapshot at N, the log floor is N+1: N itself lives inside
// the snapshot, not in the log. 1 means genuinely fresh — no entries, no snapshot —
// which is what makes "first index is 1" a usable assertion that nothing has been
// compacted away.
func (n *Node) firstIndex(ctx context.Context) (uint, error) {
	idx, err := n.store.GetFirstIndex(ctx)
	if err != nil {
		return 0, err
	}
	if idx != 0 {
		return idx, nil
	}
	if snapIdx := n.GetSnapshotLatestIndex(); snapIdx != 0 {
		return snapIdx + 1, nil
	}
	return 1, nil
}

// SetSnapshotLatest records the latest snapshot's last-included index and term
// together, under one lock, so a reader never sees a torn (index, term) pair.
func (n *Node) SetSnapshotLatest(idx, term uint) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.snapshotLatestIndex = idx
	n.snapshotLatestTerm = term
}

func (n *Node) GetSnapshotLatestIndex() uint {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.snapshotLatestIndex
}

func (n *Node) GetSnapshotLatestTerm() uint {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.snapshotLatestTerm
}

// =============================================================================
// 10. Election signals
// =============================================================================

// signalTimeoutNow tells the election-timeout goroutine to campaign immediately
// rather than wait out its timer — the receiving half of a leadership transfer.
// Non-blocking: with a signal already pending there is nothing to add, since the
// timer only needs to fire once.
//
// A signal rather than a direct becomeCandidate, because only the goroutine that
// owns a lifecycle may end it — see INVARIANTS.md.
func (n *Node) signalTimeoutNow() {
	select {
	case n.timeoutNowCh <- struct{}{}:
	default:
	}
}

// signalElectionTimeout resets the election timer: we have heard from someone
// entitled to keep us a follower — a leader's AppendEntries, or a candidate whose
// vote we just granted. stepDownAsLeader sends the same signal for the opposite
// purpose; the leader's orchestrator reads it as "stand down".
//
// Non-blocking, and that is the whole point. The channel carries a level, not a
// queue: a second pending signal says nothing the first did not, so a full buffer
// means the sender has already got what it wanted. The callers are RPC handlers
// holding clientMu, and exactly one goroutine receives from this channel — the one
// that owns the current role's timer. A blocking send would tie their liveness to
// that goroutine happening to be in its select, which is true today only because
// of arguments made in three other files: stale-term AppendEntries return before
// reaching us, pre-vote is leader-sticky, and a vote is spent for its term. None of
// those mention this channel. Not blocking needs no argument.
func (n *Node) signalElectionTimeout() {
	select {
	case n.electionTimeoutCh <- struct{}{}:
	default:
	}
}

// =============================================================================
// 11. Persistent state passthrough
// =============================================================================

func (n *Node) GetCurrentTerm(ctx context.Context) (uint, error) {
	return n.store.GetCurrentTerm(ctx)
}
