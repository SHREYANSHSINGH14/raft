package raft

import "context"

// -------------------------------------------
// Below are some helper functions to get and set server state like role, peer indexes, commit index etc
// These functions are thread safe and should be used whenever we want to read or write these state variables
// -------------------------------------------

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

// NOTE: the peer helpers below all read and write n.configurations.latest — the
// live operating configuration. cfg.Peers is only the bootstrap seed (copied into
// configurations in NewNode) and must not be used at runtime.
//
// configurations.latest holds EVERY member of the cluster, this node included.
// That is what lets a leader remove itself: membership becomes a property of the
// map rather than something inferred from the map's silence. It also makes a
// config log entry — which carries the whole map — mean the same thing on the
// leader that wrote it and on every follower that applies it.
//
// The consequence is that the helpers split three ways, and reaching for the
// wrong one is the easy mistake here:
//
//   - peerIDs / voterPeerIDs — everyone EXCEPT self. Use for anything that puts
//     an RPC on the wire; we never send to ourselves.
//   - voterCount — voters INCLUDING self. Use for majority math.
//   - peersSnapshot — the whole map, self included. Use when the configuration
//     itself is the subject: marshalling a config entry, match-index bookkeeping.

func (n *Node) GetPeerIndex(id string) Peer {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.configurations.latest[id]
}

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

func (n *Node) SetPeerState(id string, state PeerState) {
	n.mu.Lock()
	defer n.mu.Unlock()

	peer := n.configurations.latest[id]
	peer.PeerState = state
	n.configurations.latest[id] = peer
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

// peersSnapshot returns a copy of the latest peers map, safe to read without
// racing concurrent NextIndex/MatchIndex updates to n.configurations.latest.
func (n *Node) peersSnapshot() map[string]Peer {
	n.mu.Lock()
	defer n.mu.Unlock()
	return clonePeers(n.configurations.latest)
}

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

func (n *Node) SetCommitIndex(idx uint) {
	n.commitCond.L.Lock()
	defer n.commitCond.L.Unlock()
	if idx < n.commitIndex {
		return
	}
	n.commitIndex = idx

	n.commitCond.Broadcast()
}

func (n *Node) GetCommitIndex() uint {
	n.commitCond.L.Lock()
	defer n.commitCond.L.Unlock()

	return n.commitIndex
}

// setLeaderCloseCh opens a fresh leadership channel. Called by becomeLeader, so
// each leadership term gets its own — a channel closed by a previous step-down
// must never be reused.
func (n *Node) setLeaderCloseCh() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.leaderCloseCh = make(chan struct{})
}

// clearLeaderCloseCh ends the current leadership term: it closes the channel and
// clears the field, then wakes everything parked on commitCond so waiters get to
// observe the close. Without that broadcast a Propose asleep in Cond.Wait would
// never re-check and would hang until its own context expired.
//
// Taking the channel out of the field under mu before closing is what makes the
// close happen exactly once — two concurrent step-downs cannot both see non-nil.
// The broadcast is issued without mu held, matching SetCommitIndex.
func (n *Node) clearLeaderCloseCh() {
	n.mu.Lock()
	ch := n.leaderCloseCh
	n.leaderCloseCh = nil
	n.mu.Unlock()

	if ch == nil {
		return // not leader; nothing to close and nobody to wake
	}
	close(ch)
	n.commitCond.Broadcast()
}

func (n *Node) getLeaderCloseCh() chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderCloseCh
}

// signalTimeoutNow tells the election-timeout goroutine to campaign immediately
// rather than wait out its timer — the receiving half of a leadership transfer.
// Non-blocking: with a signal already pending there is nothing to add, since the
// timer only needs to fire once.
func (n *Node) signalTimeoutNow() {
	select {
	case n.timeoutNowCh <- struct{}{}:
	default:
	}
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

func (n *Node) GetCurrentTerm(ctx context.Context) (uint, error) {
	return n.store.GetCurrentTerm(ctx)
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
