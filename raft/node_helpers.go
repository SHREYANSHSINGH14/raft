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

func (n *Node) GetPeerIndex(id string) Peer {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.cfg.Peers[id]
}

func (n *Node) SetNextPeerIndex(id string, idx uint) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// map returns a copy of value so if we do
	// n.cfg.Peers[id].NextIndex = idx
	// it won't work coz we change value of copy
	// not the original thing so to change the
	// actual value assign a new struct
	// Better to use pointers if frequent change
	// but for learning we keep it like this
	peer := n.cfg.Peers[id] // copy
	peer.NextIndex = idx    // modify
	n.cfg.Peers[id] = peer  // write back
}

func (n *Node) SetMatchPeerIndex(id string, idx uint) {
	n.mu.Lock()
	defer n.mu.Unlock()

	peer := n.cfg.Peers[id]
	peer.MatchIndex = idx
	n.cfg.Peers[id] = peer
}

func (n *Node) SetPeerState(id string, state PeerState) {
	n.mu.Lock()
	defer n.mu.Unlock()

	peer := n.cfg.Peers[id]
	peer.PeerState = state
	n.cfg.Peers[id] = peer
}

// peerIDs returns a snapshot of peer IDs, safe to range over without racing
// concurrent NextIndex/MatchIndex updates to n.cfg.Peers.
func (n *Node) peerIDs() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	ids := make([]string, 0, len(n.cfg.Peers))
	for id := range n.cfg.Peers {
		ids = append(ids, id)
	}
	return ids
}

// peersSnapshot returns a copy of the peers map, safe to read without racing
// concurrent NextIndex/MatchIndex updates to n.cfg.Peers.
func (n *Node) peersSnapshot() map[string]Peer {
	n.mu.Lock()
	defer n.mu.Unlock()
	cp := make(map[string]Peer, len(n.cfg.Peers))
	for id, peer := range n.cfg.Peers {
		cp[id] = peer
	}
	return cp
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
