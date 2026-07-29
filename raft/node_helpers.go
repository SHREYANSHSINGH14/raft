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

// peerIDs returns a snapshot of peer IDs, safe to range over without racing
// concurrent NextIndex/MatchIndex updates to n.configurations.latest.
func (n *Node) peerIDs() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	ids := make([]string, 0, len(n.configurations.latest))
	for id := range n.configurations.latest {
		ids = append(ids, id)
	}
	return ids
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
