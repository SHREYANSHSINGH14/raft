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

func (n *Node) GetPeerIndex(id string) nodeIndexes {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.nodeIdxs[id]
}

func (n *Node) SetNextPeerIndex(id string, idx uint) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// map returns a copy of value so if we do
	// n.nodeIdxs[id].nextIndex = idx
	// it won't work coz we change value of copy
	// not the original thing so to change the
	// actual value assign a new struct
	// Better to use pointers if frequent change
	// but for learning we keep it like this
	peer, ok := n.nodeIdxs[id] // copy
	if !ok {
		peer = nodeIndexes{}
	}
	peer.nextIndex = idx    // modify
	n.nodeIdxs[id] = peer  // write back
}

func (n *Node) SetMatchPeerIndex(id string, idx uint) {
	n.mu.Lock()
	defer n.mu.Unlock()

	peer, ok := n.nodeIdxs[id]
	if !ok {
		peer = nodeIndexes{}
	}
	peer.matchIndex = idx
	n.nodeIdxs[id] = peer
}

func (n *Node) SetCommitIndex(idx uint) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if idx < n.commitIndex {
		return
	}
	n.commitIndex = idx
}

func (n *Node) GetCommitIndex() uint {
	n.mu.Lock()
	defer n.mu.Unlock()

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
