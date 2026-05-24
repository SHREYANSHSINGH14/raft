package raft

import "context"

// -------------------------------------------
// Below are some helper functions to get and set server state like role, peer indexes, commit index etc
// These functions are thread safe and should be used whenever we want to read or write these state variables
// -------------------------------------------

func (p *Peer) SetRole(role ServerRole) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Role = role
	return
}

func (p *Peer) GetRole() ServerRole {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.Role
}

func (p *Peer) GetID() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ID
}

func (p *Peer) GetPeerIndex(id string) PeerIndexes {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.peerIndexes[id]
}

func (p *Peer) SetNextPeerIndex(id string, idx uint) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// map returns a copy of value so if we do
	// p.peerIndexes[id].nextIndex = idx
	// it won't work coz we change value of copy
	// not the original thing so to change the
	// actual value assign a new struct
	// Better to use pointers if frequent change
	// but for learning we keep it like this
	peer, ok := p.peerIndexes[id] // copy
	if !ok {
		peer = PeerIndexes{}
	}
	peer.nextIndex = idx     // modify
	p.peerIndexes[id] = peer // write back
}

func (p *Peer) SetMatchPeerIndex(id string, idx uint) {
	p.mu.Lock()
	defer p.mu.Unlock()

	peer, ok := p.peerIndexes[id]
	if !ok {
		peer = PeerIndexes{}
	}
	peer.matchIndex = idx
	p.peerIndexes[id] = peer
}

func (p *Peer) SetCommitIndex(idx uint) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if idx < p.commitIndex {
		return
	}
	p.commitIndex = idx
}

func (p *Peer) GetCommitIndex() uint {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.commitIndex
}

func (p *Peer) SetLeaderID(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.LeaderID = id
}

func (p *Peer) GetLeaderID() string {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.LeaderID
}

func (p *Peer) GetCurrentTerm(ctx context.Context) (uint, error) {
	return p.store.GetCurrentTerm(ctx)
}
