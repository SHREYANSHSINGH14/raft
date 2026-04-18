package raft

import "github.com/SHREYANSHSINGH14/raft/types"

func NewPeerMock(store types.RaftDBInterface) *Peer {
	return &Peer{
		ID:                "node-1",
		Role:              ServerRole_Follower,
		store:             store,
		electionTimeoutCh: make(chan struct{}, 10),
		LeaderID:          "",
		commitIndex:       0,
	}
}
