package raft

import (
	"sync"

	"github.com/stretchr/testify/mock"
)

type MockTransport struct {
	mock.Mock
}

var _ Transport = &MockTransport{}

func (m *MockTransport) AppendEntries(peerID string, args AppendEntriesArgs) (AppendEntriesResponse, error) {
	ret := m.Called(peerID, args)
	return ret.Get(0).(AppendEntriesResponse), ret.Error(1)
}

func (m *MockTransport) RequestVote(peerID string, args RequestVoteArgs) (RequestVoteResponse, error) {
	ret := m.Called(peerID, args)
	return ret.Get(0).(RequestVoteResponse), ret.Error(1)
}

func NewMockTransport() *MockTransport {
	return &MockTransport{}
}

func NewNodeMock(store Storage, sm StateMachine) *Node {
	node := &Node{
		ID:    "node-1",
		Role:  ServerRole_Follower,
		store: store,
		sm:    sm,
		cfg: Config{
			ID: "node-1",
			Peers: map[string]Peer{
				"node-2": {NextIndex: 1, MatchIndex: 0},
				"node-3": {NextIndex: 1, MatchIndex: 0},
				"node-4": {NextIndex: 1, MatchIndex: 0},
				"node-5": {NextIndex: 1, MatchIndex: 0},
			},
			RPCTimeoutMs:  50,
			HeartbeatMs:   100,
			ElectionMinMs: 1000,
			ElectionMaxMs: 5000,
		},
		electionTimeoutCh: make(chan struct{}, 10),
		LeaderID:          "",
		commitIndex:       0,
	}
	node.commitCond = *sync.NewCond(&node.commitMu)
	return node
}
