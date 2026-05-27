package raft

import "github.com/stretchr/testify/mock"

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

func NewNodeMock(store Storage) *Node {
	return &Node{
		ID:    "node-1",
		Role:  ServerRole_Follower,
		store: store,
		cfg: Config{
			ID:            "node-1",
			Peers:         []string{"node-2", "node-3", "node-4", "node-5"},
			RPCTimeoutMs:  50,
			HeartbeatMs:   100,
			ElectionMinMs: 1000,
			ElectionMaxMs: 5000,
		},
		electionTimeoutCh: make(chan struct{}, 10),
		LeaderID:          "",
		commitIndex:       0,
		nodeIdxs: map[string]nodeIndexes{
			"node-2": {nextIndex: 1, matchIndex: 0},
			"node-3": {nextIndex: 1, matchIndex: 0},
			"node-4": {nextIndex: 1, matchIndex: 0},
			"node-5": {nextIndex: 1, matchIndex: 0},
		},
	}
}
