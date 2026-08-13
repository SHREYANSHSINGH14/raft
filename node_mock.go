package raft

import (
	"context"

	"github.com/stretchr/testify/mock"
)

type MockTransport struct {
	mock.Mock
}

var _ Transport = &MockTransport{}

func (m *MockTransport) AppendEntries(ctx context.Context, peerID string, args AppendEntriesArgs) (AppendEntriesResponse, error) {
	ret := m.Called(peerID, args)
	return ret.Get(0).(AppendEntriesResponse), ret.Error(1)
}

func (m *MockTransport) RequestVote(ctx context.Context, peerID string, args RequestVoteArgs) (RequestVoteResponse, error) {
	ret := m.Called(peerID, args)
	return ret.Get(0).(RequestVoteResponse), ret.Error(1)
}

func (m *MockTransport) PreVote(ctx context.Context, peerID string, args PreVoteArgs) (PreVoteResponse, error) {
	ret := m.Called(peerID, args)
	return ret.Get(0).(PreVoteResponse), ret.Error(1)
}

func (m *MockTransport) TimeoutNow(ctx context.Context, peerID string, args TimeoutNowArgs) (TimeoutNowResponse, error) {
	ret := m.Called(peerID, args)
	return ret.Get(0).(TimeoutNowResponse), ret.Error(1)
}

func (m *MockTransport) InstallSnapshot(ctx context.Context, peerID string, args InstallSnapshotArgs) (InstallSnapshotResponse, error) {
	ret := m.Called(peerID, args)
	return ret.Get(0).(InstallSnapshotResponse), ret.Error(1)
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
		timeoutNowCh:      make(chan struct{}, 1),
		LeaderID:          "",
		commitIndex:       0,
		catchUpSignal:     make(chan struct{}, 1),
	}
	// Seed the operating configuration from the bootstrap peers, same as NewNode —
	// including this node, since configurations.latest holds the whole membership.
	bootstrap := clonePeers(node.cfg.Peers)
	bootstrap[node.ID] = Peer{PeerState: PeerState_Voter}
	node.configurations = configurations{
		latest:    clonePeers(bootstrap),
		committed: clonePeers(bootstrap),
	}
	node.catchingUpIdx.Store(DefaultCatchingUpIdx)
	// Same buffer as NewNode. A nil commitCh would make every wake-up a send to a
	// nil channel, which blocks forever.
	node.commitCh = make(chan struct{}, 1)
	// Unbuffered and never written to except by close, same as NewNode. A nil
	// fatalCh would make Fatal() block forever instead of simply never firing.
	node.fatalCh = make(chan struct{})
	return node
}
