package raft

import (
	"context"

	"github.com/SHREYANSHSINGH14/raft/types"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
)

type MockRaftRpcClient struct {
	mock.Mock

	types.UnimplementedRaftRpcServer
}

var _ types.RaftRpcClient = &MockRaftRpcClient{}

func (m *MockRaftRpcClient) RequestVote(ctx context.Context, args *types.RequestVoteArgs, opts ...grpc.CallOption) (*types.RequestVoteResponse, error) {
	ret := m.Called(ctx, args)
	return ret.Get(0).(*types.RequestVoteResponse), ret.Error(1)
}

func (m *MockRaftRpcClient) AppendEntries(ctx context.Context, args *types.AppendEntriesArgs, opts ...grpc.CallOption) (*types.AppendEntriesResponse, error) {
	ret := m.Called(ctx, args)
	return ret.Get(0).(*types.AppendEntriesResponse), ret.Error(1)
}

func NewMockRaftRpcClient() *MockRaftRpcClient {
	return &MockRaftRpcClient{}
}

func NewPeerClientMock() map[string]types.RaftRpcClient {
	return map[string]types.RaftRpcClient{
		"node-2": NewMockRaftRpcClient(),
		"node-3": NewMockRaftRpcClient(),
		"node-4": NewMockRaftRpcClient(),
		"node-5": NewMockRaftRpcClient(),
	}
}

func NewPeerMock(store types.RaftDBInterface) *Peer {
	return &Peer{
		ID:                "node-1",
		Role:              ServerRole_Follower,
		store:             store,
		electionTimeoutCh: make(chan struct{}, 10),
		LeaderID:          "",
		commitIndex:       0,
		ServerIDRpcUrlMap: NewPeerClientMock(),
		peerIndexes: map[string]PeerIndexes{
			"node-2": {nextIndex: 1, matchIndex: 0},
			"node-3": {nextIndex: 1, matchIndex: 0},
			"node-4": {nextIndex: 1, matchIndex: 0},
			"node-5": {nextIndex: 1, matchIndex: 0},
		},
	}
}
