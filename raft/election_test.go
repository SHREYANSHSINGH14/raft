package raft

import (
	"context"
	"errors"
	"testing"

	"github.com/SHREYANSHSINGH14/raft/db"
	"github.com/SHREYANSHSINGH14/raft/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const (
	methodRequestVote = "RequestVote"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func setupElectionTest(t *testing.T) (*Peer, *db.MockKVStore, map[string]*MockRaftRpcClient) {
	store := db.NewMockKVStore()
	store.SetCurrentTerm(context.Background(), 5)

	clients := map[string]*MockRaftRpcClient{
		"node-2": NewMockRaftRpcClient(),
		"node-3": NewMockRaftRpcClient(),
		"node-4": NewMockRaftRpcClient(),
		"node-5": NewMockRaftRpcClient(),
	}

	rpcMap := map[string]types.RaftRpcClient{
		"node-2": clients["node-2"],
		"node-3": clients["node-3"],
		"node-4": clients["node-4"],
		"node-5": clients["node-5"],
	}

	peer := &Peer{
		ID:                "node-1",
		Role:              ServerRole_Candidate,
		store:             store,
		electionTimeoutCh: make(chan struct{}, 10),
		ServerIDRpcUrlMap: rpcMap, // same map as clients
	}

	return peer, store, clients // clients and peer.ServerIDRpcUrlMap point to same mocks
}
func grantVote(term uint64) *types.RequestVoteResponse {
	return &types.RequestVoteResponse{Term: term, VoteGranted: true}
}

func denyVote(term uint64) *types.RequestVoteResponse {
	return &types.RequestVoteResponse{Term: term, VoteGranted: false}
}

func runElection(p *Peer) ElectionResponse {
	resCh := make(chan ElectionResponse, 1)
	p.election(context.Background(), resCh)
	return <-resCh
}

// ── happy paths ───────────────────────────────────────────────────────────────

// 1. All peers grant vote → Leader
func TestElection_AllVotesGranted_BecomesLeader(t *testing.T) {
	peer, _, clients := setupElectionTest(t)

	for _, c := range clients {
		c.On(methodRequestVote, mock.Anything, mock.Anything).Return(grantVote(6), nil)
	}

	res := runElection(peer)

	assert.NoError(t, res.err)
	assert.Equal(t, ServerRole_Leader, res.transitonRole)
}

// 2. Exactly majority (3 out of 4 peers = majority of 5 node cluster) → Leader
func TestElection_ExactMajority_BecomesLeader(t *testing.T) {
	peer, _, clients := setupElectionTest(t)

	ids := []string{"node-2", "node-3", "node-4", "node-5"}
	for i, id := range ids {
		if i < 2 { // 2 peers grant + self vote = 3 = majority of 5
			clients[id].On(methodRequestVote, mock.Anything, mock.Anything).Return(grantVote(6), nil)
		} else {
			clients[id].On(methodRequestVote, mock.Anything, mock.Anything).Return(denyVote(6), nil)
		}
	}

	res := runElection(peer)

	assert.NoError(t, res.err)
	assert.Equal(t, ServerRole_Leader, res.transitonRole)
}

// ── follower transitions ──────────────────────────────────────────────────────

// 3. Majority votes no → stays Candidate
func TestElection_MajorityDenied_StaysCandidate(t *testing.T) {
	peer, _, clients := setupElectionTest(t)

	for _, c := range clients {
		c.On(methodRequestVote, mock.Anything, mock.Anything).Return(denyVote(6), nil)
	}

	res := runElection(peer)

	assert.NoError(t, res.err)
	assert.Equal(t, ServerRole_Candidate, res.transitonRole)
}

// 4. One peer responds with higher term → Follower
func TestElection_PeerHasHigherTerm_BecomesFollower(t *testing.T) {
	peer, _, clients := setupElectionTest(t)

	// one peer has seen a higher term
	clients["node-2"].On(methodRequestVote, mock.Anything, mock.Anything).Return(denyVote(10), nil)
	clients["node-3"].On(methodRequestVote, mock.Anything, mock.Anything).Return(grantVote(6), nil)
	clients["node-4"].On(methodRequestVote, mock.Anything, mock.Anything).Return(grantVote(6), nil)
	clients["node-5"].On(methodRequestVote, mock.Anything, mock.Anything).Return(grantVote(6), nil)

	res := runElection(peer)

	assert.NoError(t, res.err)
	assert.Equal(t, ServerRole_Follower, res.transitonRole)
}

// 5. All peers timeout/fail → no majority → stays Candidate
func TestElection_AllPeersFail_StaysCandidate(t *testing.T) {
	peer, _, clients := setupElectionTest(t)

	for _, c := range clients {
		c.On(methodRequestVote, mock.Anything, mock.Anything).Return(
			(*types.RequestVoteResponse)(nil), errors.New("connection refused"),
		)
	}

	res := runElection(peer)

	assert.NoError(t, res.err) // election itself didn't error — just got no votes
	assert.Equal(t, ServerRole_Candidate, res.transitonRole)
}

// 6. Mixed — some yes, some no, some error → no majority → stays Candidate
func TestElection_Mixed_NoMajority_StaysCandidate(t *testing.T) {
	peer, _, clients := setupElectionTest(t)

	clients["node-2"].On(methodRequestVote, mock.Anything, mock.Anything).Return(grantVote(6), nil)
	clients["node-3"].On(methodRequestVote, mock.Anything, mock.Anything).Return(denyVote(6), nil)
	clients["node-4"].On(methodRequestVote, mock.Anything, mock.Anything).Return(
		(*types.RequestVoteResponse)(nil), errors.New("timeout"),
	)
	clients["node-5"].On(methodRequestVote, mock.Anything, mock.Anything).Return(denyVote(6), nil)

	res := runElection(peer)

	assert.NoError(t, res.err)
	assert.Equal(t, ServerRole_Candidate, res.transitonRole)
}

// ── pre-condition failures ────────────────────────────────────────────────────

// 7. Node is not Candidate → returns error with current role
func TestElection_NotCandidate_ReturnsError(t *testing.T) {
	peer, _, _ := setupElectionTest(t)
	peer.Role = ServerRole_Follower // override to non-candidate

	res := runElection(peer)

	assert.Error(t, res.err)
	assert.Equal(t, ServerRole_Follower, res.transitonRole)
}

// ── DB error cases ────────────────────────────────────────────────────────────

// 8. GetCurrentTerm fails
func TestElection_DBErr_GetCurrentTerm(t *testing.T) {
	store := db.NewMockKVStore() // empty — GetCurrentTerm returns 0 not error by default
	// use testify mock instead to inject error
	mockStore := new(db.MockStore)
	mockStore.On("GetCurrentTerm", mock.Anything).Return(uint(0), errors.New("db error"))

	peer := &Peer{
		ID:                "node-1",
		Role:              ServerRole_Candidate,
		store:             mockStore,
		electionTimeoutCh: make(chan struct{}, 10),
		ServerIDRpcUrlMap: map[string]types.RaftRpcClient{},
	}

	_ = store
	res := runElection(peer)

	assert.Error(t, res.err)
}

// 9. SetCurrentTerm fails
func TestElection_DBErr_SetCurrentTerm(t *testing.T) {
	mockStore := new(db.MockStore)
	mockStore.On("GetCurrentTerm", mock.Anything).Return(uint(5), nil)
	mockStore.On("SetCurrentTerm", mock.Anything, uint(6)).Return(errors.New("db error"))

	peer := &Peer{
		ID:                "node-1",
		Role:              ServerRole_Candidate,
		store:             mockStore,
		electionTimeoutCh: make(chan struct{}, 10),
		ServerIDRpcUrlMap: map[string]types.RaftRpcClient{},
	}

	res := runElection(peer)

	assert.Error(t, res.err)
}

// 10. SetVotedFor fails
func TestElection_DBErr_SetVotedFor(t *testing.T) {
	mockStore := new(db.MockStore)
	mockStore.On("GetCurrentTerm", mock.Anything).Return(uint(5), nil)
	mockStore.On("SetCurrentTerm", mock.Anything, uint(6)).Return(nil)
	mockStore.On("SetVotedFor", mock.Anything, "node-1").Return(errors.New("db error"))

	peer := &Peer{
		ID:                "node-1",
		Role:              ServerRole_Candidate,
		store:             mockStore,
		electionTimeoutCh: make(chan struct{}, 10),
		ServerIDRpcUrlMap: map[string]types.RaftRpcClient{},
	}

	res := runElection(peer)

	assert.Error(t, res.err)
}

// 11. GetLastLogIndex fails
func TestElection_DBErr_GetLastLogIndex(t *testing.T) {
	mockStore := new(db.MockStore)
	mockStore.On("GetCurrentTerm", mock.Anything).Return(uint(5), nil)
	mockStore.On("SetCurrentTerm", mock.Anything, uint(6)).Return(nil)
	mockStore.On("SetVotedFor", mock.Anything, "node-1").Return(nil)
	mockStore.On("GetLastLogIndex", mock.Anything).Return(uint(0), errors.New("db error"))

	peer := &Peer{
		ID:                "node-1",
		Role:              ServerRole_Candidate,
		store:             mockStore,
		electionTimeoutCh: make(chan struct{}, 10),
		ServerIDRpcUrlMap: map[string]types.RaftRpcClient{},
	}

	res := runElection(peer)

	assert.Error(t, res.err)
}

// 12. GetLogByIndex fails (when lastLogIndex > 0)
func TestElection_DBErr_GetLogByIndex(t *testing.T) {
	mockStore := new(db.MockStore)
	mockStore.On("GetCurrentTerm", mock.Anything).Return(uint(5), nil)
	mockStore.On("SetCurrentTerm", mock.Anything, uint(6)).Return(nil)
	mockStore.On("SetVotedFor", mock.Anything, "node-1").Return(nil)
	mockStore.On("GetLastLogIndex", mock.Anything).Return(uint(3), nil) // has logs
	mockStore.On("GetLogByIndex", mock.Anything, uint(3)).Return(nil, errors.New("db error"))

	peer := &Peer{
		ID:                "node-1",
		Role:              ServerRole_Candidate,
		store:             mockStore,
		electionTimeoutCh: make(chan struct{}, 10),
		ServerIDRpcUrlMap: map[string]types.RaftRpcClient{},
	}

	res := runElection(peer)

	assert.Error(t, res.err)
}

// ── log state cases ───────────────────────────────────────────────────────────

// 13. No logs → RequestVote sent with lastLogTerm=0 lastLogIndex=0
func TestElection_NoLogs_SendsZeroLogInfo(t *testing.T) {
	peer, _, clients := setupElectionTest(t)
	// store has no logs by default

	for _, c := range clients {
		c.On(methodRequestVote, mock.Anything, mock.MatchedBy(func(req *types.RequestVoteArgs) bool {
			return req.LastLogTerm == 0 && req.LastLogIndex == 0
		})).Return(grantVote(1), nil)
	}

	res := runElection(peer)

	assert.NoError(t, res.err)
	assert.Equal(t, ServerRole_Leader, res.transitonRole)
	for _, c := range clients {
		c.AssertExpectations(t)
	}
}

// 14. Has logs → RequestVote sent with correct lastLogTerm and lastLogIndex
func TestElection_HasLogs_SendsCorrectLogInfo(t *testing.T) {
	peer, store, clients := setupElectionTest(t)

	store.AppendLogs(context.Background(), []*types.LogEntry{
		{Index: 1, Term: 3},
		{Index: 2, Term: 4},
		{Index: 3, Term: 5},
	})

	for _, c := range clients {
		c.On(methodRequestVote, mock.Anything, mock.MatchedBy(func(req *types.RequestVoteArgs) bool {
			return req.LastLogIndex == 3 && req.LastLogTerm == 5
		})).Return(grantVote(6), nil)
	}

	res := runElection(peer)

	assert.NoError(t, res.err)
	assert.Equal(t, ServerRole_Leader, res.transitonRole)
	for _, c := range clients {
		c.AssertExpectations(t)
	}
}

// 15. Term is correctly incremented before sending RequestVote
func TestElection_TermIncrementedBeforeVote(t *testing.T) {
	peer, store, clients := setupElectionTest(t)
	// current term is 5, new term should be 6

	for _, c := range clients {
		c.On(methodRequestVote, mock.Anything, mock.MatchedBy(func(req *types.RequestVoteArgs) bool {
			return req.Term == 6 // must be incremented
		})).Return(grantVote(6), nil)
	}

	res := runElection(peer)

	assert.NoError(t, res.err)
	assert.Equal(t, ServerRole_Leader, res.transitonRole)

	// verify term was persisted
	finalTerm, err := store.GetCurrentTerm(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, uint(6), finalTerm)

	for _, c := range clients {
		c.AssertExpectations(t)
	}
}

// 16. VotedFor set to self before sending RequestVote
func TestElection_VotedForSelfBeforeVote(t *testing.T) {
	peer, store, clients := setupElectionTest(t)

	for _, c := range clients {
		c.On(methodRequestVote, mock.Anything, mock.Anything).Return(denyVote(6), nil)
	}

	runElection(peer)

	votedFor, err := store.GetVotedFor(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "node-1", votedFor) // voted for itself
}
