package raft

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const (
	methodRequestVote = "RequestVote"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func setupElectionTest(t *testing.T) (*Node, *MemStorage, *MockTransport) {
	store := NewMemStorage()
	store.SetCurrentTerm(context.Background(), 5)

	transport := NewMockTransport()

	node := &Node{
		ID:    "node-1",
		Role:  ServerRole_Candidate,
		store: store,
		cfg: Config{
			ID:    "node-1",
			Peers: []string{"node-2", "node-3", "node-4", "node-5"},
		},
		electionTimeoutCh: make(chan struct{}, 10),
		transport:         transport,
	}

	return node, store, transport
}

func grantVote(term uint64) RequestVoteResponse {
	return RequestVoteResponse{Term: term, VoteGranted: true}
}

func denyVote(term uint64) RequestVoteResponse {
	return RequestVoteResponse{Term: term, VoteGranted: false}
}

func runElection(n *Node) ElectionResponse {
	resCh := make(chan ElectionResponse, 1)
	n.election(context.Background(), resCh)
	return <-resCh
}

// ── happy paths ───────────────────────────────────────────────────────────────

// 1. All peers grant vote → Leader
func TestElection_AllVotesGranted_BecomesLeader(t *testing.T) {
	node, _, transport := setupElectionTest(t)

	transport.On(methodRequestVote, mock.Anything, mock.Anything).Return(grantVote(6), nil)

	res := runElection(node)

	assert.NoError(t, res.err)
	assert.Equal(t, ServerRole_Leader, res.transitonRole)
}

// 2. Exactly majority (3 out of 4 peers = majority of 5 node cluster) → Leader
func TestElection_ExactMajority_BecomesLeader(t *testing.T) {
	node, _, transport := setupElectionTest(t)

	transport.On(methodRequestVote, "node-2", mock.Anything).Return(grantVote(6), nil)
	transport.On(methodRequestVote, "node-3", mock.Anything).Return(grantVote(6), nil)
	transport.On(methodRequestVote, "node-4", mock.Anything).Return(denyVote(6), nil)
	transport.On(methodRequestVote, "node-5", mock.Anything).Return(denyVote(6), nil)

	res := runElection(node)

	assert.NoError(t, res.err)
	assert.Equal(t, ServerRole_Leader, res.transitonRole)
}

// ── follower transitions ──────────────────────────────────────────────────────

// 3. Majority votes no → Follower
func TestElection_MajorityDenied_BecomesFollower(t *testing.T) {
	node, _, transport := setupElectionTest(t)

	transport.On(methodRequestVote, mock.Anything, mock.Anything).Return(denyVote(6), nil)

	res := runElection(node)

	assert.NoError(t, res.err)
	assert.Equal(t, ServerRole_Follower, res.transitonRole)
}

// 4. One peer responds with higher term → Follower
func TestElection_PeerHasHigherTerm_BecomesFollower(t *testing.T) {
	node, _, transport := setupElectionTest(t)

	transport.On(methodRequestVote, "node-2", mock.Anything).Return(denyVote(10), nil)
	transport.On(methodRequestVote, mock.Anything, mock.Anything).Return(grantVote(6), nil)

	res := runElection(node)

	assert.NoError(t, res.err)
	assert.Equal(t, ServerRole_Follower, res.transitonRole)
}

// 5. All peers timeout/fail → no majority → Follower
func TestElection_AllPeersFail_BecomesFollower(t *testing.T) {
	node, _, transport := setupElectionTest(t)

	transport.On(methodRequestVote, mock.Anything, mock.Anything).Return(
		RequestVoteResponse{}, errors.New("connection refused"),
	)

	res := runElection(node)

	assert.NoError(t, res.err) // election itself didn't error — just got no votes
	assert.Equal(t, ServerRole_Follower, res.transitonRole)
}

// 6. Mixed — some yes, some no, some error → no majority → Follower
func TestElection_Mixed_NoMajority_BecomesFollower(t *testing.T) {
	node, _, transport := setupElectionTest(t)

	transport.On(methodRequestVote, "node-2", mock.Anything).Return(grantVote(6), nil)
	transport.On(methodRequestVote, "node-3", mock.Anything).Return(denyVote(6), nil)
	transport.On(methodRequestVote, "node-4", mock.Anything).Return(
		RequestVoteResponse{}, errors.New("timeout"),
	)
	transport.On(methodRequestVote, "node-5", mock.Anything).Return(denyVote(6), nil)

	res := runElection(node)

	assert.NoError(t, res.err)
	assert.Equal(t, ServerRole_Follower, res.transitonRole)
}

// ── pre-condition failures ────────────────────────────────────────────────────

// 7. Node is not Candidate → returns error with current role
func TestElection_NotCandidate_ReturnsError(t *testing.T) {
	node, _, _ := setupElectionTest(t)
	node.Role = ServerRole_Follower // override to non-candidate

	res := runElection(node)

	assert.Error(t, res.err)
	assert.Equal(t, ServerRole_Follower, res.transitonRole)
}

// ── DB error cases ────────────────────────────────────────────────────────────

// 8. GetCurrentTerm fails
func TestElection_DBErr_GetCurrentTerm(t *testing.T) {
	mockStore := new(MockStorage)
	mockStore.On("GetCurrentTerm", mock.Anything).Return(uint(0), errors.New("db error"))

	node := &Node{
		ID:                "node-1",
		Role:              ServerRole_Candidate,
		store:             mockStore,
		electionTimeoutCh: make(chan struct{}, 10),
		cfg:               Config{Peers: []string{}},
	}

	res := runElection(node)

	assert.Error(t, res.err)
}

// 9. SetCurrentTerm fails
func TestElection_DBErr_SetCurrentTerm(t *testing.T) {
	mockStore := new(MockStorage)
	mockStore.On("GetCurrentTerm", mock.Anything).Return(uint(5), nil)
	mockStore.On("SetCurrentTerm", mock.Anything, uint(6)).Return(errors.New("db error"))

	node := &Node{
		ID:                "node-1",
		Role:              ServerRole_Candidate,
		store:             mockStore,
		electionTimeoutCh: make(chan struct{}, 10),
		cfg:               Config{Peers: []string{}},
	}

	res := runElection(node)

	assert.Error(t, res.err)
}

// 10. SetVotedFor fails
func TestElection_DBErr_SetVotedFor(t *testing.T) {
	mockStore := new(MockStorage)
	mockStore.On("GetCurrentTerm", mock.Anything).Return(uint(5), nil)
	mockStore.On("SetCurrentTerm", mock.Anything, uint(6)).Return(nil)
	mockStore.On("SetVotedFor", mock.Anything, "node-1").Return(errors.New("db error"))

	node := &Node{
		ID:                "node-1",
		Role:              ServerRole_Candidate,
		store:             mockStore,
		electionTimeoutCh: make(chan struct{}, 10),
		cfg:               Config{Peers: []string{}},
	}

	res := runElection(node)

	assert.Error(t, res.err)
}

// 11. GetLastLogIndex fails
func TestElection_DBErr_GetLastLogIndex(t *testing.T) {
	mockStore := new(MockStorage)
	mockStore.On("GetCurrentTerm", mock.Anything).Return(uint(5), nil)
	mockStore.On("SetCurrentTerm", mock.Anything, uint(6)).Return(nil)
	mockStore.On("SetVotedFor", mock.Anything, "node-1").Return(nil)
	mockStore.On("GetLastLogIndex", mock.Anything).Return(uint(0), errors.New("db error"))

	node := &Node{
		ID:                "node-1",
		Role:              ServerRole_Candidate,
		store:             mockStore,
		electionTimeoutCh: make(chan struct{}, 10),
		cfg:               Config{Peers: []string{}},
	}

	res := runElection(node)

	assert.Error(t, res.err)
}

// 12. GetLogByIndex fails (when lastLogIndex > 0)
func TestElection_DBErr_GetLogByIndex(t *testing.T) {
	mockStore := new(MockStorage)
	mockStore.On("GetCurrentTerm", mock.Anything).Return(uint(5), nil)
	mockStore.On("SetCurrentTerm", mock.Anything, uint(6)).Return(nil)
	mockStore.On("SetVotedFor", mock.Anything, "node-1").Return(nil)
	mockStore.On("GetLastLogIndex", mock.Anything).Return(uint(3), nil) // has logs
	mockStore.On("GetLogByIndex", mock.Anything, uint(3)).Return(LogEntry{}, errors.New("db error"))

	node := &Node{
		ID:                "node-1",
		Role:              ServerRole_Candidate,
		store:             mockStore,
		electionTimeoutCh: make(chan struct{}, 10),
		cfg:               Config{Peers: []string{}},
	}

	res := runElection(node)

	assert.Error(t, res.err)
}

// ── log state cases ───────────────────────────────────────────────────────────

// 13. No logs → RequestVote sent with lastLogTerm=0 lastLogIndex=0
func TestElection_NoLogs_SendsZeroLogInfo(t *testing.T) {
	node, _, transport := setupElectionTest(t)
	// store has no logs by default

	transport.On(methodRequestVote, mock.Anything, mock.MatchedBy(func(req RequestVoteArgs) bool {
		return req.LastLogTerm == 0 && req.LastLogIndex == 0
	})).Return(grantVote(1), nil)

	res := runElection(node)

	assert.NoError(t, res.err)
	assert.Equal(t, ServerRole_Leader, res.transitonRole)
	transport.AssertExpectations(t)
}

// 14. Has logs → RequestVote sent with correct lastLogTerm and lastLogIndex
func TestElection_HasLogs_SendsCorrectLogInfo(t *testing.T) {
	node, store, transport := setupElectionTest(t)

	store.AppendLogs(context.Background(), []LogEntry{
		{Index: 1, Term: 3},
		{Index: 2, Term: 4},
		{Index: 3, Term: 5},
	})

	transport.On(methodRequestVote, mock.Anything, mock.MatchedBy(func(req RequestVoteArgs) bool {
		return req.LastLogIndex == 3 && req.LastLogTerm == 5
	})).Return(grantVote(6), nil)

	res := runElection(node)

	assert.NoError(t, res.err)
	assert.Equal(t, ServerRole_Leader, res.transitonRole)
	transport.AssertExpectations(t)
}

// 15. Term is correctly incremented before sending RequestVote
func TestElection_TermIncrementedBeforeVote(t *testing.T) {
	node, store, transport := setupElectionTest(t)
	// current term is 5, new term should be 6

	transport.On(methodRequestVote, mock.Anything, mock.MatchedBy(func(req RequestVoteArgs) bool {
		return req.Term == 6 // must be incremented
	})).Return(grantVote(6), nil)

	res := runElection(node)

	assert.NoError(t, res.err)
	assert.Equal(t, ServerRole_Leader, res.transitonRole)

	// verify term was persisted
	finalTerm, err := store.GetCurrentTerm(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, uint(6), finalTerm)

	transport.AssertExpectations(t)
}

// 16. VotedFor set to self before sending RequestVote
func TestElection_VotedForSelfBeforeVote(t *testing.T) {
	node, store, transport := setupElectionTest(t)

	transport.On(methodRequestVote, mock.Anything, mock.Anything).Return(denyVote(6), nil)

	runElection(node)

	votedFor, err := store.GetVotedFor(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "node-1", votedFor) // voted for itself
}
