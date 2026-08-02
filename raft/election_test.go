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
	methodPreVote     = "PreVote"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func setupElectionTest(t *testing.T) (*Node, *MemStorage, *MockTransport) {
	store := NewMemStorage()
	store.SetCurrentTerm(context.Background(), 5)

	transport := NewMockTransport()

	// Voters — election only counts and requests votes from PeerState_Voter peers,
	// so the operating configuration (configurations.latest) must mark them as such.
	voters := map[string]Peer{
		"node-2": {PeerState: PeerState_Voter},
		"node-3": {PeerState: PeerState_Voter},
		"node-4": {PeerState: PeerState_Voter},
		"node-5": {PeerState: PeerState_Voter},
	}

	node := &Node{
		ID:    "node-1",
		Role:  ServerRole_Candidate,
		store: store,
		cfg: Config{
			ID:    "node-1",
			Peers: voters,
		},
		configurations: configurations{
			latest:    clonePeers(voters),
			committed: clonePeers(voters),
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

func grantPreVote(term uint64) PreVoteResponse {
	return PreVoteResponse{Term: term, VoteGranted: true}
}

func denyPreVote(term uint64) PreVoteResponse {
	return PreVoteResponse{Term: term, VoteGranted: false}
}

// passPreVote stubs the pre-vote round as unanimously granted. Every election
// that means to exercise the real vote has to get through the pre-vote gate
// first, so tests focused on the second round call this to open the first.
func passPreVote(transport *MockTransport) {
	transport.On(methodPreVote, mock.Anything, mock.Anything).Return(grantPreVote(6), nil)
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
	passPreVote(transport)

	transport.On(methodRequestVote, mock.Anything, mock.Anything).Return(grantVote(6), nil)

	res := runElection(node)

	assert.NoError(t, res.err)
	assert.Equal(t, ServerRole_Leader, res.transitonRole)
}

// 2. Exactly majority (3 out of 4 peers = majority of 5 node cluster) → Leader
func TestElection_ExactMajority_BecomesLeader(t *testing.T) {
	node, _, transport := setupElectionTest(t)
	passPreVote(transport)

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
	passPreVote(transport)

	transport.On(methodRequestVote, mock.Anything, mock.Anything).Return(denyVote(6), nil)

	res := runElection(node)

	assert.NoError(t, res.err)
	assert.Equal(t, ServerRole_Follower, res.transitonRole)
}

// 4. One peer responds with higher term → Follower
func TestElection_PeerHasHigherTerm_BecomesFollower(t *testing.T) {
	node, _, transport := setupElectionTest(t)
	passPreVote(transport)

	transport.On(methodRequestVote, "node-2", mock.Anything).Return(denyVote(10), nil)
	transport.On(methodRequestVote, mock.Anything, mock.Anything).Return(grantVote(6), nil)

	res := runElection(node)

	assert.NoError(t, res.err)
	assert.Equal(t, ServerRole_Follower, res.transitonRole)
}

// 5. All peers timeout/fail → no majority → Follower
func TestElection_AllPeersFail_BecomesFollower(t *testing.T) {
	node, _, transport := setupElectionTest(t)
	passPreVote(transport)

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
	passPreVote(transport)

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
		cfg:               Config{Peers: map[string]Peer{}},
	}

	res := runElection(node)

	assert.Error(t, res.err)
}

// 9. SetCurrentTerm fails
func TestElection_DBErr_SetCurrentTerm(t *testing.T) {
	mockStore := new(MockStorage)
	mockStore.On("GetCurrentTerm", mock.Anything).Return(uint(5), nil)
	// The log state is read before the term bump now, because the pre-vote round
	// needs it. With no voting peers the pre-vote passes on our own vote alone.
	mockStore.On("GetLastLogIndex", mock.Anything).Return(uint(0), nil)
	mockStore.On("SetCurrentTerm", mock.Anything, uint(6)).Return(errors.New("db error"))

	node := &Node{
		ID:                "node-1",
		Role:              ServerRole_Candidate,
		store:             mockStore,
		electionTimeoutCh: make(chan struct{}, 10),
		cfg:               Config{Peers: map[string]Peer{}},
	}

	res := runElection(node)

	assert.Error(t, res.err)
}

// 10. SetVotedFor fails
func TestElection_DBErr_SetVotedFor(t *testing.T) {
	mockStore := new(MockStorage)
	mockStore.On("GetCurrentTerm", mock.Anything).Return(uint(5), nil)
	mockStore.On("GetLastLogIndex", mock.Anything).Return(uint(0), nil)
	mockStore.On("SetCurrentTerm", mock.Anything, uint(6)).Return(nil)
	mockStore.On("SetVotedFor", mock.Anything, "node-1").Return(errors.New("db error"))

	node := &Node{
		ID:                "node-1",
		Role:              ServerRole_Candidate,
		store:             mockStore,
		electionTimeoutCh: make(chan struct{}, 10),
		cfg:               Config{Peers: map[string]Peer{}},
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
		cfg:               Config{Peers: map[string]Peer{}},
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
		cfg:               Config{Peers: map[string]Peer{}},
	}

	res := runElection(node)

	assert.Error(t, res.err)
}

// ── log state cases ───────────────────────────────────────────────────────────

// 13. No logs → RequestVote sent with lastLogTerm=0 lastLogIndex=0
func TestElection_NoLogs_SendsZeroLogInfo(t *testing.T) {
	node, _, transport := setupElectionTest(t)
	passPreVote(transport)
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
	passPreVote(transport)

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
	passPreVote(transport)
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
	passPreVote(transport)

	transport.On(methodRequestVote, mock.Anything, mock.Anything).Return(denyVote(6), nil)

	runElection(node)

	votedFor, err := store.GetVotedFor(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "node-1", votedFor) // voted for itself
}

// ── pre-vote gate ─────────────────────────────────────────────────────────────
//
// The point of the round is that losing it is free. These tests assert the
// "free" part as hard as the "losing" part: a denied pre-vote must leave the
// persisted term untouched, spend no vote, and put no RequestVote on the wire.
// A partitioned node looping through elections is exactly the case, and every
// one of those three would disrupt the cluster when it rejoined.

// 17. Majority denies the pre-vote → Follower, and nothing was spent
func TestElection_PreVoteDenied_BecomesFollowerWithoutBumpingTerm(t *testing.T) {
	node, store, transport := setupElectionTest(t)
	ctx := context.Background()

	transport.On(methodPreVote, mock.Anything, mock.Anything).Return(denyPreVote(6), nil)

	res := runElection(node)

	assert.NoError(t, res.err) // losing a pre-vote is a normal outcome, not an error
	assert.Equal(t, ServerRole_Follower, res.transitonRole)

	term, err := store.GetCurrentTerm(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint(5), term, "a denied pre-vote must not raise the persisted term")

	votedFor, err := store.GetVotedFor(ctx)
	assert.NoError(t, err)
	assert.Empty(t, votedFor, "a denied pre-vote must not spend our vote")

	transport.AssertNotCalled(t, methodRequestVote, mock.Anything, mock.Anything)
	transport.AssertExpectations(t)
}

// 18. Exactly a majority grants the pre-vote → proceed to the real vote
func TestElection_PreVoteExactMajority_ProceedsToRealVote(t *testing.T) {
	node, store, transport := setupElectionTest(t)

	// 5-node cluster: self + 2 peers is the majority of 3.
	transport.On(methodPreVote, "node-2", mock.Anything).Return(grantPreVote(6), nil)
	transport.On(methodPreVote, "node-3", mock.Anything).Return(grantPreVote(6), nil)
	transport.On(methodPreVote, "node-4", mock.Anything).Return(denyPreVote(6), nil)
	transport.On(methodPreVote, "node-5", mock.Anything).Return(denyPreVote(6), nil)
	transport.On(methodRequestVote, mock.Anything, mock.Anything).Return(grantVote(6), nil)

	res := runElection(node)

	assert.NoError(t, res.err)
	assert.Equal(t, ServerRole_Leader, res.transitonRole)

	term, err := store.GetCurrentTerm(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, uint(6), term, "a won pre-vote does let the real election bump the term")
}

// 19. A peer answering beyond the probed term stops us — and we do NOT adopt it.
// Following a pre-vote response would reintroduce exactly the disruption the
// round exists to prevent; the real term arrives via the next AppendEntries.
func TestElection_PreVotePeerHasHigherTerm_StopsWithoutAdoptingIt(t *testing.T) {
	node, store, transport := setupElectionTest(t)

	// Every peer answers beyond the probed term. A single higher-term peer among
	// granting ones would be racy on purpose: the round can reach a majority and
	// return early before that response lands, which is acceptable — the real
	// election's own higher-term check still catches it.
	transport.On(methodPreVote, mock.Anything, mock.Anything).Return(grantPreVote(50), nil)

	res := runElection(node)

	assert.NoError(t, res.err)
	assert.Equal(t, ServerRole_Follower, res.transitonRole)

	term, err := store.GetCurrentTerm(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, uint(5), term, "term 50 was observed, not adopted")

	transport.AssertNotCalled(t, methodRequestVote, mock.Anything, mock.Anything)
}

// 20. Every peer unreachable → no majority → Follower. An unreachable peer is a
// withheld pre-vote, not a failure: the election itself did not error.
func TestElection_PreVoteAllPeersFail_BecomesFollower(t *testing.T) {
	node, store, transport := setupElectionTest(t)

	transport.On(methodPreVote, mock.Anything, mock.Anything).Return(
		PreVoteResponse{}, errors.New("connection refused"),
	)

	res := runElection(node)

	assert.NoError(t, res.err)
	assert.Equal(t, ServerRole_Follower, res.transitonRole)

	term, err := store.GetCurrentTerm(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, uint(5), term)

	transport.AssertNotCalled(t, methodRequestVote, mock.Anything, mock.Anything)
}

// 21. The probe carries currentTerm+1 — the term we would run in, not the one we
// are in — plus our real log state, which is the same pair the real vote sends.
func TestElection_PreVoteArgs_CarryNextTermAndLogState(t *testing.T) {
	node, store, transport := setupElectionTest(t)

	store.AppendLogs(context.Background(), []LogEntry{
		{Index: 1, Term: 3},
		{Index: 2, Term: 4},
	})

	transport.On(methodPreVote, mock.Anything, mock.MatchedBy(func(req PreVoteArgs) bool {
		return req.Term == 6 && // currentTerm 5 + 1
			req.LastLogIndex == 2 &&
			req.LastLogTerm == 4 &&
			req.CandidateID == "node-1"
	})).Return(grantPreVote(6), nil)
	transport.On(methodRequestVote, mock.Anything, mock.Anything).Return(grantVote(6), nil)

	res := runElection(node)

	assert.NoError(t, res.err)
	assert.Equal(t, ServerRole_Leader, res.transitonRole)
	transport.AssertExpectations(t)
}

// 22. No voting peers → the pre-vote passes on our own vote alone and nothing is
// sent, matching how the real election treats a cluster of one.
func TestElection_PreVoteNoVotingPeers_PassesWithoutRPCs(t *testing.T) {
	node, _, transport := setupElectionTest(t)
	node.configurations = configurations{
		latest:    map[string]Peer{},
		committed: map[string]Peer{},
	}

	res := runElection(node)

	assert.NoError(t, res.err)
	assert.Equal(t, ServerRole_Leader, res.transitonRole)
	transport.AssertNotCalled(t, methodPreVote, mock.Anything, mock.Anything)
	transport.AssertNotCalled(t, methodRequestVote, mock.Anything, mock.Anything)
}
