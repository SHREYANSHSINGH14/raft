package raft

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// preVoteNode builds a node whose live operating configuration holds exactly the
// peers in states. NewNodeMock seeds its peers with the zero PeerState (Unknown),
// which HandlePreVote reads as "not a voter" — so every pre-vote test has to state
// the membership it means to exercise rather than inherit the mock's.
//
// It writes configurations directly (both views) because that, not cfg.Peers, is
// what the handler reads.
func preVoteNode(store Storage, states map[string]PeerState) *Node {
	node := NewNodeMock(store, nil)

	peers := make(map[string]Peer, len(states))
	for id, state := range states {
		peers[id] = Peer{PeerState: state}
	}
	node.configurations = configurations{
		latest:    peers,
		committed: clonePeers(peers),
	}
	return node
}

// twoVoters is the ordinary cluster shape for these tests: the candidate under
// test plus one more voter.
func twoVoters() map[string]PeerState {
	return map[string]PeerState{
		"node-2": PeerState_Voter,
		"node-3": PeerState_Voter,
	}
}

// assertNoSideEffects is the invariant that separates HandlePreVote from
// HandleRequestVote: a probe must leave no trace. Nothing persisted, no election
// timer reset. Every grant/reject test ends here.
func assertNoSideEffects(t *testing.T, node *Node, store *MockStorage) {
	t.Helper()
	store.AssertNotCalled(t, methodSetCurrentTerm, mock.Anything, mock.Anything)
	store.AssertNotCalled(t, methodSetVotedFor, mock.Anything, mock.Anything)
	store.AssertNotCalled(t, methodGetVotedFor, mock.Anything)
	assert.Empty(t, node.electionTimeoutCh, "pre-vote must not reset the election timer")
}

// ── 1. Empty candidate ID ─────────────────────────────────────────────────────

func TestPreVote_EmptyCandidateID(t *testing.T) {
	store := new(MockStorage)
	node := preVoteNode(store, twoVoters())

	_, err := node.HandlePreVote(context.Background(), PreVoteArgs{
		CandidateID: "   ",
		Term:        5,
	})

	assert.Error(t, err)
	store.AssertExpectations(t) // no DB calls should have been made
}

// ── 2. Happy path: known voter, no leader, empty log → grant ──────────────────

func TestPreVote_Grant(t *testing.T) {
	store := new(MockStorage)
	node := preVoteNode(store, twoVoters())

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(4), nil)
	store.On(methodGetLastLogEntry, mock.Anything).Return(LogEntry{}, nil)

	resp, err := node.HandlePreVote(context.Background(), PreVoteArgs{
		CandidateID: "node-2",
		Term:        5,
	})

	assert.NoError(t, err)
	assert.True(t, resp.VoteGranted)
	store.AssertExpectations(t)
	assertNoSideEffects(t, node, store)
}

// ── 3. Membership: candidate not in the latest configuration → reject ─────────

func TestPreVote_CandidateNotInConfiguration(t *testing.T) {
	store := new(MockStorage)
	node := preVoteNode(store, twoVoters())

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(4), nil)

	resp, err := node.HandlePreVote(context.Background(), PreVoteArgs{
		CandidateID: "stranger",
		Term:        5,
	})

	assert.NoError(t, err)
	assert.False(t, resp.VoteGranted)
	// The log is never consulted — membership decided it.
	store.AssertNotCalled(t, methodGetLastLogEntry, mock.Anything)
	store.AssertExpectations(t)
	assertNoSideEffects(t, node, store)
}

// ── 4. Bootstrap: empty configuration → both membership checks skipped ────────
//
// A node that has no configuration yet cannot recognise anyone, so membership
// must not be a reason to refuse — otherwise a fresh cluster could never elect
// its first leader through pre-vote.

func TestPreVote_EmptyConfiguration_GrantsToUnknownCandidate(t *testing.T) {
	store := new(MockStorage)
	node := preVoteNode(store, map[string]PeerState{})

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(0), nil)
	store.On(methodGetLastLogEntry, mock.Anything).Return(LogEntry{}, nil)

	resp, err := node.HandlePreVote(context.Background(), PreVoteArgs{
		CandidateID: "stranger",
		Term:        1,
	})

	assert.NoError(t, err)
	assert.True(t, resp.VoteGranted)
	store.AssertExpectations(t)
	assertNoSideEffects(t, node, store)
}

// ── 5. We already believe in a leader → reject the challenger ─────────────────

func TestPreVote_HaveLeader_RejectsChallenger(t *testing.T) {
	store := new(MockStorage)
	node := preVoteNode(store, twoVoters())
	node.SetLeaderID("node-3")

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(4), nil)

	resp, err := node.HandlePreVote(context.Background(), PreVoteArgs{
		CandidateID: "node-2",
		Term:        5,
	})

	assert.NoError(t, err)
	assert.False(t, resp.VoteGranted)
	store.AssertNotCalled(t, methodGetLastLogEntry, mock.Anything)
	store.AssertExpectations(t)
	assertNoSideEffects(t, node, store)
}

// ── 6. The leader itself probing is not a challenger → grant ──────────────────

func TestPreVote_HaveLeader_GrantsToThatLeader(t *testing.T) {
	store := new(MockStorage)
	node := preVoteNode(store, twoVoters())
	node.SetLeaderID("node-2")

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(4), nil)
	store.On(methodGetLastLogEntry, mock.Anything).Return(LogEntry{}, nil)

	resp, err := node.HandlePreVote(context.Background(), PreVoteArgs{
		CandidateID: "node-2",
		Term:        5,
	})

	assert.NoError(t, err)
	assert.True(t, resp.VoteGranted)
	store.AssertExpectations(t)
	assertNoSideEffects(t, node, store)
}

// ── 7. args.Term < currentTerm → reject, answer with our term ─────────────────

func TestPreVote_TermLessThanCurrent(t *testing.T) {
	store := new(MockStorage)
	node := preVoteNode(store, twoVoters())

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)

	resp, err := node.HandlePreVote(context.Background(), PreVoteArgs{
		CandidateID: "node-2",
		Term:        3,
	})

	assert.NoError(t, err)
	assert.False(t, resp.VoteGranted)
	assert.Equal(t, uint64(5), resp.Term)
	store.AssertNotCalled(t, methodGetLastLogEntry, mock.Anything)
	store.AssertExpectations(t)
	assertNoSideEffects(t, node, store)
}

// ── 8. args.Term == currentTerm → still eligible ──────────────────────────────
//
// Unlike the real vote there is no votedFor to have spent, so a probe at the
// current term is answered on its merits rather than refused as a duplicate.

func TestPreVote_TermEqualCurrent_Grants(t *testing.T) {
	store := new(MockStorage)
	node := preVoteNode(store, twoVoters())

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLastLogEntry, mock.Anything).Return(LogEntry{}, nil)

	resp, err := node.HandlePreVote(context.Background(), PreVoteArgs{
		CandidateID: "node-2",
		Term:        5,
	})

	assert.NoError(t, err)
	assert.True(t, resp.VoteGranted)
	assert.Equal(t, uint64(5), resp.Term)
	store.AssertExpectations(t)
	assertNoSideEffects(t, node, store)
}

// ── 9. args.Term > currentTerm → echo the candidate's term back, persist none ─

func TestPreVote_TermGreaterThanCurrent_EchoesCandidateTerm(t *testing.T) {
	store := new(MockStorage)
	node := preVoteNode(store, twoVoters())

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(2), nil)
	store.On(methodGetLastLogEntry, mock.Anything).Return(LogEntry{}, nil)

	resp, err := node.HandlePreVote(context.Background(), PreVoteArgs{
		CandidateID: "node-2",
		Term:        9,
	})

	assert.NoError(t, err)
	assert.True(t, resp.VoteGranted)
	assert.Equal(t, uint64(9), resp.Term, "response echoes the probed term, not ours")
	store.AssertExpectations(t)
	// The whole point: a term 7 ahead of ours did NOT move us.
	assertNoSideEffects(t, node, store)
}

// A rejected probe at a higher term must be just as inert — this is the case
// that would let a partitioned node depose a healthy leader if it leaked.
func TestPreVote_RejectedProbeAtHigherTerm_PersistsNothing(t *testing.T) {
	store := new(MockStorage)
	node := preVoteNode(store, twoVoters())
	node.SetLeaderID("node-3")

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(2), nil)

	resp, err := node.HandlePreVote(context.Background(), PreVoteArgs{
		CandidateID: "node-2",
		Term:        50,
	})

	assert.NoError(t, err)
	assert.False(t, resp.VoteGranted)
	store.AssertExpectations(t)
	assertNoSideEffects(t, node, store)
}

// ── 10. Non-voter peers can never win, so their probes are refused ────────────

func TestPreVote_StagingPeer_Rejected(t *testing.T) {
	store := new(MockStorage)
	node := preVoteNode(store, map[string]PeerState{
		"node-2": PeerState_Staging,
		"node-3": PeerState_Voter,
	})

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(4), nil)

	resp, err := node.HandlePreVote(context.Background(), PreVoteArgs{
		CandidateID: "node-2",
		Term:        5,
	})

	assert.NoError(t, err)
	assert.False(t, resp.VoteGranted)
	store.AssertNotCalled(t, methodGetLastLogEntry, mock.Anything)
	store.AssertExpectations(t)
	assertNoSideEffects(t, node, store)
}

func TestPreVote_NonVoterPeer_Rejected(t *testing.T) {
	store := new(MockStorage)
	node := preVoteNode(store, map[string]PeerState{
		"node-2": PeerState_NonVoter,
		"node-3": PeerState_Voter,
	})

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(4), nil)

	resp, err := node.HandlePreVote(context.Background(), PreVoteArgs{
		CandidateID: "node-2",
		Term:        5,
	})

	assert.NoError(t, err)
	assert.False(t, resp.VoteGranted)
	store.AssertExpectations(t)
	assertNoSideEffects(t, node, store)
}

// A demoted peer that still has the best log in the cluster is refused anyway —
// the voter check runs before the log comparison, and must.
func TestPreVote_NonVoterWithBetterLog_StillRejected(t *testing.T) {
	store := new(MockStorage)
	node := preVoteNode(store, map[string]PeerState{
		"node-2": PeerState_NonVoter,
	})

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(4), nil)

	resp, err := node.HandlePreVote(context.Background(), PreVoteArgs{
		CandidateID:  "node-2",
		Term:         5,
		LastLogTerm:  99,
		LastLogIndex: 999,
	})

	assert.NoError(t, err)
	assert.False(t, resp.VoteGranted)
	store.AssertNotCalled(t, methodGetLastLogEntry, mock.Anything)
	store.AssertExpectations(t)
	assertNoSideEffects(t, node, store)
}

// ── 11. Log up-to-date comparison ─────────────────────────────────────────────

func TestPreVote_CandidateLogTermBehind_Rejected(t *testing.T) {
	store := new(MockStorage)
	node := preVoteNode(store, twoVoters())

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLastLogEntry, mock.Anything).Return(LogEntry{Index: 10, Term: 4}, nil)

	resp, err := node.HandlePreVote(context.Background(), PreVoteArgs{
		CandidateID:  "node-2",
		Term:         5,
		LastLogTerm:  3, // behind
		LastLogIndex: 10,
	})

	assert.NoError(t, err)
	assert.False(t, resp.VoteGranted)
	store.AssertExpectations(t)
	assertNoSideEffects(t, node, store)
}

func TestPreVote_SameLogTermIndexBehind_Rejected(t *testing.T) {
	store := new(MockStorage)
	node := preVoteNode(store, twoVoters())

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLastLogEntry, mock.Anything).Return(LogEntry{Index: 10, Term: 4}, nil)

	resp, err := node.HandlePreVote(context.Background(), PreVoteArgs{
		CandidateID:  "node-2",
		Term:         5,
		LastLogTerm:  4,
		LastLogIndex: 8, // behind
	})

	assert.NoError(t, err)
	assert.False(t, resp.VoteGranted)
	store.AssertExpectations(t)
	assertNoSideEffects(t, node, store)
}

func TestPreVote_SameLogTermIndexEqual_Granted(t *testing.T) {
	store := new(MockStorage)
	node := preVoteNode(store, twoVoters())

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLastLogEntry, mock.Anything).Return(LogEntry{Index: 10, Term: 4}, nil)

	resp, err := node.HandlePreVote(context.Background(), PreVoteArgs{
		CandidateID:  "node-2",
		Term:         5,
		LastLogTerm:  4,
		LastLogIndex: 10, // equal
	})

	assert.NoError(t, err)
	assert.True(t, resp.VoteGranted)
	store.AssertExpectations(t)
	assertNoSideEffects(t, node, store)
}

// A later last term wins even on a shorter log — term dominates index.
func TestPreVote_CandidateLogTermAheadButShorter_Granted(t *testing.T) {
	store := new(MockStorage)
	node := preVoteNode(store, twoVoters())

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLastLogEntry, mock.Anything).Return(LogEntry{Index: 10, Term: 3}, nil)

	resp, err := node.HandlePreVote(context.Background(), PreVoteArgs{
		CandidateID:  "node-2",
		Term:         5,
		LastLogTerm:  5, // ahead
		LastLogIndex: 8, // but shorter
	})

	assert.NoError(t, err)
	assert.True(t, resp.VoteGranted)
	store.AssertExpectations(t)
	assertNoSideEffects(t, node, store)
}

// Our log is empty (zero-value entry, Index 0) — we can lose to nobody, so the
// comparison is skipped entirely and any candidate passes.
func TestPreVote_OurLogEmpty_Granted(t *testing.T) {
	store := new(MockStorage)
	node := preVoteNode(store, twoVoters())

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLastLogEntry, mock.Anything).Return(LogEntry{}, nil)

	resp, err := node.HandlePreVote(context.Background(), PreVoteArgs{
		CandidateID:  "node-2",
		Term:         5,
		LastLogTerm:  3,
		LastLogIndex: 10,
	})

	assert.NoError(t, err)
	assert.True(t, resp.VoteGranted)
	store.AssertExpectations(t)
	assertNoSideEffects(t, node, store)
}

// ── 12. DB error cases ────────────────────────────────────────────────────────

func TestPreVote_DBErr_GetCurrentTerm(t *testing.T) {
	store := new(MockStorage)
	node := preVoteNode(store, twoVoters())

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(0), errors.New("db error"))

	_, err := node.HandlePreVote(context.Background(), PreVoteArgs{
		CandidateID: "node-2",
		Term:        5,
	})

	assert.Error(t, err)
	store.AssertExpectations(t)
	assertNoSideEffects(t, node, store)
}

func TestPreVote_DBErr_GetLastLogEntry(t *testing.T) {
	store := new(MockStorage)
	node := preVoteNode(store, twoVoters())

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLastLogEntry, mock.Anything).Return(LogEntry{}, errors.New("db error"))

	_, err := node.HandlePreVote(context.Background(), PreVoteArgs{
		CandidateID: "node-2",
		Term:        5,
	})

	assert.Error(t, err)
	store.AssertExpectations(t)
	assertNoSideEffects(t, node, store)
}

// ── 13. Repeated probes are idempotent ────────────────────────────────────────
//
// Because nothing is spent, the same candidate probing the same term over and
// over gets the same answer — where a real RequestVote would burn its one vote.

func TestPreVote_RepeatedProbesReturnSameAnswer(t *testing.T) {
	store := new(MockStorage)
	node := preVoteNode(store, twoVoters())

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(4), nil)
	store.On(methodGetLastLogEntry, mock.Anything).Return(LogEntry{}, nil)

	args := PreVoteArgs{CandidateID: "node-2", Term: 5}
	for i := 0; i < 3; i++ {
		resp, err := node.HandlePreVote(context.Background(), args)
		assert.NoError(t, err)
		assert.True(t, resp.VoteGranted, "probe %d", i)
	}

	assertNoSideEffects(t, node, store)
}

// ── 14. Concurrent callers ────────────────────────────────────────────────────
//
// HandlePreVote is caller-facing, so server/rpc.go may run several at once.
// clientMu has to serialize them without deadlocking against mu, which the
// membership and leader lookups take underneath it. Run with -race.

func TestPreVote_ConcurrentCallers(t *testing.T) {
	store := new(MockStorage)
	node := preVoteNode(store, twoVoters())

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(4), nil)
	store.On(methodGetLastLogEntry, mock.Anything).Return(LogEntry{Index: 3, Term: 4}, nil)

	const callers = 32
	results := make([]PreVoteResponse, callers)

	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := node.HandlePreVote(context.Background(), PreVoteArgs{
				CandidateID:  "node-2",
				Term:         5,
				LastLogTerm:  4,
				LastLogIndex: 3,
			})
			assert.NoError(t, err)
			results[i] = resp
		}(i)
	}
	wg.Wait()

	for i, resp := range results {
		assert.True(t, resp.VoteGranted, "caller %d", i)
		assert.Equal(t, uint64(5), resp.Term, "caller %d", i)
	}
	assertNoSideEffects(t, node, store)
}
