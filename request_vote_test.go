package raft

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const (
	methodGetCurrentTerm = "GetCurrentTerm"
	methodSetCurrentTerm = "SetCurrentTerm"
	methodGetVotedFor    = "GetVotedFor"
	methodSetVotedFor    = "SetVotedFor"
)

// ── 1. Empty candidate ID ─────────────────────────────────────────────────────

func TestRequestVote_EmptyCandidateID(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)

	_, err := node.HandleRequestVote(context.Background(), RequestVoteArgs{
		CandidateID: "   ",
	})

	assert.Error(t, err)
	store.AssertExpectations(t) // no DB calls should have been made
}

// ── 2. args.Term < currentTerm → deny, return currentTerm ─────────────────────

func TestRequestVote_TermLessThanCurrent(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)

	resp, err := node.HandleRequestVote(ctx, RequestVoteArgs{
		CandidateID: "candidate-1",
		Term:        3,
	})

	assert.NoError(t, err)
	assert.False(t, resp.VoteGranted)
	assert.Equal(t, uint64(5), resp.Term)
	store.AssertExpectations(t)
}

// ── 3. args.Term > currentTerm → update term, reset votedFor, grant vote ──────

func TestRequestVote_TermGreaterThanCurrent(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(2), nil)
	store.On(methodSetCurrentTerm, mock.Anything, uint(5)).Return(nil)
	store.On(methodSetVotedFor, mock.Anything, "").Return(nil) // reset
	store.On(methodGetVotedFor, mock.Anything).Return("", nil) // nobody voted
	store.On(methodGetLastIndex, mock.Anything).Return(uint(0), nil)
	store.On(methodSetVotedFor, mock.Anything, "candidate-1").Return(nil)

	resp, err := node.HandleRequestVote(ctx, RequestVoteArgs{
		CandidateID:  "candidate-1",
		Term:         5,
		LastLogTerm:  0,
		LastLogIndex: 0,
	})

	assert.NoError(t, err)
	assert.True(t, resp.VoteGranted)
	assert.Equal(t, uint64(5), resp.Term)
	store.AssertExpectations(t)
}

// ── 4. args.Term == currentTerm → don't reset votedFor ────────────────────────

func TestRequestVote_TermEqualCurrent_VotedForEmpty(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	// SetCurrentTerm and SetVotedFor("") must NOT be called
	store.On(methodGetVotedFor, mock.Anything).Return("", nil)
	store.On(methodGetLastIndex, mock.Anything).Return(uint(0), nil)
	store.On(methodSetVotedFor, mock.Anything, "candidate-1").Return(nil)

	resp, err := node.HandleRequestVote(ctx, RequestVoteArgs{
		CandidateID:  "candidate-1",
		Term:         5,
		LastLogTerm:  0,
		LastLogIndex: 0,
	})

	assert.NoError(t, err)
	assert.True(t, resp.VoteGranted)
	store.AssertExpectations(t)
}

// ── 5. votedFor == "" → allow vote ────────────────────────────────────────────
// covered above in test 4

// ── 6. votedFor == candidateID → idempotent, allow vote ───────────────────────

func TestRequestVote_AlreadyVotedForSameCandidate(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetVotedFor, mock.Anything).Return("candidate-1", nil)
	store.On(methodGetLastIndex, mock.Anything).Return(uint(0), nil)
	store.On(methodSetVotedFor, mock.Anything, "candidate-1").Return(nil)

	resp, err := node.HandleRequestVote(ctx, RequestVoteArgs{
		CandidateID:  "candidate-1",
		Term:         5,
		LastLogTerm:  0,
		LastLogIndex: 0,
	})

	assert.NoError(t, err)
	assert.True(t, resp.VoteGranted)
	store.AssertExpectations(t)
}

// ── 7. votedFor != "" && votedFor != candidateID → deny ───────────────────────

func TestRequestVote_AlreadyVotedForDifferentCandidate(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetVotedFor, mock.Anything).Return("candidate-2", nil)

	resp, err := node.HandleRequestVote(ctx, RequestVoteArgs{
		CandidateID: "candidate-1",
		Term:        5,
	})

	assert.NoError(t, err)
	assert.False(t, resp.VoteGranted)
	store.AssertExpectations(t)
}

// ── 8. lastLog.Index == 0 (no logs) → allow vote ──────────────────────────────

func TestRequestVote_NoLogs_AllowVote(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetVotedFor, mock.Anything).Return("", nil)
	store.On(methodGetLastIndex, mock.Anything).Return(uint(0), nil)
	store.On(methodSetVotedFor, mock.Anything, "candidate-1").Return(nil)

	resp, err := node.HandleRequestVote(ctx, RequestVoteArgs{
		CandidateID:  "candidate-1",
		Term:         5,
		LastLogTerm:  0,
		LastLogIndex: 0,
	})

	assert.NoError(t, err)
	assert.True(t, resp.VoteGranted)
	store.AssertExpectations(t)
}

// ── 9. args.LastLogTerm < lastLog.Term → deny ─────────────────────────────────

func TestRequestVote_CandidateLogTermBehind(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetVotedFor, mock.Anything).Return("", nil)
	store.On(methodGetLastIndex, mock.Anything).Return(uint(10), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(10)).Return(LogEntry{Index: 10, Term: 4}, nil)

	resp, err := node.HandleRequestVote(ctx, RequestVoteArgs{
		CandidateID:  "candidate-1",
		Term:         5,
		LastLogTerm:  3, // behind
		LastLogIndex: 10,
	})

	assert.NoError(t, err)
	assert.False(t, resp.VoteGranted)
	store.AssertExpectations(t)
}

// ── 10. same term, candidate index behind → deny ──────────────────────────────

func TestRequestVote_SameTermCandidateIndexBehind(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetVotedFor, mock.Anything).Return("", nil)
	store.On(methodGetLastIndex, mock.Anything).Return(uint(10), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(10)).Return(LogEntry{Index: 10, Term: 4}, nil)

	resp, err := node.HandleRequestVote(ctx, RequestVoteArgs{
		CandidateID:  "candidate-1",
		Term:         5,
		LastLogTerm:  4,
		LastLogIndex: 8, // behind
	})

	assert.NoError(t, err)
	assert.False(t, resp.VoteGranted)
	store.AssertExpectations(t)
}

// ── 11. same term, candidate index equal or ahead → allow ─────────────────────

func TestRequestVote_SameTermCandidateIndexEqual(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetVotedFor, mock.Anything).Return("", nil)
	store.On(methodGetLastIndex, mock.Anything).Return(uint(10), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(10)).Return(LogEntry{Index: 10, Term: 4}, nil)
	store.On(methodSetVotedFor, mock.Anything, "candidate-1").Return(nil)

	resp, err := node.HandleRequestVote(ctx, RequestVoteArgs{
		CandidateID:  "candidate-1",
		Term:         5,
		LastLogTerm:  4,
		LastLogIndex: 10, // equal
	})

	assert.NoError(t, err)
	assert.True(t, resp.VoteGranted)
	store.AssertExpectations(t)
}

// ── 12. args.LastLogTerm > lastLog.Term → allow ───────────────────────────────

func TestRequestVote_CandidateLogTermAhead(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetVotedFor, mock.Anything).Return("", nil)
	store.On(methodGetLastIndex, mock.Anything).Return(uint(10), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(10)).Return(LogEntry{Index: 10, Term: 3}, nil)
	store.On(methodSetVotedFor, mock.Anything, "candidate-1").Return(nil)

	resp, err := node.HandleRequestVote(ctx, RequestVoteArgs{
		CandidateID:  "candidate-1",
		Term:         5,
		LastLogTerm:  5, // ahead
		LastLogIndex: 8,
	})

	assert.NoError(t, err)
	assert.True(t, resp.VoteGranted)
	store.AssertExpectations(t)
}

// ── DB error cases ────────────────────────────────────────────────────────────

// 14. GetCurrentTerm fails
func TestRequestVote_DBErr_GetCurrentTerm(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(0), errors.New("db error"))

	_, err := node.HandleRequestVote(context.Background(), RequestVoteArgs{
		CandidateID: "candidate-1",
		Term:        5,
	})

	assert.Error(t, err)
	store.AssertExpectations(t)
}

// 15. SetCurrentTerm fails (when term update needed)
func TestRequestVote_DBErr_SetCurrentTerm(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(2), nil)
	store.On(methodSetCurrentTerm, mock.Anything, uint(5)).Return(errors.New("db error"))

	_, err := node.HandleRequestVote(context.Background(), RequestVoteArgs{
		CandidateID: "candidate-1",
		Term:        5,
	})

	assert.Error(t, err)
	store.AssertExpectations(t)
}

// 16. SetVotedFor("") fails (reset after term update)
func TestRequestVote_DBErr_SetVotedForReset(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(2), nil)
	store.On(methodSetCurrentTerm, mock.Anything, uint(5)).Return(nil)
	store.On(methodSetVotedFor, mock.Anything, "").Return(errors.New("db error"))

	_, err := node.HandleRequestVote(context.Background(), RequestVoteArgs{
		CandidateID: "candidate-1",
		Term:        5,
	})

	assert.Error(t, err)
	store.AssertExpectations(t)
}

// 17. GetVotedFor fails
func TestRequestVote_DBErr_GetVotedFor(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetVotedFor, mock.Anything).Return("", errors.New("db error"))

	_, err := node.HandleRequestVote(context.Background(), RequestVoteArgs{
		CandidateID: "candidate-1",
		Term:        5,
	})

	assert.Error(t, err)
	store.AssertExpectations(t)
}

// 18. GetLastIndex fails
func TestRequestVote_DBErr_GetLastIndex(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetVotedFor, mock.Anything).Return("", nil)
	store.On(methodGetLastIndex, mock.Anything).Return(uint(0), errors.New("db error"))

	_, err := node.HandleRequestVote(context.Background(), RequestVoteArgs{
		CandidateID: "candidate-1",
		Term:        5,
	})

	assert.Error(t, err)
	store.AssertExpectations(t)
}

// 19. SetVotedFor(candidateID) fails (final vote persist)
func TestRequestVote_DBErr_SetVotedForCandidate(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetVotedFor, mock.Anything).Return("", nil)
	store.On(methodGetLastIndex, mock.Anything).Return(uint(0), nil)
	store.On(methodSetVotedFor, mock.Anything, "candidate-1").Return(errors.New("db error"))

	_, err := node.HandleRequestVote(context.Background(), RequestVoteArgs{
		CandidateID:  "candidate-1",
		Term:         5,
		LastLogTerm:  0,
		LastLogIndex: 0,
	})

	assert.Error(t, err)
	store.AssertExpectations(t)
}

// 20. ── Zero value lastLog (Index=0, Term=0) → any candidate log passes ───────────
// Covers the fix where zero value entry passes all log up-to-date checks
// because: anything < 0 is false, and 0 == 0 && anything < 0 is false.
// Both deny conditions never trigger → vote always granted by a fresh node.

func TestRequestVote_ZeroValueLastLog_CandidateAhead_AllowVote(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetVotedFor, mock.Anything).Return("", nil)
	store.On(methodGetLastIndex, mock.Anything).Return(uint(0), nil)
	store.On(methodSetVotedFor, mock.Anything, "candidate-1").Return(nil)

	// candidate has real logs, node has none — candidate is strictly ahead
	resp, err := node.HandleRequestVote(ctx, RequestVoteArgs{
		CandidateID:  "candidate-1",
		Term:         5,
		LastLogTerm:  3,
		LastLogIndex: 10,
	})

	assert.NoError(t, err)
	assert.True(t, resp.VoteGranted)
	store.AssertExpectations(t)
}
