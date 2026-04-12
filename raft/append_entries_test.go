package raft

import (
	"context"
	"errors"
	"testing"

	"github.com/SHREYANSHSINGH14/raft/db"
	"github.com/SHREYANSHSINGH14/raft/types"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const (
	methodGetLogByIndex   = "GetLogByIndex"
	methodTruncateLogs    = "TruncateLogs"
	methodAppendLogs      = "AppendLogs"
	methodGetLastLogIndex = "GetLastLogIndex"
)

// ── 1. Empty leader ID ────────────────────────────────────────────────────────

func TestAppendEntries_EmptyLeaderID(t *testing.T) {
	store := new(db.MockStore)
	srv := newTestServer(store)

	resp, err := srv.AppendEntries(context.Background(), &types.AppendEntriesArgs{
		LeaderId: "   ",
	})

	assert.Nil(t, resp)
	assert.Error(t, err)
	assert.Equal(t, "", srv.getLeaderID())
	assert.Equal(t, 0, len(srv.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 2. GetCurrentTerm DB error ────────────────────────────────────────────────

func TestAppendEntries_DBErr_GetCurrentTerm(t *testing.T) {
	store := new(db.MockStore)
	srv := newTestServer(store)

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(0), errors.New("db error"))

	resp, err := srv.AppendEntries(context.Background(), &types.AppendEntriesArgs{
		LeaderId: "leader-1",
		Term:     5,
	})

	assert.Nil(t, resp)
	assert.Error(t, err)
	assert.Equal(t, "", srv.getLeaderID())
	assert.Equal(t, 0, len(srv.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 3. args.Term < currentTerm → false, no state change ──────────────────────

func TestAppendEntries_TermLessThanCurrent(t *testing.T) {
	store := new(db.MockStore)
	srv := newTestServer(store)
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)

	resp, err := srv.AppendEntries(ctx, &types.AppendEntriesArgs{
		LeaderId: "leader-1",
		Term:     3,
	})

	assert.NoError(t, err)
	assert.False(t, resp.Success)
	assert.Equal(t, uint64(5), resp.Term)
	assert.Equal(t, "", srv.getLeaderID())
	assert.Equal(t, 0, len(srv.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 4. args.Term == currentTerm → no reset ────────────────────────────────────

func TestAppendEntries_TermEqualCurrent_NoReset(t *testing.T) {
	store := new(db.MockStore)
	srv := newTestServer(store)
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	// SetCurrentTerm and SetVotedFor("") must NOT be called
	store.On(methodGetLogByIndex, mock.Anything, uint(0)).Return(&types.LogEntry{Index: 0, Term: 0}, nil)
	store.On(methodTruncateLogs, mock.Anything, uint(1)).Return(nil)
	store.On(methodGetLastLogIndex, mock.Anything).Return(uint(0), nil)

	resp, err := srv.AppendEntries(ctx, &types.AppendEntriesArgs{
		LeaderId:     "leader-1",
		Term:         5,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		LeaderCommit: 1,
	})

	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, "leader-1", srv.getLeaderID())
	assert.Equal(t, 1, len(srv.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 5. args.Term > currentTerm → update term, reset votedFor ─────────────────

func TestAppendEntries_TermGreaterThanCurrent(t *testing.T) {
	store := new(db.MockStore)
	srv := newTestServer(store)
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(2), nil)
	store.On(methodSetCurrentTerm, mock.Anything, uint(5)).Return(nil)
	store.On(methodSetVotedFor, mock.Anything, "").Return(nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(0)).Return(&types.LogEntry{Index: 0, Term: 0}, nil)
	store.On(methodTruncateLogs, mock.Anything, uint(1)).Return(nil)
	store.On(methodGetLastLogIndex, mock.Anything).Return(uint(0), nil)

	resp, err := srv.AppendEntries(ctx, &types.AppendEntriesArgs{
		LeaderId:     "leader-1",
		Term:         5,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		LeaderCommit: 1,
	})

	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, uint64(5), resp.Term)
	assert.Equal(t, "leader-1", srv.getLeaderID())
	assert.Equal(t, 1, len(srv.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 6. args.Term > currentTerm, SetCurrentTerm fails ─────────────────────────

func TestAppendEntries_DBErr_SetCurrentTerm(t *testing.T) {
	store := new(db.MockStore)
	srv := newTestServer(store)

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(2), nil)
	store.On(methodSetCurrentTerm, mock.Anything, uint(5)).Return(errors.New("db error"))

	resp, err := srv.AppendEntries(context.Background(), &types.AppendEntriesArgs{
		LeaderId: "leader-1",
		Term:     5,
	})

	assert.Nil(t, resp)
	assert.Error(t, err)
	assert.Equal(t, "", srv.getLeaderID())
	assert.Equal(t, 0, len(srv.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 7. args.Term > currentTerm, SetVotedFor("") fails ────────────────────────

func TestAppendEntries_DBErr_SetVotedForReset(t *testing.T) {
	store := new(db.MockStore)
	srv := newTestServer(store)

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(2), nil)
	store.On(methodSetCurrentTerm, mock.Anything, uint(5)).Return(nil)
	store.On(methodSetVotedFor, mock.Anything, "").Return(errors.New("db error"))

	resp, err := srv.AppendEntries(context.Background(), &types.AppendEntriesArgs{
		LeaderId: "leader-1",
		Term:     5,
	})

	assert.Nil(t, resp)
	assert.Error(t, err)
	assert.Equal(t, "", srv.getLeaderID())
	assert.Equal(t, 0, len(srv.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 8. GetLogByIndex fails ────────────────────────────────────────────────────

func TestAppendEntries_DBErr_GetLogByIndex(t *testing.T) {
	store := new(db.MockStore)
	srv := newTestServer(store)

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(3)).Return(nil, errors.New("db error"))

	resp, err := srv.AppendEntries(context.Background(), &types.AppendEntriesArgs{
		LeaderId:     "leader-1",
		Term:         5,
		PrevLogIndex: 3,
	})

	assert.Nil(t, resp)
	assert.Error(t, err)
	assert.Equal(t, "", srv.getLeaderID())
	assert.Equal(t, 0, len(srv.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 9. prevLog.Index == 0 (no prev log) → continue ───────────────────────────

func TestAppendEntries_PrevLogNil_Continue(t *testing.T) {
	store := new(db.MockStore)
	srv := newTestServer(store)
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(0)).Return(&types.LogEntry{Index: 0, Term: 0}, nil)
	store.On(methodTruncateLogs, mock.Anything, uint(1)).Return(nil)
	store.On(methodGetLastLogIndex, mock.Anything).Return(uint(0), nil)

	resp, err := srv.AppendEntries(ctx, &types.AppendEntriesArgs{
		LeaderId:     "leader-1",
		Term:         5,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		LeaderCommit: 1,
	})

	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, "leader-1", srv.getLeaderID())
	assert.Equal(t, 1, len(srv.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 10. prevLog.Term != args.PrevLogTerm → false, no state change ─────────────

func TestAppendEntries_PrevLogTermMismatch(t *testing.T) {
	store := new(db.MockStore)
	srv := newTestServer(store)
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(3)).Return(&types.LogEntry{
		Index: 3,
		Term:  2,
	}, nil)

	resp, err := srv.AppendEntries(ctx, &types.AppendEntriesArgs{
		LeaderId:     "leader-1",
		Term:         5,
		PrevLogIndex: 3,
		PrevLogTerm:  4, // mismatch
	})

	assert.NoError(t, err)
	assert.False(t, resp.Success)
	assert.Equal(t, "", srv.getLeaderID())
	assert.Equal(t, 0, len(srv.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 11. TruncateLogs fails ────────────────────────────────────────────────────

func TestAppendEntries_DBErr_TruncateLogs(t *testing.T) {
	store := new(db.MockStore)
	srv := newTestServer(store)

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(3)).Return(&types.LogEntry{Index: 3, Term: 4}, nil)
	store.On(methodTruncateLogs, mock.Anything, uint(4)).Return(errors.New("db error"))

	resp, err := srv.AppendEntries(context.Background(), &types.AppendEntriesArgs{
		LeaderId:     "leader-1",
		Term:         5,
		PrevLogIndex: 3,
		PrevLogTerm:  4,
	})

	assert.Nil(t, resp)
	assert.Error(t, err)
	assert.Equal(t, "", srv.getLeaderID())
	assert.Equal(t, 0, len(srv.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 12. Empty entries (heartbeat) → no AppendLogs call, success ───────────────

func TestAppendEntries_Heartbeat_NoEntries(t *testing.T) {
	store := new(db.MockStore)
	srv := newTestServer(store)
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(3)).Return(&types.LogEntry{Index: 3, Term: 4}, nil)
	store.On(methodTruncateLogs, mock.Anything, uint(4)).Return(nil)
	// AppendLogs must NOT be called
	store.On(methodGetLastLogIndex, mock.Anything).Return(uint(3), nil)

	resp, err := srv.AppendEntries(ctx, &types.AppendEntriesArgs{
		LeaderId:     "leader-1",
		Term:         5,
		PrevLogIndex: 3,
		PrevLogTerm:  4,
		Entries:      []*types.LogEntry{},
		LeaderCommit: 5,
	})

	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, "leader-1", srv.getLeaderID())
	assert.Equal(t, 1, len(srv.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 13. AppendLogs fails ──────────────────────────────────────────────────────

func TestAppendEntries_DBErr_AppendLogs(t *testing.T) {
	store := new(db.MockStore)
	srv := newTestServer(store)

	entries := []*types.LogEntry{{Index: 4, Term: 5}}

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(3)).Return(&types.LogEntry{Index: 3, Term: 4}, nil)
	store.On(methodTruncateLogs, mock.Anything, uint(4)).Return(nil)
	store.On(methodAppendLogs, mock.Anything, entries).Return(errors.New("db error"))

	resp, err := srv.AppendEntries(context.Background(), &types.AppendEntriesArgs{
		LeaderId:     "leader-1",
		Term:         5,
		PrevLogIndex: 3,
		PrevLogTerm:  4,
		Entries:      entries,
	})

	assert.Nil(t, resp)
	assert.Error(t, err)
	assert.Equal(t, "", srv.getLeaderID())
	assert.Equal(t, 0, len(srv.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 14. leaderCommit <= commitIndex → no change ───────────────────────────────

func TestAppendEntries_LeaderCommitNotAhead(t *testing.T) {
	store := new(db.MockStore)
	srv := newTestServer(store)
	srv.commitIndex = 5
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(3)).Return(&types.LogEntry{Index: 3, Term: 4}, nil)
	store.On(methodTruncateLogs, mock.Anything, uint(4)).Return(nil)
	// GetLastLogIndex must NOT be called

	resp, err := srv.AppendEntries(ctx, &types.AppendEntriesArgs{
		LeaderId:     "leader-1",
		Term:         5,
		PrevLogIndex: 3,
		PrevLogTerm:  4,
		LeaderCommit: 4,
	})

	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, uint(5), srv.commitIndex) // unchanged
	assert.Equal(t, "leader-1", srv.getLeaderID())
	assert.Equal(t, 1, len(srv.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 15. leaderCommit > commitIndex → set to min(leaderCommit, lastLogIdx) ─────

func TestAppendEntries_LeaderCommitAhead_SetsMin(t *testing.T) {
	store := new(db.MockStore)
	srv := newTestServer(store)
	srv.commitIndex = 2
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(3)).Return(&types.LogEntry{Index: 3, Term: 4}, nil)
	store.On(methodTruncateLogs, mock.Anything, uint(4)).Return(nil)
	store.On(methodGetLastLogIndex, mock.Anything).Return(uint(6), nil)

	resp, err := srv.AppendEntries(ctx, &types.AppendEntriesArgs{
		LeaderId:     "leader-1",
		Term:         5,
		PrevLogIndex: 3,
		PrevLogTerm:  4,
		LeaderCommit: 8, // min(8, 6) = 6
	})

	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, uint(6), srv.commitIndex)
	assert.Equal(t, "leader-1", srv.getLeaderID())
	assert.Equal(t, 1, len(srv.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 16. GetLastLogIndex fails ─────────────────────────────────────────────────

func TestAppendEntries_DBErr_GetLastLogIndex(t *testing.T) {
	store := new(db.MockStore)
	srv := newTestServer(store)
	srv.commitIndex = 2

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(3)).Return(&types.LogEntry{Index: 3, Term: 4}, nil)
	store.On(methodTruncateLogs, mock.Anything, uint(4)).Return(nil)
	store.On(methodGetLastLogIndex, mock.Anything).Return(uint(0), errors.New("db error"))

	resp, err := srv.AppendEntries(context.Background(), &types.AppendEntriesArgs{
		LeaderId:     "leader-1",
		Term:         5,
		PrevLogIndex: 3,
		PrevLogTerm:  4,
		LeaderCommit: 8,
	})

	assert.Nil(t, resp)
	assert.Error(t, err)
	assert.Equal(t, "", srv.getLeaderID())
	assert.Equal(t, 0, len(srv.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 17. LeaderID updates from old to new ─────────────────────────────────────

func TestAppendEntries_LeaderIDUpdated(t *testing.T) {
	store := new(db.MockStore)
	srv := newTestServer(store)
	srv.LeaderID = "old-leader"
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(0)).Return(&types.LogEntry{Index: 0, Term: 0}, nil)
	store.On(methodTruncateLogs, mock.Anything, uint(1)).Return(nil)
	store.On(methodGetLastLogIndex, mock.Anything).Return(uint(0), nil)

	resp, err := srv.AppendEntries(ctx, &types.AppendEntriesArgs{
		LeaderId:     "new-leader",
		Term:         5,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		LeaderCommit: 1,
	})

	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, "new-leader", srv.getLeaderID())
	assert.Equal(t, 1, len(srv.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 18. electionTimeoutCh receives signal on success ─────────────────────────

func TestAppendEntries_ElectionTimeoutReset(t *testing.T) {
	store := new(db.MockStore)
	srv := newTestServer(store)
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(0)).Return(&types.LogEntry{Index: 0, Term: 0}, nil)
	store.On(methodTruncateLogs, mock.Anything, uint(1)).Return(nil)
	store.On(methodGetLastLogIndex, mock.Anything).Return(uint(0), nil)

	resp, err := srv.AppendEntries(ctx, &types.AppendEntriesArgs{
		LeaderId:     "leader-1",
		Term:         5,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		LeaderCommit: 1,
	})

	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, "leader-1", srv.getLeaderID())
	assert.Equal(t, 1, len(srv.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 19. Happy path ────────────────────────────────────────────────────────────

func TestAppendEntries_HappyPath(t *testing.T) {
	store := new(db.MockStore)
	srv := newTestServer(store)
	srv.commitIndex = 2
	ctx := context.Background()

	entries := []*types.LogEntry{
		{Index: 4, Term: 5, Data: []byte("cmd1")},
		{Index: 5, Term: 5, Data: []byte("cmd2")},
	}

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(3)).Return(&types.LogEntry{Index: 3, Term: 4}, nil)
	store.On(methodTruncateLogs, mock.Anything, uint(4)).Return(nil)
	store.On(methodAppendLogs, mock.Anything, entries).Return(nil)
	store.On(methodGetLastLogIndex, mock.Anything).Return(uint(5), nil)

	resp, err := srv.AppendEntries(ctx, &types.AppendEntriesArgs{
		LeaderId:     "leader-1",
		Term:         5,
		PrevLogIndex: 3,
		PrevLogTerm:  4,
		Entries:      entries,
		LeaderCommit: 5,
	})

	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, uint64(5), resp.Term)
	assert.Equal(t, uint(5), srv.commitIndex)
	assert.Equal(t, "leader-1", srv.getLeaderID())
	assert.Equal(t, 1, len(srv.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 20. PrevLogIndex == 0 ────────────────────────────────────────────────────────────
// ── store returns ErrNotFound → fresh follower, continue ───
// This is the key fix: index 0 is a sentinel, ErrNotFound here is NOT
// log inconsistency. Leader is sending from the beginning, allow it.

func TestAppendEntries_PrevLogIndex0_NotFound_Continues(t *testing.T) {
	store := new(db.MockStore)
	srv := newTestServer(store)
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(0)).Return(nil, pebble.ErrNotFound)
	store.On(methodTruncateLogs, mock.Anything, uint(1)).Return(nil)
	store.On(methodGetLastLogIndex, mock.Anything).Return(uint(0), nil)

	resp, err := srv.AppendEntries(ctx, &types.AppendEntriesArgs{
		LeaderId:     "leader-1",
		Term:         5,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		LeaderCommit: 1,
	})

	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, "leader-1", srv.getLeaderID())
	assert.Equal(t, 1, len(srv.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 21. PrevLogIndex > 0 ────────────────────────────────────────────────────────────
// ── PrevLogIndex > 0, store returns ErrNotFound → log inconsistency, false ────
// Leader expects a log at prevLogIndex but follower doesn't have it.
// Paper section 5.3 — reply false, leader will back off and retry
// with lower nextIndex until it finds common ground.

func TestAppendEntries_PrevLogIndexNonZero_NotFound_ReturnsFalse(t *testing.T) {
	store := new(db.MockStore)
	srv := newTestServer(store)
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(5)).Return(nil, pebble.ErrNotFound)
	// TruncateLogs, AppendLogs, electionTimeoutCh must NOT be touched

	resp, err := srv.AppendEntries(ctx, &types.AppendEntriesArgs{
		LeaderId:     "leader-1",
		Term:         5,
		PrevLogIndex: 5, // follower doesn't have this
		PrevLogTerm:  3,
	})

	assert.NoError(t, err)
	assert.False(t, resp.Success)
	assert.Equal(t, uint64(5), resp.Term)
	assert.Equal(t, "", srv.getLeaderID())         // leaderID not set on false
	assert.Equal(t, 0, len(srv.electionTimeoutCh)) // no signal on false
	store.AssertExpectations(t)
}
