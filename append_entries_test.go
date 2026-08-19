package raft

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const (
	methodGetLogByIndex = "GetLogByIndex"
	methodDeleteLogs    = "DeleteLogs"
	methodAppendLogs    = "AppendLogs"
	methodGetLastIndex  = "GetLastIndex"
)

// ── 1. Empty leader ID ────────────────────────────────────────────────────────

func TestAppendEntries_EmptyLeaderID(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)

	_, err := node.HandleAppendEntries(context.Background(), AppendEntriesArgs{
		LeaderID: "   ",
	})

	assert.Error(t, err)
	assert.Equal(t, "", node.GetLeaderID())
	assert.Equal(t, 0, len(node.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 2. GetCurrentTerm DB error ────────────────────────────────────────────────

func TestAppendEntries_DBErr_GetCurrentTerm(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(0), errors.New("db error"))

	_, err := node.HandleAppendEntries(context.Background(), AppendEntriesArgs{
		LeaderID: "leader-1",
		Term:     5,
	})

	assert.Error(t, err)
	assert.Equal(t, "", node.GetLeaderID())
	assert.Equal(t, 0, len(node.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 3. args.Term < currentTerm → false, no state change ──────────────────────

func TestAppendEntries_TermLessThanCurrent(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)

	resp, err := node.HandleAppendEntries(ctx, AppendEntriesArgs{
		LeaderID: "leader-1",
		Term:     3,
	})

	assert.NoError(t, err)
	assert.False(t, resp.Success)
	assert.Equal(t, uint64(5), resp.Term)
	assert.Equal(t, "", node.GetLeaderID())
	assert.Equal(t, 0, len(node.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 4. args.Term == currentTerm → no reset ────────────────────────────────────

func TestAppendEntries_TermEqualCurrent_NoReset(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	// SetCurrentTerm and SetVotedFor("") must NOT be called
	store.On(methodGetLastIndex, mock.Anything).Return(uint(0), nil)

	resp, err := node.HandleAppendEntries(ctx, AppendEntriesArgs{
		LeaderID:     "leader-1",
		Term:         5,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		LeaderCommit: 1,
	})

	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, "leader-1", node.GetLeaderID())
	assert.Equal(t, 1, len(node.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 5. args.Term > currentTerm → update term, reset votedFor ─────────────────

func TestAppendEntries_TermGreaterThanCurrent(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(2), nil)
	store.On(methodSetCurrentTerm, mock.Anything, uint(5)).Return(nil)
	store.On(methodSetVotedFor, mock.Anything, "").Return(nil)
	store.On(methodGetLastIndex, mock.Anything).Return(uint(0), nil)

	resp, err := node.HandleAppendEntries(ctx, AppendEntriesArgs{
		LeaderID:     "leader-1",
		Term:         5,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		LeaderCommit: 1,
	})

	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, uint64(5), resp.Term)
	assert.Equal(t, "leader-1", node.GetLeaderID())
	assert.Equal(t, 1, len(node.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 6. args.Term > currentTerm, SetCurrentTerm fails ─────────────────────────

func TestAppendEntries_DBErr_SetCurrentTerm(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(2), nil)
	store.On(methodSetCurrentTerm, mock.Anything, uint(5)).Return(errors.New("db error"))

	_, err := node.HandleAppendEntries(context.Background(), AppendEntriesArgs{
		LeaderID: "leader-1",
		Term:     5,
	})

	assert.Error(t, err)
	assert.Equal(t, "", node.GetLeaderID())
	assert.Equal(t, 0, len(node.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 7. args.Term > currentTerm, SetVotedFor("") fails ────────────────────────

func TestAppendEntries_DBErr_SetVotedForReset(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(2), nil)
	store.On(methodSetCurrentTerm, mock.Anything, uint(5)).Return(nil)
	store.On(methodSetVotedFor, mock.Anything, "").Return(errors.New("db error"))

	_, err := node.HandleAppendEntries(context.Background(), AppendEntriesArgs{
		LeaderID: "leader-1",
		Term:     5,
	})

	assert.Error(t, err)
	assert.Equal(t, "", node.GetLeaderID())
	assert.Equal(t, 0, len(node.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 8. GetLogByIndex fails ────────────────────────────────────────────────────

func TestAppendEntries_DBErr_GetLogByIndex(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(3)).Return(LogEntry{}, errors.New("db error"))

	_, err := node.HandleAppendEntries(context.Background(), AppendEntriesArgs{
		LeaderID:     "leader-1",
		Term:         5,
		PrevLogIndex: 3,
	})

	assert.Error(t, err)
	assert.Equal(t, "", node.GetLeaderID())
	assert.Equal(t, 0, len(node.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 9. prevLog.Index == 0 (no prev log) → continue ───────────────────────────

func TestAppendEntries_PrevLogNil_Continue(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLastIndex, mock.Anything).Return(uint(0), nil)

	resp, err := node.HandleAppendEntries(ctx, AppendEntriesArgs{
		LeaderID:     "leader-1",
		Term:         5,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		LeaderCommit: 1,
	})

	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, "leader-1", node.GetLeaderID())
	assert.Equal(t, 1, len(node.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 10. prevLog.Term != args.PrevLogTerm → false, no state change ─────────────

func TestAppendEntries_PrevLogTermMismatch(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(3)).Return(LogEntry{Index: 3, Term: 2}, nil)

	resp, err := node.HandleAppendEntries(ctx, AppendEntriesArgs{
		LeaderID:     "leader-1",
		Term:         5,
		PrevLogIndex: 3,
		PrevLogTerm:  4, // mismatch
	})

	assert.NoError(t, err)
	assert.False(t, resp.Success)
	assert.Equal(t, "", node.GetLeaderID())
	assert.Equal(t, 0, len(node.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 11. DeleteLogs fails ──────────────────────────────────────────────────────

func TestAppendEntries_DBErr_DeleteLogs(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(3)).Return(LogEntry{Index: 3, Term: 4}, nil)
	store.On(methodGetLastIndex, mock.Anything).Return(uint(4), nil)
	// stored entry at index 4 is from term 2; the incoming one is term 5 → conflict,
	// which is what triggers the suffix truncation this test exercises.
	store.On(methodGetLogByIndex, mock.Anything, uint(4)).Return(LogEntry{Index: 4, Term: 2}, nil)
	store.On(methodDeleteLogs, mock.Anything, uint(4), uint(0)).Return(errors.New("db error"))

	_, err := node.HandleAppendEntries(context.Background(), AppendEntriesArgs{
		LeaderID:     "leader-1",
		Term:         5,
		PrevLogIndex: 3,
		PrevLogTerm:  4,
		Entries:      []LogEntry{{Index: 4, Term: 5}},
	})

	assert.Error(t, err)
	assert.Equal(t, "", node.GetLeaderID())
	assert.Equal(t, 0, len(node.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 12. Empty entries (heartbeat) → no AppendLogs call, success ───────────────

func TestAppendEntries_Heartbeat_NoEntries(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(3)).Return(LogEntry{Index: 3, Term: 4}, nil)
	// No entries → no conflict scan hits, no DeleteLogs, no AppendLogs.
	store.On(methodGetLastIndex, mock.Anything).Return(uint(3), nil)

	resp, err := node.HandleAppendEntries(ctx, AppendEntriesArgs{
		LeaderID:     "leader-1",
		Term:         5,
		PrevLogIndex: 3,
		PrevLogTerm:  4,
		Entries:      []LogEntry{},
		LeaderCommit: 5,
	})

	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, "leader-1", node.GetLeaderID())
	assert.Equal(t, 1, len(node.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 13. AppendLogs fails ──────────────────────────────────────────────────────

func TestAppendEntries_DBErr_AppendLogs(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)

	entries := []LogEntry{{Index: 4, Term: 5}}

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(3)).Return(LogEntry{Index: 3, Term: 4}, nil)
	// lastLogIdx is 3, so entry index 4 is genuinely new → no conflict, no DeleteLogs.
	store.On(methodGetLastIndex, mock.Anything).Return(uint(3), nil)
	store.On(methodAppendLogs, mock.Anything, entries).Return(errors.New("db error"))

	_, err := node.HandleAppendEntries(context.Background(), AppendEntriesArgs{
		LeaderID:     "leader-1",
		Term:         5,
		PrevLogIndex: 3,
		PrevLogTerm:  4,
		Entries:      entries,
	})

	assert.Error(t, err)
	assert.Equal(t, "", node.GetLeaderID())
	assert.Equal(t, 0, len(node.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 14. leaderCommit <= commitIndex → no change ───────────────────────────────

func TestAppendEntries_LeaderCommitNotAhead(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	node.commitIndex = 5
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(3)).Return(LogEntry{Index: 3, Term: 4}, nil)
	// GetLastIndex is called exactly ONCE (the conflict scan). Because
	// leaderCommit (4) <= commitIndex (5), the commit block is skipped and does
	// not call it a second time — Times(1) asserts that.
	store.On(methodGetLastIndex, mock.Anything).Return(uint(3), nil).Times(1)

	resp, err := node.HandleAppendEntries(ctx, AppendEntriesArgs{
		LeaderID:     "leader-1",
		Term:         5,
		PrevLogIndex: 3,
		PrevLogTerm:  4,
		LeaderCommit: 4,
	})

	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, uint(5), node.commitIndex) // unchanged
	assert.Equal(t, "leader-1", node.GetLeaderID())
	assert.Equal(t, 1, len(node.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 15. leaderCommit > commitIndex → set to min(leaderCommit, lastLogIdx) ─────

func TestAppendEntries_LeaderCommitAhead_SetsMin(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	node.commitIndex = 2
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(3)).Return(LogEntry{Index: 3, Term: 4}, nil)
	store.On(methodGetLastIndex, mock.Anything).Return(uint(6), nil)

	resp, err := node.HandleAppendEntries(ctx, AppendEntriesArgs{
		LeaderID:     "leader-1",
		Term:         5,
		PrevLogIndex: 3,
		PrevLogTerm:  4,
		LeaderCommit: 8, // min(8, 6) = 6
	})

	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, uint(6), node.commitIndex)
	assert.Equal(t, "leader-1", node.GetLeaderID())
	assert.Equal(t, 1, len(node.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 16. GetLastIndex fails ─────────────────────────────────────────────────

func TestAppendEntries_DBErr_GetLastIndex(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	node.commitIndex = 2

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(3)).Return(LogEntry{Index: 3, Term: 4}, nil)
	store.On(methodGetLastIndex, mock.Anything).Return(uint(0), errors.New("db error"))

	_, err := node.HandleAppendEntries(context.Background(), AppendEntriesArgs{
		LeaderID:     "leader-1",
		Term:         5,
		PrevLogIndex: 3,
		PrevLogTerm:  4,
		LeaderCommit: 8,
	})

	assert.Error(t, err)
	assert.Equal(t, "", node.GetLeaderID())
	assert.Equal(t, 0, len(node.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 17. LeaderID updates from old to new ─────────────────────────────────────

func TestAppendEntries_LeaderIDUpdated(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	node.LeaderID = "old-leader"
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLastIndex, mock.Anything).Return(uint(0), nil)

	resp, err := node.HandleAppendEntries(ctx, AppendEntriesArgs{
		LeaderID:     "new-leader",
		Term:         5,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		LeaderCommit: 1,
	})

	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, "new-leader", node.GetLeaderID())
	assert.Equal(t, 1, len(node.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 18. electionTimeoutCh receives signal on success ─────────────────────────

func TestAppendEntries_ElectionTimeoutReset(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLastIndex, mock.Anything).Return(uint(0), nil)

	resp, err := node.HandleAppendEntries(ctx, AppendEntriesArgs{
		LeaderID:     "leader-1",
		Term:         5,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		LeaderCommit: 1,
	})

	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, "leader-1", node.GetLeaderID())
	assert.Equal(t, 1, len(node.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 19. Happy path ────────────────────────────────────────────────────────────

func TestAppendEntries_HappyPath(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	node.commitIndex = 2
	ctx := context.Background()

	entries := []LogEntry{
		{Index: 4, Term: 5, Data: []byte("cmd1")},
		{Index: 5, Term: 5, Data: []byte("cmd2")},
	}

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(3)).Return(LogEntry{Index: 3, Term: 4}, nil)
	// entries 4,5 are beyond our last index (3) → new, no conflict, no DeleteLogs.
	// GetLastIndex is called twice: first (3) during the conflict scan, then (5)
	// in the commit block after the append has grown the log.
	store.On(methodGetLastIndex, mock.Anything).Return(uint(3), nil).Once()
	store.On(methodAppendLogs, mock.Anything, entries).Return(nil)
	store.On(methodGetLastIndex, mock.Anything).Return(uint(5), nil).Once()

	resp, err := node.HandleAppendEntries(ctx, AppendEntriesArgs{
		LeaderID:     "leader-1",
		Term:         5,
		PrevLogIndex: 3,
		PrevLogTerm:  4,
		Entries:      entries,
		LeaderCommit: 5,
	})

	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, uint64(5), resp.Term)
	assert.Equal(t, uint(5), node.commitIndex)
	assert.Equal(t, "leader-1", node.GetLeaderID())
	assert.Equal(t, 1, len(node.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 20. PrevLogIndex == 0 ────────────────────────────────────────────────────────────
// ── store returns ErrNotFound → fresh follower, continue ───
// This is the key fix: index 0 is a sentinel, ErrNotFound here is NOT
// log inconsistency. Leader is sending from the beginning, allow it.

func TestAppendEntries_PrevLogIndex0_NotFound_Continues(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLastIndex, mock.Anything).Return(uint(0), nil)

	resp, err := node.HandleAppendEntries(ctx, AppendEntriesArgs{
		LeaderID:     "leader-1",
		Term:         5,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		LeaderCommit: 1,
	})

	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, "leader-1", node.GetLeaderID())
	assert.Equal(t, 1, len(node.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 21. PrevLogIndex > 0 ────────────────────────────────────────────────────────────
// ── PrevLogIndex > 0, store returns ErrNotFound → log inconsistency, false ────
// Leader expects a log at prevLogIndex but follower doesn't have it.
// Paper section 5.3 — reply false, leader will back off and retry
// with lower nextIndex until it finds common ground.

func TestAppendEntries_PrevLogIndexNonZero_NotFound_ReturnsFalse(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(5)).Return(LogEntry{}, ErrNotFound)
	// DeleteLogs, AppendLogs, electionTimeoutCh must NOT be touched

	resp, err := node.HandleAppendEntries(ctx, AppendEntriesArgs{
		LeaderID:     "leader-1",
		Term:         5,
		PrevLogIndex: 5, // follower doesn't have this
		PrevLogTerm:  3,
	})

	assert.NoError(t, err)
	assert.False(t, resp.Success)
	assert.Equal(t, uint64(5), resp.Term)
	assert.Equal(t, "", node.GetLeaderID())         // leaderID not set on false
	assert.Equal(t, 0, len(node.electionTimeoutCh)) // no signal on false
	store.AssertExpectations(t)
}

// ── 22. Conflict-only truncation ─────────────────────────────────────────────
// Matching entries at the head are skipped (idempotent); the log is truncated
// only from the first genuine TERM conflict, and only the suffix from that index
// is appended. Entries the follower already holds unchanged must NOT be deleted.

func TestAppendEntries_ConflictOnlyTruncation(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	ctx := context.Background()

	// Leader sends indices 2,3,4 (all term 5). Follower already holds 2 and 3 at
	// term 5 (matching) but 4 at term 2 (conflict).
	entries := []LogEntry{
		{Index: 2, Term: 5},
		{Index: 3, Term: 5},
		{Index: 4, Term: 5},
	}

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(1)).Return(LogEntry{Index: 1, Term: 5}, nil) // prevLog
	store.On(methodGetLastIndex, mock.Anything).Return(uint(4), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(2)).Return(LogEntry{Index: 2, Term: 5}, nil) // match → skip
	store.On(methodGetLogByIndex, mock.Anything, uint(3)).Return(LogEntry{Index: 3, Term: 5}, nil) // match → skip
	store.On(methodGetLogByIndex, mock.Anything, uint(4)).Return(LogEntry{Index: 4, Term: 2}, nil) // conflict
	store.On(methodDeleteLogs, mock.Anything, uint(4), uint(0)).Return(nil)
	// only the conflicting suffix (index 4) is re-appended, not 2 and 3.
	store.On(methodAppendLogs, mock.Anything, []LogEntry{{Index: 4, Term: 5}}).Return(nil)

	resp, err := node.HandleAppendEntries(ctx, AppendEntriesArgs{
		LeaderID:     "leader-1",
		Term:         5,
		PrevLogIndex: 1,
		PrevLogTerm:  5,
		Entries:      entries,
		LeaderCommit: 0,
	})

	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, "leader-1", node.GetLeaderID())
	assert.Equal(t, 1, len(node.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 23. Truncating an uncommitted config entry rolls latest back to committed ──
// If the suffix we truncate contains the entry that produced the latest
// configuration AND the leader's replacement is not itself a config entry, latest
// is no longer backed by the log and must revert to the last committed config.

func TestAppendEntries_TruncationRollsBackLatestConfig(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	ctx := context.Background()

	// committed config (index 1) has just node-2; latest (index 3) additionally
	// staged node-6 via an as-yet-uncommitted config entry.
	node.configurations = configurations{
		committed:      map[string]Peer{"node-2": {PeerState: PeerState_Voter}},
		committedIndex: 1,
		latest: map[string]Peer{
			"node-2": {PeerState: PeerState_Voter},
			"node-6": {PeerState: PeerState_Staging},
		},
		latestIndex: 3,
	}

	// The leader replaces the conflicting index 3 with a plain command, NOT a
	// config entry — so nothing re-applies a config after the rollback.
	entries := []LogEntry{{Index: 3, Term: 5, Type: EntryType_Command}}

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(2)).Return(LogEntry{Index: 2, Term: 5}, nil) // prevLog
	store.On(methodGetLastIndex, mock.Anything).Return(uint(3), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(3)).Return(LogEntry{Index: 3, Term: 2}, nil) // conflict at the config index
	store.On(methodDeleteLogs, mock.Anything, uint(3), uint(0)).Return(nil)
	store.On(methodAppendLogs, mock.Anything, entries).Return(nil)

	resp, err := node.HandleAppendEntries(ctx, AppendEntriesArgs{
		LeaderID:     "leader-1",
		Term:         5,
		PrevLogIndex: 2,
		PrevLogTerm:  5,
		Entries:      entries,
		LeaderCommit: 0,
	})

	assert.NoError(t, err)
	assert.True(t, resp.Success)
	// latest rolled back to committed: node-6 gone, index reverted to 1.
	assert.Equal(t, uint64(1), node.configurations.latestIndex)
	assert.Len(t, node.configurations.latest, 1)
	_, hasStaged := node.configurations.latest["node-6"]
	assert.False(t, hasStaged)
	store.AssertExpectations(t)
}

// ── 24. A new config entry installs the whole config into latest ─────────────
// EntryType_Config entries carry the entire cluster configuration as a JSON
// map[string]Peer. Appending one replaces latest and advances latestIndex.

func TestAppendEntries_ConfigEntryUpdatesLatest(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	ctx := context.Background()

	// Start with a single-voter config committed at index 1.
	node.configurations = configurations{
		committed:      map[string]Peer{"node-2": {PeerState: PeerState_Voter}},
		committedIndex: 1,
		latest:         map[string]Peer{"node-2": {PeerState: PeerState_Voter}},
		latestIndex:    1,
	}

	// Leader's new config (index 2) stages node-6, carried as the full map.
	newConfig := map[string]Peer{
		"node-2": {PeerState: PeerState_Voter},
		"node-6": {PeerState: PeerState_Staging},
	}
	data, _ := json.Marshal(newConfig)
	entries := []LogEntry{{Index: 2, Term: 5, Type: EntryType_Config, Data: data}}

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(1)).Return(LogEntry{Index: 1, Term: 5}, nil) // prevLog
	store.On(methodGetLastIndex, mock.Anything).Return(uint(1), nil)                               // entry 2 is new, no conflict
	store.On(methodAppendLogs, mock.Anything, entries).Return(nil)

	resp, err := node.HandleAppendEntries(ctx, AppendEntriesArgs{
		LeaderID:     "leader-1",
		Term:         5,
		PrevLogIndex: 1,
		PrevLogTerm:  5,
		Entries:      entries,
		LeaderCommit: 0,
	})

	assert.NoError(t, err)
	assert.True(t, resp.Success)
	// latest now reflects the entry's config; committed is untouched.
	assert.Equal(t, uint64(2), node.configurations.latestIndex)
	assert.Len(t, node.configurations.latest, 2)
	assert.Equal(t, PeerState_Staging, node.configurations.latest["node-6"].PeerState)
	assert.Len(t, node.configurations.committed, 1) // committed unchanged
	store.AssertExpectations(t)
}

// ── 25. prevLogIndex at the snapshot boundary → anchored via the cached term ──
// Right after an InstallSnapshot the leader sends prevLogIndex == the follower's
// snapshot last-included index, whose entry is compacted. GetLogByIndex is never
// called for it; the match is validated against snapshotLatestTerm.

func TestAppendEntries_PrevLogAtSnapshotBoundary_Accepted(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	node.SetSnapshotLatest(5, 3, "appendEntry test") // snapshot covers up to index 5, term 3
	ctx := context.Background()

	entries := []LogEntry{{Index: 6, Term: 4}}

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(4), nil)
	// index 5 is compacted; logTermAt resolves it from the snapshot, so NO
	// GetLogByIndex(5) is expected. lastLogIdx is 5, so entry 6 is new.
	store.On(methodGetLastIndex, mock.Anything).Return(uint(5), nil)
	store.On(methodAppendLogs, mock.Anything, entries).Return(nil)

	resp, err := node.HandleAppendEntries(ctx, AppendEntriesArgs{
		LeaderID:     "leader-1",
		Term:         4,
		PrevLogIndex: 5, // the snapshot boundary
		PrevLogTerm:  3, // matches snapshotLatestTerm
		Entries:      entries,
		LeaderCommit: 0,
	})

	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, "leader-1", node.GetLeaderID())
	assert.Equal(t, 1, len(node.electionTimeoutCh))
	store.AssertExpectations(t)
}

// ── 26. prevLogIndex at the snapshot boundary but wrong term → rejected ───────

func TestAppendEntries_PrevLogAtSnapshotBoundary_TermMismatch_Rejected(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	node.SetSnapshotLatest(5, 3, "appendEntry test")
	ctx := context.Background()

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(4), nil)

	resp, err := node.HandleAppendEntries(ctx, AppendEntriesArgs{
		LeaderID:     "leader-1",
		Term:         4,
		PrevLogIndex: 5,
		PrevLogTerm:  99, // does NOT match snapshotLatestTerm (3)
		Entries:      []LogEntry{{Index: 6, Term: 4}},
	})

	assert.NoError(t, err)
	assert.False(t, resp.Success)
	assert.Equal(t, "", node.GetLeaderID())
	assert.Equal(t, 0, len(node.electionTimeoutCh))
	store.AssertExpectations(t)
}
