package db

import (
	"context"
	"testing"

	"github.com/SHREYANSHSINGH14/raft/types"
	"github.com/stretchr/testify/assert"
)

func uintPtr(u uint) *uint {
	return &u
}

func newStore(t *testing.T) *Store {
	dir := t.TempDir()

	store, err := NewStore(context.Background(), dir)
	assert.NoError(t, err)

	return store
}

func TestVotedFor_GetValidResponse(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	nodeID := "node-1"

	err := store.SetVotedFor(ctx, nodeID)
	assert.NoError(t, err, "error setting votedFor")

	id, err := store.GetVotedFor(ctx)
	assert.NoError(t, err, "error getting votedFor")

	assert.Equal(t, nodeID, id, "expected: %s, actual: %s", nodeID, id)
}

func TestVotedFor_EmptyResponse(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	err := store.SetVotedFor(ctx, "")
	assert.NoError(t, err, "error setting votedFor")

	id, err := store.GetVotedFor(ctx)
	assert.NoError(t, err, "error getting votedFor")

	assert.Equal(t, "", id, "expected: empty string, actual: %s", id)
}

func TestVotedFor_KeyNotPresent(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	id, err := store.GetVotedFor(ctx)
	assert.Error(t, err, "error getting votedFor")

	assert.Equal(t, "", id, "expected: empty string, actual: %s", id)
}

func TestCurrentTerm_GetValidResponse(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	err := store.SetCurrentTerm(ctx, 5)
	assert.NoError(t, err, "error setting current term")

	term, err := store.GetCurrentTerm(ctx)
	assert.NoError(t, err, "error getting current term")

	assert.Equal(t, uint(5), term, "expected: %d, actual: %d", uint(5), term)
}

func TestCurrentTerm_KeyNotPresent(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	term, err := store.GetCurrentTerm(ctx)
	assert.Error(t, err, "should throw error when current term key not present")

	assert.Equal(t, uint(0), term, "expected: %d, actual: %d", uint(5), term)
}

func TestAppendLogs_ThenGetLogs(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	logs := []*types.LogEntry{
		{Index: 1, Term: 1, Data: []byte("cmd-1"), ClientRequestId: "c:1"},
		{Index: 2, Term: 1, Data: []byte("cmd-2"), ClientRequestId: "c:2"},
		{Index: 3, Term: 2, Data: []byte("cmd-3"), ClientRequestId: "c:3"},
	}

	err := store.AppendLogs(ctx, logs)
	assert.NoError(t, err)

	got, err := store.GetLogs(ctx, uintPtr(1), uintPtr(4))
	assert.NoError(t, err)
	assert.Len(t, got, 3)

	for i, entry := range got {
		assert.Equal(t, logs[i].Index, entry.Index)
		assert.Equal(t, logs[i].Term, entry.Term)
		assert.Equal(t, logs[i].Data, entry.Data)
		assert.Equal(t, logs[i].ClientRequestId, entry.ClientRequestId)
	}
}

func TestAppendLogs_MultipleAppends_AllPersisted(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	firstBatch := []*types.LogEntry{
		{Index: 1, Term: 1, Data: []byte("cmd-1")},
		{Index: 2, Term: 1, Data: []byte("cmd-2")},
	}
	err := store.AppendLogs(ctx, firstBatch)
	assert.NoError(t, err)

	secondBatch := []*types.LogEntry{
		{Index: 3, Term: 2, Data: []byte("cmd-3")},
		{Index: 4, Term: 2, Data: []byte("cmd-4")},
	}
	err = store.AppendLogs(ctx, secondBatch)
	assert.NoError(t, err)

	got, err := store.GetLogs(ctx, uintPtr(1), uintPtr(5))
	assert.NoError(t, err)
	assert.Len(t, got, 4)
}

func TestAppendLogs_EmptySlice(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	err := store.AppendLogs(ctx, []*types.LogEntry{})
	assert.NoError(t, err)

	idx, err := store.GetLastLogIndex(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint(0), idx)
}

// ── GetLogs with endIdx ───────────────────────────────────────────────────────

func TestGetLogs_WithoutAppending(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	got, err := store.GetLogs(ctx, uintPtr(1), uintPtr(5))
	assert.NoError(t, err)
	assert.Empty(t, got)
}

func TestGetLogs_PartialRange(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	logs := []*types.LogEntry{
		{Index: 1, Term: 1, Data: []byte("cmd-1")},
		{Index: 2, Term: 1, Data: []byte("cmd-2")},
		{Index: 3, Term: 2, Data: []byte("cmd-3")},
		{Index: 4, Term: 2, Data: []byte("cmd-4")},
		{Index: 5, Term: 3, Data: []byte("cmd-5")},
	}
	err := store.AppendLogs(ctx, logs)
	assert.NoError(t, err)

	got, err := store.GetLogs(ctx, uintPtr(2), uintPtr(5)) // indexes 2,3,4
	assert.NoError(t, err)
	assert.Len(t, got, 3)
	assert.Equal(t, []byte("cmd-2"), got[0].Data)
	assert.Equal(t, []byte("cmd-3"), got[1].Data)
	assert.Equal(t, []byte("cmd-4"), got[2].Data)
}

func TestGetLogs_ExclusiveUpperBound(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	logs := []*types.LogEntry{
		{Index: 1, Term: 1, Data: []byte("cmd-1")},
		{Index: 2, Term: 1, Data: []byte("cmd-2")},
		{Index: 3, Term: 1, Data: []byte("cmd-3")},
	}
	err := store.AppendLogs(ctx, logs)
	assert.NoError(t, err)

	got, err := store.GetLogs(ctx, uintPtr(1), uintPtr(3)) // index 3 excluded
	assert.NoError(t, err)
	assert.Len(t, got, 2, "endIdx should be exclusive")
	assert.Equal(t, []byte("cmd-1"), got[0].Data)
	assert.Equal(t, []byte("cmd-2"), got[1].Data)
}

// ── GetLogs with nil endIdx ───────────────────────────────────────────────────

func TestGetLogs_NilEndIdx_ReturnsAllFromStart(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	logs := []*types.LogEntry{
		{Index: 1, Term: 1, Data: []byte("cmd-1")},
		{Index: 2, Term: 1, Data: []byte("cmd-2")},
		{Index: 3, Term: 2, Data: []byte("cmd-3")},
		{Index: 4, Term: 2, Data: []byte("cmd-4")},
	}
	err := store.AppendLogs(ctx, logs)
	assert.NoError(t, err)

	got, err := store.GetLogs(ctx, uintPtr(1), nil)
	assert.NoError(t, err)
	assert.Len(t, got, 4, "nil endIdx should return all logs from startIdx")
}

func TestGetLogs_NilEndIdx_FromMiddle(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	logs := []*types.LogEntry{
		{Index: 1, Term: 1, Data: []byte("cmd-1")},
		{Index: 2, Term: 1, Data: []byte("cmd-2")},
		{Index: 3, Term: 2, Data: []byte("cmd-3")},
		{Index: 4, Term: 2, Data: []byte("cmd-4")},
	}
	err := store.AppendLogs(ctx, logs)
	assert.NoError(t, err)

	got, err := store.GetLogs(ctx, uintPtr(3), nil)
	assert.NoError(t, err)
	assert.Len(t, got, 2, "nil endIdx from index 3 should return indexes 3 and 4")
	assert.Equal(t, uint64(3), got[0].Index)
	assert.Equal(t, uint64(4), got[1].Index)
}

func TestGetLogs_NilEndIdx_EmptyStore(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	got, err := store.GetLogs(ctx, uintPtr(1), nil)
	assert.NoError(t, err)
	assert.Empty(t, got)
}

// ── GetLogByIndex ─────────────────────────────────────────────────────────────

func TestGetLogByIndex_ReturnsCorrectEntry(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	logs := []*types.LogEntry{
		{Index: 1, Term: 1, Data: []byte("cmd-1"), ClientRequestId: "c:1"},
		{Index: 2, Term: 1, Data: []byte("cmd-2"), ClientRequestId: "c:2"},
		{Index: 3, Term: 2, Data: []byte("cmd-3"), ClientRequestId: "c:3"},
	}
	err := store.AppendLogs(ctx, logs)
	assert.NoError(t, err)

	entry, err := store.GetLogByIndex(ctx, 2)
	assert.NoError(t, err)
	assert.NotNil(t, entry)
	assert.Equal(t, uint64(2), entry.Index)
	assert.Equal(t, uint64(1), entry.Term)
	assert.Equal(t, []byte("cmd-2"), entry.Data)
	assert.Equal(t, "c:2", entry.ClientRequestId)
}

func TestGetLogByIndex_FirstEntry(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	logs := []*types.LogEntry{
		{Index: 1, Term: 3, Data: []byte("cmd-1")},
		{Index: 2, Term: 3, Data: []byte("cmd-2")},
	}
	err := store.AppendLogs(ctx, logs)
	assert.NoError(t, err)

	entry, err := store.GetLogByIndex(ctx, 1)
	assert.NoError(t, err)
	assert.NotNil(t, entry)
	assert.Equal(t, uint64(1), entry.Index)
	assert.Equal(t, uint64(3), entry.Term)
}

func TestGetLogByIndex_LastEntry(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	logs := []*types.LogEntry{
		{Index: 1, Term: 1, Data: []byte("cmd-1")},
		{Index: 2, Term: 2, Data: []byte("cmd-2")},
		{Index: 3, Term: 3, Data: []byte("cmd-3")},
	}
	err := store.AppendLogs(ctx, logs)
	assert.NoError(t, err)

	entry, err := store.GetLogByIndex(ctx, 3)
	assert.NoError(t, err)
	assert.NotNil(t, entry)
	assert.Equal(t, uint64(3), entry.Index)
	assert.Equal(t, uint64(3), entry.Term)
	assert.Equal(t, []byte("cmd-3"), entry.Data)
}

func TestGetLogByIndex_NotPresent(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	entry, err := store.GetLogByIndex(ctx, 99)
	assert.Error(t, err, "expected error for missing index")
	assert.Nil(t, entry)
}

func TestGetLogByIndex_EmptyStore(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	entry, err := store.GetLogByIndex(ctx, 1)
	assert.Error(t, err)
	assert.Nil(t, entry)
}

// ── GetLogsByTerm ─────────────────────────────────────────────────────────────

func TestGetLogsByTerm_SingleTerm(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	logs := []*types.LogEntry{
		{Index: 1, Term: 1, Data: []byte("cmd-1")},
		{Index: 2, Term: 1, Data: []byte("cmd-2")},
		{Index: 3, Term: 2, Data: []byte("cmd-3")},
	}
	err := store.AppendLogs(ctx, logs)
	assert.NoError(t, err)

	got, err := store.GetLogsByTerm(ctx, 1)
	assert.NoError(t, err)
	assert.Len(t, got, 2)
	for _, entry := range got {
		assert.Equal(t, uint64(1), entry.Term)
	}
}

func TestGetLogsByTerm_MultipleTerms(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	logs := []*types.LogEntry{
		{Index: 1, Term: 1, Data: []byte("cmd-1")},
		{Index: 2, Term: 1, Data: []byte("cmd-2")},
		{Index: 3, Term: 2, Data: []byte("cmd-3")},
		{Index: 4, Term: 3, Data: []byte("cmd-4")},
		{Index: 5, Term: 3, Data: []byte("cmd-5")},
	}
	err := store.AppendLogs(ctx, logs)
	assert.NoError(t, err)

	term1, err := store.GetLogsByTerm(ctx, 1)
	assert.NoError(t, err)
	assert.Len(t, term1, 2)

	term2, err := store.GetLogsByTerm(ctx, 2)
	assert.NoError(t, err)
	assert.Len(t, term2, 1)
	assert.Equal(t, []byte("cmd-3"), term2[0].Data)

	term3, err := store.GetLogsByTerm(ctx, 3)
	assert.NoError(t, err)
	assert.Len(t, term3, 2)
}

func TestGetLogsByTerm_TermNotPresent(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	// terms 1 and 3 exist, term 2 does not
	// since we do a full scan with no early break, this must still return empty
	logs := []*types.LogEntry{
		{Index: 1, Term: 1, Data: []byte("cmd-1")},
		{Index: 2, Term: 3, Data: []byte("cmd-2")},
	}
	err := store.AppendLogs(ctx, logs)
	assert.NoError(t, err)

	got, err := store.GetLogsByTerm(ctx, 2)
	assert.NoError(t, err)
	assert.Empty(t, got)
}

func TestGetLogsByTerm_EmptyStore(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	got, err := store.GetLogsByTerm(ctx, 1)
	assert.NoError(t, err)
	assert.Empty(t, got)
}

// ── TruncateLogs ─────────────────────────────────────────────────────────────

func TestTruncateLogs_RemovesFromIndexOnwards(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	logs := []*types.LogEntry{
		{Index: 1, Term: 1, Data: []byte("cmd-1")},
		{Index: 2, Term: 1, Data: []byte("cmd-2")},
		{Index: 3, Term: 2, Data: []byte("cmd-3")},
		{Index: 4, Term: 2, Data: []byte("cmd-4")},
	}
	err := store.AppendLogs(ctx, logs)
	assert.NoError(t, err)

	err = store.TruncateLogs(ctx, 3)
	assert.NoError(t, err)

	// range scan confirms only 1 and 2 remain
	got, err := store.GetLogs(ctx, uintPtr(1), uintPtr(5))
	assert.NoError(t, err)
	assert.Len(t, got, 2)
	assert.Equal(t, uint64(1), got[0].Index)
	assert.Equal(t, uint64(2), got[1].Index)

	// point lookups confirm 3 and 4 are actually deleted
	entry, err := store.GetLogByIndex(ctx, 3)
	assert.Error(t, err, "index 3 should be deleted")
	assert.Nil(t, entry)

	entry, err = store.GetLogByIndex(ctx, 4)
	assert.Error(t, err, "index 4 should be deleted")
	assert.Nil(t, entry)
}

func TestTruncateLogs_FromFirstIndex_RemovesAll(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	logs := []*types.LogEntry{
		{Index: 1, Term: 1, Data: []byte("cmd-1")},
		{Index: 2, Term: 1, Data: []byte("cmd-2")},
		{Index: 3, Term: 2, Data: []byte("cmd-3")},
	}
	err := store.AppendLogs(ctx, logs)
	assert.NoError(t, err)

	err = store.TruncateLogs(ctx, 1)
	assert.NoError(t, err)

	got, err := store.GetLogs(ctx, uintPtr(1), uintPtr(4))
	assert.NoError(t, err)
	assert.Empty(t, got, "all logs should be removed when truncating from index 1")

	idx, err := store.GetLastLogIndex(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint(0), idx)
}

func TestTruncateLogs_ThenReappend(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	logs := []*types.LogEntry{
		{Index: 1, Term: 1, Data: []byte("cmd-1")},
		{Index: 2, Term: 1, Data: []byte("cmd-2")},
		{Index: 3, Term: 2, Data: []byte("cmd-3")},
	}
	err := store.AppendLogs(ctx, logs)
	assert.NoError(t, err)

	err = store.TruncateLogs(ctx, 2)
	assert.NoError(t, err)

	idx, err := store.GetLastLogIndex(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint(1), idx, "last index should reflect truncation")

	// re-append from where truncation left off
	newLogs := []*types.LogEntry{
		{Index: 2, Term: 3, Data: []byte("cmd-new-2")},
		{Index: 3, Term: 3, Data: []byte("cmd-new-3")},
	}
	err = store.AppendLogs(ctx, newLogs)
	assert.NoError(t, err)

	entry, err := store.GetLogByIndex(ctx, 2)
	assert.NoError(t, err)
	assert.Equal(t, []byte("cmd-new-2"), entry.Data)
	assert.Equal(t, uint64(3), entry.Term)

	entry, err = store.GetLogByIndex(ctx, 3)
	assert.NoError(t, err)
	assert.Equal(t, []byte("cmd-new-3"), entry.Data)
}

func TestTruncateLogs_EmptyStore(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	err := store.TruncateLogs(ctx, 1)
	assert.NoError(t, err, "truncating empty store should not error")
}
