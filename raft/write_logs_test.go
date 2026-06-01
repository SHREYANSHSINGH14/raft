package raft

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func setupProposeTest(t *testing.T) (*Node, *MockStorage) {
	t.Helper()
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	node.Role = ServerRole_Leader
	return node, store
}

// proposeAsync runs Propose in a goroutine and returns a channel that receives
// the error when Propose returns.
func proposeAsync(node *Node, ctx context.Context, data []byte) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- node.Propose(ctx, data) }()
	return ch
}

// ── pre-condition checks ──────────────────────────────────────────────────────

// 1. Node is not leader → error before any DB call
func TestPropose_NotLeader_ReturnsError(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	// Role defaults to Follower

	err := node.Propose(context.Background(), []byte("cmd"))

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not the leader")
	store.AssertNotCalled(t, methodGetLastLogIndex, mock.Anything)
}

// ── DB error cases ────────────────────────────────────────────────────────────

// 2. GetLastLogIndex fails → error, nothing appended
func TestPropose_GetLastLogIndexFails_ReturnsError(t *testing.T) {
	node, store := setupProposeTest(t)

	store.On(methodGetLastLogIndex, mock.Anything).Return(uint(0), errors.New("db error"))

	err := node.Propose(context.Background(), []byte("cmd"))

	assert.Error(t, err)
	store.AssertNotCalled(t, methodGetCurrentTerm, mock.Anything)
	store.AssertExpectations(t)
}

// 3. GetCurrentTerm fails → error, nothing appended
func TestPropose_GetCurrentTermFails_ReturnsError(t *testing.T) {
	node, store := setupProposeTest(t)

	store.On(methodGetLastLogIndex, mock.Anything).Return(uint(0), nil)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(0), errors.New("db error"))

	err := node.Propose(context.Background(), []byte("cmd"))

	assert.Error(t, err)
	store.AssertNotCalled(t, methodAppendLogs, mock.Anything, mock.Anything)
	store.AssertExpectations(t)
}

// 4. AppendLogs fails → error, Propose never blocks on commitCond
func TestPropose_AppendLogsFails_ReturnsError(t *testing.T) {
	node, store := setupProposeTest(t)

	expected := LogEntry{Index: 1, Term: 5, Data: []byte("cmd")}
	store.On(methodGetLastLogIndex, mock.Anything).Return(uint(0), nil)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodAppendLogs, mock.Anything, []LogEntry{expected}).Return(errors.New("db error"))

	err := node.Propose(context.Background(), []byte("cmd"))

	assert.Error(t, err)
	store.AssertExpectations(t)
}

// ── wait condition ────────────────────────────────────────────────────────────

// 5. commitIndex already >= entry.Index before Propose acquires the lock →
// returns nil without ever calling Wait
func TestPropose_CommitIndexAlreadyAhead_ReturnsNilImmediately(t *testing.T) {
	node, store := setupProposeTest(t)
	node.SetCommitIndex(10)

	expected := LogEntry{Index: 1, Term: 5, Data: []byte("cmd")}
	store.On(methodGetLastLogIndex, mock.Anything).Return(uint(0), nil)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodAppendLogs, mock.Anything, []LogEntry{expected}).Return(nil)

	err := node.Propose(context.Background(), []byte("cmd"))

	assert.NoError(t, err)
	store.AssertExpectations(t)
}

// 6. commitIndex starts below entry.Index; single broadcast advances to
// exactly entry.Index → Propose unblocks and returns nil
func TestPropose_CommitIndexAdvancesToEntryIndex_ReturnsNil(t *testing.T) {
	node, store := setupProposeTest(t)

	expected := LogEntry{Index: 1, Term: 5, Data: []byte("cmd")}
	appended := make(chan struct{})
	store.On(methodGetLastLogIndex, mock.Anything).Return(uint(0), nil)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodAppendLogs, mock.Anything, []LogEntry{expected}).
		Run(func(_ mock.Arguments) { close(appended) }).
		Return(nil)

	errCh := proposeAsync(node, context.Background(), []byte("cmd"))

	awaitCall(t, appended, "AppendLogs")
	time.Sleep(10 * time.Millisecond) // let goroutine reach Wait

	node.SetCommitIndex(1)

	assert.NoError(t, <-errCh)
	store.AssertExpectations(t)
}

// 7. Multiple broadcasts with commitIndex below entry.Index; goroutine re-waits
// on each one and only unblocks when commitIndex finally reaches entry.Index
func TestPropose_MultiplePartialBroadcasts_EventuallyReturnsNil(t *testing.T) {
	node, store := setupProposeTest(t)

	// entry will land at index 3 (lastLogIndex=2, +1)
	expected := LogEntry{Index: 3, Term: 5, Data: []byte("cmd")}
	appended := make(chan struct{})
	store.On(methodGetLastLogIndex, mock.Anything).Return(uint(2), nil)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodAppendLogs, mock.Anything, []LogEntry{expected}).
		Run(func(_ mock.Arguments) { close(appended) }).
		Return(nil)

	errCh := proposeAsync(node, context.Background(), []byte("cmd"))

	awaitCall(t, appended, "AppendLogs")
	time.Sleep(10 * time.Millisecond) // let goroutine reach Wait

	// each advance is below entry.Index — goroutine wakes, re-checks, re-waits
	node.SetCommitIndex(1) // 1 < 3 → re-waits
	time.Sleep(5 * time.Millisecond)
	node.SetCommitIndex(2) // 2 < 3 → re-waits
	time.Sleep(5 * time.Millisecond)
	node.SetCommitIndex(3) // 3 >= 3 → unblocks

	assert.NoError(t, <-errCh)
	store.AssertExpectations(t)
}

// ── context cancellation ──────────────────────────────────────────────────────

// 8. Context cancelled while goroutine is blocked on Wait → returns error
func TestPropose_ContextCancelledWhileWaiting_ReturnsError(t *testing.T) {
	node, store := setupProposeTest(t)
	ctx, cancel := context.WithCancel(context.Background())

	expected := LogEntry{Index: 1, Term: 5, Data: []byte("cmd")}
	appended := make(chan struct{})
	store.On(methodGetLastLogIndex, mock.Anything).Return(uint(0), nil)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodAppendLogs, mock.Anything, []LogEntry{expected}).
		Run(func(_ mock.Arguments) { close(appended) }).
		Return(nil)

	errCh := proposeAsync(node, ctx, []byte("cmd"))

	awaitCall(t, appended, "AppendLogs")
	time.Sleep(10 * time.Millisecond) // let goroutine reach Wait

	cancel()
	node.commitCond.Broadcast() // unblock Wait so goroutine can observe ctx.Err

	err := <-errCh
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "context cancelled")
	store.AssertExpectations(t)
}

// 9. Context already cancelled before Propose acquires commitCond lock →
// inner-loop condition short-circuits, returns error without waiting
func TestPropose_ContextAlreadyCancelled_ReturnsError(t *testing.T) {
	node, store := setupProposeTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancelled before Propose is called

	expected := LogEntry{Index: 1, Term: 5, Data: []byte("cmd")}
	store.On(methodGetLastLogIndex, mock.Anything).Return(uint(0), nil)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodAppendLogs, mock.Anything, []LogEntry{expected}).Return(nil)

	err := node.Propose(ctx, []byte("cmd"))

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "context cancelled")
	store.AssertExpectations(t)
}
