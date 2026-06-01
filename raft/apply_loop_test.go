package raft

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const (
	methodGetLastApplied = "GetLastApplied"
	methodSetLastApplied = "SetLastApplied"
	methodGetLogs        = "GetLogs"
	methodApply          = "Apply"
)

// awaitCall blocks until done is closed, failing the test if it takes too long.
func awaitCall(t *testing.T, done <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timed out waiting for %s", label)
	}
}

// ── Startup ───────────────────────────────────────────────────────────────────

// 1. GetLastApplied fails at startup → goroutine exits, nothing applied
func TestApplyLoop_GetLastAppliedFails_GoroutineExits(t *testing.T) {
	store := new(MockStorage)
	sm := new(MockStateMachine)
	node := NewNodeMock(store, sm)

	called := make(chan struct{})
	store.On(methodGetLastApplied, mock.Anything).
		Run(func(_ mock.Arguments) { close(called) }).
		Return(uint(0), errors.New("db error"))

	node.startApplyLoop(context.Background())

	awaitCall(t, called, "GetLastApplied")
	time.Sleep(20 * time.Millisecond)

	sm.AssertNotCalled(t, methodApply, mock.Anything)
	store.AssertExpectations(t)
}

// 2. GetLastApplied returns non-zero → apply loop starts from correct offset, not 0
func TestApplyLoop_NonZeroLastApplied_StartsFromCorrectOffset(t *testing.T) {
	store := new(MockStorage)
	sm := new(MockStateMachine)
	node := NewNodeMock(store, sm)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// lastApplied = 2 → first fetch must be [3, 4)
	start, end := uint(3), uint(4)
	entries := []LogEntry{{Index: 3, Term: 1, Data: []byte("c")}}

	done := make(chan struct{})
	store.On(methodGetLastApplied, mock.Anything).Return(uint(2), nil)
	store.On(methodGetLogs, mock.Anything, &start, &end).Return(entries, nil)
	sm.On(methodApply, entries).Return(nil)
	store.On(methodSetLastApplied, mock.Anything, uint(3)).
		Run(func(_ mock.Arguments) { close(done) }).
		Return(nil)

	node.startApplyLoop(ctx)
	node.SetCommitIndex(3)

	awaitCall(t, done, "SetLastApplied")

	store.AssertExpectations(t)
	sm.AssertExpectations(t)
}

// ── Happy path ────────────────────────────────────────────────────────────────

// 3. Single broadcast, entries 1-3 committed → GetLogs called with correct range,
// Apply called with correct entries, SetLastApplied called with 3
func TestApplyLoop_SingleBroadcast_AppliesEntriesAndPersists(t *testing.T) {
	store := new(MockStorage)
	sm := new(MockStateMachine)
	node := NewNodeMock(store, sm)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	entries := []LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	}
	start, end := uint(1), uint(4)

	done := make(chan struct{})
	store.On(methodGetLastApplied, mock.Anything).Return(uint(0), nil)
	store.On(methodGetLogs, mock.Anything, &start, &end).Return(entries, nil)
	sm.On(methodApply, entries).Return(nil)
	store.On(methodSetLastApplied, mock.Anything, uint(3)).
		Run(func(_ mock.Arguments) { close(done) }).
		Return(nil)

	node.startApplyLoop(ctx)
	node.SetCommitIndex(3)

	awaitCall(t, done, "SetLastApplied")

	store.AssertExpectations(t)
	sm.AssertExpectations(t)
}

// ── The race case ─────────────────────────────────────────────────────────────

// 4. Single broadcast with commitIndex=3; during Apply commitIndex advances to 6
// → second iteration applies 4-6 without a second broadcast
func TestApplyLoop_CommitAdvancesDuringApply_SecondIterationAppliesWithoutBroadcast(t *testing.T) {
	store := new(MockStorage)
	sm := new(MockStateMachine)
	node := NewNodeMock(store, sm)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	entries1 := []LogEntry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}}
	entries2 := []LogEntry{{Index: 4, Term: 1}, {Index: 5, Term: 1}, {Index: 6, Term: 1}}
	start1, end1 := uint(1), uint(4)
	start2, end2 := uint(4), uint(7)

	done := make(chan struct{})
	store.On(methodGetLastApplied, mock.Anything).Return(uint(0), nil)
	store.On(methodGetLogs, mock.Anything, &start1, &end1).Return(entries1, nil)
	store.On(methodGetLogs, mock.Anything, &start2, &end2).Return(entries2, nil)
	sm.On(methodApply, entries1).
		Run(func(_ mock.Arguments) {
			// advance commitIndex while the goroutine is outside commitMu (slow work)
			node.SetCommitIndex(6)
		}).
		Return(nil)
	sm.On(methodApply, entries2).Return(nil)
	store.On(methodSetLastApplied, mock.Anything, uint(3)).Return(nil)
	store.On(methodSetLastApplied, mock.Anything, uint(6)).
		Run(func(_ mock.Arguments) { close(done) }).
		Return(nil)

	node.startApplyLoop(ctx)
	node.SetCommitIndex(3)

	awaitCall(t, done, "second SetLastApplied")

	store.AssertExpectations(t)
	sm.AssertExpectations(t)
}

// ── Spurious wakeup ───────────────────────────────────────────────────────────

// 5. Broadcast fires but commitIndex == lastApplied → Apply never called,
// SetLastApplied never called, goroutine stays alive
func TestApplyLoop_SpuriousWakeup_NothingApplied(t *testing.T) {
	store := new(MockStorage)
	sm := new(MockStateMachine)
	node := NewNodeMock(store, sm)
	ctx, cancel := context.WithCancel(context.Background())

	store.On(methodGetLastApplied, mock.Anything).Return(uint(0), nil)

	node.startApplyLoop(ctx)

	// commitIndex is still 0 — broadcast should not trigger an apply
	node.commitCond.Broadcast()
	time.Sleep(50 * time.Millisecond)

	sm.AssertNotCalled(t, methodApply, mock.Anything)
	store.AssertNotCalled(t, methodSetLastApplied, mock.Anything, mock.Anything)

	cancel()
	node.commitCond.Broadcast() // unblock Wait so goroutine can see ctx.Err
	time.Sleep(20 * time.Millisecond)
}

// ── Error exits ───────────────────────────────────────────────────────────────

// 6. GetLogs fails → goroutine exits
func TestApplyLoop_GetLogsFails_GoroutineExits(t *testing.T) {
	store := new(MockStorage)
	sm := new(MockStateMachine)
	node := NewNodeMock(store, sm)

	start, end := uint(1), uint(4)
	called := make(chan struct{})
	store.On(methodGetLastApplied, mock.Anything).Return(uint(0), nil)
	store.On(methodGetLogs, mock.Anything, &start, &end).
		Run(func(_ mock.Arguments) { close(called) }).
		Return(nil, errors.New("db error"))

	node.startApplyLoop(context.Background())
	node.SetCommitIndex(3)

	awaitCall(t, called, "GetLogs")
	time.Sleep(20 * time.Millisecond)

	sm.AssertNotCalled(t, methodApply, mock.Anything)
	store.AssertExpectations(t)
}

// 7. sm.Apply fails → goroutine exits
func TestApplyLoop_ApplyFails_GoroutineExits(t *testing.T) {
	store := new(MockStorage)
	sm := new(MockStateMachine)
	node := NewNodeMock(store, sm)

	start, end := uint(1), uint(4)
	entries := []LogEntry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}}
	called := make(chan struct{})
	store.On(methodGetLastApplied, mock.Anything).Return(uint(0), nil)
	store.On(methodGetLogs, mock.Anything, &start, &end).Return(entries, nil)
	sm.On(methodApply, entries).
		Run(func(_ mock.Arguments) { close(called) }).
		Return(errors.New("apply error"))

	node.startApplyLoop(context.Background())
	node.SetCommitIndex(3)

	awaitCall(t, called, "Apply")
	time.Sleep(20 * time.Millisecond)

	store.AssertNotCalled(t, methodSetLastApplied, mock.Anything, mock.Anything)
	store.AssertExpectations(t)
	sm.AssertExpectations(t)
}

// 8. SetLastApplied fails → goroutine exits
func TestApplyLoop_SetLastAppliedFails_GoroutineExits(t *testing.T) {
	store := new(MockStorage)
	sm := new(MockStateMachine)
	node := NewNodeMock(store, sm)

	start, end := uint(1), uint(4)
	entries := []LogEntry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}}
	called := make(chan struct{})
	store.On(methodGetLastApplied, mock.Anything).Return(uint(0), nil)
	store.On(methodGetLogs, mock.Anything, &start, &end).Return(entries, nil)
	sm.On(methodApply, entries).Return(nil)
	store.On(methodSetLastApplied, mock.Anything, uint(3)).
		Run(func(_ mock.Arguments) { close(called) }).
		Return(errors.New("db error"))

	node.startApplyLoop(context.Background())
	node.SetCommitIndex(3)

	awaitCall(t, called, "SetLastApplied")
	time.Sleep(20 * time.Millisecond)

	store.AssertExpectations(t)
	sm.AssertExpectations(t)
}

// ── Context cancellation ──────────────────────────────────────────────────────

// 9. Context cancelled while blocked on Wait() → goroutine exits cleanly
func TestApplyLoop_ContextCancelledWhileWaiting_GoroutineExits(t *testing.T) {
	store := new(MockStorage)
	sm := new(MockStateMachine)
	node := NewNodeMock(store, sm)
	ctx, cancel := context.WithCancel(context.Background())

	store.On(methodGetLastApplied, mock.Anything).Return(uint(0), nil)

	node.startApplyLoop(ctx)
	time.Sleep(20 * time.Millisecond) // let goroutine reach Wait()

	cancel()
	node.commitCond.Broadcast() // unblock Wait so goroutine can observe ctx.Err
	time.Sleep(20 * time.Millisecond)

	sm.AssertNotCalled(t, methodApply, mock.Anything)
	store.AssertExpectations(t)
}

// 10. Context cancelled between iterations (after apply, before reacquire) →
// goroutine exits without applying subsequent entries
func TestApplyLoop_ContextCancelledBetweenIterations_GoroutineExits(t *testing.T) {
	store := new(MockStorage)
	sm := new(MockStateMachine)
	node := NewNodeMock(store, sm)
	ctx, cancel := context.WithCancel(context.Background())

	start1, end1 := uint(1), uint(4)
	entries1 := []LogEntry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}}

	firstCycleDone := make(chan struct{})
	store.On(methodGetLastApplied, mock.Anything).Return(uint(0), nil)
	store.On(methodGetLogs, mock.Anything, &start1, &end1).Return(entries1, nil)
	sm.On(methodApply, entries1).Return(nil)
	store.On(methodSetLastApplied, mock.Anything, uint(3)).
		Run(func(_ mock.Arguments) { close(firstCycleDone) }).
		Return(nil)

	node.startApplyLoop(ctx)
	node.SetCommitIndex(3)

	awaitCall(t, firstCycleDone, "first SetLastApplied")

	// cancel between iterations; advance commitIndex to show it won't be applied
	cancel()
	node.SetCommitIndex(6)
	node.commitCond.Broadcast() // unblock Wait if the goroutine re-entered it
	time.Sleep(20 * time.Millisecond)

	assert.Equal(t, 1, len(sm.Calls))
	store.AssertExpectations(t)
	sm.AssertExpectations(t)
}

// ── Boundary ──────────────────────────────────────────────────────────────────

// 11. commitIndex=0, lastApplied=0 → broadcast fires, inner loop blocks correctly,
// nothing applied
func TestApplyLoop_CommitIndexZero_BroadcastDoesNotApply(t *testing.T) {
	store := new(MockStorage)
	sm := new(MockStateMachine)
	node := NewNodeMock(store, sm)
	ctx, cancel := context.WithCancel(context.Background())

	store.On(methodGetLastApplied, mock.Anything).Return(uint(0), nil)

	node.startApplyLoop(ctx)

	// broadcast with commitIndex still 0 — inner loop condition (0 <= 0) stays true
	node.commitCond.Broadcast()
	time.Sleep(50 * time.Millisecond)

	sm.AssertNotCalled(t, methodApply, mock.Anything)
	store.AssertNotCalled(t, methodGetLogs, mock.Anything, mock.Anything, mock.Anything)
	store.AssertNotCalled(t, methodSetLastApplied, mock.Anything, mock.Anything)

	cancel()
	node.commitCond.Broadcast()
	time.Sleep(20 * time.Millisecond)
}
