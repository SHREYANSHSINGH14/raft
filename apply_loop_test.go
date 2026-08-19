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
	methodGetLogs = "GetLogs"
	methodApply   = "Apply"
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
// lastApplied is volatile node state, so the startup read cannot fail and there is no
// "store error at startup" path left to test. What is still worth pinning is what that
// read is FOR: the loop seeds from the node's own lastApplied, and applies nothing at
// or below it — a loop that seeded from zero would re-apply the whole log on a node
// that had already consumed it.
func TestApplyLoop_SeedsLastAppliedFromNode(t *testing.T) {
	store := new(MockStorage)
	sm := new(MockStateMachine)
	node := NewNodeMock(store, sm)

	node.SetLastApplied(5)
	node.SetCommitIndex(5) // nothing committed above what is already applied

	node.startApplyLoop(context.Background())
	time.Sleep(20 * time.Millisecond)

	sm.AssertNotCalled(t, methodApply, mock.Anything, mock.Anything)
	store.AssertNotCalled(t, methodGetLogs, mock.Anything, mock.Anything, mock.Anything)
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

	// lastApplied lives on the node now, not in the store, so it is seeded directly
	// and its advance is observed the same way — there is no SetLastApplied call to
	// hang the assertion on.
	node.SetLastApplied(2)

	done := make(chan struct{})
	store.On(methodGetLogs, mock.Anything, &start, &end).Return(entries, nil)
	sm.On(methodApply, mock.Anything, entries).
		Run(func(_ mock.Arguments) { close(done) }).
		Return(nil)

	node.startApplyLoop(ctx)
	node.SetCommitIndex(3)

	awaitCall(t, done, "Apply")

	assert.Equal(t, uint(3), node.GetLastApplied())
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
	store.On(methodGetLogs, mock.Anything, &start, &end).Return(entries, nil)
	sm.On(methodApply, mock.Anything, entries).
		Run(func(_ mock.Arguments) { close(done) }).
		Return(nil)

	node.startApplyLoop(ctx)
	node.SetCommitIndex(3)

	awaitCall(t, done, "Apply")

	assert.Equal(t, uint(3), node.GetLastApplied())

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
	store.On(methodGetLogs, mock.Anything, &start1, &end1).Return(entries1, nil)
	store.On(methodGetLogs, mock.Anything, &start2, &end2).Return(entries2, nil)
	sm.On(methodApply, mock.Anything, entries1).
		Run(func(_ mock.Arguments) {
			// advance commitIndex while the goroutine is outside commitMu (slow work)
			node.SetCommitIndex(6)
		}).
		Return(nil)
	sm.On(methodApply, mock.Anything, entries2).
		Run(func(_ mock.Arguments) { close(done) }).
		Return(nil)

	node.startApplyLoop(ctx)
	node.SetCommitIndex(3)

	awaitCall(t, done, "second Apply")

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

	node.startApplyLoop(ctx)

	// commitIndex is still 0 — a wake-up should not trigger an apply
	node.signalCommit()
	time.Sleep(50 * time.Millisecond)

	sm.AssertNotCalled(t, methodApply, mock.Anything, mock.Anything)

	cancel()
	node.signalCommit() // wake the loop so it can observe ctx.Err
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
	store.On(methodGetLogs, mock.Anything, &start, &end).
		Run(func(_ mock.Arguments) { close(called) }).
		Return(nil, errors.New("db error"))

	node.startApplyLoop(context.Background())
	node.SetCommitIndex(3)

	awaitCall(t, called, "GetLogs")
	time.Sleep(20 * time.Millisecond)

	sm.AssertNotCalled(t, methodApply, mock.Anything, mock.Anything)
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
	store.On(methodGetLogs, mock.Anything, &start, &end).Return(entries, nil)
	sm.On(methodApply, mock.Anything, entries).
		Run(func(_ mock.Arguments) { close(called) }).
		Return(errors.New("apply error"))

	node.startApplyLoop(context.Background())
	node.SetCommitIndex(3)

	awaitCall(t, called, "Apply")
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

	node.startApplyLoop(ctx)
	time.Sleep(20 * time.Millisecond) // let goroutine reach Wait()

	cancel()
	node.signalCommit() // wake the loop so it can observe ctx.Err
	time.Sleep(20 * time.Millisecond)

	sm.AssertNotCalled(t, methodApply, mock.Anything, mock.Anything)
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
	store.On(methodGetLogs, mock.Anything, &start1, &end1).Return(entries1, nil)
	sm.On(methodApply, mock.Anything, entries1).
		Run(func(_ mock.Arguments) { close(firstCycleDone) }).
		Return(nil)

	node.startApplyLoop(ctx)
	node.SetCommitIndex(3)

	awaitCall(t, firstCycleDone, "first Apply")

	// cancel between iterations; advance commitIndex to show it won't be applied
	cancel()
	node.SetCommitIndex(6)
	node.signalCommit() // wake the loop in case it went back to waiting
	time.Sleep(20 * time.Millisecond)

	assert.Equal(t, 1, len(sm.Calls))
	store.AssertExpectations(t)
	sm.AssertExpectations(t)
}

// ── Boundary ──────────────────────────────────────────────────────────────────

// 11. commitIndex=0, lastApplied=0 → a wake-up fires, inner loop blocks correctly,
// nothing applied
func TestApplyLoop_CommitIndexZero_WakeDoesNotApply(t *testing.T) {
	store := new(MockStorage)
	sm := new(MockStateMachine)
	node := NewNodeMock(store, sm)
	ctx, cancel := context.WithCancel(context.Background())

	node.startApplyLoop(ctx)

	// wake with commitIndex still 0 — inner loop condition (0 <= 0) stays true
	node.signalCommit()
	time.Sleep(50 * time.Millisecond)

	sm.AssertNotCalled(t, methodApply, mock.Anything, mock.Anything)
	store.AssertNotCalled(t, methodGetLogs, mock.Anything, mock.Anything, mock.Anything)

	cancel()
	node.signalCommit()
	time.Sleep(20 * time.Millisecond)
}

// ── Signalling: the sender must never be able to wedge the loop ───────────────
//
// Both of these guard the commitMu/commitCh interaction, which has now produced
// four distinct hangs: a spin holding the lock, a double unlock, a self-deadlock
// on a non-reentrant Lock, and a blocking send made while holding the very lock
// the loop needs to drain the channel. None of the tests above catch any of them,
// because they all advance the commit index before the loop has parked — the one
// ordering a real node never has.

// 12. Commits keep arriving while the loop is inside a slow Apply. commitCh is
// buffered, so a burst fills it; the senders must not wedge, and the loop must
// still finish. The gate matters: without proof that the loop is inside Apply,
// the burst can complete before the goroutine is even scheduled and the test
// passes without exercising anything.
func TestApplyLoop_CommitBurstDuringSlowApply_DoesNotWedge(t *testing.T) {
	store := new(MockStorage)
	sm := new(MockStateMachine)
	node := NewNodeMock(store, sm)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	applying := make(chan struct{})
	store.On(methodGetLogs, mock.Anything, mock.Anything, mock.Anything).
		Return([]LogEntry{{Index: 1, Term: 1}}, nil)
	sm.On(methodApply, mock.Anything, mock.Anything).
		Run(func(_ mock.Arguments) {
			select {
			case applying <- struct{}{}: // only the first Apply reports in
			default:
			}
			time.Sleep(300 * time.Millisecond)
		}).
		Return(nil)

	node.startApplyLoop(ctx)
	node.SetCommitIndex(1)

	<-applying // the loop is now provably inside Apply, not at its receive

	done := make(chan struct{})
	go func() {
		defer close(done)
		// more commits than commitCh can hold
		for i := 2; i <= 40; i++ {
			node.SetCommitIndex(uint(i))
		}
		// the other two senders: a step-down and a finished snapshot
		for i := 0; i < 20; i++ {
			node.setLeaderCloseCh()
			node.clearLeaderCloseCh()
			node.signalCommit()
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a sender wedged: commitCh filled while the loop could not reach its receive")
	}
}

// 13. A commit index below the current one is ignored — and the early return must
// still release commitMu. Leaking it there wedges every commit, every apply and
// every future for the life of the process, and no other test takes that branch
// because they all advance monotonically.
func TestSetCommitIndex_LowerIndex_IgnoredAndReleasesLock(t *testing.T) {
	node := NewNodeMock(new(MockStorage), nil)

	node.SetCommitIndex(5)
	node.SetCommitIndex(3) // lower — takes the early return

	done := make(chan struct{})
	go func() {
		defer close(done)
		assert.Equal(t, uint(5), node.GetCommitIndex(), "a lower index must not move commitIndex")
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("commitMu was leaked by the early return in SetCommitIndex")
	}
}

// ── Fatal ─────────────────────────────────────────────────────────────────────

// awaitFatal blocks until the node reports a fatal failure, failing the test if
// it does not arrive.
func awaitFatal(t *testing.T, node *Node) error {
	t.Helper()
	select {
	case <-node.Fatal():
		return node.FatalErr()
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for Fatal")
		return nil
	}
}

// A failed Apply leaves the state machine permanently behind a log that cannot be
// retracted, so the caller has to be told — the apply goroutine exiting quietly is
// exactly the silent-divergence case Fatal exists to prevent.
func TestApplyLoop_ApplyFails_ReportsFatal(t *testing.T) {
	store := new(MockStorage)
	sm := new(MockStateMachine)
	node := NewNodeMock(store, sm)

	start, end := uint(1), uint(4)
	entries := []LogEntry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}}
	store.On(methodGetLogs, mock.Anything, &start, &end).Return(entries, nil)
	sm.On(methodApply, mock.Anything, entries).Return(errors.New("apply error"))

	node.startApplyLoop(context.Background())
	node.SetCommitIndex(3)

	err := awaitFatal(t, node)
	assert.ErrorContains(t, err, "apply error", "the cause must survive to the caller")
	assert.ErrorContains(t, err, "3", "the commit index we could not reach is the useful detail")
}

// Shutdown is not a failure. If a cancelled context tripped Fatal, every clean stop
// would look like a broken replica and callers would learn to ignore the signal.
func TestApplyLoop_ContextCancelled_DoesNotReportFatal(t *testing.T) {
	store := new(MockStorage)
	sm := new(MockStateMachine)
	node := NewNodeMock(store, sm)
	ctx, cancel := context.WithCancel(context.Background())

	node.startApplyLoop(ctx)
	cancel()
	time.Sleep(20 * time.Millisecond)

	select {
	case <-node.Fatal():
		t.Fatalf("clean shutdown reported as fatal: %v", node.FatalErr())
	default:
	}
	assert.NoError(t, node.FatalErr())
}
