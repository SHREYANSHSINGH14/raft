package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func setupProposeTest(t *testing.T) (*Node, *MockStorage) {
	t.Helper()
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	node.Role = ServerRole_Leader
	// Faking the role is not enough: newFuture captures leaderCloseCh at registration
	// time and Future.Wait reads a nil one as "not leading", failing the wait
	// immediately. becomeLeader opens it for real; a test that skips becomeLeader has
	// to open it itself.
	node.setLeaderCloseCh()
	return node, store
}

// waitAsync runs future.Wait in a goroutine and returns a channel that receives
// the error when it returns. Propose itself no longer blocks, so this — not
// Propose — is what the wait-condition tests drive.
func waitAsync(future Future, ctx context.Context) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- future.Wait(ctx) }()
	return ch
}

// commitTo advances commitIndex and then completes every future it covers, in
// that order: processFutures takes commitMu, which SetCommitIndex holds for its
// whole body, so calling it from inside would deadlock on a non-reentrant mutex.
func commitTo(node *Node, idx uint) {
	node.SetCommitIndex(idx)
	node.processFutures(uint64(idx))
}

// startAutoCommitter drains futures against the node's current commitIndex until
// the test ends, standing in for the commit-index updater that does it in a real
// leadership term.
//
// Tests that pre-set a large commitIndex and then drive a flow which proposes —
// AddMember, RemoveMember — need this. A high commitIndex was enough back when
// Propose read it directly; a future is completed only by processFutures, so with
// nothing calling that, the flow parks in Future.Wait until its context dies.
func startAutoCommitter(t *testing.T, node *Node) {
	t.Helper()
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				node.processFutures(uint64(node.GetCommitIndex()))
				time.Sleep(time.Millisecond)
			}
		}
	}()
	t.Cleanup(func() {
		close(stop)
		<-done // the drainer touches futureList; let it finish before the test does
	})
}

// assertStillWaiting fails if the wait has already returned. The window is a
// judgement call: long enough that a wrongly-completed future is caught, short
// enough not to dominate the suite.
func assertStillWaiting(t *testing.T, errCh <-chan error, why string) {
	t.Helper()
	select {
	case err := <-errCh:
		t.Fatalf("Wait returned early (%s): %v", why, err)
	case <-time.After(20 * time.Millisecond):
	}
}

// awaitWait returns the wait's error, failing the test rather than hanging if the
// waiter was never woken.
func awaitWait(t *testing.T, errCh <-chan error) error {
	t.Helper()
	select {
	case err := <-errCh:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("Wait never returned — the waiter was never woken")
		return nil
	}
}

// pendingFutures reports the futures still registered. futureList is guarded by
// commitMu, the same lock newFuture and processFutures take.
func pendingFutures(node *Node) []*Future {
	node.commitMu.Lock()
	defer node.commitMu.Unlock()
	return append([]*Future(nil), node.futureList...)
}

// ── pre-condition checks ──────────────────────────────────────────────────────

// 1. Node is not leader → error before any DB call, and no future to wait on
func TestPropose_NotLeader_ReturnsError(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	// Role defaults to Follower

	future, err := node.Propose(context.Background(), EntryType_Command, []byte("cmd"))

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not the leader")
	store.AssertNotCalled(t, methodGetLastIndex, mock.Anything)
	assert.Empty(t, pendingFutures(node), "a rejected proposal must not register a waiter")
	// The zero Future is not a live one: its nil leaderClose means "not leading",
	// so a caller that ignores err and waits anyway is told so rather than hanging.
	assert.ErrorIs(t, future.Wait(context.Background()), ErrLeadershipLost)
}

// ── DB error cases ────────────────────────────────────────────────────────────

// 2. GetLastIndex fails → error, nothing appended
func TestPropose_GetLastIndexFails_ReturnsError(t *testing.T) {
	node, store := setupProposeTest(t)

	store.On(methodGetLastIndex, mock.Anything).Return(uint(0), errors.New("db error"))

	_, err := node.Propose(context.Background(), EntryType_Command, []byte("cmd"))

	assert.Error(t, err)
	store.AssertNotCalled(t, methodGetCurrentTerm, mock.Anything)
	store.AssertExpectations(t)
}

// 3. GetCurrentTerm fails → error, nothing appended
func TestPropose_GetCurrentTermFails_ReturnsError(t *testing.T) {
	node, store := setupProposeTest(t)

	store.On(methodGetLastIndex, mock.Anything).Return(uint(0), nil)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(0), errors.New("db error"))

	_, err := node.Propose(context.Background(), EntryType_Command, []byte("cmd"))

	assert.Error(t, err)
	store.AssertNotCalled(t, methodAppendLogs, mock.Anything, mock.Anything)
	store.AssertExpectations(t)
}

// 4. AppendLogs fails → error, and no future is registered. A waiter for an entry
// that never reached the log would sit in futureList forever, blocking the drain
// of every later future behind it — processFutures stops at the first index past
// the commit index.
func TestPropose_AppendLogsFails_ReturnsError(t *testing.T) {
	node, store := setupProposeTest(t)

	expected := LogEntry{Index: 1, Term: 5, Data: []byte("cmd")}
	store.On(methodGetLastIndex, mock.Anything).Return(uint(0), nil)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodAppendLogs, mock.Anything, []LogEntry{expected}).Return(errors.New("db error"))

	_, err := node.Propose(context.Background(), EntryType_Command, []byte("cmd"))

	assert.Error(t, err)
	assert.Empty(t, pendingFutures(node), "a failed append must not register a waiter")
	store.AssertExpectations(t)
}

// ── the returned future ───────────────────────────────────────────────────────

// 5. A successful Propose returns without waiting for commit, and hands back a
// future registered against the index the entry actually landed at.
func TestPropose_Success_RegistersFutureForAppendedIndex(t *testing.T) {
	node, store := setupProposeTest(t)

	// entry lands at index 3 (lastLogIndex=2, +1)
	expected := LogEntry{Index: 3, Term: 5, Data: []byte("cmd")}
	store.On(methodGetLastIndex, mock.Anything).Return(uint(2), nil)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodAppendLogs, mock.Anything, []LogEntry{expected}).Return(nil)

	future, err := node.Propose(context.Background(), EntryType_Command, []byte("cmd"))

	require.NoError(t, err)
	assert.Equal(t, uint64(3), future.idx)

	pending := pendingFutures(node)
	require.Len(t, pending, 1)
	assert.Equal(t, uint64(3), pending[0].idx)
	store.AssertExpectations(t)
}

// ── wait condition ────────────────────────────────────────────────────────────

// 6. The entry is already committed by the time the caller waits → Wait returns
// nil without blocking
func TestFutureWait_AlreadyCommitted_ReturnsNilImmediately(t *testing.T) {
	node, store := setupProposeTest(t)

	expected := LogEntry{Index: 1, Term: 5, Data: []byte("cmd")}
	store.On(methodGetLastIndex, mock.Anything).Return(uint(0), nil)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodAppendLogs, mock.Anything, []LogEntry{expected}).Return(nil)

	future, err := node.Propose(context.Background(), EntryType_Command, []byte("cmd"))
	require.NoError(t, err)

	commitTo(node, 10) // commit index runs past the entry

	assert.NoError(t, future.Wait(context.Background()))
	assert.Empty(t, pendingFutures(node), "a completed future must be dropped from the list")
	store.AssertExpectations(t)
}

// 7. commitIndex starts below entry.Index; a single advance to exactly entry.Index
// completes the future
func TestFutureWait_CommitAdvancesToEntryIndex_ReturnsNil(t *testing.T) {
	node, store := setupProposeTest(t)

	expected := LogEntry{Index: 1, Term: 5, Data: []byte("cmd")}
	store.On(methodGetLastIndex, mock.Anything).Return(uint(0), nil)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodAppendLogs, mock.Anything, []LogEntry{expected}).Return(nil)

	future, err := node.Propose(context.Background(), EntryType_Command, []byte("cmd"))
	require.NoError(t, err)

	errCh := waitAsync(future, context.Background())
	assertStillWaiting(t, errCh, "entry is not committed yet")

	commitTo(node, 1)

	assert.NoError(t, awaitWait(t, errCh))
	store.AssertExpectations(t)
}

// 8. Commit advances repeatedly below entry.Index; the future stays pending and
// only completes when the commit index finally reaches it
func TestFutureWait_MultiplePartialCommits_EventuallyReturnsNil(t *testing.T) {
	node, store := setupProposeTest(t)

	// entry will land at index 3 (lastLogIndex=2, +1)
	expected := LogEntry{Index: 3, Term: 5, Data: []byte("cmd")}
	store.On(methodGetLastIndex, mock.Anything).Return(uint(2), nil)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodAppendLogs, mock.Anything, []LogEntry{expected}).Return(nil)

	future, err := node.Propose(context.Background(), EntryType_Command, []byte("cmd"))
	require.NoError(t, err)

	errCh := waitAsync(future, context.Background())

	commitTo(node, 1) // 1 < 3 → still pending
	assertStillWaiting(t, errCh, "commit index is 1, entry is at 3")
	commitTo(node, 2) // 2 < 3 → still pending
	assertStillWaiting(t, errCh, "commit index is 2, entry is at 3")

	commitTo(node, 3) // 3 >= 3 → completes

	assert.NoError(t, awaitWait(t, errCh))
	assert.Empty(t, pendingFutures(node))
	store.AssertExpectations(t)
}

// ── context cancellation ──────────────────────────────────────────────────────

// 9. Context cancelled while the caller is parked in Wait → returns the cause.
// Note the entry stays in futureList: the caller walking away does not un-propose
// it, and it will still be completed if it commits.
func TestFutureWait_ContextCancelledWhileWaiting_ReturnsError(t *testing.T) {
	node, store := setupProposeTest(t)
	ctx, cancel := context.WithCancel(context.Background())

	expected := LogEntry{Index: 1, Term: 5, Data: []byte("cmd")}
	store.On(methodGetLastIndex, mock.Anything).Return(uint(0), nil)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodAppendLogs, mock.Anything, []LogEntry{expected}).Return(nil)

	future, err := node.Propose(ctx, EntryType_Command, []byte("cmd"))
	require.NoError(t, err)

	errCh := waitAsync(future, ctx)
	assertStillWaiting(t, errCh, "neither committed nor cancelled yet")

	cancel()

	waitErr := awaitWait(t, errCh)
	assert.Error(t, waitErr)
	// The cause survives, which is what distinguishes cancellation from
	// ErrLeadershipLost — a substring check could not.
	assert.ErrorIs(t, waitErr, context.Canceled)
	assert.NotErrorIs(t, waitErr, ErrLeadershipLost)
	store.AssertExpectations(t)
}

// 10. Context already cancelled before Wait is entered → returns without blocking
func TestFutureWait_ContextAlreadyCancelled_ReturnsError(t *testing.T) {
	node, store := setupProposeTest(t)
	ctx, cancel := context.WithCancel(context.Background())

	expected := LogEntry{Index: 1, Term: 5, Data: []byte("cmd")}
	store.On(methodGetLastIndex, mock.Anything).Return(uint(0), nil)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodAppendLogs, mock.Anything, []LogEntry{expected}).Return(nil)

	future, err := node.Propose(ctx, EntryType_Command, []byte("cmd"))
	require.NoError(t, err)

	cancel() // cancelled before the caller ever waits

	waitErr := future.Wait(ctx)
	assert.Error(t, waitErr)
	assert.ErrorIs(t, waitErr, context.Canceled)
	assert.NotErrorIs(t, waitErr, ErrLeadershipLost)
	store.AssertExpectations(t)
}

// ── leadership lost while waiting ─────────────────────────────────────────────

// 11. A caller parked in Wait must fail when we step down, not hang until its own
// context expires — the entry it appended may never commit under the next leader.
// This is also what pins the capture-at-registration design: the future holds the
// leadership term's channel, so one close fails every waiter in flight at once.
func TestFutureWait_LeadershipLostWhileWaiting_ReturnsErrLeadershipLost(t *testing.T) {
	node, store := setupProposeTest(t)

	expected := LogEntry{Index: 1, Term: 5, Data: []byte("cmd")}
	store.On(methodGetLastIndex, mock.Anything).Return(uint(0), nil)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodAppendLogs, mock.Anything, []LogEntry{expected}).Return(nil)

	future, err := node.Propose(context.Background(), EntryType_Command, []byte("cmd"))
	require.NoError(t, err)

	errCh := waitAsync(future, context.Background())
	assertStillWaiting(t, errCh, "still leading")

	node.clearLeaderCloseCh() // what becomeFollower does on step-down

	waitErr := awaitWait(t, errCh)
	assert.ErrorIs(t, waitErr, ErrLeadershipLost)
	assert.NotErrorIs(t, waitErr, context.Canceled)
	store.AssertExpectations(t)
}

// 12. A future registered while not leading has a nil leaderClose, and Wait must
// read that as "not leading" rather than blocking on a channel nobody will ever
// close. This is the window becomeLeader avoids by opening leaderCloseCh before
// flipping the role.
func TestFutureWait_RegisteredWithoutLeaderChannel_ReturnsErrLeadershipLost(t *testing.T) {
	store := new(MockStorage)
	node := NewNodeMock(store, nil)
	node.Role = ServerRole_Leader // role says leader, but leaderCloseCh was never opened

	expected := LogEntry{Index: 1, Term: 5, Data: []byte("cmd")}
	store.On(methodGetLastIndex, mock.Anything).Return(uint(0), nil)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodAppendLogs, mock.Anything, []LogEntry{expected}).Return(nil)

	future, err := node.Propose(context.Background(), EntryType_Command, []byte("cmd"))
	require.NoError(t, err)

	assert.ErrorIs(t, future.Wait(context.Background()), ErrLeadershipLost)
	store.AssertExpectations(t)
}

// 13. Committed wins over a step-down that lands in the same wakeup: the entry is
// committed either way, so reporting failure would invite a pointless retry. Run
// repeatedly because both channels are ready at once — a Wait that picks between
// them at random passes a single attempt about half the time.
func TestFutureWait_CommittedBeforeStepDown_ReturnsNil(t *testing.T) {
	for i := 0; i < 100; i++ {
		node, store := setupProposeTest(t)

		expected := LogEntry{Index: 1, Term: 5, Data: []byte("cmd")}
		store.On(methodGetLastIndex, mock.Anything).Return(uint(0), nil)
		store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
		store.On(methodAppendLogs, mock.Anything, []LogEntry{expected}).Return(nil)

		future, err := node.Propose(context.Background(), EntryType_Command, []byte("cmd"))
		require.NoError(t, err)

		commitTo(node, 1)         // entry 1 is committed
		node.clearLeaderCloseCh() // and we have since stepped down

		require.NoErrorf(t, future.Wait(context.Background()),
			"attempt %d: entry was committed before the step-down", i)
		store.AssertExpectations(t)
	}
}

// ── concurrency ───────────────────────────────────────────────────────────────

// 14. Concurrent Propose calls against a real, stateful store must not race: the
// read-lastLogIndex-then-append-then-register sequence gives every entry a unique,
// sequential index, futureList stays ordered by index so processFutures drains it
// as a prefix, and every caller unblocks once its own entry commits.
func TestPropose_ConcurrentCallers_ProduceConsistentLog(t *testing.T) {
	store := NewMemStorage()
	node := NewNodeMock(store, nil)
	node.Role = ServerRole_Leader
	node.setLeaderCloseCh()

	const n = 50
	ctx := context.Background()

	// background committer: keeps commitIndex in lockstep with the log and completes
	// the futures it covers, so waiting callers unblock once their entry lands
	stopCh := make(chan struct{})
	var committerWg sync.WaitGroup
	committerWg.Add(1)
	go func() {
		defer committerWg.Done()
		for {
			select {
			case <-stopCh:
				return
			default:
				lastIdx, _ := store.GetLastIndex(ctx)
				commitTo(node, lastIdx)
				time.Sleep(time.Millisecond)
			}
		}
	}()

	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			future, err := node.Propose(ctx, EntryType_Command, []byte(fmt.Sprintf("cmd-%d", i)))
			if err != nil {
				errs[i] = err
				return
			}
			// bounded: a stranded future must fail the test, not hang it
			waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			errs[i] = future.Wait(waitCtx)
		}(i)
	}
	wg.Wait()
	close(stopCh)
	committerWg.Wait()

	for i, err := range errs {
		assert.NoErrorf(t, err, "propose %d", i)
	}

	logs, err := store.GetLogs(ctx, nil, nil)
	assert.NoError(t, err)
	assert.Lenf(t, logs, n, "expected exactly one log entry per Propose call, no duplicates or lost writes")

	seen := make(map[uint64]bool, n)
	for _, e := range logs {
		assert.Falsef(t, seen[e.Index], "duplicate log index %d — concurrent Propose calls raced", e.Index)
		seen[e.Index] = true
	}
	for idx := uint64(1); idx <= uint64(n); idx++ {
		assert.Truef(t, seen[idx], "missing log index %d — gap left in the log", idx)
	}

	assert.Empty(t, pendingFutures(node), "every future should have been drained once its entry committed")
}

// ── admission control ─────────────────────────────────────────────────────────

// 15. Past MaxPendingProposals, Propose rejects — and rejects having written
// nothing. The order matters more than the limit: a rejection issued after the
// append would leave a durable entry that replicates and commits while its caller
// was told the proposal failed.
func TestPropose_PendingLimitReached_RejectsWithoutAppending(t *testing.T) {
	node, store := setupProposeTest(t)
	node.cfg.MaxPendingProposals = 3

	store.On(methodGetLastIndex, mock.Anything).Return(uint(0), nil)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodAppendLogs, mock.Anything, mock.Anything).Return(nil)

	for i := 0; i < 3; i++ {
		_, err := node.Propose(context.Background(), EntryType_Command, []byte("cmd"))
		require.NoErrorf(t, err, "proposal %d is within the limit", i)
	}
	appendsAtLimit := len(store.Calls)

	_, err := node.Propose(context.Background(), EntryType_Command, []byte("cmd"))

	assert.ErrorIs(t, err, ErrTooManyPendingProposals)
	assert.Len(t, pendingFutures(node), 3, "a rejected proposal must not register a waiter")
	assert.Equal(t, appendsAtLimit, len(store.Calls),
		"a rejected proposal must not touch the store at all — no append, no reads")
}

// 16. Draining the pending list admits proposals again: the limit is on entries
// in flight, not on total throughput.
func TestPropose_PendingLimitClearsAfterCommit_AdmitsAgain(t *testing.T) {
	node, store := setupProposeTest(t)
	node.cfg.MaxPendingProposals = 2

	store.On(methodGetLastIndex, mock.Anything).Return(uint(0), nil)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodAppendLogs, mock.Anything, mock.Anything).Return(nil)

	for i := 0; i < 2; i++ {
		_, err := node.Propose(context.Background(), EntryType_Command, []byte("cmd"))
		require.NoError(t, err)
	}
	_, err := node.Propose(context.Background(), EntryType_Command, []byte("cmd"))
	require.ErrorIs(t, err, ErrTooManyPendingProposals)

	commitTo(node, 1) // MockStorage always reports lastLogIndex 0, so every entry is idx 1

	_, err = node.Propose(context.Background(), EntryType_Command, []byte("cmd"))
	assert.NoError(t, err, "the list drained, so there is room again")
}

// 17. Config entries bypass the limit. The list only fills when commitIndex has
// stalled, which usually means lost quorum — rejecting the membership change that
// could restore it would be exactly the wrong moment to apply backpressure.
func TestPropose_PendingLimitReached_ConfigEntryStillAdmitted(t *testing.T) {
	node, store := setupProposeTest(t)
	node.cfg.MaxPendingProposals = 1

	store.On(methodGetLastIndex, mock.Anything).Return(uint(0), nil)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodAppendLogs, mock.Anything, mock.Anything).Return(nil)

	_, err := node.Propose(context.Background(), EntryType_Command, []byte("cmd"))
	require.NoError(t, err)

	_, err = node.Propose(context.Background(), EntryType_Command, []byte("cmd"))
	require.ErrorIs(t, err, ErrTooManyPendingProposals, "a command is over the limit")

	_, err = node.Propose(context.Background(), EntryType_Config, []byte("{}"))
	assert.NoError(t, err, "a config entry is exempt")
}

// 18. An unset limit falls back to the default rather than rejecting everything —
// a zero-valued Config must not mean "admit nothing".
func TestPropose_UnsetLimit_UsesDefault(t *testing.T) {
	node, store := setupProposeTest(t)
	node.cfg.MaxPendingProposals = 0

	store.On(methodGetLastIndex, mock.Anything).Return(uint(0), nil)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)
	store.On(methodAppendLogs, mock.Anything, mock.Anything).Return(nil)

	_, err := node.Propose(context.Background(), EntryType_Command, []byte("cmd"))
	assert.NoError(t, err)
}
