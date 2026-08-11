package raft

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// timeoutNowNode builds a follower that already recognises leaderID as its
// leader — the ordinary state a transfer target is in. Pass "" for a node that
// has not heard from anyone yet.
func timeoutNowNode(store Storage, leaderID string) *Node {
	node := NewNodeMock(store, nil)
	node.SetLeaderID(leaderID)
	return node
}

// assertSignalled / assertNotSignalled read the size-1 buffer directly. The
// handler's only output besides its response is this signal, so every test
// asserts on it — a handler that returns Success but never signals would
// otherwise look correct.
func assertSignalled(t *testing.T, node *Node) {
	t.Helper()
	assert.Len(t, node.timeoutNowCh, 1, "expected a TimeoutNow signal to be queued")
}

func assertNotSignalled(t *testing.T, node *Node) {
	t.Helper()
	assert.Empty(t, node.timeoutNowCh, "a rejected TimeoutNow must not signal the election timer")
}

// ── 1. Empty leader ID ────────────────────────────────────────────────────────

func TestTimeoutNow_EmptyLeaderID(t *testing.T) {
	store := new(MockStorage)
	node := timeoutNowNode(store, "node-2")

	_, err := node.HandleTimeoutNow(context.Background(), TimeoutNowArgs{
		LeaderID: "   ",
		Term:     5,
	})

	assert.Error(t, err)
	assertNotSignalled(t, node)
	store.AssertExpectations(t) // no DB calls should have been made
}

// ── 2. Happy path: our leader hands leadership over ───────────────────────────

func TestTimeoutNow_Accepted(t *testing.T) {
	store := new(MockStorage)
	node := timeoutNowNode(store, "node-2")

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)

	resp, err := node.HandleTimeoutNow(context.Background(), TimeoutNowArgs{
		LeaderID: "node-2",
		Term:     5,
	})

	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, uint64(5), resp.Term)
	assertSignalled(t, node)
	store.AssertExpectations(t)
	// Nothing persisted, and — importantly — the election timer is NOT reset.
	// Resetting it would delay the very election this RPC exists to trigger.
	assertNoSideEffects(t, node, store)
}

// A node that has not heard from any leader yet has nobody to contradict the
// sender, so the transfer is accepted.
func TestTimeoutNow_NoKnownLeader_Accepted(t *testing.T) {
	store := new(MockStorage)
	node := timeoutNowNode(store, "")

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)

	resp, err := node.HandleTimeoutNow(context.Background(), TimeoutNowArgs{
		LeaderID: "node-2",
		Term:     5,
	})

	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assertSignalled(t, node)
	store.AssertExpectations(t)
	assertNoSideEffects(t, node, store)
}

// ── 3. Stale term → reject ────────────────────────────────────────────────────
//
// A deposed leader's in-flight transfer must not start elections after the
// cluster has moved on.

func TestTimeoutNow_StaleTerm_Rejected(t *testing.T) {
	store := new(MockStorage)
	node := timeoutNowNode(store, "node-2")

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(7), nil)

	resp, err := node.HandleTimeoutNow(context.Background(), TimeoutNowArgs{
		LeaderID: "node-2",
		Term:     3,
	})

	assert.NoError(t, err)
	assert.False(t, resp.Success)
	assert.Equal(t, uint64(7), resp.Term)
	assertNotSignalled(t, node)
	store.AssertExpectations(t)
	assertNoSideEffects(t, node, store)
}

// ── 4. Higher term is accepted but never persisted ────────────────────────────
//
// Unlike HandleAppendEntries, this handler does not adopt the sender's term. The
// recipient raises its own term when it actually campaigns, through the normal
// election path — so a TimeoutNow at term 50 leaves us at term 5.

func TestTimeoutNow_HigherTerm_AcceptedWithoutPersisting(t *testing.T) {
	store := new(MockStorage)
	node := timeoutNowNode(store, "node-2")

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)

	resp, err := node.HandleTimeoutNow(context.Background(), TimeoutNowArgs{
		LeaderID: "node-2",
		Term:     50,
	})

	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, uint64(5), resp.Term, "we answer with our own term, not the sender's")
	assertSignalled(t, node)
	store.AssertExpectations(t)
	assertNoSideEffects(t, node, store)
}

// ── 5. Wrong sender → reject ──────────────────────────────────────────────────
//
// Without this, any peer could force elections at will — a disruption in its own
// right, and the opposite of what pre-vote is for.

func TestTimeoutNow_NotOurLeader_Rejected(t *testing.T) {
	store := new(MockStorage)
	node := timeoutNowNode(store, "node-2")

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)

	resp, err := node.HandleTimeoutNow(context.Background(), TimeoutNowArgs{
		LeaderID: "node-9", // not the leader we recognise
		Term:     5,
	})

	assert.NoError(t, err)
	assert.False(t, resp.Success)
	assertNotSignalled(t, node)
	store.AssertExpectations(t)
	assertNoSideEffects(t, node, store)
}

// An impostor at a higher term is still refused — the sender check runs on its
// own, not as a tiebreak behind the term.
func TestTimeoutNow_NotOurLeaderAtHigherTerm_Rejected(t *testing.T) {
	store := new(MockStorage)
	node := timeoutNowNode(store, "node-2")

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)

	resp, err := node.HandleTimeoutNow(context.Background(), TimeoutNowArgs{
		LeaderID: "node-9",
		Term:     99,
	})

	assert.NoError(t, err)
	assert.False(t, resp.Success)
	assertNotSignalled(t, node)
	store.AssertExpectations(t)
	assertNoSideEffects(t, node, store)
}

// ── 6. Already leader → reject ────────────────────────────────────────────────
//
// Campaigning here would depose us in favour of ourselves at a higher term.

func TestTimeoutNow_AlreadyLeader_Rejected(t *testing.T) {
	store := new(MockStorage)
	node := timeoutNowNode(store, "")
	node.Role = ServerRole_Leader

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)

	resp, err := node.HandleTimeoutNow(context.Background(), TimeoutNowArgs{
		LeaderID: "node-2",
		Term:     5,
	})

	assert.NoError(t, err)
	assert.False(t, resp.Success)
	assertNotSignalled(t, node)
	store.AssertExpectations(t)
	assertNoSideEffects(t, node, store)
}

// ── 7. Candidate accepts; the signal waits for the next timer ─────────────────
//
// No election-timeout goroutine is listening while we are a candidate, so the
// signal sits in the buffer. That is the right outcome: if this election is lost,
// becomeFollower restarts the timer and the pending signal makes it retry at once
// instead of waiting out another randomized timeout.

func TestTimeoutNow_Candidate_AcceptedAndBuffered(t *testing.T) {
	store := new(MockStorage)
	node := timeoutNowNode(store, "node-2")
	node.Role = ServerRole_Candidate

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)

	resp, err := node.HandleTimeoutNow(context.Background(), TimeoutNowArgs{
		LeaderID: "node-2",
		Term:     5,
	})

	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assertSignalled(t, node)
	store.AssertExpectations(t)
	assertNoSideEffects(t, node, store)
}

// ── 8. Repeated transfers collapse into one pending signal ────────────────────
//
// The send is non-blocking on a size-1 buffer, so a retrying leader can never
// wedge the handler or queue up a backlog of elections.

func TestTimeoutNow_RepeatedCalls_CollapseToOneSignal(t *testing.T) {
	store := new(MockStorage)
	node := timeoutNowNode(store, "node-2")

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)

	args := TimeoutNowArgs{LeaderID: "node-2", Term: 5}
	for i := 0; i < 5; i++ {
		resp, err := node.HandleTimeoutNow(context.Background(), args)
		assert.NoError(t, err, "call %d", i)
		assert.True(t, resp.Success, "call %d", i)
	}

	assert.Len(t, node.timeoutNowCh, 1, "buffer is size 1; extra signals are dropped")
	assertNoSideEffects(t, node, store)
}

// ── 9. DB error ───────────────────────────────────────────────────────────────

func TestTimeoutNow_DBErr_GetCurrentTerm(t *testing.T) {
	store := new(MockStorage)
	node := timeoutNowNode(store, "node-2")

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(0), errors.New("db error"))

	_, err := node.HandleTimeoutNow(context.Background(), TimeoutNowArgs{
		LeaderID: "node-2",
		Term:     5,
	})

	assert.Error(t, err)
	assertNotSignalled(t, node)
	store.AssertExpectations(t)
	assertNoSideEffects(t, node, store)
}

// ── 10. Concurrent callers ────────────────────────────────────────────────────

func TestTimeoutNow_ConcurrentCallers(t *testing.T) {
	store := new(MockStorage)
	node := timeoutNowNode(store, "node-2")

	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(5), nil)

	const callers = 32
	results := make([]TimeoutNowResponse, callers)

	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := node.HandleTimeoutNow(context.Background(), TimeoutNowArgs{
				LeaderID: "node-2",
				Term:     5,
			})
			assert.NoError(t, err)
			results[i] = resp
		}(i)
	}
	wg.Wait()

	for i, resp := range results {
		assert.True(t, resp.Success, "caller %d", i)
	}
	assert.Len(t, node.timeoutNowCh, 1)
	assertNoSideEffects(t, node, store)
}

// ── 11. End to end: the signal actually drives the transition ─────────────────
//
// The unit tests above stop at the buffer. This one proves the other half: the
// election-timeout goroutine is selecting on timeoutNowCh and leaves Follower
// because of it, without waiting out its timer. The timer here is set to minutes,
// so a transition inside a second can only have come from the signal.
//
// It also pins the ownership rule — HandleTimeoutNow does not transition, the
// goroutine that owns the election timer does.

func TestTimeoutNow_DrivesElectionTimeoutGoroutine(t *testing.T) {
	store := NewMemStorage()
	node := NewNodeMock(store, nil)
	node.SetLeaderID("node-2")

	// becomeCandidate logs through n.ctx and starts an election on it.
	ctx, cancel := context.WithCancel(context.Background())
	node.ctx, node.cancel = ctx, cancel
	t.Cleanup(cancel)

	// Long enough that the ticker cannot plausibly fire during the test.
	node.cfg.ElectionMinMs = 120_000
	node.cfg.ElectionMaxMs = 240_000

	node.startElectionOut(ctx)

	resp, err := node.HandleTimeoutNow(ctx, TimeoutNowArgs{LeaderID: "node-2", Term: 0})
	assert.NoError(t, err)
	assert.True(t, resp.Success)

	assert.Eventually(t, func() bool {
		return node.GetRole() != ServerRole_Follower
	}, 2*time.Second, 5*time.Millisecond,
		"election timer goroutine did not act on the TimeoutNow signal")
}
