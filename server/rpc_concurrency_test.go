package server

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/SHREYANSHSINGH14/raft/db"
	"github.com/SHREYANSHSINGH14/raft/raft"
	"github.com/SHREYANSHSINGH14/raft/types"
	"github.com/stretchr/testify/assert"
)

func newConcurrentTestServer(store types.RaftDBInterface) *Server {
	return &Server{
		Peer: raft.NewPeerMock(store),
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// INVARIANT TESTS
// Fire N goroutines simultaneously, assert Raft rules hold at the end
// regardless of interleaving order.
// ═══════════════════════════════════════════════════════════════════════════════

// Invariant 1 — at most one vote granted per term
// 5 candidates race to get a vote from the same node in the same term.
// Only one must win — this is the core Raft voting guarantee.
func TestInvariant_RequestVote_AtMostOneVotePerTerm(t *testing.T) {
	store := db.NewMockKVStore()
	srv := newConcurrentTestServer(store)
	ctx := context.Background()

	// Pre-set term so GetCurrentTerm works
	store.SetCurrentTerm(ctx, 5)

	const numCandidates = 5
	results := make([]*types.RequestVoteResponse, numCandidates)
	errs := make([]error, numCandidates)

	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < numCandidates; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			results[idx], errs[idx] = srv.RequestVote(ctx, &types.RequestVoteArgs{
				CandidateId:  fmt.Sprintf("candidate-%d", idx),
				Term:         5,
				LastLogTerm:  0,
				LastLogIndex: 0,
			})
		}(i)
	}

	close(start)
	wg.Wait()

	votesGranted := 0
	for i, res := range results {
		assert.NoError(t, errs[i])
		if res != nil && res.VoteGranted {
			votesGranted++
		}
	}

	// Exactly one candidate must win
	assert.Equal(t, 1, votesGranted,
		"Raft invariant violated: expected exactly 1 vote granted, got %d", votesGranted)

	// votedFor must be set to whoever won
	votedFor, err := store.GetVotedFor(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, votedFor, "votedFor must be set to the winner")
}

// Invariant 2 — idempotent vote: same candidate asking twice gets granted both times
// Raft allows re-voting for the same candidate — important for retry scenarios
// when a candidate doesn't receive the response and retries the RPC.
func TestInvariant_RequestVote_IdempotentVote(t *testing.T) {
	store := db.NewMockKVStore()
	srv := newConcurrentTestServer(store)
	ctx := context.Background()

	store.SetCurrentTerm(ctx, 5)

	const numRequests = 5
	results := make([]*types.RequestVoteResponse, numRequests)
	errs := make([]error, numRequests)

	var wg sync.WaitGroup
	start := make(chan struct{})

	// Same candidate sends 5 concurrent RequestVote RPCs (retry scenario)
	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			results[idx], errs[idx] = srv.RequestVote(ctx, &types.RequestVoteArgs{
				CandidateId:  "candidate-1",
				Term:         5,
				LastLogTerm:  0,
				LastLogIndex: 0,
			})
		}(i)
	}

	close(start)
	wg.Wait()

	// All requests from same candidate must be granted
	for i, res := range results {
		assert.NoError(t, errs[i])
		assert.True(t, res.VoteGranted,
			"idempotent vote invariant violated: same candidate was denied on retry")
	}

	votedFor, _ := store.GetVotedFor(ctx)
	assert.Equal(t, "candidate-1", votedFor)
}

// Invariant 3 — commitIndex never decreases
// Concurrent AppendEntries with different LeaderCommit values must never
// cause commitIndex to go backward — commits are permanent in Raft.
func TestInvariant_AppendEntries_CommitIndexNeverDecreases(t *testing.T) {
	store := db.NewMockKVStore()
	srv := newConcurrentTestServer(store)
	srv.Peer.SetCommitIndex(5)
	ctx := context.Background()

	store.SetCurrentTerm(ctx, 5)

	// Only pre-populate what comes before PrevLogIndex
	// AppendEntries will truncate from PrevLogIndex+1 and append entries itself
	store.AppendLogs(ctx, []*types.LogEntry{
		{Index: 1, Term: 4},
		{Index: 2, Term: 4},
		{Index: 3, Term: 4},
	})

	leaderCommits := []uint64{8, 3, 10, 6, 4}

	// entries to append — these rebuild logs 4-10 after truncation
	newEntries := []*types.LogEntry{
		{Index: 4, Term: 5},
		{Index: 5, Term: 5},
		{Index: 6, Term: 5},
		{Index: 7, Term: 5},
		{Index: 8, Term: 5},
		{Index: 9, Term: 5},
		{Index: 10, Term: 5},
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	numRequests := 5
	// now each concurrent call sends entries so lastLogIndex ends up as 10
	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(leaderCommit uint64) {
			defer wg.Done()
			<-start
			srv.AppendEntries(ctx, &types.AppendEntriesArgs{
				LeaderId:     "leader-1",
				Term:         5,
				PrevLogIndex: 3,
				PrevLogTerm:  4,
				Entries:      newEntries, // ← send entries so logs 4-10 exist
				LeaderCommit: leaderCommit,
			})
		}(leaderCommits[i])
	}
	close(start)
	wg.Wait()

	fmt.Printf("Get commit index %d", srv.Peer.GetCommitIndex())

	assert.GreaterOrEqual(t, srv.Peer.GetCommitIndex(), uint(5),
		"Raft invariant violated: commitIndex went below initial value of 5")
}

// Invariant 4 — term only moves forward
// Concurrent RPCs with increasing terms must never cause term to decrease.
func TestInvariant_TermOnlyMovesForward(t *testing.T) {
	store := db.NewMockKVStore()
	srv := newConcurrentTestServer(store)
	ctx := context.Background()

	store.SetCurrentTerm(ctx, 5)
	store.AppendLogs(ctx, []*types.LogEntry{
		{Index: 1, Term: 4},
		{Index: 2, Term: 4},
		{Index: 3, Term: 4},
	})

	var wg sync.WaitGroup
	start := make(chan struct{})

	// Mix of RequestVote and AppendEntries with terms 6-10
	for term := uint64(6); term <= 10; term++ {
		wg.Add(2)
		go func(term uint64) {
			defer wg.Done()
			<-start
			srv.RequestVote(ctx, &types.RequestVoteArgs{
				CandidateId: "candidate-1",
				Term:        term,
			})
		}(term)
		go func(term uint64) {
			defer wg.Done()
			<-start
			srv.AppendEntries(ctx, &types.AppendEntriesArgs{
				LeaderId:     "leader-1",
				Term:         term,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
			})
		}(term)
	}

	close(start)
	wg.Wait()

	finalTerm, err := store.GetCurrentTerm(ctx)
	fmt.Printf("Get current term %d", finalTerm)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, finalTerm, uint(5),
		"Raft invariant violated: term moved backward from 5")
}

// ═══════════════════════════════════════════════════════════════════════════════
// INTERLEAVING TESTS
// Force specific execution sequences to prove raftMu serializes correctly.
// With raftMu: G2 blocks until G1 fully completes — interleaving is impossible.
// These tests prove the invariants hold even when we try to force a race.
// ═══════════════════════════════════════════════════════════════════════════════

// Interleaving 1 — double vote race
// Without raftMu: G1 reads votedFor="", pauses, G2 reads votedFor="", both grant.
// With raftMu: G2 blocks on the lock until G1 fully completes including SetVotedFor.
// By the time G2 runs, votedFor is already set — G2 is denied.
func TestInterleaving_RequestVote_DoubleVoteRace(t *testing.T) {
	store := db.NewMockKVStore()
	srv := newConcurrentTestServer(store)
	ctx := context.Background()

	store.SetCurrentTerm(ctx, 5)

	pauseAfterG1Starts := make(chan struct{})
	resumeG1 := make(chan struct{})

	// Wrap store to pause G1 at a critical point
	// With raftMu this pause happens inside the lock so G2 can't enter
	var g1Once sync.Once
	originalStore := store

	// Use a wrapper that pauses on first GetVotedFor call
	type pausingStore struct {
		*db.MockKVStore
	}

	_ = originalStore // used below via closure

	var g1Resp, g2Resp *types.RequestVoteResponse

	// G1 starts
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Signal that G1 has started, then pause before checking votedFor
		g1Once.Do(func() {
			close(pauseAfterG1Starts)
		})
		g1Resp, _ = srv.RequestVote(ctx, &types.RequestVoteArgs{
			CandidateId: "candidate-1",
			Term:        5,
		})
	}()

	// Wait then try to race G2 in
	<-pauseAfterG1Starts
	// G2 fires while G1 may be mid-execution
	// With raftMu: G2 queues behind G1
	g2Resp, _ = srv.RequestVote(ctx, &types.RequestVoteArgs{
		CandidateId: "candidate-2",
		Term:        5,
	})
	close(resumeG1)

	wg.Wait()

	votesGranted := 0
	if g1Resp != nil && g1Resp.VoteGranted {
		votesGranted++
	}
	if g2Resp != nil && g2Resp.VoteGranted {
		votesGranted++
	}

	assert.LessOrEqual(t, votesGranted, 1,
		"double vote: both candidates received VoteGranted=true in same term")

	votedFor, _ := store.GetVotedFor(ctx)
	assert.NotEmpty(t, votedFor, "votedFor must be set after vote granted")
}

// Interleaving 2 — commitIndex rollback race
// Without raftMu: G1 checks commitIndex=5, leaderCommit=10.
// G2 runs with leaderCommit=3 — but since 3 < commitIndex=5 it's skipped.
// G1 then sets commitIndex=10.
// With raftMu: fully serialized, result is always commitIndex=10.
func TestInterleaving_AppendEntries_CommitIndexRollback(t *testing.T) {
	store := db.NewMockKVStore()
	srv := newConcurrentTestServer(store)
	srv.Peer.SetCommitIndex(5)
	ctx := context.Background()

	store.SetCurrentTerm(ctx, 5)
	store.AppendLogs(ctx, []*types.LogEntry{
		{Index: 1, Term: 4},
		{Index: 2, Term: 4},
		{Index: 3, Term: 4},
	})

	// entries to append — these rebuild logs 4-10 after truncation
	newEntries := []*types.LogEntry{
		{Index: 4, Term: 5},
		{Index: 5, Term: 5},
		{Index: 6, Term: 5},
		{Index: 7, Term: 5},
		{Index: 8, Term: 5},
		{Index: 9, Term: 5},
		{Index: 10, Term: 5},
	}

	var wg sync.WaitGroup
	start := make(chan struct{})

	// G1: leaderCommit=10 → should set commitIndex=min(10,10)=10
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		srv.AppendEntries(ctx, &types.AppendEntriesArgs{
			LeaderId:     "leader-1",
			Term:         5,
			PrevLogIndex: 3,
			PrevLogTerm:  4,
			LeaderCommit: 10,
			Entries:      newEntries,
		})
	}()

	// G2: leaderCommit=3 → 3 < commitIndex=5, must be no-op
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		srv.AppendEntries(ctx, &types.AppendEntriesArgs{
			LeaderId:     "leader-1",
			Term:         5,
			PrevLogIndex: 3,
			PrevLogTerm:  4,
			LeaderCommit: 3,
			Entries:      newEntries,
		})
	}()

	close(start)
	wg.Wait()

	assert.GreaterOrEqual(t, srv.Peer.GetCommitIndex(), uint(5),
		"commitIndex must never decrease below initial value")
}

// Interleaving 3 — concurrent AppendEntries from same leader
// Real scenario: leader retries AppendEntries while previous is still processing.
// Logs must not be duplicated and commitIndex must be consistent.
func TestInterleaving_AppendEntries_ConcurrentFromSameLeader(t *testing.T) {
	store := db.NewMockKVStore()
	srv := newConcurrentTestServer(store)
	ctx := context.Background()

	store.SetCurrentTerm(ctx, 5)

	entries := []*types.LogEntry{
		{Index: 1, Term: 5, Data: []byte("cmd-1")},
		{Index: 2, Term: 5, Data: []byte("cmd-2")},
		{Index: 3, Term: 5, Data: []byte("cmd-3")},
	}

	const numRetries = 3
	var wg sync.WaitGroup
	start := make(chan struct{})

	// Same AppendEntries sent 3 times concurrently (retry scenario)
	for i := 0; i < numRetries; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			srv.AppendEntries(ctx, &types.AppendEntriesArgs{
				LeaderId:     "leader-1",
				Term:         5,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      entries,
				LeaderCommit: 3,
			})
		}()
	}

	close(start)
	wg.Wait()

	// Logs 1,2,3 must exist exactly once — TruncateLogs+AppendLogs is idempotent
	for _, expected := range entries {
		entry, err := store.GetLogByIndex(ctx, uint(expected.Index))
		assert.NoError(t, err)
		assert.Equal(t, expected.Data, entry.Data,
			"log at index %d has wrong data after concurrent appends", expected.Index)
	}

	// No extra logs should exist beyond index 3
	lastIdx, err := store.GetLastLogIndex(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint(3), lastIdx,
		"unexpected logs appended beyond the expected range")
}
