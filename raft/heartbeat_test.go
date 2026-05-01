package raft

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/SHREYANSHSINGH14/raft/config"
	"github.com/SHREYANSHSINGH14/raft/db"
	"github.com/SHREYANSHSINGH14/raft/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const (
	methodAppendEntries = "AppendEntries"
)

func setupSendLogsTest(t *testing.T) (*Peer, *db.MockKVStore, map[string]*MockRaftRpcClient) {
	store := db.NewMockKVStore()
	store.SetCurrentTerm(context.Background(), 5)
	config.LoadConfig()

	peer := NewPeerMock(store)
	peer.Role = ServerRole_Leader

	clients := map[string]*MockRaftRpcClient{}
	for id, client := range peer.ServerIDRpcUrlMap {
		clients[id] = client.(*MockRaftRpcClient)
	}

	return peer, store, clients
}

// runSendLogs fans out sendLogs to every peer concurrently, mirroring what
// sendLogsPerPeer does on each ticker tick, but synchronously so tests can
// assert state immediately after. Returns the first error across all peers.
func runSendLogs(p *Peer) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(p.ServerIDRpcUrlMap))

	// mirror the production buffer size — all peers could signal step-down simultaneously
	stepDownCh := make(chan struct{}, len(p.ServerIDRpcUrlMap))

	for peerID := range p.ServerIDRpcUrlMap {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			ch := make(chan error, 1)
			p.sendLogs(context.Background(), id, ch, stepDownCh)
			if err := <-ch; err != nil {
				errCh <- err
			}
		}(peerID)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		return err
	}
	return nil
}

func appendLogsHelper(store *db.MockKVStore, logs []*types.LogEntry) {
	store.AppendLogs(context.Background(), logs)
}

func successResponse() *types.AppendEntriesResponse {
	return &types.AppendEntriesResponse{Term: 5, Success: true}
}

func failResponse() *types.AppendEntriesResponse {
	return &types.AppendEntriesResponse{Term: 5, Success: false}
}

func higherTermResponse(term uint64) *types.AppendEntriesResponse {
	return &types.AppendEntriesResponse{Term: term, Success: false}
}

// ════════════════════════════════════════════════════════════════════════════
// sendLogs tests
//
// sendLogs handles one peer at a time: it fetches the prevLog based on that
// peer's nextIndex, fetches the logs to send, fires the AppendEntries RPC,
// and updates nextIndex/matchIndex based on the response.
// ════════════════════════════════════════════════════════════════════════════

// ── 1. Logs sent per peer based on nextIndex ──────────────────────────────────

func TestSendLogs_LogsSentBasedOnNextIndex(t *testing.T) {
	peer, store, clients := setupSendLogsTest(t)

	appendLogsHelper(store, []*types.LogEntry{
		{Index: 1, Term: 5, Data: []byte("cmd-1")},
		{Index: 2, Term: 5, Data: []byte("cmd-2")},
		{Index: 3, Term: 5, Data: []byte("cmd-3")},
	})

	peer.peerIndexes["node-2"] = PeerIndexes{nextIndex: 2, matchIndex: 1}
	peer.peerIndexes["node-3"] = PeerIndexes{nextIndex: 1, matchIndex: 0}
	peer.peerIndexes["node-4"] = PeerIndexes{nextIndex: 1, matchIndex: 0}
	peer.peerIndexes["node-5"] = PeerIndexes{nextIndex: 1, matchIndex: 0}

	clients["node-2"].On(methodAppendEntries, mock.Anything, mock.MatchedBy(func(req *types.AppendEntriesArgs) bool {
		return len(req.Entries) == 2 && req.Entries[0].Index == 2
	})).Return(successResponse(), nil)

	for _, id := range []string{"node-3", "node-4", "node-5"} {
		clients[id].On(methodAppendEntries, mock.Anything, mock.MatchedBy(func(req *types.AppendEntriesArgs) bool {
			return len(req.Entries) == 3 && req.Entries[0].Index == 1
		})).Return(successResponse(), nil)
	}

	err := runSendLogs(peer)
	assert.NoError(t, err)
	for _, c := range clients {
		c.AssertExpectations(t)
	}
}

// ── 2. nextIndex == 1 → prevLog is zero value ─────────────────────────────────

func TestSendLogs_NextIndexOne_ZeroPrevLog(t *testing.T) {
	peer, store, clients := setupSendLogsTest(t)

	appendLogsHelper(store, []*types.LogEntry{
		{Index: 1, Term: 5, Data: []byte("cmd-1")},
	})

	for _, c := range clients {
		c.On(methodAppendEntries, mock.Anything, mock.MatchedBy(func(req *types.AppendEntriesArgs) bool {
			return req.PrevLogIndex == 0 && req.PrevLogTerm == 0
		})).Return(successResponse(), nil)
	}

	err := runSendLogs(peer)
	assert.NoError(t, err)
	for _, c := range clients {
		c.AssertExpectations(t)
	}
}

// ── 3. nextIndex > 1 → fetches correct prevLog ────────────────────────────────

func TestSendLogs_NextIndexGreaterThanOne_CorrectPrevLog(t *testing.T) {
	peer, store, clients := setupSendLogsTest(t)

	appendLogsHelper(store, []*types.LogEntry{
		{Index: 1, Term: 4, Data: []byte("cmd-1")},
		{Index: 2, Term: 5, Data: []byte("cmd-2")},
		{Index: 3, Term: 5, Data: []byte("cmd-3")},
	})

	for id := range peer.peerIndexes {
		peer.peerIndexes[id] = PeerIndexes{nextIndex: 3, matchIndex: 2}
	}

	for _, c := range clients {
		c.On(methodAppendEntries, mock.Anything, mock.MatchedBy(func(req *types.AppendEntriesArgs) bool {
			return req.PrevLogIndex == 2 && req.PrevLogTerm == 5
		})).Return(successResponse(), nil)
	}

	err := runSendLogs(peer)
	assert.NoError(t, err)
	for _, c := range clients {
		c.AssertExpectations(t)
	}
}

// ── 4. All peers succeed → nextIndex and matchIndex advance ───────────────────

func TestSendLogs_AllSucceed_IndexesAdvance(t *testing.T) {
	peer, store, clients := setupSendLogsTest(t)

	appendLogsHelper(store, []*types.LogEntry{
		{Index: 1, Term: 5, Data: []byte("cmd-1")},
		{Index: 2, Term: 5, Data: []byte("cmd-2")},
	})

	for _, c := range clients {
		c.On(methodAppendEntries, mock.Anything, mock.Anything).Return(successResponse(), nil)
	}

	err := runSendLogs(peer)
	assert.NoError(t, err)

	for id := range clients {
		assert.Equal(t, uint(3), peer.GetPeerIndex(id).nextIndex,
			"nextIndex should advance to 3 for %s", id)
		assert.Equal(t, uint(2), peer.GetPeerIndex(id).matchIndex,
			"matchIndex should be 2 for %s", id)
	}
}

// ── 5. All peers fail → nextIndex decrements, matchIndex unchanged ────────────

func TestSendLogs_AllFail_NextIndexDecrements(t *testing.T) {
	peer, store, clients := setupSendLogsTest(t)

	appendLogsHelper(store, []*types.LogEntry{
		{Index: 1, Term: 5, Data: []byte("cmd-1")},
	})

	for id := range peer.peerIndexes {
		peer.peerIndexes[id] = PeerIndexes{nextIndex: 2, matchIndex: 0}
	}

	for _, c := range clients {
		c.On(methodAppendEntries, mock.Anything, mock.Anything).Return(failResponse(), nil)
	}

	err := runSendLogs(peer)
	assert.NoError(t, err)

	for id := range clients {
		assert.Equal(t, uint(1), peer.GetPeerIndex(id).nextIndex,
			"nextIndex should decrement to 1 for %s", id)
		assert.Equal(t, uint(0), peer.GetPeerIndex(id).matchIndex,
			"matchIndex should stay 0 for %s", id)
	}
}

// ── 6. Some succeed some fail → correct per-peer index updates ───────────────

func TestSendLogs_PartialSuccess_PerPeerIndexUpdates(t *testing.T) {
	peer, store, clients := setupSendLogsTest(t)

	appendLogsHelper(store, []*types.LogEntry{
		{Index: 1, Term: 5, Data: []byte("cmd-1")},
		{Index: 2, Term: 5, Data: []byte("cmd-2")},
	})

	for id := range peer.peerIndexes {
		peer.peerIndexes[id] = PeerIndexes{nextIndex: 2, matchIndex: 1}
	}

	clients["node-2"].On(methodAppendEntries, mock.Anything, mock.Anything).Return(successResponse(), nil)
	clients["node-3"].On(methodAppendEntries, mock.Anything, mock.Anything).Return(failResponse(), nil)
	clients["node-4"].On(methodAppendEntries, mock.Anything, mock.Anything).Return(failResponse(), nil)
	clients["node-5"].On(methodAppendEntries, mock.Anything, mock.Anything).Return(failResponse(), nil)

	err := runSendLogs(peer)
	assert.NoError(t, err)

	assert.Equal(t, uint(3), peer.GetPeerIndex("node-2").nextIndex)
	assert.Equal(t, uint(2), peer.GetPeerIndex("node-2").matchIndex)
	assert.Equal(t, uint(1), peer.GetPeerIndex("node-3").nextIndex)
	assert.Equal(t, uint(1), peer.GetPeerIndex("node-4").nextIndex)
	assert.Equal(t, uint(1), peer.GetPeerIndex("node-5").nextIndex)
}

// ── 7. Heartbeat — no logs to send ───────────────────────────────────────────

func TestSendLogs_NoLogs_HeartbeatSent(t *testing.T) {
	peer, _, clients := setupSendLogsTest(t)

	for _, c := range clients {
		c.On(methodAppendEntries, mock.Anything, mock.MatchedBy(func(req *types.AppendEntriesArgs) bool {
			return len(req.Entries) == 0
		})).Return(successResponse(), nil)
	}

	err := runSendLogs(peer)
	assert.NoError(t, err)
	for _, c := range clients {
		c.AssertExpectations(t)
	}
}

// ── 8. Peer responds with higher term → becomeFollower ────────────────────────

func TestSendLogs_PeerHigherTerm_BecomesFollower(t *testing.T) {
	peer, store, clients := setupSendLogsTest(t)

	appendLogsHelper(store, []*types.LogEntry{
		{Index: 1, Term: 5, Data: []byte("cmd-1")},
	})

	clients["node-2"].On(methodAppendEntries, mock.Anything, mock.Anything).Return(higherTermResponse(10), nil)
	clients["node-3"].On(methodAppendEntries, mock.Anything, mock.Anything).Return(successResponse(), nil)
	clients["node-4"].On(methodAppendEntries, mock.Anything, mock.Anything).Return(successResponse(), nil)
	clients["node-5"].On(methodAppendEntries, mock.Anything, mock.Anything).Return(successResponse(), nil)

	err := runSendLogs(peer)
	assert.NoError(t, err)
	assert.Equal(t, ServerRole_Follower, peer.GetRole())
}

// ── 9. Some peers get heartbeat, some get logs ───────────────────────────────

func TestSendLogs_MixedHeartbeatAndLogs(t *testing.T) {
	peer, store, clients := setupSendLogsTest(t)

	appendLogsHelper(store, []*types.LogEntry{
		{Index: 1, Term: 5, Data: []byte("cmd-1")},
		{Index: 2, Term: 5, Data: []byte("cmd-2")},
		{Index: 3, Term: 5, Data: []byte("cmd-3")},
	})

	peer.peerIndexes["node-2"] = PeerIndexes{nextIndex: 4, matchIndex: 3}
	peer.peerIndexes["node-3"] = PeerIndexes{nextIndex: 1, matchIndex: 0}
	peer.peerIndexes["node-4"] = PeerIndexes{nextIndex: 2, matchIndex: 1}
	peer.peerIndexes["node-5"] = PeerIndexes{nextIndex: 3, matchIndex: 2}

	clients["node-2"].On(methodAppendEntries, mock.Anything, mock.MatchedBy(func(req *types.AppendEntriesArgs) bool {
		return len(req.Entries) == 0
	})).Return(successResponse(), nil)

	clients["node-3"].On(methodAppendEntries, mock.Anything, mock.MatchedBy(func(req *types.AppendEntriesArgs) bool {
		return len(req.Entries) == 3 && req.Entries[0].Index == 1
	})).Return(successResponse(), nil)

	clients["node-4"].On(methodAppendEntries, mock.Anything, mock.MatchedBy(func(req *types.AppendEntriesArgs) bool {
		return len(req.Entries) == 2 && req.Entries[0].Index == 2
	})).Return(successResponse(), nil)

	clients["node-5"].On(methodAppendEntries, mock.Anything, mock.MatchedBy(func(req *types.AppendEntriesArgs) bool {
		return len(req.Entries) == 1 && req.Entries[0].Index == 3
	})).Return(successResponse(), nil)

	err := runSendLogs(peer)
	assert.NoError(t, err)
	for _, c := range clients {
		c.AssertExpectations(t)
	}
}

// ── 10. GetCurrentTerm fails ──────────────────────────────────────────────────

func TestSendLogs_DBErr_GetCurrentTerm(t *testing.T) {
	mockStore := new(db.MockStore)
	mockStore.On("GetCurrentTerm", mock.Anything).Return(uint(0), errors.New("db error"))

	peer := &Peer{
		ID:                "node-1",
		Role:              ServerRole_Leader,
		store:             mockStore,
		electionTimeoutCh: make(chan struct{}, 10),
		peerIndexes: map[string]PeerIndexes{
			"node-2": {nextIndex: 1, matchIndex: 0},
		},
		ServerIDRpcUrlMap: map[string]types.RaftRpcClient{
			"node-2": NewMockRaftRpcClient(),
		},
	}

	err := runSendLogs(peer)
	assert.Error(t, err)
}

// ── 11. GetLogByIndex fails (fetching prevLog) ────────────────────────────────

func TestSendLogs_DBErr_GetLogByIndex(t *testing.T) {
	mockStore := new(db.MockStore)
	mockStore.On("GetCurrentTerm", mock.Anything).Return(uint(5), nil)
	mockStore.On("GetLogByIndex", mock.Anything, uint(1)).Return(nil, errors.New("db error"))

	peer := &Peer{
		ID:                "node-1",
		Role:              ServerRole_Leader,
		store:             mockStore,
		electionTimeoutCh: make(chan struct{}, 10),
		peerIndexes: map[string]PeerIndexes{
			"node-2": {nextIndex: 2, matchIndex: 1},
		},
		ServerIDRpcUrlMap: map[string]types.RaftRpcClient{
			"node-2": NewMockRaftRpcClient(),
		},
	}

	err := runSendLogs(peer)
	assert.Error(t, err)
}

// ── 12. GetLogs fails ─────────────────────────────────────────────────────────

func TestSendLogs_DBErr_GetLogs(t *testing.T) {
	mockStore := new(db.MockStore)
	mockStore.On("GetCurrentTerm", mock.Anything).Return(uint(5), nil)
	mockStore.On("GetLogs", mock.Anything, mock.Anything, mock.Anything).Return(
		([]*types.LogEntry)(nil), errors.New("db error"),
	)

	nextIdx := uint(1)
	peer := &Peer{
		ID:                "node-1",
		Role:              ServerRole_Leader,
		store:             mockStore,
		electionTimeoutCh: make(chan struct{}, 10),
		peerIndexes: map[string]PeerIndexes{
			"node-2": {nextIndex: nextIdx, matchIndex: 0},
		},
		ServerIDRpcUrlMap: map[string]types.RaftRpcClient{
			"node-2": NewMockRaftRpcClient(),
		},
	}

	err := runSendLogs(peer)
	assert.Error(t, err)
}

// ════════════════════════════════════════════════════════════════════════════
// getMajorityMatchIndex unit tests
//
// Pure function — no goroutines, no sleep. Tests cover the commit index
// calculation logic directly. startCommitIndexUpdater calls this to decide
// what index is safe to commit.
// ════════════════════════════════════════════════════════════════════════════

// ── 13. Majority replicated → highest index with majority returned ────────────
// n2:5, n3:3, n4:4, n5:6, self:7
// sorted desc: [7,6,5,4,3] → freq: 7:1, 6:2, 5:3, 4:4, 3:5
// majority count = (4+1)/2+1 = 3 → first index with freq >= 3 is 5

func TestGetMajorityMatchIndex_MajorityReplicated(t *testing.T) {
	peerIndexes := map[string]PeerIndexes{
		"node-2": {matchIndex: 5},
		"node-3": {matchIndex: 3},
		"node-4": {matchIndex: 4},
		"node-5": {matchIndex: 6},
	}

	result := getMajorityMatchIndex(peerIndexes, 7)
	assert.Equal(t, uint(5), result)
}

// ── 14. No majority → returns 0 ───────────────────────────────────────────────

func TestGetMajorityMatchIndex_NoMajority(t *testing.T) {
	peerIndexes := map[string]PeerIndexes{
		"node-2": {matchIndex: 0},
		"node-3": {matchIndex: 0},
		"node-4": {matchIndex: 0},
		"node-5": {matchIndex: 0},
	}

	// only self has index 1 — not a majority of 5
	result := getMajorityMatchIndex(peerIndexes, 1)
	assert.Equal(t, uint(0), result)
}

// ── 15. All peers at same index → that index returned ─────────────────────────

func TestGetMajorityMatchIndex_AllSameIndex(t *testing.T) {
	peerIndexes := map[string]PeerIndexes{
		"node-2": {matchIndex: 3},
		"node-3": {matchIndex: 3},
		"node-4": {matchIndex: 3},
		"node-5": {matchIndex: 3},
	}

	result := getMajorityMatchIndex(peerIndexes, 3)
	assert.Equal(t, uint(3), result)
}

// ════════════════════════════════════════════════════════════════════════════
// startCommitIndexUpdater tests
//
// Periodically computes getMajorityMatchIndex and calls SetCommitIndex only
// when the log at commitIndex belongs to the current term (Raft §5.4.2).
// Skips when commitIndex == 0 (no replication yet). Continues on DB errors
// rather than dying — transient failures just delay the next update.
//
// Since this runs in a goroutine with a sleep, tests use assert.Eventually
// to poll for the expected state rather than asserting synchronously.
// ════════════════════════════════════════════════════════════════════════════

// ── 16. Majority replicated, current term log → commitIndex advances ──────────

func TestStartCommitIndexUpdater_CurrentTermLog_CommitIndexAdvances(t *testing.T) {
	store := db.NewMockKVStore()
	store.SetCurrentTerm(context.Background(), 5)
	store.AppendLogs(context.Background(), []*types.LogEntry{
		{Index: 1, Term: 5, Data: []byte("cmd-1")},
		{Index: 2, Term: 5, Data: []byte("cmd-2")},
	})

	peer := NewPeerMock(store)
	// self + node-2 + node-3 = 3 = majority of 5, all at index 2
	peer.peerIndexes["node-2"] = PeerIndexes{matchIndex: 2}
	peer.peerIndexes["node-3"] = PeerIndexes{matchIndex: 2}
	peer.peerIndexes["node-4"] = PeerIndexes{matchIndex: 0}
	peer.peerIndexes["node-5"] = PeerIndexes{matchIndex: 0}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go peer.startCommitIndexUpdater(ctx)

	assert.Eventually(t, func() bool {
		return peer.GetCommitIndex() == uint(2)
	}, 2*time.Second, 10*time.Millisecond,
		"commitIndex should advance to 2 once majority replicates current-term log")
}

// ── 17. Previous term log → commitIndex does NOT advance (Raft §5.4.2) ────────

func TestStartCommitIndexUpdater_PreviousTermLog_CommitIndexUnchanged(t *testing.T) {
	store := db.NewMockKVStore()
	store.SetCurrentTerm(context.Background(), 5)
	// log is from term 4, not current term 5
	store.AppendLogs(context.Background(), []*types.LogEntry{
		{Index: 1, Term: 4, Data: []byte("cmd-1")},
	})

	peer := NewPeerMock(store)
	// all peers have replicated index 1 — majority achieved
	for id := range peer.peerIndexes {
		peer.peerIndexes[id] = PeerIndexes{matchIndex: 1}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	go peer.startCommitIndexUpdater(ctx)

	// let it run for the full timeout — commitIndex must never move
	<-ctx.Done()
	assert.Equal(t, uint(0), peer.GetCommitIndex(),
		"commitIndex must not advance for logs from previous terms (Raft §5.4.2)")
}

// ── 18. No majority → commitIndex stays 0 ────────────────────────────────────

func TestStartCommitIndexUpdater_NoMajority_CommitIndexUnchanged(t *testing.T) {
	store := db.NewMockKVStore()
	store.SetCurrentTerm(context.Background(), 5)
	store.AppendLogs(context.Background(), []*types.LogEntry{
		{Index: 1, Term: 5, Data: []byte("cmd-1")},
	})

	peer := NewPeerMock(store)
	// only self has the log — no peer has replicated it
	for id := range peer.peerIndexes {
		peer.peerIndexes[id] = PeerIndexes{matchIndex: 0}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	go peer.startCommitIndexUpdater(ctx)

	<-ctx.Done()
	assert.Equal(t, uint(0), peer.GetCommitIndex(),
		"commitIndex must not advance without majority replication")
}

// ── 19. DB error on GetLastLogIndex → continues, does not die ────────────────

func TestStartCommitIndexUpdater_DBErr_GetLastLogIndex_Continues(t *testing.T) {
	mockStore := new(db.MockStore)

	// first call errors, all subsequent calls succeed — proves the loop continued past the error
	mockStore.On("GetLastLogIndex", mock.Anything).Return(uint(0), errors.New("db error")).Once()
	mockStore.On("GetLastLogIndex", mock.Anything).Return(uint(1), nil)
	mockStore.On("GetLogByIndex", mock.Anything, uint(1)).Return(&types.LogEntry{Index: 1, Term: 5}, nil)
	mockStore.On("GetCurrentTerm", mock.Anything).Return(uint(5), nil)

	peer := NewPeerMock(mockStore)
	for id := range peer.peerIndexes {
		peer.peerIndexes[id] = PeerIndexes{matchIndex: 1}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go peer.startCommitIndexUpdater(ctx)

	// if the goroutine dies on first error, commitIndex never advances
	assert.Eventually(t, func() bool {
		return peer.GetCommitIndex() > 0
	}, 2*time.Second, 10*time.Millisecond,
		"updater must continue past DB errors and eventually commit")
}

// ── 20. Context cancelled → goroutine exits cleanly ──────────────────────────

func TestStartCommitIndexUpdater_ContextCancelled_Exits(t *testing.T) {
	store := db.NewMockKVStore()
	store.SetCurrentTerm(context.Background(), 5)

	peer := NewPeerMock(store)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		peer.startCommitIndexUpdater(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
		// exited cleanly
	case <-time.After(500 * time.Millisecond):
		t.Fatal("startCommitIndexUpdater did not exit after context cancellation")
	}
}
