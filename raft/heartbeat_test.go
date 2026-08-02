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

const (
	methodAppendEntries = "AppendEntries"
)

func setupSendLogsTest(t *testing.T) (*Node, *MemStorage, *MockTransport) {
	store := NewMemStorage()
	store.SetCurrentTerm(context.Background(), 5)

	transport := NewMockTransport()
	node := NewNodeMock(store, nil)
	node.transport = transport
	node.Role = ServerRole_Leader

	return node, store, transport
}

// runSendLogs fans out sendLogs to every peer concurrently, mirroring what
// sendLogsPerPeer does on each ticker tick, but synchronously so tests can
// assert state immediately after. Returns (stepDown, error): stepDown is true
// if any peer signalled a higher term (the orchestrator would normally handle
// the role transition, but that path is not exercised here).
func runSendLogs(n *Node) (bool, error) {
	var wg sync.WaitGroup

	// snapshot the peer IDs before fanning out — sendLogs calls SetMatchPeerIndex/
	// SetNextPeerIndex, which write to n.configurations.latest. Ranging the live map here would
	// race with those writes. This is why production uses n.peerIDs(), not the map.
	peerIDs := n.peerIDs()

	errCh := make(chan error, len(peerIDs))

	// mirror the production buffer size — all peers could signal step-down simultaneously
	stepDownCh := make(chan struct{}, len(peerIDs))
	updateCommitIndexCh := make(chan struct{}, len(peerIDs))

	for _, peerID := range peerIDs {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			ch := make(chan error, 1)
			n.sendLogs(context.Background(), id, ch, stepDownCh, updateCommitIndexCh)
			if err := <-ch; err != nil {
				errCh <- err
			}
		}(peerID)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		return false, err
	}

	select {
	case <-stepDownCh:
		return true, nil
	default:
		return false, nil
	}
}

func appendLogsHelper(store *MemStorage, logs []LogEntry) {
	store.AppendLogs(context.Background(), logs)
}

func successResponse() AppendEntriesResponse {
	return AppendEntriesResponse{Term: 5, Success: true}
}

func failResponse() AppendEntriesResponse {
	return AppendEntriesResponse{Term: 5, Success: false}
}

func higherTermResponse(term uint64) AppendEntriesResponse {
	return AppendEntriesResponse{Term: term, Success: false}
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
	node, store, transport := setupSendLogsTest(t)

	appendLogsHelper(store, []LogEntry{
		{Index: 1, Term: 5, Data: []byte("cmd-1")},
		{Index: 2, Term: 5, Data: []byte("cmd-2")},
		{Index: 3, Term: 5, Data: []byte("cmd-3")},
	})

	node.configurations.latest["node-2"] = Peer{NextIndex: 2, MatchIndex: 1}
	node.configurations.latest["node-3"] = Peer{NextIndex: 1, MatchIndex: 0}
	node.configurations.latest["node-4"] = Peer{NextIndex: 1, MatchIndex: 0}
	node.configurations.latest["node-5"] = Peer{NextIndex: 1, MatchIndex: 0}

	transport.On(methodAppendEntries, "node-2", mock.MatchedBy(func(args AppendEntriesArgs) bool {
		return len(args.Entries) == 2 && args.Entries[0].Index == 2
	})).Return(successResponse(), nil)

	for _, id := range []string{"node-3", "node-4", "node-5"} {
		transport.On(methodAppendEntries, id, mock.MatchedBy(func(args AppendEntriesArgs) bool {
			return len(args.Entries) == 3 && args.Entries[0].Index == 1
		})).Return(successResponse(), nil)
	}

	_, err := runSendLogs(node)
	assert.NoError(t, err)
	transport.AssertExpectations(t)
}

// ── 2. nextIndex == 1 → prevLog is zero value ─────────────────────────────────

func TestSendLogs_NextIndexOne_ZeroPrevLog(t *testing.T) {
	node, store, transport := setupSendLogsTest(t)

	appendLogsHelper(store, []LogEntry{
		{Index: 1, Term: 5, Data: []byte("cmd-1")},
	})

	transport.On(methodAppendEntries, mock.Anything, mock.MatchedBy(func(args AppendEntriesArgs) bool {
		return args.PrevLogIndex == 0 && args.PrevLogTerm == 0
	})).Return(successResponse(), nil)

	_, err := runSendLogs(node)
	assert.NoError(t, err)
	transport.AssertExpectations(t)
}

// ── 3. nextIndex > 1 → fetches correct prevLog ────────────────────────────────

func TestSendLogs_NextIndexGreaterThanOne_CorrectPrevLog(t *testing.T) {
	node, store, transport := setupSendLogsTest(t)

	appendLogsHelper(store, []LogEntry{
		{Index: 1, Term: 4, Data: []byte("cmd-1")},
		{Index: 2, Term: 5, Data: []byte("cmd-2")},
		{Index: 3, Term: 5, Data: []byte("cmd-3")},
	})

	for _, id := range node.peerIDs() {
		node.configurations.latest[id] = Peer{NextIndex: 3, MatchIndex: 2}
	}

	transport.On(methodAppendEntries, mock.Anything, mock.MatchedBy(func(args AppendEntriesArgs) bool {
		return args.PrevLogIndex == 2 && args.PrevLogTerm == 5
	})).Return(successResponse(), nil)

	_, err := runSendLogs(node)
	assert.NoError(t, err)
	transport.AssertExpectations(t)
}

// ── 4. All peers succeed → nextIndex and matchIndex advance ───────────────────

func TestSendLogs_AllSucceed_IndexesAdvance(t *testing.T) {
	node, store, transport := setupSendLogsTest(t)

	appendLogsHelper(store, []LogEntry{
		{Index: 1, Term: 5, Data: []byte("cmd-1")},
		{Index: 2, Term: 5, Data: []byte("cmd-2")},
	})

	transport.On(methodAppendEntries, mock.Anything, mock.Anything).Return(successResponse(), nil)

	_, err := runSendLogs(node)
	assert.NoError(t, err)

	for _, id := range node.peerIDs() {
		assert.Equal(t, uint(3), node.GetPeerIndex(id).NextIndex,
			"nextIndex should advance to 3 for %s", id)
		assert.Equal(t, uint(2), node.GetPeerIndex(id).MatchIndex,
			"matchIndex should be 2 for %s", id)
	}
}

// ── 5. All peers fail → nextIndex decrements, matchIndex unchanged ────────────

func TestSendLogs_AllFail_NextIndexDecrements(t *testing.T) {
	node, store, transport := setupSendLogsTest(t)

	appendLogsHelper(store, []LogEntry{
		{Index: 1, Term: 5, Data: []byte("cmd-1")},
	})

	for _, id := range node.peerIDs() {
		node.configurations.latest[id] = Peer{NextIndex: 2, MatchIndex: 0}
	}

	transport.On(methodAppendEntries, mock.Anything, mock.Anything).Return(failResponse(), nil)

	_, err := runSendLogs(node)
	assert.NoError(t, err)

	for _, id := range node.peerIDs() {
		assert.Equal(t, uint(1), node.GetPeerIndex(id).NextIndex,
			"nextIndex should decrement to 1 for %s", id)
		assert.Equal(t, uint(0), node.GetPeerIndex(id).MatchIndex,
			"matchIndex should stay 0 for %s", id)
	}
}

// ── 6. Some succeed some fail → correct per-peer index updates ───────────────

func TestSendLogs_PartialSuccess_PerPeerIndexUpdates(t *testing.T) {
	node, store, transport := setupSendLogsTest(t)

	appendLogsHelper(store, []LogEntry{
		{Index: 1, Term: 5, Data: []byte("cmd-1")},
		{Index: 2, Term: 5, Data: []byte("cmd-2")},
	})

	for _, id := range node.peerIDs() {
		node.configurations.latest[id] = Peer{NextIndex: 2, MatchIndex: 1}
	}

	transport.On(methodAppendEntries, "node-2", mock.Anything).Return(successResponse(), nil)
	transport.On(methodAppendEntries, "node-3", mock.Anything).Return(failResponse(), nil)
	transport.On(methodAppendEntries, "node-4", mock.Anything).Return(failResponse(), nil)
	transport.On(methodAppendEntries, "node-5", mock.Anything).Return(failResponse(), nil)

	_, err := runSendLogs(node)
	assert.NoError(t, err)

	assert.Equal(t, uint(3), node.GetPeerIndex("node-2").NextIndex)
	assert.Equal(t, uint(2), node.GetPeerIndex("node-2").MatchIndex)
	assert.Equal(t, uint(1), node.GetPeerIndex("node-3").NextIndex)
	assert.Equal(t, uint(1), node.GetPeerIndex("node-4").NextIndex)
	assert.Equal(t, uint(1), node.GetPeerIndex("node-5").NextIndex)
}

// ── 7. Heartbeat — no logs to send ───────────────────────────────────────────

func TestSendLogs_NoLogs_HeartbeatSent(t *testing.T) {
	node, _, transport := setupSendLogsTest(t)

	transport.On(methodAppendEntries, mock.Anything, mock.MatchedBy(func(args AppendEntriesArgs) bool {
		return len(args.Entries) == 0
	})).Return(successResponse(), nil)

	_, err := runSendLogs(node)
	assert.NoError(t, err)
	transport.AssertExpectations(t)
}

// ── 8. Peer responds with higher term → step-down signalled ──────────────────
// Role transition is owned by the orchestrator (startSendLogs), not sendLogs.
// sendLogs only sends on stepDownCh and returns; runSendLogs exposes that signal.

func TestSendLogs_PeerHigherTerm_StepDownSignalled(t *testing.T) {
	node, store, transport := setupSendLogsTest(t)

	appendLogsHelper(store, []LogEntry{
		{Index: 1, Term: 5, Data: []byte("cmd-1")},
	})

	transport.On(methodAppendEntries, "node-2", mock.Anything).Return(higherTermResponse(10), nil)
	transport.On(methodAppendEntries, "node-3", mock.Anything).Return(successResponse(), nil)
	transport.On(methodAppendEntries, "node-4", mock.Anything).Return(successResponse(), nil)
	transport.On(methodAppendEntries, "node-5", mock.Anything).Return(successResponse(), nil)

	stepDown, err := runSendLogs(node)
	assert.NoError(t, err)
	assert.True(t, stepDown, "sendLogs should signal step-down when a peer returns a higher term")
}

// ── 9. Some peers get heartbeat, some get logs ───────────────────────────────

func TestSendLogs_MixedHeartbeatAndLogs(t *testing.T) {
	node, store, transport := setupSendLogsTest(t)

	appendLogsHelper(store, []LogEntry{
		{Index: 1, Term: 5, Data: []byte("cmd-1")},
		{Index: 2, Term: 5, Data: []byte("cmd-2")},
		{Index: 3, Term: 5, Data: []byte("cmd-3")},
	})

	node.configurations.latest["node-2"] = Peer{NextIndex: 4, MatchIndex: 3}
	node.configurations.latest["node-3"] = Peer{NextIndex: 1, MatchIndex: 0}
	node.configurations.latest["node-4"] = Peer{NextIndex: 2, MatchIndex: 1}
	node.configurations.latest["node-5"] = Peer{NextIndex: 3, MatchIndex: 2}

	transport.On(methodAppendEntries, "node-2", mock.MatchedBy(func(args AppendEntriesArgs) bool {
		return len(args.Entries) == 0
	})).Return(successResponse(), nil)

	transport.On(methodAppendEntries, "node-3", mock.MatchedBy(func(args AppendEntriesArgs) bool {
		return len(args.Entries) == 3 && args.Entries[0].Index == 1
	})).Return(successResponse(), nil)

	transport.On(methodAppendEntries, "node-4", mock.MatchedBy(func(args AppendEntriesArgs) bool {
		return len(args.Entries) == 2 && args.Entries[0].Index == 2
	})).Return(successResponse(), nil)

	transport.On(methodAppendEntries, "node-5", mock.MatchedBy(func(args AppendEntriesArgs) bool {
		return len(args.Entries) == 1 && args.Entries[0].Index == 3
	})).Return(successResponse(), nil)

	_, err := runSendLogs(node)
	assert.NoError(t, err)
	transport.AssertExpectations(t)
}

// ── 10. GetCurrentTerm fails ──────────────────────────────────────────────────

func TestSendLogs_DBErr_GetCurrentTerm(t *testing.T) {
	mockStore := new(MockStorage)
	mockStore.On("GetCurrentTerm", mock.Anything).Return(uint(0), errors.New("db error"))

	node := &Node{
		ID:    "node-1",
		Role:  ServerRole_Leader,
		store: mockStore,
		cfg: Config{
			Peers: map[string]Peer{"node-2": {NextIndex: 1, MatchIndex: 0}},
		},
		configurations: configurations{
			latest: map[string]Peer{"node-2": {NextIndex: 1, MatchIndex: 0}},
		},
		electionTimeoutCh: make(chan struct{}, 10),
	}

	_, err := runSendLogs(node)
	assert.Error(t, err)
}

// ── 11. GetLogByIndex fails (fetching prevLog) ────────────────────────────────

func TestSendLogs_DBErr_GetLogByIndex(t *testing.T) {
	mockStore := new(MockStorage)
	mockStore.On("GetCurrentTerm", mock.Anything).Return(uint(5), nil)
	mockStore.On("GetLogByIndex", mock.Anything, uint(1)).Return(LogEntry{}, errors.New("db error"))

	node := &Node{
		ID:    "node-1",
		Role:  ServerRole_Leader,
		store: mockStore,
		cfg: Config{
			Peers: map[string]Peer{"node-2": {NextIndex: 2, MatchIndex: 1}},
		},
		configurations: configurations{
			latest: map[string]Peer{"node-2": {NextIndex: 2, MatchIndex: 1}},
		},
		electionTimeoutCh: make(chan struct{}, 10),
	}

	_, err := runSendLogs(node)
	assert.Error(t, err)
}

// ── 12. GetLogs fails ─────────────────────────────────────────────────────────

func TestSendLogs_DBErr_GetLogs(t *testing.T) {
	mockStore := new(MockStorage)
	mockStore.On("GetCurrentTerm", mock.Anything).Return(uint(5), nil)
	mockStore.On("GetLogs", mock.Anything, mock.Anything, mock.Anything).Return(
		([]LogEntry)(nil), errors.New("db error"),
	)

	node := &Node{
		ID:    "node-1",
		Role:  ServerRole_Leader,
		store: mockStore,
		cfg: Config{
			Peers: map[string]Peer{"node-2": {NextIndex: 1, MatchIndex: 0}},
		},
		configurations: configurations{
			latest: map[string]Peer{"node-2": {NextIndex: 1, MatchIndex: 0}},
		},
		electionTimeoutCh: make(chan struct{}, 10),
	}

	_, err := runSendLogs(node)
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
// sorted desc: [7,6,5,4,3], majorityCount=3 → matchIndexes[2]=5

func TestGetMajorityMatchIndex_MajorityReplicated(t *testing.T) {
	peers := map[string]Peer{
		"node-2": {PeerState: PeerState_Voter, MatchIndex: 5},
		"node-3": {PeerState: PeerState_Voter, MatchIndex: 3},
		"node-4": {PeerState: PeerState_Voter, MatchIndex: 4},
		"node-5": {PeerState: PeerState_Voter, MatchIndex: 6},
		// self is a member of the configuration now; its stored MatchIndex is
		// ignored in favour of the selfLastIndex argument.
		"node-1": {PeerState: PeerState_Voter},
	}

	result := getMajorityMatchIndex(peers, "node-1", 7)
	assert.Equal(t, uint(5), result)
}

// ── 14. No majority → returns 0 ───────────────────────────────────────────────

func TestGetMajorityMatchIndex_NoMajority(t *testing.T) {
	peers := map[string]Peer{
		"node-2": {PeerState: PeerState_Voter, MatchIndex: 0},
		"node-3": {PeerState: PeerState_Voter, MatchIndex: 0},
		"node-4": {PeerState: PeerState_Voter, MatchIndex: 0},
		"node-5": {PeerState: PeerState_Voter, MatchIndex: 0},
		"node-1": {PeerState: PeerState_Voter},
	}

	// only self has index 1 — not a majority of 5
	result := getMajorityMatchIndex(peers, "node-1", 1)
	assert.Equal(t, uint(0), result)
}

// ── 15. All peers at same index → that index returned ─────────────────────────

func TestGetMajorityMatchIndex_AllSameIndex(t *testing.T) {
	peers := map[string]Peer{
		"node-2": {PeerState: PeerState_Voter, MatchIndex: 3},
		"node-3": {PeerState: PeerState_Voter, MatchIndex: 3},
		"node-4": {PeerState: PeerState_Voter, MatchIndex: 3},
		"node-5": {PeerState: PeerState_Voter, MatchIndex: 3},
		"node-1": {PeerState: PeerState_Voter},
	}

	result := getMajorityMatchIndex(peers, "node-1", 3)
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
	store := NewMemStorage()
	store.SetCurrentTerm(context.Background(), 5)
	store.AppendLogs(context.Background(), []LogEntry{
		{Index: 1, Term: 5, Data: []byte("cmd-1")},
		{Index: 2, Term: 5, Data: []byte("cmd-2")},
	})

	node := NewNodeMock(store, nil)
	// self + node-2 + node-3 = 3 = majority of 5, all at index 2
	node.configurations.latest["node-2"] = Peer{PeerState: PeerState_Voter, MatchIndex: 2}
	node.configurations.latest["node-3"] = Peer{PeerState: PeerState_Voter, MatchIndex: 2}
	node.configurations.latest["node-4"] = Peer{PeerState: PeerState_Voter, MatchIndex: 0}
	node.configurations.latest["node-5"] = Peer{PeerState: PeerState_Voter, MatchIndex: 0}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	updateCommitCh := make(chan struct{}, 1)
	go node.startCommitIndexUpdater(ctx, updateCommitCh)
	updateCommitCh <- struct{}{}

	assert.Eventually(t, func() bool {
		return node.GetCommitIndex() == uint(2)
	}, 2*time.Second, 10*time.Millisecond,
		"commitIndex should advance to 2 once majority replicates current-term log")
}

// ── 17. Previous term log → commitIndex does NOT advance (Raft §5.4.2) ────────

func TestStartCommitIndexUpdater_PreviousTermLog_CommitIndexUnchanged(t *testing.T) {
	store := NewMemStorage()
	store.SetCurrentTerm(context.Background(), 5)
	// log is from term 4, not current term 5
	store.AppendLogs(context.Background(), []LogEntry{
		{Index: 1, Term: 4, Data: []byte("cmd-1")},
	})

	node := NewNodeMock(store, nil)
	// all peers have replicated index 1 — majority achieved
	for id := range node.configurations.latest {
		node.configurations.latest[id] = Peer{PeerState: PeerState_Voter, MatchIndex: 1}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	updateCommitCh := make(chan struct{}, 1)
	go node.startCommitIndexUpdater(ctx, updateCommitCh)
	updateCommitCh <- struct{}{}

	// let it run for the full timeout — commitIndex must never move
	<-ctx.Done()
	assert.Equal(t, uint(0), node.GetCommitIndex(),
		"commitIndex must not advance for logs from previous terms (Raft §5.4.2)")
}

// ── 18. No majority → commitIndex stays 0 ────────────────────────────────────

func TestStartCommitIndexUpdater_NoMajority_CommitIndexUnchanged(t *testing.T) {
	store := NewMemStorage()
	store.SetCurrentTerm(context.Background(), 5)
	store.AppendLogs(context.Background(), []LogEntry{
		{Index: 1, Term: 5, Data: []byte("cmd-1")},
	})

	node := NewNodeMock(store, nil)
	// only self has the log — no peer has replicated it
	for id := range node.configurations.latest {
		node.configurations.latest[id] = Peer{PeerState: PeerState_Voter, MatchIndex: 0}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	updateCommitCh := make(chan struct{}, 1)
	go node.startCommitIndexUpdater(ctx, updateCommitCh)
	updateCommitCh <- struct{}{}

	<-ctx.Done()
	assert.Equal(t, uint(0), node.GetCommitIndex(),
		"commitIndex must not advance without majority replication")
}

// ── 19. DB error on GetLastLogIndex → continues, does not die ────────────────

func TestStartCommitIndexUpdater_DBErr_GetLastLogIndex_Continues(t *testing.T) {
	mockStore := new(MockStorage)

	// first call errors, all subsequent calls succeed — proves the loop continued past the error
	mockStore.On("GetLastLogIndex", mock.Anything).Return(uint(0), errors.New("db error")).Once()
	mockStore.On("GetLastLogIndex", mock.Anything).Return(uint(1), nil)
	mockStore.On("GetLogByIndex", mock.Anything, uint(1)).Return(LogEntry{Index: 1, Term: 5}, nil)
	mockStore.On("GetCurrentTerm", mock.Anything).Return(uint(5), nil)

	node := NewNodeMock(mockStore, nil)
	for id := range node.configurations.latest {
		node.configurations.latest[id] = Peer{PeerState: PeerState_Voter, MatchIndex: 1}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	updateCommitCh := make(chan struct{}, 1)
	go node.startCommitIndexUpdater(ctx, updateCommitCh)
	updateCommitCh <- struct{}{} // first signal → GetLastLogIndex errors, loop continues

	// if the goroutine dies on first error, commitIndex never advances.
	// each Eventually poll tries to send another signal so the goroutine gets a retry.
	assert.Eventually(t, func() bool {
		select {
		case updateCommitCh <- struct{}{}:
		default:
		}
		return node.GetCommitIndex() > 0
	}, 2*time.Second, 10*time.Millisecond,
		"updater must continue past DB errors and eventually commit")
}

// ── 20. Context cancelled → goroutine exits cleanly ──────────────────────────

func TestStartCommitIndexUpdater_ContextCancelled_Exits(t *testing.T) {
	store := NewMemStorage()
	store.SetCurrentTerm(context.Background(), 5)

	node := NewNodeMock(store, nil)

	ctx, cancel := context.WithCancel(context.Background())

	updateCommitCh := make(chan struct{})
	done := make(chan struct{})
	go func() {
		node.startCommitIndexUpdater(ctx, updateCommitCh)
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
