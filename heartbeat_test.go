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

// ── 19. DB error on GetLastIndex → continues, does not die ────────────────

func TestStartCommitIndexUpdater_DBErr_GetLastIndex_Continues(t *testing.T) {
	mockStore := new(MockStorage)

	// first call errors, all subsequent calls succeed — proves the loop continued past the error
	mockStore.On("GetLastIndex", mock.Anything).Return(uint(0), errors.New("db error")).Once()
	mockStore.On("GetLastIndex", mock.Anything).Return(uint(1), nil)
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
	updateCommitCh <- struct{}{} // first signal → GetLastIndex errors, loop continues

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

// ════════════════════════════════════════════════════════════════════════════
// startSendLogs — the orchestrator
//
// It owns the leadership term: it picks the peer set once, spawns one
// sendLogsPerPeer per peer, and then sits in a select that ENDS the term on
// step-down / election-timeout / cancellation, but keeps going when a member is
// added mid-term. That asymmetry is the whole reason the select is inside a
// loop, and it is what these tests pin.
// ════════════════════════════════════════════════════════════════════════════

// setupOrchestratorTest builds a leader with the per-term fan-out bookkeeping
// becomeLeader would have created, so startSendLogs can be exercised on its own.
func setupOrchestratorTest(t *testing.T) (*Node, *callRecorder, context.Context) {
	t.Helper()

	store := NewMemStorage()
	store.SetCurrentTerm(context.Background(), 5)

	rec := newCallRecorder()
	transport := NewMockTransport()
	transport.On(methodAppendEntries, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { rec.add(args.String(0)) }).
		Return(AppendEntriesResponse{Term: 5, Success: true}, nil)

	node := NewNodeMock(store, nil)
	node.transport = transport
	node.Role = ServerRole_Leader
	for _, id := range []string{"node-2", "node-3", "node-4", "node-5"} {
		node.addPeer(id, Peer{PeerState: PeerState_Voter, NextIndex: 1})
	}

	// What becomeLeader sets up for the term.
	node.memberRemovedCh = map[string]chan struct{}{}
	for _, id := range node.peerIDs() {
		node.memberRemovedCh[id] = make(chan struct{}, 1)
	}
	node.memberAddedCh = make(chan string, 1)

	// becomeFollower logs through n.ctx and starts an election timer on it.
	ctx, cancel := context.WithCancel(context.Background())
	node.ctx, node.cancel = ctx, cancel
	t.Cleanup(cancel)

	return node, rec, ctx
}

// runOrchestrator runs startSendLogs on its own goroutine, returning a channel
// closed when it returns — which is how these tests tell "still leading" from
// "term over" without reaching into internal state.
func runOrchestrator(n *Node, ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		n.startSendLogs(ctx)
	}()
	return done
}

func assertStillRunning(t *testing.T, done <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-done:
		t.Fatal(msg)
	default:
	}
}

func assertReturned(t *testing.T, done <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal(msg)
	}
}

// callRecorder tracks which peers received an AppendEntries. testify appends to
// mock.Calls from every peer goroutine, so reading that slice from the test
// goroutine races; record through the Run hook into our own guarded set instead.
type callRecorder struct {
	mu   sync.Mutex
	seen map[string]int
}

func newCallRecorder() *callRecorder {
	return &callRecorder{seen: map[string]int{}}
}

func (r *callRecorder) add(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen[id]++
}

func (r *callRecorder) sawPeer(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seen[id] > 0
}

func (r *callRecorder) distinctPeers() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.seen)
}

// ── 20. A member added mid-term does NOT end the term ─────────────────────────
//
// The regression test for the missing loop: without it, control fell out of the
// select on the first member-added, startSendLogs returned, and its deferred
// cancel killed every peer goroutine — while the node still believed it was
// leader. Silent, so the assertion has to be that the orchestrator is still
// alive AND still able to end the term afterwards.

func TestStartSendLogs_MemberAdded_TermContinues(t *testing.T) {
	node, rec, ctx := setupOrchestratorTest(t)
	done := runOrchestrator(node, ctx)

	node.addPeer("node-99", Peer{PeerState: PeerState_Voter, NextIndex: 1})
	node.memberAddedCh <- "node-99"

	// The new member starts receiving heartbeats...
	assert.Eventually(t, func() bool {
		return rec.sawPeer("node-99")
	}, 2*time.Second, 10*time.Millisecond,
		"a member added mid-term should get its own replication goroutine")

	// ...and the orchestrator is still running the term.
	assertStillRunning(t, done, "startSendLogs returned after a member was added")

	// Still able to end the term, which the pre-loop version could not do:
	// it had already returned and nothing was reading electionTimeoutCh.
	node.electionTimeoutCh <- struct{}{}
	assertReturned(t, done, "orchestrator did not step down after a member was added")
	assert.Equal(t, ServerRole_Follower, node.GetRole())
}

// ── 21. A member added mid-term gets a stop channel ──────────────────────────

func TestStartSendLogs_MemberAdded_GetsStopChannel(t *testing.T) {
	node, _, ctx := setupOrchestratorTest(t)
	done := runOrchestrator(node, ctx)
	t.Cleanup(func() { node.electionTimeoutCh <- struct{}{}; <-done })

	assert.Nil(t, removedChFor(node, "node-99"), "not a member yet")

	node.addPeer("node-99", Peer{PeerState: PeerState_Voter, NextIndex: 1})
	node.memberAddedCh <- "node-99"

	assert.Eventually(t, func() bool {
		return removedChFor(node, "node-99") != nil
	}, 2*time.Second, 10*time.Millisecond,
		"a member added mid-term must be stoppable like any other")
}

// ── 22. The peer set skips self and Staging members ──────────────────────────
//
// Self would heartbeat its own address forever; Staging members are driven by
// AddMember's catch-up out of band, and a second replication stream would race it.

func TestStartSendLogs_SkipsSelfAndStagingPeers(t *testing.T) {
	node, rec, ctx := setupOrchestratorTest(t)
	node.addPeer("node-staging", Peer{PeerState: PeerState_Staging, NextIndex: 1})

	done := runOrchestrator(node, ctx)
	t.Cleanup(func() { node.electionTimeoutCh <- struct{}{}; <-done })

	assert.Eventually(t, func() bool {
		return rec.distinctPeers() >= 4
	}, 2*time.Second, 10*time.Millisecond, "the four voters should be replicated to")

	assert.False(t, rec.sawPeer(node.GetID()), "a leader must never replicate to itself")
	assert.False(t, rec.sawPeer("node-staging"), "Staging members are AddMember's job, not the fan-out's")
}

// ── 23. Terminal cases end the term ──────────────────────────────────────────

func TestStartSendLogs_ContextCancelled_Returns(t *testing.T) {
	node, _, _ := setupOrchestratorTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := runOrchestrator(node, ctx)

	cancel()

	assertReturned(t, done, "orchestrator did not return on context cancellation")
	assert.Equal(t, ServerRole_Leader, node.GetRole(),
		"cancellation is shutdown, not a step-down — the role is not touched")
}

func TestStartSendLogs_StepDownSignal_BecomesFollower(t *testing.T) {
	node, _, ctx := setupOrchestratorTest(t)
	done := runOrchestrator(node, ctx)

	node.electionTimeoutCh <- struct{}{}

	assertReturned(t, done, "orchestrator did not return on the step-down signal")
	assert.Equal(t, ServerRole_Follower, node.GetRole())
}

// ════════════════════════════════════════════════════════════════════════════
// sendLogsPerPeer — the per-peer loop
// ════════════════════════════════════════════════════════════════════════════

// ── 24. A removed peer's goroutine exits ─────────────────────────────────────

func TestSendLogsPerPeer_MemberRemoved_Exits(t *testing.T) {
	node, _, ctx := setupOrchestratorTest(t)

	removeCh := make(chan struct{}, 1)
	stepDownCh := make(chan struct{}, 1)
	updateCommitCh := make(chan struct{}, 1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		node.sendLogsPerPeer(ctx, "node-2", stepDownCh, updateCommitCh, removeCh)
	}()

	removeCh <- struct{}{}

	assertReturned(t, done, "peer goroutine did not stop after its member was removed")
}

func TestSendLogsPerPeer_ContextCancelled_Exits(t *testing.T) {
	node, _, _ := setupOrchestratorTest(t)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		node.sendLogsPerPeer(ctx, "node-2", make(chan struct{}, 1), make(chan struct{}, 1), make(chan struct{}, 1))
	}()

	cancel()

	assertReturned(t, done, "peer goroutine did not exit on context cancellation")
}

// ════════════════════════════════════════════════════════════════════════════
// Fan-out notifications
//
// The channels exist only for a leadership term, so every notification has to
// survive their absence. A bare send here is what hung the whole suite: nil
// channel, blocked forever, and AddMember never returned.
// ════════════════════════════════════════════════════════════════════════════

func TestNotifyMemberAdded_NotLeading_IsNoOp(t *testing.T) {
	node := NewNodeMock(NewMemStorage(), nil) // memberAddedCh is nil

	done := make(chan struct{})
	go func() {
		defer close(done)
		node.notifyMemberAdded(context.Background(), "node-99")
	}()

	assertReturned(t, done, "notifyMemberAdded blocked on a nil channel")
}

func TestNotifyMemberRemoved_UnknownPeer_IsNoOp(t *testing.T) {
	node := NewNodeMock(NewMemStorage(), nil) // memberRemovedCh is nil

	done := make(chan struct{})
	go func() {
		defer close(done)
		node.notifyMemberRemoved("node-99")
	}()

	assertReturned(t, done, "notifyMemberRemoved blocked on a nil channel")
}

func TestNotifyMemberRemoved_DeliversToThatPeerOnly(t *testing.T) {
	node, _, _ := setupOrchestratorTest(t)

	node.notifyMemberRemoved("node-3")

	assert.Len(t, node.memberRemovedCh["node-3"], 1, "node-3 should have been told to stop")
	assert.Empty(t, node.memberRemovedCh["node-2"], "no other peer should be disturbed")
}

// A second notification while one is pending is dropped rather than blocking —
// the peer only needs telling once.
func TestNotifyMemberRemoved_RepeatedIsNonBlocking(t *testing.T) {
	node, _, _ := setupOrchestratorTest(t)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 5; i++ {
			node.notifyMemberRemoved("node-3")
		}
	}()

	assertReturned(t, done, "repeated notifyMemberRemoved blocked once the buffer filled")
	assert.Len(t, node.memberRemovedCh["node-3"], 1)
}

// ── The per-term bookkeeping behind becomeLeader ─────────────────────────────
//
// This targets initLeaderTermState directly rather than going through
// becomeLeader, and that is load-bearing. becomeLeader ends in startSendLogs,
// which backfills any missing stop channel via ensureMemberRemovedCh — so a test
// driven through becomeLeader cannot distinguish correct bookkeeping here from
// bookkeeping startSendLogs quietly repaired, and passes even with the map
// created inside the loop.

func TestInitLeaderTermState_StopChannelForEveryNonStagingPeer(t *testing.T) {
	node, _, _ := setupOrchestratorTest(t)
	node.memberRemovedCh = nil
	node.memberAddedCh = nil
	node.addPeer("node-staging", Peer{PeerState: PeerState_Staging, NextIndex: 1})

	node.initLeaderTermState(7)

	for _, id := range []string{"node-2", "node-3", "node-4", "node-5"} {
		assert.NotNil(t, removedChFor(node, id),
			"every non-Staging peer needs its own stop channel, not just the last one: %s", id)
	}
	assert.Nil(t, removedChFor(node, "node-staging"), "Staging peers get no fan-out goroutine")
	assert.Nil(t, removedChFor(node, node.GetID()), "self is never replicated to")
}

func TestInitLeaderTermState_SeedsReplicationIndexes(t *testing.T) {
	node, _, _ := setupOrchestratorTest(t)
	node.SetMatchPeerIndex("node-2", 99) // stale state from a previous term

	node.initLeaderTermState(7)

	for _, id := range node.peerIDs() {
		assert.Equal(t, uint(8), node.GetPeerIndex(id).NextIndex, "nextIndex = lastIndex+1 for %s", id)
		assert.Equal(t, uint(0), node.GetPeerIndex(id).MatchIndex, "matchIndex resets for %s", id)
	}
	assert.Equal(t, uint(0), node.GetPeerIndex(node.GetID()).NextIndex,
		"our own entry has no replication index to seed")
}

// ════════════════════════════════════════════════════════════════════════════
// Committed configuration
//
// A config change replicates as an ordinary log entry, so `latest` can point at
// a configuration that has not committed yet. `committed` is the safe fallback a
// truncation rolls back to, and it only advances once the entry that produced
// `latest` is actually committed — which is what startCommitIndexUpdater does
// after it moves commitIndex.
// ════════════════════════════════════════════════════════════════════════════

// removedChFor reads a peer's stop channel WITHOUT creating one. Production has
// only ensureMemberRemovedCh, which would manufacture the very channel these
// assertions check for the absence of — so the non-creating read lives here,
// where it is the only thing that needs it.
func removedChFor(n *Node, id string) <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.memberRemovedCh[id]
}

// committedConfig reads the committed view under mu, so assertions do not race
// the updater goroutine that writes it.
func committedConfig(n *Node) (map[string]Peer, uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return clonePeers(n.configurations.committed), n.configurations.committedIndex
}

func latestConfig(n *Node) (map[string]Peer, uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return clonePeers(n.configurations.latest), n.configurations.latestIndex
}

// setupCommittedConfigTest builds a leader whose four voter peers have all
// replicated up to matchIndex, so the majority match index is matchIndex.
func setupCommittedConfigTest(t *testing.T, matchIndex uint, entries []LogEntry) *Node {
	t.Helper()

	store := NewMemStorage()
	store.SetCurrentTerm(context.Background(), 5)
	store.AppendLogs(context.Background(), entries)

	node := NewNodeMock(store, nil)
	for _, id := range []string{"node-2", "node-3", "node-4", "node-5"} {
		node.addPeer(id, Peer{PeerState: PeerState_Voter, MatchIndex: matchIndex})
	}
	return node
}

func runCommitIndexUpdaterOnce(t *testing.T, node *Node) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	updateCommitCh := make(chan struct{}, 1)
	go node.startCommitIndexUpdater(ctx, updateCommitCh)
	updateCommitCh <- struct{}{}
}

// ── 25. The entry that produced `latest` commits → `committed` catches up ────

func TestCommitIndexUpdater_ConfigEntryCommitted_AdvancesCommitted(t *testing.T) {
	node := setupCommittedConfigTest(t, 2, []LogEntry{
		{Index: 1, Term: 5, Data: []byte("cmd-1")},
		{Index: 2, Term: 5, Type: EntryType_Config},
	})

	// A config entry at index 2 is live but not yet known committed. This mirrors
	// what the leader's appendEntry does: mutate latest, then record the index the
	// config entry landed at.
	node.addPeer("node-99", Peer{PeerState: PeerState_Voter})
	node.setLatestConfiguration(node.peersSnapshot(), 2)

	before, _ := committedConfig(node)
	assert.NotContains(t, before, "node-99", "committed should still be the bootstrap config")

	runCommitIndexUpdaterOnce(t, node)

	assert.Eventually(t, func() bool {
		_, idx := committedConfig(node)
		return idx == 2
	}, 2*time.Second, 10*time.Millisecond,
		"committed should advance once the config entry commits")

	got, _ := committedConfig(node)
	assert.Contains(t, got, "node-99", "committed should now hold the live membership")
}

// ── 26. A config entry still uncommitted leaves `committed` alone ────────────

func TestCommitIndexUpdater_ConfigEntryNotYetCommitted_LeavesCommitted(t *testing.T) {
	// Peers have only replicated up to index 1, but the config entry is at 3.
	node := setupCommittedConfigTest(t, 1, []LogEntry{
		{Index: 1, Term: 5, Data: []byte("cmd-1")},
		{Index: 2, Term: 5, Data: []byte("cmd-2")},
		{Index: 3, Term: 5, Type: EntryType_Config},
	})
	node.addPeer("node-99", Peer{PeerState: PeerState_Voter})
	node.setLatestConfiguration(node.peersSnapshot(), 3)

	runCommitIndexUpdaterOnce(t, node)

	assert.Eventually(t, func() bool {
		return node.GetCommitIndex() == 1
	}, 2*time.Second, 10*time.Millisecond, "commitIndex should reach 1")

	got, idx := committedConfig(node)
	assert.Equal(t, uint64(0), idx, "the config entry at 3 has not committed")
	assert.NotContains(t, got, "node-99", "committed must not adopt an uncommitted configuration")
}

// ── 27. A previous-term entry advances neither (Raft §5.4.2) ─────────────────

func TestCommitIndexUpdater_PreviousTermEntry_AdvancesNeither(t *testing.T) {
	node := setupCommittedConfigTest(t, 1, []LogEntry{
		{Index: 1, Term: 4, Type: EntryType_Config}, // term 4, current term is 5
	})
	node.setLatestConfiguration(node.peersSnapshot(), 1)
	node.addPeer("node-99", Peer{PeerState: PeerState_Voter})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	updateCommitCh := make(chan struct{}, 1)
	go node.startCommitIndexUpdater(ctx, updateCommitCh)
	updateCommitCh <- struct{}{}

	<-ctx.Done()

	assert.Equal(t, uint(0), node.GetCommitIndex(), "§5.4.2: no commit from a previous term")
	_, idx := committedConfig(node)
	assert.Equal(t, uint64(0), idx, "and therefore no committed configuration either")
}

// ── 28. committed must not alias latest ──────────────────────────────────────
//
// They are separate views on purpose. If a mutation to one showed up in the
// other, rollbackLatestIfTruncated would roll back to the configuration it was
// supposed to be escaping.

func TestSetCommittedConfiguration_DoesNotAliasLatest(t *testing.T) {
	node := NewNodeMock(NewMemStorage(), nil)
	node.addPeer("node-2", Peer{PeerState: PeerState_Voter})

	snapshot, _ := latestConfig(node)
	node.setCommittedConfiguration(snapshot, 7)

	node.addPeer("node-99", Peer{PeerState: PeerState_Voter}) // mutate latest afterwards

	got, idx := committedConfig(node)
	assert.Equal(t, uint64(7), idx)
	assert.NotContains(t, got, "node-99", "committed must be a deep copy, not a view of latest")
}

// ── 29. The payoff: a rollback reverts to the last committed config ──────────
//
// Before committed advanced, a truncation that invalidated `latest` rolled back
// all the way to the bootstrap configuration — losing every membership change
// that had legitimately committed in between.

func TestRollback_RevertsToLastCommittedConfig_NotBootstrap(t *testing.T) {
	node := NewNodeMock(NewMemStorage(), nil)

	// A membership change commits at index 4.
	node.addPeer("node-99", Peer{PeerState: PeerState_Voter})
	committedSnapshot, _ := latestConfig(node)
	node.setCommittedConfiguration(committedSnapshot, 4)
	node.setLatestConfiguration(committedSnapshot, 4)

	// A later, still-uncommitted config entry at index 7 adds another member.
	node.addPeer("node-100", Peer{PeerState: PeerState_Voter})
	uncommitted, _ := latestConfig(node)
	node.setLatestConfiguration(uncommitted, 7)

	// That suffix turns out to conflict and is truncated from index 7.
	node.rollbackLatestIfTruncated(7)

	got, idx := latestConfig(node)
	assert.Equal(t, uint64(4), idx, "latest should revert to the committed config's index")
	assert.Contains(t, got, "node-99", "a membership change that DID commit must survive the rollback")
	assert.NotContains(t, got, "node-100", "the uncommitted one must not")
}

// ── 30. The leader records which index produced `latest` ─────────────────────
//
// The follower does this in processConfigurationLogEntry. The leader mutates
// configurations.latest directly (addPeer/removePeer/SetPeerState), so if it did
// not record the index here, latestIndex would stay 0 for the node's whole life —
// and "has the entry behind latest committed yet?" would answer yes to
// everything, letting an uncommitted configuration be marked committed.

func TestAppendEntry_ConfigEntry_RecordsLatestIndex(t *testing.T) {
	store := NewMemStorage()
	store.SetCurrentTerm(context.Background(), 5)
	node := NewNodeMock(store, nil)

	_, idx := latestConfig(node)
	assert.Equal(t, uint64(0), idx, "nothing has produced a configuration yet")

	node.addPeer("node-99", Peer{PeerState: PeerState_Staging}) // what AddMember does
	entry, err := node.appendEntry(context.Background(), EntryType_Config, []byte("{}"))
	assert.NoError(t, err)

	got, gotIdx := latestConfig(node)
	assert.Equal(t, entry.Index, gotIdx, "latestIndex must be the config entry's index")
	assert.Contains(t, got, "node-99")
}

func TestAppendEntry_OrdinaryEntry_LeavesLatestIndexAlone(t *testing.T) {
	store := NewMemStorage()
	store.SetCurrentTerm(context.Background(), 5)
	node := NewNodeMock(store, nil)

	_, err := node.appendEntry(context.Background(), EntryType_Command, []byte("cmd"))
	assert.NoError(t, err)

	_, idx := latestConfig(node)
	assert.Equal(t, uint64(0), idx, "an ordinary command does not produce a configuration")
}

// ── 31. An uncommitted configuration is never marked committed ───────────────
//
// The case that motivated all of the above: AddMember stages a peer locally and
// only then replicates the config entry. If an unrelated client entry commits in
// that window, `committed` must not adopt the staged membership — a later
// rollback would otherwise revert to a configuration that never committed.

func TestCommitIndexUpdater_UncommittedConfig_NotAdopted(t *testing.T) {
	// Peers have replicated index 1 only; the config entry sits at index 2.
	node := setupCommittedConfigTest(t, 1, []LogEntry{
		{Index: 1, Term: 5, Data: []byte("ordinary client command")},
		{Index: 2, Term: 5, Type: EntryType_Config},
	})
	node.addPeer("node-99", Peer{PeerState: PeerState_Staging})
	node.setLatestConfiguration(node.peersSnapshot(), 2)

	runCommitIndexUpdaterOnce(t, node)

	assert.Eventually(t, func() bool {
		return node.GetCommitIndex() == 1
	}, 2*time.Second, 10*time.Millisecond, "the client entry at 1 commits")

	got, idx := committedConfig(node)
	assert.NotContains(t, got, "node-99",
		"a staged peer whose config entry has not committed must not enter committed")
	assert.Equal(t, uint64(0), idx)
}

// ── 32. committedIndex is the config's index, not the commit index ───────────
//
// They are different facts: commitIndex is how far the log has got, latestIndex
// is where this configuration came from. rollbackLatestIfTruncated compares
// truncation points against the latter, so conflating them mis-sizes the window
// in which a rollback fires.

func TestCommitIndexUpdater_CommittedIndexIsTheConfigsIndex(t *testing.T) {
	node := setupCommittedConfigTest(t, 3, []LogEntry{
		{Index: 1, Term: 5, Data: []byte("cmd-1")},
		{Index: 2, Term: 5, Type: EntryType_Config},
		{Index: 3, Term: 5, Data: []byte("cmd-3")},
	})
	node.addPeer("node-99", Peer{PeerState: PeerState_Voter})
	node.setLatestConfiguration(node.peersSnapshot(), 2)

	runCommitIndexUpdaterOnce(t, node)

	assert.Eventually(t, func() bool {
		_, idx := committedConfig(node)
		return idx != 0
	}, 2*time.Second, 10*time.Millisecond, "committed should advance")

	got, idx := committedConfig(node)
	assert.Equal(t, uint(3), node.GetCommitIndex(), "the log committed up to 3")
	assert.Equal(t, uint64(2), idx, "but the configuration came from index 2")
	assert.Contains(t, got, "node-99")
}
