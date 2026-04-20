package raft

import (
	"context"
	"errors"
	"testing"

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

	peer := NewPeerMock(store)
	peer.Role = ServerRole_Leader

	// extract typed clients for setting expectations
	clients := map[string]*MockRaftRpcClient{}
	for id, client := range peer.ServerIDRpcUrlMap {
		clients[id] = client.(*MockRaftRpcClient)
	}

	return peer, store, clients
}

func runSendLogs(p *Peer) error {
	errChan := make(chan error, 1)
	p.sendLogs(context.Background(), errChan)
	return <-errChan
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

	// all peers at nextIndex=1, prevLog should be zero
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

	// all peers at nextIndex=3, prevLog should be index=2, term=5
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
	// nextIndex should advance by number of logs sent (2)
	// matchIndex should be nextIndex + logsLen - 1 = 1 + 2 - 1 = 2
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

	// set nextIndex to 2 so decrement gives 1
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
	// no logs in store — all peers get empty entries

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

// ── Some peers get heartbeat, some get logs ───────────────────────────────────
// node-2 is up to date → heartbeat only
// node-3, node-4, node-5 are behind → get actual logs

func TestSendLogs_MixedHeartbeatAndLogs(t *testing.T) {
	peer, store, clients := setupSendLogsTest(t)

	appendLogsHelper(store, []*types.LogEntry{
		{Index: 1, Term: 5, Data: []byte("cmd-1")},
		{Index: 2, Term: 5, Data: []byte("cmd-2")},
		{Index: 3, Term: 5, Data: []byte("cmd-3")},
	})

	// node-2 already has all logs — nextIndex points beyond last log
	peer.peerIndexes["node-2"] = PeerIndexes{nextIndex: 4, matchIndex: 3}
	// rest are behind
	peer.peerIndexes["node-3"] = PeerIndexes{nextIndex: 1, matchIndex: 0}
	peer.peerIndexes["node-4"] = PeerIndexes{nextIndex: 2, matchIndex: 1}
	peer.peerIndexes["node-5"] = PeerIndexes{nextIndex: 3, matchIndex: 2}

	// node-2 gets heartbeat — no entries
	clients["node-2"].On(methodAppendEntries, mock.Anything, mock.MatchedBy(func(req *types.AppendEntriesArgs) bool {
		return len(req.Entries) == 0
	})).Return(successResponse(), nil)

	// node-3 gets all 3 logs
	clients["node-3"].On(methodAppendEntries, mock.Anything, mock.MatchedBy(func(req *types.AppendEntriesArgs) bool {
		return len(req.Entries) == 3 && req.Entries[0].Index == 1
	})).Return(successResponse(), nil)

	// node-4 gets logs from index 2
	clients["node-4"].On(methodAppendEntries, mock.Anything, mock.MatchedBy(func(req *types.AppendEntriesArgs) bool {
		return len(req.Entries) == 2 && req.Entries[0].Index == 2
	})).Return(successResponse(), nil)

	// node-5 gets only the last log
	clients["node-5"].On(methodAppendEntries, mock.Anything, mock.MatchedBy(func(req *types.AppendEntriesArgs) bool {
		return len(req.Entries) == 1 && req.Entries[0].Index == 3
	})).Return(successResponse(), nil)

	err := runSendLogs(peer)

	assert.NoError(t, err)
	for _, c := range clients {
		c.AssertExpectations(t)
	}
}

// ── 9. commitIndex advances when majority replicated ─────────────────────────

func TestSendLogs_MajorityReplicated_CommitIndexAdvances(t *testing.T) {
	peer, store, clients := setupSendLogsTest(t)

	appendLogsHelper(store, []*types.LogEntry{
		{Index: 1, Term: 5, Data: []byte("cmd-1")},
		{Index: 2, Term: 5, Data: []byte("cmd-2")},
	})

	// self + node-2 + node-3 = 3 = majority of 5
	clients["node-2"].On(methodAppendEntries, mock.Anything, mock.Anything).Return(successResponse(), nil)
	clients["node-3"].On(methodAppendEntries, mock.Anything, mock.Anything).Return(successResponse(), nil)
	clients["node-4"].On(methodAppendEntries, mock.Anything, mock.Anything).Return(failResponse(), nil)
	clients["node-5"].On(methodAppendEntries, mock.Anything, mock.Anything).Return(failResponse(), nil)

	err := runSendLogs(peer)
	assert.NoError(t, err)
	assert.Greater(t, peer.GetCommitIndex(), uint(0),
		"commitIndex should have advanced after majority replication")
}

// ── 10. commitIndex does NOT advance when not majority ────────────────────────

func TestSendLogs_NoMajority_CommitIndexUnchanged(t *testing.T) {
	peer, store, clients := setupSendLogsTest(t)

	appendLogsHelper(store, []*types.LogEntry{
		{Index: 1, Term: 5, Data: []byte("cmd-1")},
	})

	// all peers fail — no majority
	for _, c := range clients {
		c.On(methodAppendEntries, mock.Anything, mock.Anything).Return(failResponse(), nil)
	}

	err := runSendLogs(peer)

	assert.NoError(t, err)
	assert.Equal(t, uint(0), peer.GetCommitIndex(),
		"commitIndex must not advance without majority")
}

// ── 11. commitIndex does NOT advance for previous term logs (5.4.2) ──────────

func TestSendLogs_PreviousTermLog_CommitIndexUnchanged(t *testing.T) {
	peer, store, clients := setupSendLogsTest(t)

	// log is from term 4, current term is 5
	// Raft 5.4.2: leader must not commit logs from previous terms by counting replicas
	appendLogsHelper(store, []*types.LogEntry{
		{Index: 1, Term: 4, Data: []byte("cmd-1")}, // old term
	})

	for _, c := range clients {
		c.On(methodAppendEntries, mock.Anything, mock.Anything).Return(successResponse(), nil)
	}

	err := runSendLogs(peer)

	assert.NoError(t, err)
	assert.Equal(t, uint(0), peer.GetCommitIndex(),
		"commitIndex must not advance for logs from previous terms")
}

// ── 12. GetCurrentTerm fails ──────────────────────────────────────────────────

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

// ── 13. GetLogByIndex fails (fetching prevLog) ────────────────────────────────

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
			"node-2": {nextIndex: 2, matchIndex: 1}, // nextIndex > 1 triggers GetLogByIndex
		},
		ServerIDRpcUrlMap: map[string]types.RaftRpcClient{
			"node-2": NewMockRaftRpcClient(),
		},
	}

	err := runSendLogs(peer)
	assert.Error(t, err)
}

// ── 14. GetLogs fails ─────────────────────────────────────────────────────────

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
