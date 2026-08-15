package raft

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const methodInstallSnapshot = "InstallSnapshot"

// writeSnapshotFixture drops a real snapshot directory (meta.json + a non-empty
// snapshot file) into dir so callInstallSnapshot's on-disk reads succeed. The
// meta's Index/Term become the anchor catchUpMember starts the log catch-up from.
func writeSnapshotFixture(t *testing.T, dir string, index, term uint) {
	t.Helper()
	sdir := filepath.Join(dir, generateLatestSnapshotDirName(index, term, time.Now()))
	if err := os.MkdirAll(sdir, 0o755); err != nil {
		t.Fatalf("mkdir snapshot fixture: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sdir, snapshotFileName), []byte("snapshot-bytes"), 0o644); err != nil {
		t.Fatalf("write snapshot file: %v", err)
	}
	f, err := os.Create(filepath.Join(sdir, metaFileName))
	if err != nil {
		t.Fatalf("create meta file: %v", err)
	}
	defer f.Close()
	if err := json.NewEncoder(f).Encode(SnapshotMeta{Index: index, Term: term, ID: "node-1", Timestamp: time.Now()}); err != nil {
		t.Fatalf("encode meta: %v", err)
	}
}

// newCatchUpTestNode wires a leader node whose catchUpMember can run: a temp
// SnapshotDir for callInstallSnapshot to read, a MockTransport, and non-zero
// InstallSnapshot deadline-scale config (else callInstallSnapshot divides by zero).
func newCatchUpTestNode(t *testing.T, store Storage) (*Node, *MockTransport) {
	t.Helper()
	transport := NewMockTransport()
	node := NewNodeMock(store, nil) // seeds node-2..5 into configurations.latest
	node.transport = transport
	node.Role = ServerRole_Leader
	node.setLeaderCloseCh() // AddMember proposes; newFuture captures this channel
	// Nothing replicates here, so no commit-index updater runs and no future would
	// ever be completed; AddMember would park in Future.Wait. The tests that propose
	// pair this with a large SetCommitIndex.
	startAutoCommitter(t, node)
	node.cfg.SnapshotDir = t.TempDir()
	node.cfg.InstallSnapshotDeadlineScaleSizeByte = 1 << 20
	node.cfg.InstallSnapshotDeadlineScaleTimeMs = 1
	return node, transport
}

// ── InstallSnapshot RPC fails → error ─────────────────────────────────────────

func TestCatchUpMember_InstallSnapshotError(t *testing.T) {
	store := new(MockStorage)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(2), nil)
	node, transport := newCatchUpTestNode(t, store)
	writeSnapshotFixture(t, node.cfg.SnapshotDir, 5, 2)

	transport.On(methodInstallSnapshot, "node-2", mock.Anything).
		Return(InstallSnapshotResponse{}, errors.New("rpc error"))

	err := node.catchUpMember(context.Background(), "node-2")
	assert.Error(t, err)
	transport.AssertExpectations(t)
}

// ── InstallSnapshot replies !Success → error ─────────────────────────────────

func TestCatchUpMember_InstallSnapshotUnsuccessful(t *testing.T) {
	store := new(MockStorage)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(2), nil)
	node, transport := newCatchUpTestNode(t, store)
	writeSnapshotFixture(t, node.cfg.SnapshotDir, 5, 2)

	transport.On(methodInstallSnapshot, "node-2", mock.Anything).
		Return(InstallSnapshotResponse{Success: false, Term: 2}, nil)

	err := node.catchUpMember(context.Background(), "node-2")
	assert.Error(t, err)
	transport.AssertExpectations(t)
}

// ── Happy path: caught up in one fast round ──────────────────────────────────

func TestCatchUpMember_CaughtUpOneRound(t *testing.T) {
	store := new(MockStorage)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(2), nil)
	// after the snapshot (index 5), the next entry (6) exists...
	store.On(methodGetLogByIndex, mock.Anything, uint(6)).Return(LogEntry{Index: 6, Term: 2}, nil)
	// ...the round sends [6,7]...
	store.On(methodGetLogs, mock.Anything, mock.Anything, mock.Anything).
		Return([]LogEntry{{Index: 6, Term: 2}, {Index: 7, Term: 2}}, nil)
	// ...and after success the next needed entry (8) still exists, so no resend.
	store.On(methodGetLogByIndex, mock.Anything, uint(8)).Return(LogEntry{Index: 8, Term: 2}, nil)

	node, transport := newCatchUpTestNode(t, store)
	writeSnapshotFixture(t, node.cfg.SnapshotDir, 5, 2)
	transport.On(methodInstallSnapshot, "node-2", mock.Anything).
		Return(InstallSnapshotResponse{Success: true, Term: 2}, nil)
	transport.On(methodAppendEntries, "node-2", mock.Anything).
		Return(AppendEntriesResponse{Success: true, Term: 2}, nil)

	err := node.catchUpMember(context.Background(), "node-2")
	assert.NoError(t, err)
	// matchIndex/nextIndex advanced to the last-sent entry.
	assert.Equal(t, uint(7), node.GetPeerIndex("node-2").MatchIndex)
	assert.Equal(t, uint(8), node.GetPeerIndex("node-2").NextIndex)
	// retain floor released on exit.
	assert.Equal(t, DefaultCatchingUpIdx, node.catchingUpIdx.Load())
}

// ── First AppendEntries anchors on the snapshot boundary ─────────────────────

func TestCatchUpMember_FirstRoundAnchor(t *testing.T) {
	store := new(MockStorage)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(2), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(6)).Return(LogEntry{Index: 6, Term: 2}, nil)
	store.On(methodGetLogs, mock.Anything, mock.Anything, mock.Anything).
		Return([]LogEntry{{Index: 6, Term: 2}}, nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(7)).Return(LogEntry{Index: 7, Term: 2}, nil)

	node, transport := newCatchUpTestNode(t, store)
	writeSnapshotFixture(t, node.cfg.SnapshotDir, 5, 3) // snapshot at index 5, term 3
	transport.On(methodInstallSnapshot, "node-2", mock.Anything).
		Return(InstallSnapshotResponse{Success: true, Term: 2}, nil)
	// The first AppendEntries must carry prevLogIndex=5, prevLogTerm=3 (the anchor)
	// and start at index 6.
	transport.On(methodAppendEntries, "node-2", mock.MatchedBy(func(a AppendEntriesArgs) bool {
		return a.PrevLogIndex == 5 && a.PrevLogTerm == 3 &&
			len(a.Entries) == 1 && a.Entries[0].Index == 6
	})).Return(AppendEntriesResponse{Success: true, Term: 2}, nil)

	err := node.catchUpMember(context.Background(), "node-2")
	assert.NoError(t, err)
	transport.AssertExpectations(t)
}

// ── Peer reports a higher term → step down ───────────────────────────────────

func TestCatchUpMember_HigherTermStepsDown(t *testing.T) {
	store := new(MockStorage)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(2), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(6)).Return(LogEntry{Index: 6, Term: 2}, nil)
	store.On(methodGetLogs, mock.Anything, mock.Anything, mock.Anything).
		Return([]LogEntry{{Index: 6, Term: 2}}, nil)

	node, transport := newCatchUpTestNode(t, store)
	writeSnapshotFixture(t, node.cfg.SnapshotDir, 5, 2)
	transport.On(methodInstallSnapshot, "node-2", mock.Anything).
		Return(InstallSnapshotResponse{Success: true, Term: 2}, nil)
	transport.On(methodAppendEntries, "node-2", mock.Anything).
		Return(AppendEntriesResponse{Success: false, Term: 99}, nil)

	err := node.catchUpMember(context.Background(), "node-2")
	assert.ErrorContains(t, err, "stepped down")
}

// ── Nothing left after the snapshot → already caught up ──────────────────────

func TestCatchUpMember_ZeroLogsCaughtUp(t *testing.T) {
	store := new(MockStorage)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(2), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(6)).Return(LogEntry{Index: 6, Term: 2}, nil)
	store.On(methodGetLogs, mock.Anything, mock.Anything, mock.Anything).
		Return([]LogEntry{}, nil) // nothing after the snapshot

	node, transport := newCatchUpTestNode(t, store)
	writeSnapshotFixture(t, node.cfg.SnapshotDir, 5, 2)
	transport.On(methodInstallSnapshot, "node-2", mock.Anything).
		Return(InstallSnapshotResponse{Success: true, Term: 2}, nil)

	err := node.catchUpMember(context.Background(), "node-2")
	assert.NoError(t, err)
	// AppendEntries never fired — there was nothing to send.
	transport.AssertNotCalled(t, methodAppendEntries, "node-2", mock.Anything)
}

// ── !Success → back off one entry, then the next round succeeds ───────────────

func TestCatchUpMember_BackoffThenSuccess(t *testing.T) {
	store := new(MockStorage)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(2), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(6)).Return(LogEntry{Index: 6, Term: 2}, nil) // initial check
	store.On(methodGetLogByIndex, mock.Anything, uint(4)).Return(LogEntry{Index: 4, Term: 2}, nil) // backoff anchor
	store.On(methodGetLogByIndex, mock.Anything, uint(8)).Return(LogEntry{Index: 8, Term: 2}, nil) // after 2nd round
	store.On(methodGetLogs, mock.Anything, mock.Anything, mock.Anything).
		Return([]LogEntry{{Index: 6, Term: 2}, {Index: 7, Term: 2}}, nil)

	node, transport := newCatchUpTestNode(t, store)
	writeSnapshotFixture(t, node.cfg.SnapshotDir, 5, 2)
	transport.On(methodInstallSnapshot, "node-2", mock.Anything).
		Return(InstallSnapshotResponse{Success: true, Term: 2}, nil)
	// First round rejected (log inconsistency) → back off; second round succeeds.
	transport.On(methodAppendEntries, "node-2", mock.Anything).
		Return(AppendEntriesResponse{Success: false, Term: 2}, nil).Once()
	transport.On(methodAppendEntries, "node-2", mock.Anything).
		Return(AppendEntriesResponse{Success: true, Term: 2}, nil).Once()

	err := node.catchUpMember(context.Background(), "node-2")
	assert.NoError(t, err)
	transport.AssertNumberOfCalls(t, methodAppendEntries, 2)
}

// ── Still slow after the last round → abort ──────────────────────────────────

func TestCatchUpMember_TooSlowAborts(t *testing.T) {
	store := new(MockStorage)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(2), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(6)).Return(LogEntry{Index: 6, Term: 2}, nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(8)).Return(LogEntry{Index: 8, Term: 2}, nil)
	store.On(methodGetLogs, mock.Anything, mock.Anything, mock.Anything).
		Return([]LogEntry{{Index: 6, Term: 2}, {Index: 7, Term: 2}}, nil)

	node, transport := newCatchUpTestNode(t, store)
	node.cfg.ElectionMinMs = 0 // every round counts as "slower than an election timeout"
	writeSnapshotFixture(t, node.cfg.SnapshotDir, 5, 2)
	transport.On(methodInstallSnapshot, "node-2", mock.Anything).
		Return(InstallSnapshotResponse{Success: true, Term: 2}, nil)
	transport.On(methodAppendEntries, "node-2", mock.Anything).
		Return(AppendEntriesResponse{Success: true, Term: 2}, nil)

	err := node.catchUpMember(context.Background(), "node-2")
	assert.ErrorContains(t, err, "could not catch up within")
}

// ── Rejected all the way to the start of the log → abort ─────────────────────

func TestCatchUpMember_RejectedAtStartOfLog(t *testing.T) {
	store := new(MockStorage)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(1), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(2)).Return(LogEntry{Index: 2, Term: 1}, nil)
	store.On(methodGetLogs, mock.Anything, mock.Anything, mock.Anything).
		Return([]LogEntry{{Index: 2, Term: 1}}, nil)

	node, transport := newCatchUpTestNode(t, store)
	writeSnapshotFixture(t, node.cfg.SnapshotDir, 1, 1) // snapshot at index 1 → prevLogIdx starts at 1
	transport.On(methodInstallSnapshot, "node-2", mock.Anything).
		Return(InstallSnapshotResponse{Success: true, Term: 1}, nil)
	// Always rejected: round 1 backs off 1→0 (logTermAt(0) is the empty-log anchor),
	// round 2 has prevLogIdx==0 and is rejected → give up.
	transport.On(methodAppendEntries, "node-2", mock.Anything).
		Return(AppendEntriesResponse{Success: false, Term: 1}, nil)

	err := node.catchUpMember(context.Background(), "node-2")
	assert.ErrorContains(t, err, "rejected at start of log")
}

// ── DB error reading the logs to send ────────────────────────────────────────

func TestCatchUpMember_GetLogsDBError(t *testing.T) {
	store := new(MockStorage)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(2), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(6)).Return(LogEntry{Index: 6, Term: 2}, nil)
	store.On(methodGetLogs, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("db error"))

	node, transport := newCatchUpTestNode(t, store)
	writeSnapshotFixture(t, node.cfg.SnapshotDir, 5, 2)
	transport.On(methodInstallSnapshot, "node-2", mock.Anything).
		Return(InstallSnapshotResponse{Success: true, Term: 2}, nil)

	err := node.catchUpMember(context.Background(), "node-2")
	assert.Error(t, err)
}

// ── AppendEntries transport error ────────────────────────────────────────────

func TestCatchUpMember_AppendEntriesTransportError(t *testing.T) {
	store := new(MockStorage)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(2), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(6)).Return(LogEntry{Index: 6, Term: 2}, nil)
	store.On(methodGetLogs, mock.Anything, mock.Anything, mock.Anything).
		Return([]LogEntry{{Index: 6, Term: 2}}, nil)

	node, transport := newCatchUpTestNode(t, store)
	writeSnapshotFixture(t, node.cfg.SnapshotDir, 5, 2)
	transport.On(methodInstallSnapshot, "node-2", mock.Anything).
		Return(InstallSnapshotResponse{Success: true, Term: 2}, nil)
	transport.On(methodAppendEntries, "node-2", mock.Anything).
		Return(AppendEntriesResponse{}, errors.New("rpc error"))

	err := node.catchUpMember(context.Background(), "node-2")
	assert.Error(t, err)
}

// ── The entry right after the snapshot was compacted → resend, then give up ──

func TestCatchUpMember_CompactedResendAborts(t *testing.T) {
	store := new(MockStorage)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(2), nil)
	// The first entry after the snapshot is never there → resend snapshot each time.
	store.On(methodGetLogByIndex, mock.Anything, uint(6)).Return(LogEntry{}, ErrNotFound)

	node, transport := newCatchUpTestNode(t, store)
	writeSnapshotFixture(t, node.cfg.SnapshotDir, 5, 2)
	transport.On(methodInstallSnapshot, "node-2", mock.Anything).
		Return(InstallSnapshotResponse{Success: true, Term: 2}, nil)

	err := node.catchUpMember(context.Background(), "node-2")
	assert.ErrorContains(t, err, "install snapshot is slow")
}

// ── Non-NotFound DB error on the post-snapshot log check → return it ──────────

func TestCatchUpMember_GetLogByIndexDBError(t *testing.T) {
	store := new(MockStorage)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(2), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(6)).Return(LogEntry{}, errors.New("db error"))

	node, transport := newCatchUpTestNode(t, store)
	writeSnapshotFixture(t, node.cfg.SnapshotDir, 5, 2)
	transport.On(methodInstallSnapshot, "node-2", mock.Anything).
		Return(InstallSnapshotResponse{Success: true, Term: 2}, nil)

	err := node.catchUpMember(context.Background(), "node-2")
	assert.Error(t, err)
	// A real DB error is not a "resend" — it should not retry the snapshot 6 times.
	transport.AssertNumberOfCalls(t, methodInstallSnapshot, 1)
}

// ════════════════════════════════════════════════════════════════════════════
// AddMember-level tests
// ════════════════════════════════════════════════════════════════════════════

// ── Not the leader → reject ──────────────────────────────────────────────────

func TestAddMember_NotLeader(t *testing.T) {
	store := new(MockStorage)
	node, _ := newCatchUpTestNode(t, store)
	node.SetRole(ServerRole_Follower)

	err := node.AddMember(context.Background(), "node-99", PeerState_Voter)
	assert.ErrorContains(t, err, "not the leader")
}

// ── Another addition already in progress → reject ────────────────────────────

func TestAddMember_StagingInProgress(t *testing.T) {
	store := new(MockStorage)
	node, _ := newCatchUpTestNode(t, store)
	node.addPeer("node-88", Peer{PeerState: PeerState_Staging})

	err := node.AddMember(context.Background(), "node-99", PeerState_Voter)
	assert.ErrorContains(t, err, "already in progress")
}

// ── Catch-up fails → the staging peer is rolled back out of the config ────────

func TestAddMember_RollbackRemovesStagingPeer(t *testing.T) {
	store := new(MockStorage)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(2), nil)
	store.On(methodGetLastLogIndex, mock.Anything).Return(uint(10), nil) // for appendEntry
	store.On(methodAppendLogs, mock.Anything, mock.Anything).Return(nil)

	node, transport := newCatchUpTestNode(t, store)
	node.SetCommitIndex(1000) // so Propose's wait-for-commit returns immediately
	writeSnapshotFixture(t, node.cfg.SnapshotDir, 5, 2)
	// Catch-up fails at InstallSnapshot.
	transport.On(methodInstallSnapshot, "node-99", mock.Anything).
		Return(InstallSnapshotResponse{}, errors.New("rpc error"))

	err := node.AddMember(context.Background(), "node-99", PeerState_Voter)
	assert.Error(t, err)
	_, present := node.configurations.latest["node-99"]
	assert.False(t, present, "staging peer should be removed on rollback")
}

// ── Happy path: caught up → promoted to the target state ─────────────────────

func TestAddMember_PromotesOnSuccess(t *testing.T) {
	store := new(MockStorage)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(2), nil)
	store.On(methodGetLastLogIndex, mock.Anything).Return(uint(10), nil) // for appendEntry
	store.On(methodAppendLogs, mock.Anything, mock.Anything).Return(nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(6)).Return(LogEntry{Index: 6, Term: 2}, nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(8)).Return(LogEntry{Index: 8, Term: 2}, nil)
	store.On(methodGetLogs, mock.Anything, mock.Anything, mock.Anything).
		Return([]LogEntry{{Index: 6, Term: 2}, {Index: 7, Term: 2}}, nil)

	node, transport := newCatchUpTestNode(t, store)
	node.SetCommitIndex(1000)
	writeSnapshotFixture(t, node.cfg.SnapshotDir, 5, 2)
	transport.On(methodInstallSnapshot, "node-99", mock.Anything).
		Return(InstallSnapshotResponse{Success: true, Term: 2}, nil)
	transport.On(methodAppendEntries, "node-99", mock.Anything).
		Return(AppendEntriesResponse{Success: true, Term: 2}, nil)

	err := node.AddMember(context.Background(), "node-99", PeerState_NonVoter)
	assert.NoError(t, err)
	assert.Equal(t, PeerState_NonVoter, node.configurations.latest["node-99"].PeerState)
}
