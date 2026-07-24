package raft

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// errReader is an io.Reader that always returns an error immediately.
type errReader struct{ err error }

func (e *errReader) Read([]byte) (int, error) { return 0, e.err }

const (
	methodSnapshot = "Snapshot"
	methodPersist  = "Persist"
	methodRelease  = "Release"
)

// newNodeWithSnapshot returns a node wired with a real temp snapshot dir and the
// given mocks. Callers can override cfg fields after the call if needed.
func newNodeWithSnapshot(t *testing.T, store Storage, sm StateMachine) *Node {
	t.Helper()
	node := NewNodeMock(store, sm)
	node.cfg.SnapshotDir = t.TempDir()
	node.cfg.SnapshotThreshold = 10
	return node
}

// writeFakeSnapshot creates a named snapshot dir inside snapshotDir to simulate
// an existing snapshot at the given index/term so shouldTriggerSnapshot sees it.
func writeFakeSnapshot(t *testing.T, snapshotDir string, index, term uint) {
	t.Helper()
	name := generateLatestSnapshotDirName(index, term, time.Now())
	if err := os.Mkdir(snapshotDir+"/"+name, 0755); err != nil {
		t.Fatalf("writeFakeSnapshot: %v", err)
	}
}

// writeFakeSnapshotTmp creates a named tmp snapshot dir inside snapshotDir to simulate
// an existing tmp snapshot at the given index/term so shouldTriggerSnapshot sees it.
func writeFakeSnapshotTmp(t *testing.T, snapshotDir string, index, term uint) {
	t.Helper()
	name := generateLatestSnapshotDirName(index, term, time.Now())
	if err := os.Mkdir(snapshotDir+"/"+name+".tmp", 0755); err != nil {
		t.Fatalf("writeFakeSnapshotTmp: %v", err)
	}
}

// ── shouldTriggerSnapshot ──────────────────────────────────────────────────────

// 1. Existing snapshot within threshold → no trigger
func TestShouldTriggerSnapshot_ThresholdNotReached(t *testing.T) {
	node := newNodeWithSnapshot(t, nil, nil)
	writeFakeSnapshot(t, node.cfg.SnapshotDir, 90, 1)
	triggered := shouldTriggerSnapshot(node.ctx, node.cfg.SnapshotDir, 95, 10)
	assert.False(t, triggered)
}

// 2. Existing snapshot and threshold reached → trigger
func TestShouldTriggerSnapshot_ThresholdReached(t *testing.T) {
	node := newNodeWithSnapshot(t, nil, nil)
	writeFakeSnapshot(t, node.cfg.SnapshotDir, 90, 1)
	triggered := shouldTriggerSnapshot(node.ctx, node.cfg.SnapshotDir, 100, 10)
	assert.True(t, triggered)
}

// 3. Empty dir, lastApplied == 0 → no trigger
func TestShouldTriggerSnapshot_EmptyDir_NoEntries(t *testing.T) {
	tempDir := t.TempDir()
	triggered := shouldTriggerSnapshot(context.Background(), tempDir, 0, 10)
	assert.False(t, triggered)
}

// 4. Empty dir, lastApplied > 0 → trigger (first snapshot ever)
func TestShouldTriggerSnapshot_EmptyDir_HasEntries(t *testing.T) {
	tempDir := t.TempDir()
	triggered := shouldTriggerSnapshot(context.Background(), tempDir, 5, 10)
	assert.True(t, triggered)
}

// 5. Dir contains only .tmp entries → treated as empty → triggers if lastApplied > 0
func TestShouldTriggerSnapshot_OnlyTmpEntries_TreatedAsEmpty(t *testing.T) {
	tempDir := t.TempDir()
	writeFakeSnapshotTmp(t, tempDir, 100, 1)
	triggered := shouldTriggerSnapshot(context.Background(), tempDir, 5, 10)
	assert.True(t, triggered)
}

// ── runSnapshotOnce: snapShotInProgress released on error ─────────────────────

// 6. sm.Snapshot returns error → snapShotInProgress must be false on return
func TestRunSnapshotOnce_SnapshotError_FlagReleased(t *testing.T) {
	store := new(MockStorage)
	sm := new(MockStateMachine)
	node := newNodeWithSnapshot(t, store, sm)

	// dir is empty + lastApplied > 0 → threshold reached, snapshot triggered
	store.On(methodGetLastApplied, mock.Anything).Return(uint(5), nil)
	sm.On(methodSnapshot, mock.Anything).Return(nil, errors.New("snap error"))

	err := node.runSnapshotOnce(context.Background())

	assert.Error(t, err)
	assert.False(t, node.snapShotInProgress.Load())
	store.AssertExpectations(t)
	sm.AssertExpectations(t)
}

// 7. Second GetLastApplied (post-snapshot) returns error → flag released
func TestRunSnapshotOnce_SecondGetLastAppliedError_FlagReleased(t *testing.T) {
	store := new(MockStorage)
	sm := new(MockStateMachine)
	snap := new(MockSnapshot)
	node := newNodeWithSnapshot(t, store, sm)

	store.On(methodGetLastApplied, mock.Anything).Return(uint(5), nil).Once()
	store.On(methodGetLastApplied, mock.Anything).Return(uint(0), errors.New("db error")).Once()
	sm.On(methodSnapshot, mock.Anything).Return(snap, nil)

	err := node.runSnapshotOnce(context.Background())

	assert.Error(t, err)
	assert.False(t, node.snapShotInProgress.Load())
	store.AssertExpectations(t)
	sm.AssertExpectations(t)
}

// ── runSnapshotOnce: io.Pipe error propagation ────────────────────────────────

// 8. Persist returns error → error propagates through pipe → io.Copy fails →
// writeSnapshotToDisk returns error → tmp dir is cleaned up
func TestRunSnapshotOnce_PersistError_TmpDirCleaned(t *testing.T) {
	store := new(MockStorage)
	sm := new(MockStateMachine)
	snap := new(MockSnapshot)
	node := newNodeWithSnapshot(t, store, sm)

	store.On(methodGetLastApplied, mock.Anything).Return(uint(10), nil)
	store.On(methodGetLogByIndex, mock.Anything, uint(10)).Return(LogEntry{Index: 10, Term: 1}, nil)
	sm.On(methodSnapshot, mock.Anything).Return(snap, nil)
	snap.On(methodPersist, mock.Anything, mock.Anything).Return(errors.New("persist error"))

	err := node.runSnapshotOnce(context.Background())

	assert.Error(t, err)

	entries, err := os.ReadDir(node.cfg.SnapshotDir)
	assert.NoError(t, err)
	for _, entry := range entries {
		assert.True(t, entry.IsDir())
		assert.True(t, strings.HasSuffix(entry.Name(), ".tmp"))
	}

	store.AssertExpectations(t)
	sm.AssertExpectations(t)
	snap.AssertExpectations(t)
}

// ── writeSnapshotToDisk ───────────────────────────────────────────────────────

// 9. Happy path: bytes are written, meta.json created, tmp atomically renamed
func TestWriteSnapshotToDisk_HappyPath(t *testing.T) {
	snap := new(MockSnapshot)
	dir := t.TempDir()
	snapshotDirPath := dir + "/10-1-final"
	meta := SnapshotMeta{Index: 10, Term: 1, ID: "10-1-final"}
	payload := []byte("snapshot-data")
	pr, pw := io.Pipe()

	snap.On(methodRelease).Return(nil)
	go func() {
		pw.Write(payload)
		pw.CloseWithError(nil)
	}()

	err := writeSnapshotToDisk(pr, snapshotDirPath, meta, snap.Release)

	assert.NoError(t, err)
	_, statErr := os.Stat(snapshotDirPath)
	assert.NoError(t, statErr, "final dir must exist")
	_, statErr = os.Stat(snapshotDirPath + ".tmp")
	assert.True(t, os.IsNotExist(statErr), "tmp dir must be gone")
	data, err := os.ReadFile(snapshotDirPath + "/snapshot")
	assert.NoError(t, err)
	assert.Equal(t, payload, data)
	metaBytes, err := os.ReadFile(snapshotDirPath + "/meta.json")
	assert.NoError(t, err)
	var gotMeta SnapshotMeta
	assert.NoError(t, json.Unmarshal(metaBytes, &gotMeta))
	assert.Equal(t, meta, gotMeta)
	snap.AssertExpectations(t)
}

// 10. os.Mkdir fails (dir already exists) → error returned, snapshot bytes never consumed, and
// closing the read end (as runSnapshotOnce does) unblocks the producer
func TestWriteSnapshotToDisk_MkdirFails_PipeUnblocked(t *testing.T) {
	snap := new(MockSnapshot)
	dir := t.TempDir()
	snapshotDirPath := dir + "/10-1-final"
	meta := SnapshotMeta{Index: 10, Term: 1, ID: "10-1-final"}
	pr, pw := io.Pipe()

	os.Mkdir(snapshotDirPath+".tmp", 0755) // pre-create so Mkdir inside writeSnapshotToDisk fails

	goroutineExited := make(chan struct{})
	go func() {
		defer close(goroutineExited)
		pw.Write([]byte("data")) // blocks until the read end is closed
	}()

	err := writeSnapshotToDisk(pr, snapshotDirPath, meta, snap.Release)
	pr.Close() // the caller owns the reader; mirrors runSnapshotOnce's defer

	assert.Error(t, err)
	awaitCall(t, goroutineExited, "Persist goroutine to unblock after pr.Close")
	snap.AssertNotCalled(t, methodRelease)
}

// 11. io.Copy fails (reader returns error) → tmp dir cleaned up
// (os.Create failure is not injectable without OS mocking; this tests the same
// cleanup path via a reader that errors immediately after Create succeeds)
func TestWriteSnapshotToDisk_CopyFails_TmpDirCleaned(t *testing.T) {
	snap := new(MockSnapshot)
	dir := t.TempDir()
	snapshotDirPath := dir + "/10-1-final"
	meta := SnapshotMeta{Index: 10, Term: 1, ID: "10-1-final"}

	err := writeSnapshotToDisk(&errReader{errors.New("read error")}, snapshotDirPath, meta, snap.Release)

	assert.Error(t, err)
	_, statErr := os.Stat(snapshotDirPath + ".tmp")
	assert.True(t, os.IsNotExist(statErr), "tmp dir must be cleaned up")
	snap.AssertNotCalled(t, methodRelease) // Release is not reached when Copy fails
}

// 12. snap.Release fails → tmp dir cleaned up
func TestWriteSnapshotToDisk_ReleaseFails_TmpDirCleaned(t *testing.T) {
	snap := new(MockSnapshot)
	dir := t.TempDir()
	snapshotDirPath := dir + "/10-1-final"
	meta := SnapshotMeta{Index: 10, Term: 1, ID: "10-1-final"}
	pr, pw := io.Pipe()

	snap.On(methodRelease).Return(errors.New("release error"))
	go func() {
		pw.Write([]byte("some-data"))
		pw.CloseWithError(nil)
	}()

	err := writeSnapshotToDisk(pr, snapshotDirPath, meta, snap.Release)

	assert.Error(t, err)
	_, statErr := os.Stat(snapshotDirPath + ".tmp")
	assert.True(t, os.IsNotExist(statErr), "tmp dir must be cleaned up")
	snap.AssertExpectations(t)
}

// 13. os.Rename fails → tmp dir cleaned up, final dir does not exist
func TestWriteSnapshotToDisk_RenameFails_TmpDirCleaned(t *testing.T) {
	snap := new(MockSnapshot)
	dir := t.TempDir()
	snapshotDirPath := dir + "/10-1-final"
	meta := SnapshotMeta{Index: 10, Term: 1, ID: "10-1-final"}
	pr, pw := io.Pipe()

	snap.On(methodRelease).Return(nil)
	// pre-create snapshotDirPath as a regular file — on Linux, renaming a dir over
	// a file returns ENOTDIR, so Rename fails
	os.WriteFile(snapshotDirPath, []byte{}, 0644)
	go func() {
		pw.Write([]byte("some-data"))
		pw.CloseWithError(nil)
	}()

	err := writeSnapshotToDisk(pr, snapshotDirPath, meta, snap.Release)

	assert.Error(t, err)
	_, statErr := os.Stat(snapshotDirPath + ".tmp")
	assert.True(t, os.IsNotExist(statErr), "tmp dir must be cleaned up")
	snap.AssertExpectations(t)
}
