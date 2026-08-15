package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

const (
	snapshotFileName = "snapshot"
	metaFileName     = "meta.json"
)

type SnapshotMeta struct {
	Index        uint                 `json:"index"`
	Term         uint                 `json:"term"`
	PrevIndex    uint                 `json:"prev_index"`
	PrevTerm     uint                 `json:"prev_term"`
	ID           string               `json:"id"` // Server ID of the node that created/got the snapshot
	LeaderID     string               `json:"leader_id"`
	MemberConfig map[string]PeerState `json:"member_config"`
	Timestamp    time.Time            `json:"timestamp"`
}

// TODO: execute apply loop and snapshot in single goroutine and use channels for communication instead of locks and condition variables, this way we can avoid all the complexity around locks and condition variables and make the code simpler and easier to reason about, but for simplicity we are using locks and condition variables for now
func (n *Node) startSnapshotLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(n.cfg.SnapshotInterval) * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				if err := n.runSnapshotOnce(ctx); err != nil {
					zerolog.Ctx(ctx).Error().Err(err).Msg("snapshot loop: error running snapshot")
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (n *Node) runSnapshotOnce(ctx context.Context) error {
	latestAppliedIndex, err := n.store.GetLastApplied(ctx)
	if err != nil {
		return fmt.Errorf("getting last applied index: %w", err)
	}
	if !shouldTriggerSnapshot(ctx, n.cfg.SnapshotDir, latestAppliedIndex, n.cfg.SnapshotThreshold) {
		return nil
	}

	n.snapShotInProgress.Store(true)
	snap, err := n.sm.Snapshot(ctx)
	if err != nil {
		n.snapShotInProgress.Store(false)
		return fmt.Errorf("taking snapshot: %w", err)
	}
	latestAppliedIndex, err = n.store.GetLastApplied(ctx)
	if err != nil {
		n.snapShotInProgress.Store(false)
		return fmt.Errorf("getting latest applied index: %w", err)
	}
	n.snapShotInProgress.Store(false)

	// snapShotInProgress just went false, and that flag is half of the apply loop's
	// wait condition — without this the loop stays parked until the next commit.
	n.signalCommit()

	latestAppliedLog, err := n.store.GetLogByIndex(ctx, latestAppliedIndex)
	if err != nil {
		return fmt.Errorf("getting latest log entry: %w", err)
	}

	// prevLastAppliedLog anchors the snapshot to the entry just before its last
	// included index. If that entry does not exist — latestAppliedIndex is 1 (there
	// is no entry at index 0) or index-1 was already compacted into an earlier
	// snapshot — fall back to the empty-log anchor {Index:0, Term:0} rather than
	// failing the whole snapshot.
	var prevLastAppliedLog LogEntry
	if latestAppliedIndex > 1 {
		prevLastAppliedLog, err = n.store.GetLogByIndex(ctx, latestAppliedIndex-1)
		if err != nil {
			if !errors.Is(err, ErrNotFound) {
				return fmt.Errorf("getting prev latest log entry: %w", err)
			}
			prevLastAppliedLog = LogEntry{}
		}
	}

	// Persist streams the snapshot into the pipe on its own goroutine; the main
	// goroutine reads the other end through writeSnapshotToDisk. `done` is the join
	// point: the deferred receive guarantees runSnapshotOnce cannot return until
	// this goroutine has fully finished CloseWithError. Without the join, the
	// pipe's close wakes the reader partway through CloseWithError and the main
	// goroutine races the goroutine's trailing writes to pw (see JOURNEY.md).
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() {
		err := snap.Persist(ctx, pw)
		pw.CloseWithError(err) // the reader needs the close to see EOF/err
		done <- err
	}()
	defer func() {
		pr.Close() // unblock a Persist stuck on pw.Write if we bailed mid-stream
		<-done     // wait for the goroutine to fully return before we do
	}()

	timestamp := time.Now()
	snapshotDirName := generateLatestSnapshotDirName(latestAppliedIndex, uint(latestAppliedLog.Term), timestamp)
	snapshotDirPath := n.cfg.SnapshotDir + "/" + snapshotDirName
	memberConfig := make(map[string]PeerState)
	for id, peer := range n.peersSnapshot() {
		memberConfig[id] = peer.PeerState
	}
	meta := SnapshotMeta{
		Index:        latestAppliedIndex,
		Term:         uint(latestAppliedLog.Term),
		ID:           n.GetID(),
		LeaderID:     n.GetLeaderID(),
		MemberConfig: memberConfig,
		Timestamp:    timestamp,
		PrevIndex:    uint(prevLastAppliedLog.Index),
		PrevTerm:     uint(prevLastAppliedLog.Term),
	}
	err = writeSnapshotToDisk(pr, snapshotDirPath, meta, snap.Release)
	if err != nil {
		return fmt.Errorf("writing snapshot to disk: %w", err)
	}
	n.SetSnapshotLatest(latestAppliedIndex, uint(latestAppliedLog.Term))

	// Delay (do not skip) compaction while a catching-up member still needs logs at
	// or below latestAppliedIndex. Parks until the floor clears rather than busy-
	// waiting, and exits on shutdown.
	if err := n.waitForCatchUpFloor(ctx, latestAppliedIndex); err != nil {
		return err
	}
	return n.store.DeleteLogs(ctx, 0, latestAppliedIndex)
}

// waitForCatchUpFloor blocks until it is safe to compact up to upto — i.e. no
// catch-up retain floor sits at or below upto — or ctx is cancelled. It re-Loads
// the floor after every wake because the floor is level state, not a one-shot
// event: intermediate changes (the floor advancing but still <= upto) must keep it
// waiting. add_member pokes catchUpSignal on every floor change via setCatchingUpIdx.
func (n *Node) waitForCatchUpFloor(ctx context.Context, upto uint) error {
	for {
		floor := n.catchingUpIdx.Load()
		if floor == DefaultCatchingUpIdx || uint(floor) > upto {
			return nil
		}
		select {
		case <-n.catchUpSignal:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// setCatchingUpIdx updates the catch-up retain floor and wakes any snapshot
// goroutine parked in waitForCatchUpFloor. The send is non-blocking on a size-1
// buffered channel: if a wake is already queued, dropping the duplicate is fine
// because the waiter re-Loads the latest floor when it runs.
func (n *Node) setCatchingUpIdx(idx int64) {
	n.catchingUpIdx.Store(idx)
	select {
	case n.catchUpSignal <- struct{}{}:
	default:
	}
}

func generateLatestSnapshotDirName(latestIndex, latestTerm uint, timestamp time.Time) string {
	return strconv.Itoa(int(latestIndex)) + "-" + strconv.Itoa(int(latestTerm)) + "-" + strconv.FormatInt(timestamp.UnixNano(), 10)
}

func parseSnapshotDirName(dirName string) (latestIdx uint, latestTerm uint, timestamp time.Time, err error) {
	nameElems := strings.Split(dirName, "-")
	if len(nameElems) != 3 {
		err = fmt.Errorf("invalid snapshot dir name: %s", dirName)
		return
	}
	idx, err := strconv.Atoi(nameElems[0])
	if err != nil {
		err = fmt.Errorf("invalid snapshot dir name: %s, error: %v", dirName, err)
		return
	}
	term, err := strconv.Atoi(nameElems[1])
	if err != nil {
		err = fmt.Errorf("invalid snapshot dir name: %s, error: %v", dirName, err)
		return
	}
	tsInt, err := strconv.ParseInt(nameElems[2], 10, 64)
	if err != nil {
		err = fmt.Errorf("invalid snapshot dir name: %s, error: %v", dirName, err)
		return
	}
	timestamp = time.Unix(0, tsInt)
	latestIdx = uint(idx)
	latestTerm = uint(term)
	return
}

func shouldTriggerSnapshot(ctx context.Context, snapshotDir string, lastApplied, snapshotThreshold uint) bool {
	entries, err := os.ReadDir(snapshotDir)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("shouldTriggerSnapshot: error reading snapshot directory")
		return false
	}
	files := make([]os.DirEntry, 0, len(entries))
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".tmp") {
			files = append(files, e)
		}
	}
	if len(files) == 0 && lastApplied > 0 {
		return true
	}
	latestSnapshotIndex, err := getLatestSnapshotIndex(files)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("shouldTriggerSnapshot: error getting latest snapshot index")
		return false
	}
	if lastApplied-latestSnapshotIndex >= snapshotThreshold {
		return true
	}
	return false
}

func getLatestSnapshotIndex(dirs []os.DirEntry) (uint, error) {
	maxIdx := uint(0)
	for _, dir := range dirs {
		if !dir.IsDir() {
			continue
		}
		idx, _, _, err := parseSnapshotDirName(dir.Name())
		if err != nil {
			continue
		}
		if uint(idx) > maxIdx {
			maxIdx = uint(idx)
		}
	}
	return maxIdx, nil
}

// getLatestSnapshotDir returns the name of the snapshot directory with the
// highest last-included index.
//
// "There is no snapshot" is reported as ErrNoSnapshot rather than a generic
// error, because the two callers want opposite things from it: callInstallSnapshot
// cannot ship a snapshot that does not exist and should fail, while startup
// recovery must treat it as the ordinary state of a node that has never
// snapshotted. A directory that exists but cannot be read is a real error to both.
func getLatestSnapshotDir(snapshotDir string) (string, error) {
	if snapshotDir == "" {
		return "", ErrNoSnapshot
	}

	snapShotDirEntries, err := os.ReadDir(snapshotDir)
	if err != nil {
		if os.IsNotExist(err) {
			return "", ErrNoSnapshot
		}
		return "", fmt.Errorf("getLatestSnapshotDir: reading snapshot directory: %w", err)
	}
	var latestSnapshotDir string
	maxIdx := uint(0)
	for _, dir := range snapShotDirEntries {
		if !dir.IsDir() {
			continue
		}
		idx, _, _, err := parseSnapshotDirName(dir.Name())
		if err != nil {
			continue
		}
		if uint(idx) > maxIdx {
			maxIdx = uint(idx)
			latestSnapshotDir = dir.Name()
		}
	}
	if latestSnapshotDir == "" {
		return "", ErrNoSnapshot
	}
	return latestSnapshotDir, nil
}

// writeSnapshotToDisk streams r into a .tmp directory and atomically renames it into place on
// success. On any failure after the tmp dir is created, the partial tmp directory is removed.
//
// release, if non-nil, runs once the snapshot bytes are on disk but before the directory is
// committed. The caller owns r: on error it must close or drain r to unblock whatever is
// writing to it.
//
// Previously this took (ctx, snap Snapshot, pr io.Reader, pw *io.PipeWriter, dir, meta) and served
// only the leader's snapshot loop. It was generalized so the follower's HandleInstallSnapshot can
// reuse the same atomic-write path instead of duplicating the mkdir/fsync/rename/cleanup sequence:
//
//   - snap Snapshot was removed: it was used solely for snap.Release(). That is now the release
//     hook, which the follower passes as nil (it has no Snapshot to release).
//   - pw *io.PipeWriter was removed: it was used solely to pw.CloseWithError() and unblock the
//     Persist goroutine when we bailed before consuming the reader. That is the caller's concern,
//     and the leader does it more directly by closing the read end (a blocked pw.Write then
//     returns io.ErrClosedPipe). The follower has no pipe at all — its reader is the RPC body.
//   - ctx was removed: it was unused in the body.
func writeSnapshotToDisk(r io.Reader, snapshotDirPath string, meta SnapshotMeta, release func() error) error {
	tmpDirPath := snapshotDirPath + ".tmp"
	if err := os.Mkdir(tmpDirPath, 0755); err != nil {
		return fmt.Errorf("creating tmp dir: %w", err)
	}

	committed := false
	defer func() {
		if !committed {
			os.RemoveAll(tmpDirPath)
		}
	}()

	if err := writeFileSynced(tmpDirPath+"/"+snapshotFileName, func(f *os.File) error {
		_, err := io.Copy(f, r)
		return err
	}); err != nil {
		return fmt.Errorf("writing snapshot data: %w", err)
	}

	if release != nil {
		if err := release(); err != nil {
			return fmt.Errorf("releasing snapshot: %w", err)
		}
	}

	if err := writeFileSynced(tmpDirPath+"/"+metaFileName, func(f *os.File) error {
		return json.NewEncoder(f).Encode(meta)
	}); err != nil {
		return fmt.Errorf("writing meta: %w", err)
	}

	if err := os.Rename(tmpDirPath, snapshotDirPath); err != nil {
		return fmt.Errorf("renaming tmp dir: %w", err)
	}
	committed = true
	return nil
}

// writeFileSynced creates path, hands the file to write, then fsyncs and closes it.
func writeFileSynced(path string, write func(*os.File) error) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := write(f); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return f.Close()
}

func (n *Node) callInstallSnapshot(ctx context.Context, target string) (res *InstallSnapshotResponse, snapshotMeta SnapshotMeta, err error) {
	latestSnapshotDir, err := getLatestSnapshotDir(n.cfg.SnapshotDir)
	if err != nil {
		return nil, SnapshotMeta{}, fmt.Errorf("callInstallSnapshot: getting latest snapshot directory: %w", err)
	}
	snapshotDirPath := n.cfg.SnapshotDir + "/" + latestSnapshotDir
	metafile, err := os.Open(snapshotDirPath + "/" + metaFileName)
	if err != nil {
		return nil, SnapshotMeta{}, fmt.Errorf("callInstallSnapshot: opening snapshot meta file: %w", err)
	}
	defer metafile.Close()
	var meta SnapshotMeta
	err = json.NewDecoder(metafile).Decode(&meta)
	if err != nil {
		return nil, SnapshotMeta{}, fmt.Errorf("callInstallSnapshot: reading snapshot meta: %w", err)
	}

	snapshotFilePath := snapshotDirPath + "/" + snapshotFileName
	snapshotFile, err := os.Open(snapshotFilePath)
	if err != nil {
		return nil, SnapshotMeta{}, fmt.Errorf("callInstallSnapshot: opening snapshot file: %w", err)
	}
	defer snapshotFile.Close()
	snapshotFileInfo, err := snapshotFile.Stat()
	if err != nil {
		return nil, SnapshotMeta{}, fmt.Errorf("callInstallSnapshot: getting snapshot file info: %w", err)
	}
	snapshotFileSize := snapshotFileInfo.Size()
	if snapshotFileSize <= 0 {
		return nil, SnapshotMeta{}, fmt.Errorf("callInstallSnapshot: snapshot file size is zero")
	}

	currentTerm, err := n.store.GetCurrentTerm(ctx)
	if err != nil {
		return nil, SnapshotMeta{}, fmt.Errorf("callInstallSnapshot: getting current term: %w", err)
	}

	req := InstallSnapshotArgs{
		Term:     uint64(currentTerm),
		LeaderID: n.GetLeaderID(),
		SnapshotMetadata: SnapshotMetadata{
			LastIncludedIndex: uint64(meta.Index),
			LastIncludedTerm:  uint64(meta.Term),
			TimeStamp:         meta.Timestamp,
			MemberConfig:      meta.MemberConfig,
		},
		Reader:       snapshotFile,
		SnapshotSize: uint64(snapshotFileSize),
	}

	deadLineTime := n.cfg.RPCTimeoutMs + ((int(snapshotFileSize) / n.cfg.InstallSnapshotDeadlineScaleSizeByte) * n.cfg.InstallSnapshotDeadlineScaleTimeMs)
	deadLineCtx, cancel := context.WithTimeout(ctx, time.Duration(deadLineTime)*time.Millisecond)
	defer cancel()
	resp, err := n.transport.InstallSnapshot(deadLineCtx, target, req)
	if err != nil {
		return nil, SnapshotMeta{}, err
	}
	return &resp, meta, nil
}
