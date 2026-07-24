package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

const (
	SnapshotFileName = "snapshot"
	MetaFileName     = "meta.json"
)

type SnapshotMeta struct {
	Index        uint                 `json:"index"`
	Term         uint                 `json:"term"`
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
	lastAppliedIndex, err := n.store.GetLastApplied(ctx)
	if err != nil {
		return fmt.Errorf("getting last applied index: %w", err)
	}
	if !shouldTriggerSnapshot(ctx, n.cfg.SnapshotDir, lastAppliedIndex, n.cfg.SnapshotThreshold) {
		return nil
	}

	n.snapShotInProgress.Store(true)
	snap, err := n.sm.Snapshot(ctx)
	if err != nil {
		n.snapShotInProgress.Store(false)
		return fmt.Errorf("taking snapshot: %w", err)
	}
	latestAppliedIndex, err := n.store.GetLastApplied(ctx)
	if err != nil {
		n.snapShotInProgress.Store(false)
		return fmt.Errorf("getting latest applied index: %w", err)
	}
	n.snapShotInProgress.Store(false)
	n.commitCond.Broadcast()
	latestLog, err := n.store.GetLogByIndex(ctx, latestAppliedIndex)
	if err != nil {
		return fmt.Errorf("getting latest log entry: %w", err)
	}

	pr, pw := io.Pipe()
	go func() {
		err := snap.Persist(ctx, pw)
		pw.CloseWithError(err)
	}()
	// closing the read end unblocks Persist if writeSnapshotToDisk bails before draining pr
	defer pr.Close()

	timestamp := time.Now()
	snapshotDirName := generateLatestSnapshotDirName(latestAppliedIndex, uint(latestLog.Term), timestamp)
	snapshotDirPath := n.cfg.SnapshotDir + "/" + snapshotDirName
	memberConfig := make(map[string]PeerState)
	for id, peer := range n.peersSnapshot() {
		memberConfig[id] = peer.PeerState
	}
	meta := SnapshotMeta{
		Index:        latestAppliedIndex,
		Term:         uint(latestLog.Term),
		ID:           n.GetID(),
		LeaderID:     n.GetLeaderID(),
		MemberConfig: memberConfig,
		Timestamp:    timestamp,
	}
	err = writeSnapshotToDisk(pr, snapshotDirPath, meta, snap.Release)
	if err != nil {
		return fmt.Errorf("writing snapshot to disk: %w", err)
	}

	return n.store.DeleteLogs(ctx, 0, latestAppliedIndex)
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

func (n *Node) getLatestSnapshotDir() (string, error) {
	snapShotDirEntries, err := os.ReadDir(n.cfg.SnapshotDir)
	if err != nil {
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
		return "", fmt.Errorf("getLatestSnapshotDir: no snapshot directory found")
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

	if err := writeFileSynced(tmpDirPath+"/"+SnapshotFileName, func(f *os.File) error {
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

	if err := writeFileSynced(tmpDirPath+"/"+MetaFileName, func(f *os.File) error {
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

func (n *Node) callInstallSnapshot(ctx context.Context, target string) (*InstallSnapshotResponse, uint, error) {
	latestSnapshotDir, err := n.getLatestSnapshotDir()
	if err != nil {
		return nil, 0, fmt.Errorf("callInstallSnapshot: getting latest snapshot directory: %w", err)
	}
	snapshotDirPath := n.cfg.SnapshotDir + "/" + latestSnapshotDir
	metafile, err := os.Open(snapshotDirPath + "/" + MetaFileName)
	if err != nil {
		return nil, 0, fmt.Errorf("callInstallSnapshot: opening snapshot meta file: %w", err)
	}
	defer metafile.Close()
	var meta SnapshotMeta
	err = json.NewDecoder(metafile).Decode(&meta)
	if err != nil {
		return nil, 0, fmt.Errorf("callInstallSnapshot: reading snapshot meta: %w", err)
	}

	snapshotFilePath := snapshotDirPath + "/" + SnapshotFileName
	snapshotFile, err := os.Open(snapshotFilePath)
	if err != nil {
		return nil, 0, fmt.Errorf("callInstallSnapshot: opening snapshot file: %w", err)
	}
	defer snapshotFile.Close()
	snapshotFileInfo, err := snapshotFile.Stat()
	if err != nil {
		return nil, 0, fmt.Errorf("callInstallSnapshot: getting snapshot file info: %w", err)
	}
	snapshotFileSize := snapshotFileInfo.Size()
	if snapshotFileSize <= 0 {
		return nil, 0, fmt.Errorf("callInstallSnapshot: snapshot file size is zero")
	}

	currentTerm, err := n.store.GetCurrentTerm(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("callInstallSnapshot: getting current term: %w", err)
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
		return nil, 0, err
	}
	return &resp, meta.Index, nil
}
