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

	snapshotDirName := generateLatestSnapshotDirName(latestAppliedIndex, uint(latestLog.Term))
	snapshotDirPath := n.cfg.SnapshotDir + "/" + snapshotDirName
	meta := SnapshotMeta{Index: latestAppliedIndex, Term: uint(latestLog.Term), ID: snapshotDirName}
	err = writeSnapshotToDisk(pr, snapshotDirPath, meta, snap.Release)
	if err != nil {
		return fmt.Errorf("writing snapshot to disk: %w", err)
	}

	return n.store.CompactLogs(ctx, latestAppliedIndex)
}

func generateLatestSnapshotDirName(latestIndex, latestTerm uint) string {
	return strconv.Itoa(int(latestIndex)) + "-" + strconv.Itoa(int(latestTerm)) + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)
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
