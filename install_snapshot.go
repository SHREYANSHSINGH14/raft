package raft

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
)

func (n *Node) HandleInstallSnapshot(ctx context.Context, req *InstallSnapshotArgs) (resp *InstallSnapshotResponse, err error) {
	n.clientMu.Lock()
	term, err := n.store.GetCurrentTerm(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error getting current term")
		n.clientMu.Unlock()
		return nil, err
	}
	n.clientMu.Unlock()

	success := false
	defer func() {
		_, _ = io.Copy(io.Discard, req.Reader)
		resp = &InstallSnapshotResponse{
			Term:    uint64(term),
			Success: success,
		}
		// Close last, and never let it overwrite a real failure: the handler's error
		// is what the caller needs, and a close error on a stream we have just
		// drained is almost never the interesting half. It is still logged, and it
		// still surfaces as the return when nothing else went wrong.
		if closer, ok := req.Reader.(io.Closer); ok {
			if cerr := closer.Close(); cerr != nil {
				zerolog.Ctx(ctx).Error().Err(cerr).Msg("install snapshot: error closing snapshot reader")
				if err == nil {
					err = cerr
				}
			}
		}
	}()

	if req.Term < uint64(term) {
		zerolog.Ctx(ctx).Debug().Msgf("install snapshot: ignoring request from %s with term %d, current term is %d", req.LeaderID, req.Term, term)
		success = false
		return
	}

	dirs, err := os.ReadDir(n.cfg.SnapshotDir)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error reading snapshot directory")
		success = false
		return
	}

	if len(dirs) > 0 {
		var latestSnapshotIdx uint
		latestSnapshotIdx, err = getLatestSnapshotIndex(dirs)
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error getting latest snapshot index")
			success = false
			return
		}

		if req.SnapshotMetadata.LastIncludedIndex <= uint64(latestSnapshotIdx) {
			zerolog.Ctx(ctx).Debug().Msgf("install snapshot: ignoring request from %s with snapshot index %d, latest snapshot index is %d", req.LeaderID, req.SnapshotMetadata.LastIncludedIndex, latestSnapshotIdx)
			n.clientMu.Lock()
			err = n.store.SetCurrentTerm(ctx, uint(req.Term))
			if err != nil {
				zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error setting current term")
				n.clientMu.Unlock()
				success = false
				return
			}

			err = n.store.SetVotedFor(ctx, req.LeaderID)
			if err != nil {
				zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error setting voted for")
				n.clientMu.Unlock()
				success = false
				return
			}

			n.SetLeaderID(req.LeaderID)
			n.clientMu.Unlock()
			success = true
			return
		}
	}

	// NOTHING DESTRUCTIVE MAY HAPPEN ABOVE THIS POINT.
	//
	// Deleting the old snapshots and truncating the log used to run here, before
	// the new snapshot was written. A crash in that window left the node with no
	// snapshot, no log, and a lastApplied pointing at state nothing on disk could
	// account for — and nothing to signal that an install had been interrupted.
	//
	// writeSnapshotToDisk renames a fully-synced temp directory into place, so the
	// new snapshot appearing IS the commit point. Everything destructive happens
	// after it, and a crash before it leaves the previous snapshot and the log
	// exactly as they were.

	timestamp := time.Now()
	snapshotDir := generateLatestSnapshotDirName(uint(req.SnapshotMetadata.LastIncludedIndex), uint(req.SnapshotMetadata.LastIncludedTerm), timestamp)

	snapshotDirPath := n.cfg.SnapshotDir + "/" + snapshotDir

	err = writeSnapshotToDisk(req.Reader, snapshotDirPath, SnapshotMeta{
		Index:        uint(req.SnapshotMetadata.LastIncludedIndex),
		Term:         uint(req.SnapshotMetadata.LastIncludedTerm),
		ID:           n.cfg.ID,
		LeaderID:     req.LeaderID,
		MemberConfig: req.SnapshotMetadata.MemberConfig,
	}, nil)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error writing snapshot to disk")
		success = false
		return
	}

	// The new snapshot is durable, so the old ones are now dead weight. Failing to
	// remove them is not worth failing the install over: getLatestSnapshotIndex
	// takes the maximum, and the one just written has the highest index, so a
	// leftover directory is inert.
	//
	// dirs was listed before the write, so it cannot contain the new directory.
	// Note the path: dir.Name() alone is relative to the process working
	// directory, which is not where snapshots live.
	for _, dir := range dirs {
		if !dir.IsDir() {
			continue
		}
		if rmErr := os.RemoveAll(n.cfg.SnapshotDir + "/" + dir.Name()); rmErr != nil {
			zerolog.Ctx(ctx).Warn().Err(rmErr).Msgf("install snapshot: could not delete superseded snapshot %s", dir.Name())
		}
	}

	// Apply the snapshot to the state machine and update the log store
	snapshotFilePath := snapshotDirPath + "/" + snapshotFileName
	snapshotFile, err := os.Open(snapshotFilePath)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error opening snapshot file")
		success = false
		return
	}
	defer snapshotFile.Close()

	n.commitMu.Lock()
	err = n.sm.Restore(ctx, snapshotFile)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error restoring state machine from snapshot")
		success = false
		n.commitMu.Unlock()
		return
	}

	err = n.store.SetLastApplied(ctx, uint(req.SnapshotMetadata.LastIncludedIndex))
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error setting last applied index")
		success = false
		n.commitMu.Unlock()
		return
	}
	n.commitMu.Unlock()

	lastLogIndex, err := n.store.GetLastIndex(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error getting last log index")
		success = false
		return
	}

	log, err := n.store.GetLogByIndex(ctx, uint(req.SnapshotMetadata.LastIncludedIndex))
	if err != nil {
		if err == ErrNotFound {
			// We hold nothing at the snapshot's last-included index, so no entry we
			// have is provably consistent with it. The whole log goes; the snapshot
			// is now the only truth on this node.
			zerolog.Ctx(ctx).Info().
				Uint("delete_from", 1).
				Uint("delete_to", lastLogIndex).
				Uint64("snapshot_index", req.SnapshotMetadata.LastIncludedIndex).
				Str("leader", req.LeaderID).
				Msg("discarding entire log: no entry at the snapshot's last-included index")

			err = n.store.DeleteLogs(ctx, 0, lastLogIndex)
			if err != nil {
				zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error compacting logs")
				success = false
				return
			}
		} else {
			zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error getting log by index")
			success = false
			return
		}
	} else if log.Term != req.SnapshotMetadata.LastIncludedTerm {
		// We have an entry at that index but from a different term, so it belongs to
		// a branch the cluster discarded. Everything we hold is suspect.
		zerolog.Ctx(ctx).Info().
			Uint("delete_from", 1).
			Uint("delete_to", lastLogIndex).
			Uint64("snapshot_index", req.SnapshotMetadata.LastIncludedIndex).
			Uint64("our_term_at_index", log.Term).
			Uint64("snapshot_term", req.SnapshotMetadata.LastIncludedTerm).
			Str("leader", req.LeaderID).
			Msg("discarding entire log: our entry at the snapshot index is from a different term")

		err = n.store.DeleteLogs(ctx, 0, lastLogIndex)
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error compacting logs")
			success = false
			return
		}
	} else {
		// Our entry at that index agrees with the snapshot, so everything after it is
		// still good. Only the prefix the snapshot covers is dropped.
		zerolog.Ctx(ctx).Info().
			Uint("delete_from", 1).
			Uint64("delete_to", req.SnapshotMetadata.LastIncludedIndex).
			Uint("keeping_through", lastLogIndex).
			Str("leader", req.LeaderID).
			Msg("compacting log: the snapshot matches our entry at its index, keeping the suffix")

		err = n.store.DeleteLogs(ctx, 0, uint(req.SnapshotMetadata.LastIncludedIndex))
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error compacting logs")
			success = false
			return
		}
	}

	n.clientMu.Lock()
	err = n.store.SetCurrentTerm(ctx, uint(req.Term))
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error setting current term")
		n.clientMu.Unlock()
		success = false
		return
	}

	err = n.store.SetVotedFor(ctx, req.LeaderID)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error setting voted for")
		n.clientMu.Unlock()
		success = false
		return
	}

	n.SetLeaderID(req.LeaderID)

	// Cache the snapshot boundary so the next AppendEntries from the leader — whose
	// prevLogIndex is this snapshot's last-included index — can be validated against
	// it in logTermAt even though the entry itself is now compacted.
	n.SetSnapshotLatest(uint(req.SnapshotMetadata.LastIncludedIndex), uint(req.SnapshotMetadata.LastIncludedTerm))

	for peerID, peerState := range req.SnapshotMetadata.MemberConfig {
		n.SetPeerState(peerID, peerState)
	}
	n.clientMu.Unlock()

	zerolog.Ctx(ctx).Info().Msgf("install snapshot: successfully installed snapshot from leader %s with index %d and term %d", req.LeaderID, req.SnapshotMetadata.LastIncludedIndex, req.SnapshotMetadata.LastIncludedTerm)

	success = true
	return
}
