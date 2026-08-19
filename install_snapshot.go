package raft

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
)

// discardBatch throws a staged batch away and logs if the store could not release
// it. Warn rather than Error: every caller is already on a failure path that has
// logged and is about to return, and a batch that will not close changes nothing
// about the outcome — it leaks whatever the store held for it, which is worth seeing
// but not worth reporting to the leader.
func (n *Node) discardBatch(ctx context.Context, batch Batch) {
	if err := n.store.Close(batch); err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Msg("error closing storage batch")
	}
}

// HandleInstallSnapshot receives a snapshot pushed by the leader: it writes the
// payload to disk, restores the state machine from it, and compacts the log the
// snapshot supersedes.
//
// CONTRACT FOR req.Reader — get this wrong and the failure is a hang, not an error.
//
// This function reads req.Reader to EOF. It does so on *every* exit path, including
// its rejections: the deferred io.Copy(io.Discard, ...) drains whatever is left, so a
// sender is never blocked mid-stream on a receiver that has already decided the
// answer is no.
//
// Two things follow for whoever builds that Reader — typically an adapter over a
// streaming RPC (see example/server/rpc.go):
//
//  1. The write end MUST be closed when the inbound stream ends, NOT when the
//     surrounding handler returns. This function cannot return until it has read to
//     EOF, and the pipe cannot reach EOF until the write end closes. Deferring that
//     close to the enclosing function's exit deadlocks the pair: the handler waits
//     for EOF, the enclosing function waits for the handler. Nothing breaks it except
//     the sender's deadline expiring, which surfaces here as a context cancellation
//     during Restore — a symptom several layers from the cause.
//
//  2. A failed stream MUST close the write end WITH an error (io.PipeWriter's
//     CloseWithError, or equivalent). A clean close is indistinguishable from a
//     complete transfer: the payload is copied, fsynced, renamed into place and
//     installed, and a snapshot truncated by a dropped connection becomes this node's
//     state machine. Note also that the first close wins — closing cleanly and then
//     reporting an error afterwards silently loses the error.
//
// The snapshot file becoming durable is the commit point. Everything after it is
// either staged (the storage batch, applied only once Restore succeeds) or reverted
// on failure (captureNodeState/restoreNodeState), so a partial install leaves nothing
// for the next attempt to trip over.
func (n *Node) HandleInstallSnapshot(ctx context.Context, req *InstallSnapshotArgs) (resp *InstallSnapshotResponse, err error) {
	// Hold off the election timer for the whole handler. Deferred rather than cleared
	// at each exit so it releases on every path, including a panic unwind — a flag
	// left set would stop this node campaigning for the life of the process.
	n.installSnapshotInProgress.Store(true)
	defer n.installSnapshotInProgress.Store(false)

	n.clientMu.Lock()
	term, err := n.store.GetCurrentTerm(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error getting current term")
		n.clientMu.Unlock()
		return nil, err
	}
	n.clientMu.Unlock()

	success := false
	dirPath := ""
	defer func() {
		_, _ = io.Copy(io.Discard, req.Reader)
		if !success {
			rerr := removeSnapshotDir(dirPath)
			if rerr != nil {
				zerolog.Ctx(ctx).Error().Err(rerr).Msg("install snapshot: error removing persisted snapshotDir")
				if err == nil {
					err = rerr
				}
			}
		}
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
		n.signalElectionTimeout()
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

			batch := n.store.NewBatch()
			n.clientMu.Lock()
			err = batch.SetCurrentTerm(ctx, uint(req.Term))
			if err != nil {
				zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error setting current term")
				n.clientMu.Unlock()
				n.discardBatch(ctx, batch)
				success = false
				return
			}

			err = batch.SetVotedFor(ctx, req.LeaderID)
			if err != nil {
				zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error setting voted for")
				n.clientMu.Unlock()
				n.discardBatch(ctx, batch)
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
	dirPath = snapshotDirPath

	err = writeSnapshotToDisk(req.Reader, snapshotDirPath, SnapshotMeta{
		Index:        uint(req.SnapshotMetadata.LastIncludedIndex),
		Term:         uint(req.SnapshotMetadata.LastIncludedTerm),
		ID:           n.cfg.ID,
		LeaderID:     req.LeaderID,
		Caller:       snapshotCallerInstall,
		MemberConfig: req.SnapshotMetadata.MemberConfig,
	}, nil)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error writing snapshot to disk")
		success = false
		return
	}

	lastLogIndex, err := n.store.GetLastIndex(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error getting last log index")
		success = false
		return
	}

	n.clientMu.Lock()

	batch := n.store.NewBatch()
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

			err = batch.DeleteLogs(ctx, 0, lastLogIndex)
			if err != nil {
				zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error compacting logs")
				success = false
				n.discardBatch(ctx, batch)
				n.clientMu.Unlock()
				return
			}
		} else {
			zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error getting log by index")
			success = false
			n.discardBatch(ctx, batch)
			n.clientMu.Unlock()
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

		err = batch.DeleteLogs(ctx, 0, lastLogIndex)
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error compacting logs")
			success = false
			n.discardBatch(ctx, batch)
			n.clientMu.Unlock()
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

		err = batch.DeleteLogs(ctx, 0, uint(req.SnapshotMetadata.LastIncludedIndex))
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error compacting logs")
			success = false
			n.discardBatch(ctx, batch)
			n.clientMu.Unlock()
			return
		}
	}

	// Staged into the batch we already hold rather than through setTermAndVote: that
	// helper opens a batch of its own, which would commit the term and vote
	// immediately while the log deletion is still staged — two writes where the point
	// of the batch is one. Here all three land with the same Apply, after Restore has
	// succeeded.
	err = batch.SetCurrentTerm(ctx, uint(req.Term))
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error staging current term")
		success = false
		n.discardBatch(ctx, batch)
		n.clientMu.Unlock()
		return
	}

	err = batch.SetVotedFor(ctx, req.LeaderID)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error staging voted for")
		success = false
		n.discardBatch(ctx, batch)
		n.clientMu.Unlock()
		return
	}

	prevState := n.captureNodeState()

	n.SetLeaderID(req.LeaderID)

	// Cache the snapshot boundary so the next AppendEntries from the leader — whose
	// prevLogIndex is this snapshot's last-included index — can be validated against
	// it in logTermAt even though the entry itself is now compacted.
	zerolog.Ctx(ctx).Info().
		Uint("from_index", n.GetSnapshotLatestIndex()).
		Uint("from_term", n.GetSnapshotLatestTerm()).
		Uint64("to_index", req.SnapshotMetadata.LastIncludedIndex).
		Uint64("to_term", req.SnapshotMetadata.LastIncludedTerm).
		Str("leader", req.LeaderID).
		Str("caller", snapshotCallerInstall).
		Msg("snapshotLatest: a leader pushed a snapshot to us")
	n.SetSnapshotLatest(uint(req.SnapshotMetadata.LastIncludedIndex), uint(req.SnapshotMetadata.LastIncludedTerm), snapshotCallerInstall)

	for peerID, peerState := range req.SnapshotMetadata.MemberConfig {
		n.SetPeerState(peerID, peerState)
	}
	n.clientMu.Unlock()

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
		// clientMu is already released here; only the batch is still outstanding.
		n.discardBatch(ctx, batch)
		n.restoreNodeState(ctx, prevState)
		return
	}
	defer snapshotFile.Close()

	n.commitMu.Lock()
	err = n.sm.Restore(ctx, snapshotFile)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error restoring state machine from snapshot")
		success = false
		n.discardBatch(ctx, batch)
		n.commitMu.Unlock()
		n.restoreNodeState(ctx, prevState)
		return
	}
	n.commitMu.Unlock()
	n.SetLastApplied(uint(req.SnapshotMetadata.LastIncludedIndex))

	// Commit the staged log compaction last, once the state machine is provably at
	// the snapshot's index. Ordering it here is what makes a failure survivable: the
	// destructive half was only ever staged, so nothing needs reverting.
	n.clientMu.Lock()
	err = n.store.Apply(batch)
	if err != nil {
		// The snapshot is durable and the state machine is current — this node is
		// correct, it just still holds log entries the snapshot supersedes. In the two
		// branches above that discard the whole log, those retained entries are
		// divergent, and a later AppendEntries that backs off below the snapshot index
		// would consult them. That is why this fails the install rather than treating
		// leftover entries as inert the way superseded snapshot directories are.
		//
		// Failing is cheap because the handler is re-runnable: the leader resends, the
		// re-Restore is inert on an already-current state machine, and the compaction
		// is simply retried.
		zerolog.Ctx(ctx).Error().Err(err).
			Uint64("snapshot_index", req.SnapshotMetadata.LastIncludedIndex).
			Msg("install snapshot: error committing log compaction; snapshot is installed but the log is not compacted")
		success = false
		n.discardBatch(ctx, batch)
		n.clientMu.Unlock()
		n.restoreNodeState(ctx, prevState)
		return
	}
	n.clientMu.Unlock()

	zerolog.Ctx(ctx).Info().Msgf("install snapshot: successfully installed snapshot from leader %s with index %d and term %d", req.LeaderID, req.SnapshotMetadata.LastIncludedIndex, req.SnapshotMetadata.LastIncludedTerm)

	success = true
	return
}
