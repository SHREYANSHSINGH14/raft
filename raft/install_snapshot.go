package raft

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
)

func (n *Node) HandleInstallSnapshot(ctx context.Context, req *InstallSnapshotArgs) (resp *InstallSnapshotResponse, err error) {

	term, err := n.store.GetCurrentTerm(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error getting current term")
		return nil, err
	}
	success := false
	defer func() {
		_, _ = io.Copy(io.Discard, req.Reader)
		resp = &InstallSnapshotResponse{
			Term:    uint64(term),
			Success: success,
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
			err = n.store.SetCurrentTerm(ctx, uint(req.Term))
			if err != nil {
				zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error setting current term")
				success = false
				return
			}

			err = n.store.SetVotedFor(ctx, req.LeaderID)
			if err != nil {
				zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error setting voted for")
				success = false
				return
			}

			n.SetLeaderID(req.LeaderID)
			success = true
			return
		} else {
			for _, dir := range dirs {
				if !dir.IsDir() {
					continue
				}
				err = os.RemoveAll(dir.Name())
				if err != nil {
					zerolog.Ctx(ctx).Error().Err(err).Msgf("install snapshot: error deleting %s", dir.Name())
					return
				}
			}

			var lastIndex uint
			lastIndex, err = n.store.GetLastLogIndex(ctx)
			if err != nil {
				zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error getting lastIndex")
				return
			}

			if lastIndex > 0 {
				err = n.store.DeleteLogs(ctx, 0, lastIndex)
				if err != nil {
					zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error compacting logs")
					return
				}
			}
		}
	}

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

	// Apply the snapshot to the state machine and update the log store
	snapshotFilePath := snapshotDirPath + "/" + SnapshotFileName
	snapshotFile, err := os.Open(snapshotFilePath)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error opening snapshot file")
		success = false
		return
	}
	defer snapshotFile.Close()

	n.commitCond.L.Lock()
	err = n.sm.Restore(ctx, snapshotFile)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error restoring state machine from snapshot")
		success = false
		n.commitCond.L.Unlock()
		return
	}

	err = n.store.SetLastApplied(ctx, uint(req.SnapshotMetadata.LastIncludedIndex))
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error setting last applied index")
		success = false
		n.commitCond.L.Unlock()
		return
	}
	n.commitCond.L.Unlock()

	lastLogIndex, err := n.store.GetLastLogIndex(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error getting last log index")
		success = false
		return
	}

	log, err := n.store.GetLogByIndex(ctx, uint(req.SnapshotMetadata.LastIncludedIndex))
	if err != nil {
		if err == ErrNotFound {
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
		err = n.store.DeleteLogs(ctx, 0, lastLogIndex)
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error compacting logs")
			success = false
			return
		}
	} else {
		err = n.store.DeleteLogs(ctx, 0, uint(req.SnapshotMetadata.LastIncludedIndex))
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error compacting logs")
			success = false
			return
		}
	}

	err = n.store.SetCurrentTerm(ctx, uint(req.Term))
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error setting current term")
		success = false
		return
	}

	err = n.store.SetVotedFor(ctx, req.LeaderID)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("install snapshot: error setting voted for")
		success = false
		return
	}

	n.SetLeaderID(req.LeaderID)

	for peerID, peerState := range req.SnapshotMetadata.MemberConfig {
		n.SetPeerState(peerID, peerState)
	}

	zerolog.Ctx(ctx).Info().Msgf("install snapshot: successfully installed snapshot from leader %s with index %d and term %d", req.LeaderID, req.SnapshotMetadata.LastIncludedIndex, req.SnapshotMetadata.LastIncludedTerm)

	success = true
	return
}
