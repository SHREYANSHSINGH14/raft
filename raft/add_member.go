package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog"
)

func (n *Node) AddMember(ctx context.Context, peerID string, peerState PeerState) error {
	n.clientMu.Lock()
	if n.GetRole() != ServerRole_Leader {
		n.clientMu.Unlock()
		return fmt.Errorf("not the leader: current leader is %q", n.GetLeaderID())
	}

	if n.hasStagingPeer() {
		n.clientMu.Unlock()
		return fmt.Errorf("addMember: one member addition already in progress")
	}

	n.addPeer(peerID, Peer{
		PeerState:  PeerState_Staging,
		NextIndex:  0,
		MatchIndex: 0,
	})

	memberConfigs := n.peersSnapshot()

	data, err := json.Marshal(memberConfigs)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("addMember: failed to marshal member info")
		return err
	}

	// 1. Append the member additon log
	entry, err := n.appendEntry(ctx, EntryType_Config, data)
	if err != nil {
		n.clientMu.Unlock()
		return fmt.Errorf("addMember: %w", err)
	}
	n.clientMu.Unlock()

	// 2. Wait for it to commit
	n.commitCond.L.Lock()
	for n.commitIndex < uint(entry.Index) && ctx.Err() == nil {
		n.commitCond.Wait()
	}
	if ctx.Err() != nil {
		n.commitCond.L.Unlock()
		return fmt.Errorf("addMember: context cancelled before commit")
	}
	n.commitCond.L.Unlock()

	// NOTE: we keep a single addition path that handles both Voter and NonVoter
	// targets, rather than specializing the flow per target state. It is kept this
	// way for simplicity and to complete the project, which is the primary goal —
	// the path is correct for both, just not separately optimized for each.

	err = func(context.Context, string) error {
		// 3. Send snapshot
		sendSnapshotCount := 0
	send_snapshot:
		if sendSnapshotCount > 5 { // TODO: make 5 configurable
			err := fmt.Errorf("install snapshot is slow and node not able to catchup aborting")
			zerolog.Ctx(ctx).Error().Err(err).Msg("addMember: " + err.Error())
			return err
		}
		sendSnapshotCount++
		res, snapshotMeta, err := n.callInstallSnapshot(ctx, peerID)
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msg("addMember: failed to send snapshot")
			return err
		}

		if !res.Success {
			err = fmt.Errorf("install snapshot unsuccessfull")
			zerolog.Ctx(ctx).Error().Err(err).Msg("addMember: install snapshot unsuccessful")
			return err
		}

		defer n.setCatchingUpIdx(DefaultCatchingUpIdx)
		n.setCatchingUpIdx(int64(snapshotMeta.Index + 1))
		_, err = n.store.GetLogByIndex(ctx, snapshotMeta.Index+1)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				zerolog.Ctx(ctx).Debug().Msg("addMember: logs compacted sending snapshot again")
				goto send_snapshot
			}
			zerolog.Ctx(ctx).Debug().Msgf("addMember: error getting log at index: %d", snapshotMeta.Index)
			return err
		}

		// Here if the TOCTOU occurs what if when I checked it was there and next instant deleted we need a way to synchronize

		// 4. Catch the new member up to the log head over a bounded number of
		// rounds. Each round sends everything from startIdx to the end of the log;
		// the member advances and the next round sends whatever committed meanwhile.
		// When a round completes within an election timeout the member is keeping
		// pace and is caught up enough to promote (Ongaro dissertation §4.2.1); if
		// it is still slow after the last round, we abort.
		const maxCatchUpRounds = 10 // TODO: make configurable
		electionTimeout := time.Duration(n.cfg.ElectionMinMs) * time.Millisecond

		startIdx := snapshotMeta.Index + 1
		prevLogIdx := snapshotMeta.Index
		prevLogTerm := snapshotMeta.Term
		caughtUp := false

		for i := range maxCatchUpRounds {
			// Publish the retain floor first so the snapshot loop won't compact away
			// the entries this round is about to read and send.
			n.setCatchingUpIdx(int64(startIdx))

			logs, err := n.store.GetLogs(ctx, &startIdx, nil)
			if err != nil {
				zerolog.Ctx(ctx).Error().Err(err).Msg("addMember: error getting logs")
				return err
			}
			if len(logs) == 0 {
				// Nothing left to replicate — the member is already at the head.
				caughtUp = true
				break
			}

			// We do this cuz we already now have the copy of logs, so if snapshot goroutine Deletes the logs no issues
			n.setCatchingUpIdx(DefaultCatchingUpIdx)

			currentTerm, err := n.GetCurrentTerm(ctx)
			if err != nil {
				zerolog.Ctx(ctx).Error().Err(err).Msg("addMember: error getting current term")
				return err
			}

			reqTime := time.Now()
			deadlineCtx, cancel := context.WithTimeout(ctx, n.appendEntriesDeadline(len(logs)))
			res, err := n.transport.AppendEntries(deadlineCtx, peerID, AppendEntriesArgs{
				Term:         uint64(currentTerm),
				LeaderID:     n.GetLeaderID(),
				PrevLogIndex: uint64(prevLogIdx),
				PrevLogTerm:  uint64(prevLogTerm),
				Entries:      logs,
				LeaderCommit: uint64(n.GetCommitIndex()),
			})
			cancel()
			resTime := time.Now()

			if err != nil {
				zerolog.Ctx(ctx).Error().Err(err).Msg("addMember: append entries to catching-up member failed")
				return err
			}

			// A higher term means we are no longer the legitimate leader — stop.
			if res.Term > uint64(currentTerm) {
				return fmt.Errorf("addMember: stepped down, peer %q term %d > current term %d", peerID, res.Term, currentTerm)
			}

			if !res.Success {
				// Log inconsistency: back off one entry and retry from there.
				// logTermAt resolves the term even at the snapshot boundary
				// (prevLogIdx-1 == snapshotLatestIndex), so backing off onto the anchor
				// works without a full resend. ok == false means prevLogIdx-1 is below
				// the snapshot — the member is further behind than the log now holds, so
				// restart from a snapshot.
				if prevLogIdx == 0 {
					return fmt.Errorf("addMember: append entries rejected at start of log for peer %q", peerID)
				}
				backoffIdx := prevLogIdx - 1
				term, ok, termErr := n.logTermAt(ctx, uint64(backoffIdx))
				if termErr != nil {
					return termErr
				}
				if !ok {
					zerolog.Ctx(ctx).Debug().Msg("addMember: back-off entry compacted, resending snapshot")
					goto send_snapshot
				}
				startIdx = backoffIdx + 1
				prevLogIdx = backoffIdx
				prevLogTerm = uint(term)
				continue
			}

			// Success: the member now holds everything we just sent. Advance our
			// view of it and the window for the next round.
			lastSent := logs[len(logs)-1]
			n.SetMatchPeerIndex(peerID, uint(lastSent.Index))
			n.SetNextPeerIndex(peerID, uint(lastSent.Index)+1)
			startIdx = uint(lastSent.Index) + 1
			prevLogIdx = uint(lastSent.Index)
			prevLogTerm = uint(lastSent.Term)

			n.setCatchingUpIdx(int64(startIdx))
			_, err = n.store.GetLogByIndex(ctx, startIdx)
			if err != nil {
				if errors.Is(err, ErrNotFound) {
					zerolog.Ctx(ctx).Debug().Msg("addMember: logs compacted sending snapshot again")
					goto send_snapshot
				}
				zerolog.Ctx(ctx).Debug().Msgf("addMember: error getting log at index: %d", startIdx)
				return err
			}

			// Caught-up decision (Ongaro §4.2.1): a round that took longer than an
			// election timeout means the member is still lagging — try again, or
			// abort if that was the last round. A fast round means it is keeping
			// pace: done.
			if resTime.Sub(reqTime) > electionTimeout {
				if i == maxCatchUpRounds-1 {
					return fmt.Errorf("addMember: member %q could not catch up within %d rounds, aborting", peerID, maxCatchUpRounds)
				}
				continue
			}
			caughtUp = true
			break
		}

		if !caughtUp {
			return fmt.Errorf("addMember: member %q did not catch up, aborting", peerID)
		}

		return nil
	}(ctx, peerID)
	if err != nil {
		// The catch-up failed and the member is still PeerState_Staging in our
		// configuration, which would wedge every future AddMember (the hasStagingPeer
		// guard). Roll it back the same way we added it: drop the peer and replicate
		// the resulting configuration as a new Config entry, then wait for it to
		// commit.
		n.clientMu.Lock()
		n.removePeer(peerID)
		data, mErr := json.Marshal(n.peersSnapshot())
		if mErr != nil {
			n.clientMu.Unlock()
			zerolog.Ctx(ctx).Error().Err(mErr).Msg("addMember: rollback failed to marshal config")
			return fmt.Errorf("addMember: %w (rollback failed: %v)", err, mErr)
		}
		rollbackEntry, aErr := n.appendEntry(ctx, EntryType_Config, data)
		if aErr != nil {
			n.clientMu.Unlock()
			zerolog.Ctx(ctx).Error().Err(aErr).Msg("addMember: rollback failed to append config")
			return fmt.Errorf("addMember: %w (rollback failed: %v)", err, aErr)
		}
		n.clientMu.Unlock()

		// Wait for the removal to commit. Best effort: the original failure is what
		// we return, and a stepped-down leader may never advance commitIndex here
		// (ctx cancellation breaks the wait).
		n.commitCond.L.Lock()
		for n.commitIndex < uint(rollbackEntry.Index) && ctx.Err() == nil {
			n.commitCond.Wait()
		}
		n.commitCond.L.Unlock()

		return fmt.Errorf("addMember: %w", err)
	}

	// 5. The member is caught up. Promote it from Staging to its target state
	// (Voter/NonVoter) and replicate the new configuration as a Config entry, then
	// wait for it to commit — same shape as steps 1-2.
	n.clientMu.Lock()
	n.SetPeerState(peerID, peerState)
	data, err = json.Marshal(n.peersSnapshot())
	if err != nil {
		n.clientMu.Unlock()
		zerolog.Ctx(ctx).Error().Err(err).Msg("addMember: failed to marshal promotion config")
		return fmt.Errorf("addMember: %w", err)
	}
	promoteEntry, err := n.appendEntry(ctx, EntryType_Config, data)
	if err != nil {
		n.clientMu.Unlock()
		return fmt.Errorf("addMember: %w", err)
	}
	n.clientMu.Unlock()

	n.commitCond.L.Lock()
	for n.commitIndex < uint(promoteEntry.Index) && ctx.Err() == nil {
		n.commitCond.Wait()
	}
	if ctx.Err() != nil {
		n.commitCond.L.Unlock()
		return fmt.Errorf("addMember: context cancelled before promotion commit")
	}
	n.commitCond.L.Unlock()

	return nil
}
