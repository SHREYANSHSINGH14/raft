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
	// Decide and stage the new member atomically under clientMu: the leader +
	// no-other-staging-peer guards and the addPeer must not interleave with a
	// concurrent AddMember, and the config we marshal must reflect the addPeer.
	n.clientMu.Lock()
	if n.GetRole() != ServerRole_Leader {
		n.clientMu.Unlock()
		return fmt.Errorf("not the leader: current leader is %q", n.GetLeaderID())
	}
	if n.hasStagingPeer() {
		n.clientMu.Unlock()
		return fmt.Errorf("addMember: one member addition already in progress")
	}
	n.addPeer(peerID, Peer{PeerState: PeerState_Staging, NextIndex: 0, MatchIndex: 0})
	data, err := json.Marshal(n.peersSnapshot())
	n.clientMu.Unlock()
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("addMember: failed to marshal member info")
		return err
	}

	// 1 + 2. Replicate the staging config change and wait for it to commit.
	if future, err := n.Propose(ctx, EntryType_Config, data); err != nil {
		return fmt.Errorf("addMember: %w", err)
	} else {
		if err := future.Wait(ctx); err != nil {
			return fmt.Errorf("addMember: %w", err)
		}
	}

	if !n.IsLeader() {
		// The member addition was committed, but we are no longer the leader. The
		// new member is still Staging, and the next leader will either finish the
		// addition or roll it back. We are done here.
		return fmt.Errorf("addMember: leadership lost")
	}

	// NOTE: we keep a single addition path that handles both Voter and NonVoter
	// targets, rather than specializing the flow per target state. It is kept this
	// way for simplicity and to complete the project, which is the primary goal —
	// the path is correct for both, just not separately optimized for each.

	// 3 + 4. Send a snapshot and catch the new member up to the log head.
	err = n.catchUpMember(ctx, peerID)
	if err != nil {
		// The catch-up failed and the member is still PeerState_Staging in our
		// configuration, which would wedge every future AddMember (the hasStagingPeer
		// guard). Roll it back the same way we added it: drop the peer and replicate
		// the resulting configuration as a new Config entry, then wait for it to
		// commit.
		n.clientMu.Lock()
		n.removePeer(peerID)
		data, mErr := json.Marshal(n.peersSnapshot())
		n.clientMu.Unlock()
		if mErr != nil {
			zerolog.Ctx(ctx).Error().Err(mErr).Msg("addMember: rollback failed to marshal config")
			return fmt.Errorf("addMember: %w (rollback failed: %v)", err, mErr)
		}

		// Replicate the removal. Best effort: the original failure is what we
		// return; a Propose failure (e.g. we already stepped down) just leaves the
		// staging peer to be cleaned up later, so we log it rather than mask err.
		if pFuture, pErr := n.Propose(ctx, EntryType_Config, data); pErr != nil {
			zerolog.Ctx(ctx).Warn().Err(pErr).Msg("addMember: rollback propose failed; staging peer may remain")
		} else if wErr := pFuture.Wait(ctx); wErr != nil {
			// Named wErr, not err: shadowing here would lose the catch-up failure this
			// whole branch exists to report. Logged and not returned, for the same
			// reason the propose failure above is — the rollback is best effort.
			zerolog.Ctx(ctx).Warn().Err(wErr).Msg("addMember: rollback did not commit; staging peer may remain")
		}
		return fmt.Errorf("addMember: %w", err)
	}

	// 5. The member is caught up. Promote it from Staging to its target state
	// (Voter/NonVoter) and replicate the new configuration, waiting for commit.
	n.clientMu.Lock()
	n.SetPeerState(peerID, peerState)
	data, err = json.Marshal(n.peersSnapshot())
	n.clientMu.Unlock()
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("addMember: failed to marshal promotion config")
		return fmt.Errorf("addMember: %w", err)
	}
	if future, err := n.Propose(ctx, EntryType_Config, data); err != nil {
		return fmt.Errorf("addMember: %w", err)
	} else {
		if err := future.Wait(ctx); err != nil {
			return fmt.Errorf("addMember: %w", err)
		}
	}

	// The member is promoted and committed; tell the heartbeat orchestrator to
	// start replicating to it, since it fixed its peer set when the term began.
	n.notifyMemberAdded(ctx, peerID)
	return nil
}

// catchUpMember brings a freshly-added Staging member up to the log head: it sends
// the latest snapshot, then replicates the log tail after it over a bounded number
// of rounds, resending a fresh snapshot if the member falls behind the log. It
// returns nil once the member is caught up (a round that beats an election
// timeout, or nothing left to send — Ongaro §4.2.1) or an error if it cannot. The
// retain floor (catchingUpIdx) is released on every exit.
func (n *Node) catchUpMember(ctx context.Context, peerID string) error {
	// 3. Send snapshot
	sendSnapshotCount := 0
send_snapshot:
	if !n.IsLeader() {
		return fmt.Errorf("addMember: stepped down while catching up member %q", peerID)
	}
	if sendSnapshotCount > 5 { // TODO: make 5 configurable
		err := fmt.Errorf("install snapshot is slow and node not able to catchup aborting")
		zerolog.Ctx(ctx).Error().Err(err).Msg("addMember: " + err.Error())
		return err
	}
	sendSnapshotCount++
	res, snapshotMeta, err := n.callInstallSnapshot(ctx, peerID)
	if err != nil {
		if !errors.Is(err, ErrNoSnapshot) {
			zerolog.Ctx(ctx).Error().Err(err).Msg("addMember: failed to send snapshot")
			return err
		}
		firstIdx, err := n.firstIndex(ctx)
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msg("addMember: failed to get first log index")
			return err
		}
		// firstIndex reports 1 for a genuinely fresh node, so this stays a real
		// assertion: no snapshot exists, therefore the log must still start at 1 or
		// entries the new member needs have been compacted away with nothing to
		// replace them.
		if firstIdx != 1 {
			err = fmt.Errorf("snapshot not present and first index is not equal to 1")
			zerolog.Ctx(ctx).Error().Err(err).Msg("addMember: " + err.Error())
			return err
		}
	}

	if !res.Success {
		err = fmt.Errorf("install snapshot unsuccessfull")
		zerolog.Ctx(ctx).Error().Err(err).Msg("addMember: install snapshot unsuccessful")
		return err
	}

	defer n.setCatchingUpIdx(DefaultCatchingUpIdx)
	if snapshotMeta.Index == 0 {
		snapshotMeta = SnapshotMeta{
			Index: 0,
			Term:  0,
		}
	}
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
		if !n.IsLeader() {
			return fmt.Errorf("addMember: stepped down while catching up member %q", peerID)
		}
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
				// TODO: make this more robust as in something like exponential less retries
				sendSnapshotCount = 0
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
}
