package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rs/zerolog"
)

// RemoveMember takes peerID out of the cluster configuration. It is the
// counterpart to AddMember and follows the same single-server-change shape
// (Ongaro §4.1): mutate the live configuration, replicate the whole resulting
// membership as one EntryType_Config entry, and wait for it to commit.
//
// peerID may be this node. That case is the interesting one and is handled after
// the commit — see stepDownAfterSelfRemoval.
//
// On any failure before the entry commits, the peer is put back: a half-applied
// removal would leave the live configuration disagreeing with the log.
func (n *Node) RemoveMember(ctx context.Context, peerID string) error {
	// Decide and mutate atomically under clientMu, the same way AddMember does:
	// the leader and membership guards and the removePeer must not interleave with
	// a concurrent membership change, and the config we marshal has to reflect the
	// removal we just made.
	n.clientMu.Lock()

	if n.GetRole() != ServerRole_Leader {
		n.clientMu.Unlock()
		return fmt.Errorf("not the leader: current leader is %q", n.GetLeaderID())
	}

	// Raft allows one membership change at a time. AddMember parks its new server
	// in Staging for the duration, so its presence means a change is still running.
	if n.hasStagingPeer() {
		n.clientMu.Unlock()
		return fmt.Errorf("removeMember: one membership change already in progress")
	}

	removed, present, _ := n.lookupPeer(peerID)
	if !present {
		n.clientMu.Unlock()
		return fmt.Errorf("removeMember: %q is not a member of the cluster", peerID)
	}

	// Removing the last voter leaves a cluster that can never commit anything
	// again, including the entry that would undo this.
	if removed.PeerState == PeerState_Voter && n.voterCount() <= 1 {
		n.clientMu.Unlock()
		return fmt.Errorf("removeMember: %q is the only voter left", peerID)
	}

	n.removePeer(peerID)
	data, err := json.Marshal(n.peersSnapshot())
	n.clientMu.Unlock()

	if err != nil {
		n.addPeer(peerID, removed) // undo the local mutation; nothing was replicated
		zerolog.Ctx(ctx).Error().Err(err).Msg("removeMember: failed to marshal configuration")
		return fmt.Errorf("removeMember: %w", err)
	}

	// Replicate the new configuration and wait for it to commit. Note this is the
	// point where removing OURSELVES starts to pay off: we are already out of
	// configurations.latest, so voterCount and getMajorityMatchIndex have stopped
	// counting us, and the commit is decided by the remaining voters alone —
	// Ongaro §4.2.2, a leader being removed keeps replicating C_new but does not
	// count itself toward it.
	if future, err := n.Propose(ctx, EntryType_Config, data); err != nil {
		n.addPeer(peerID, removed)
		return fmt.Errorf("removeMember: %w", err)
	} else {
		if err := future.Wait(ctx); err != nil {
			n.addPeer(peerID, removed)
			return fmt.Errorf("removeMember: %w", err)
		}
	}

	if peerID == n.GetID() {
		n.stepDownAfterSelfRemoval(ctx)
	} else {
		// Stop replicating to a peer that is no longer a member. Best-effort: if we
		// have already stepped down the goroutine is gone and there is nothing to
		// tell.
		n.notifyMemberRemoved(peerID)
	}

	return nil
}

// stepDownAfterSelfRemoval runs once a configuration removing this node has
// committed. At that point we are no longer a member, so we must not keep
// leading — the only question is how abruptly leadership ends.
//
// We try to hand it over first: TimeoutNow tells the most caught-up remaining
// voter to campaign immediately, which costs one election round-trip instead of a
// full election timeout of nobody being in charge.
//
// If that fails, we step down anyway. Staying leader is not an option on offer:
// the committed configuration says we are not a member, and a node leading a
// cluster it does not belong to is the state this whole operation exists to
// leave. Ongaro §4.2.2 in fact prescribes exactly this — the removed leader steps
// down once C_new commits — which makes the handoff an optimisation on top of the
// baseline rather than a precondition for it. So there is no retry against the
// next-best target: a brief leaderless gap, closed by the ordinary randomized
// election timer, is the documented behaviour and is strictly safer than clinging
// to a role we no longer hold.
func (n *Node) stepDownAfterSelfRemoval(ctx context.Context) {
	if target, ok := n.mostCaughtUpVoter(); ok {
		if err := n.sendTimeoutNow(ctx, target); err != nil {
			zerolog.Ctx(ctx).Warn().Err(err).Msgf("removeMember: leadership handoff to %s failed, stepping down anyway", target)
		} else {
			zerolog.Ctx(ctx).Info().Msgf("removeMember: handed leadership to %s", target)
		}
	} else {
		// Defensive: RemoveMember's last-voter guard means another voter always
		// exists by the time we get here, so this branch is not reachable through
		// that path. It costs one line and covers a future caller that skips the
		// guard.
		zerolog.Ctx(ctx).Warn().Msg("removeMember: no voter left to hand leadership to, stepping down anyway")
	}

	n.stepDownAsLeader()
}

// mostCaughtUpVoter picks the remaining voter with the highest MatchIndex — the
// one that needs the least catching up and is therefore likeliest to win an
// election right now. Self is skipped defensively; by the time this runs we are
// already out of the configuration.
func (n *Node) mostCaughtUpVoter() (string, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	best := ""
	var bestMatch uint
	for id, peer := range n.configurations.latest {
		if id == n.ID || peer.PeerState != PeerState_Voter {
			continue
		}
		if best == "" || peer.MatchIndex > bestMatch {
			best, bestMatch = id, peer.MatchIndex
		}
	}
	return best, best != ""
}

// sendTimeoutNow asks peerID to start an election immediately. A rejected
// transfer (Success false) is an error to the caller: the peer declined, so
// leadership was not handed anywhere.
func (n *Node) sendTimeoutNow(ctx context.Context, peerID string) error {
	currentTerm, err := n.store.GetCurrentTerm(ctx)
	if err != nil {
		return fmt.Errorf("reading current term: %w", err)
	}

	deadlineCtx, cancel := context.WithTimeout(ctx, time.Duration(n.cfg.RPCTimeoutMs)*time.Millisecond)
	defer cancel()

	resp, err := n.transport.TimeoutNow(deadlineCtx, peerID, TimeoutNowArgs{
		Term:     uint64(currentTerm),
		LeaderID: n.GetID(),
	})
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("peer %s declined the leadership transfer", peerID)
	}
	return nil
}

// stepDownAsLeader asks the heartbeat orchestrator to relinquish leadership.
//
// It deliberately does NOT call becomeFollower itself. startSendLogs owns the
// leader lifecycle, and a caller flipping the role behind its back would leave
// every sendLogsPerPeer goroutine replicating on behalf of a node that is no
// longer leader — the zombie-leader shape from JOURNEY.md Bug 3. Signalling
// electionTimeoutCh is how that orchestrator is already told to stand down: it
// cancels the heartbeat context first, then calls becomeFollower exactly once.
//
// The send is non-blocking — see signalElectionTimeout. If the buffer is full a
// step-down is already pending, which is all we wanted.
func (n *Node) stepDownAsLeader() {
	n.signalElectionTimeout()
}
