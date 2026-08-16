package raft

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"math/big"
	"time"

	"github.com/rs/zerolog"
)

// -------------------------------------------
// Role transition functions
// These functions are called when we want to transition from one role to another role
// They also starts the necessary goroutines for that role like election timeout for follower and send logs for leader
// -------------------------------------------

// becomeFollower steps down to follower. reason says what caused it — a role change
// with no cause in the log is unreadable after the fact, because the interesting
// question is never "what did it become" but "what made it".
func (n *Node) becomeFollower(reason string) {
	zerolog.Ctx(n.ctx).Info().
		Str("from", string(n.GetRole())).
		Str("reason", reason).
		Msg("becoming follower")
	// End the leadership term before anything else: any Propose still waiting on
	// commit has to fail now rather than block on entries a new leader may never
	// commit. A no-op when we were not leader (candidate losing, startup).
	n.clearLeaderCloseCh()
	// The fan-out channels belong to the leadership term that just ended; a later
	// becomeLeader builds a fresh set.
	n.clearMemberChannels()
	// Every future registered this term captured the channel clearLeaderCloseCh just
	// closed, so dropping the list is enough — the waiters are already awake and
	// deciding for themselves. Must stay after clearLeaderCloseCh, or they are
	// dropped while still asleep.
	n.clearFutureList()
	n.SetRole(ServerRole_Follower)
	n.startElectionOut(n.ctx)
}

func (n *Node) becomeCandidate(reason string) {
	zerolog.Ctx(n.ctx).Info().
		Str("from", string(n.GetRole())).
		Str("reason", reason).
		Msg("becoming candidate")
	n.SetRole(ServerRole_Candidate)
	n.SetLeaderID("")
	n.startElection(n.ctx)
}

// initLeaderTermState seeds everything a fresh leadership term needs: per-peer
// replication indexes, a stop channel for each peer the heartbeat will replicate
// to, and the member-added channel.
//
// Split out of becomeLeader so it can be tested on its own. becomeLeader ends by
// calling startSendLogs, which blocks for the rest of the term AND backfills any
// missing stop channel through ensureMemberRemovedCh — so a test that went through
// becomeLeader could not tell correct bookkeeping here from bookkeeping that
// startSendLogs quietly repaired.
func (n *Node) initLeaderTermState(lastIndex uint) {
	// Seeded before mu is taken, not inside the hold below: futureList is guarded by
	// commitMu, and holding mu across it would create a mu -> commitMu ordering that
	// exists nowhere else in this codebase.
	n.initFutureList()

	n.mu.Lock()
	defer n.mu.Unlock()

	// The map is created ONCE, before the loop — creating it inside would reset it
	// on every iteration and leave only the last peer with a stop channel.
	n.memberRemovedCh = make(map[string]chan struct{})
	for id, peer := range n.configurations.latest {
		// Replication bookkeeping is per-peer; our own entry has no NextIndex to
		// seed, and getMajorityMatchIndex substitutes our real last index for it.
		if id == n.ID {
			continue
		}
		peer.NextIndex = lastIndex + 1
		peer.MatchIndex = 0
		n.configurations.latest[id] = peer
		// Staging peers are driven by AddMember's catch-up, not the heartbeat
		// fan-out, so they get no goroutine and need no way to stop one.
		if peer.PeerState != PeerState_Staging {
			n.memberRemovedCh[id] = make(chan struct{}, 1)
		}
	}
	n.memberAddedCh = make(chan string, 1)
}

func (n *Node) becomeLeader(reason string) {
	zerolog.Ctx(n.ctx).Info().
		Str("from", string(n.GetRole())).
		Str("reason", reason).
		Msg("becoming leader")

	lastIndex, err := n.store.GetLastIndex(n.ctx)
	if err != nil {
		zerolog.Ctx(n.ctx).Error().Err(err).Msg("error getting latest log index")
		n.becomeFollower("becomeLeader: could not read last log index")
		return
	}

	n.initLeaderTermState(lastIndex)

	n.clientMu.Lock()
	_, err = n.appendEntry(n.ctx, EntryType_NoOp, nil)
	if err != nil {
		zerolog.Ctx(n.ctx).Error().Err(err).Msg("error appending no-op log entry")
		n.clientMu.Unlock()
		n.becomeFollower("becomeLeader: could not initialise leadership state")
		return
	}
	n.clientMu.Unlock()

	// A staging peer at the start of a term belongs to some previous leader's
	// AddMember that never finished: the catch-up goroutine died with that leader, so
	// nothing left alive will ever promote it. Abort the addition the way AddMember's
	// own rollback does — append the configuration without it.
	//
	// One pass over the snapshot answers both "is there one" and "which one";
	// hasStagingPeer would just scan the same map a second time. The guard matters:
	// with an empty id, removePeer deletes nothing and we would append a config entry
	// identical to the live one, bumping latestIndex for no change at all.
	stagingID := ""
	for id, peer := range n.peersSnapshot() {
		if peer.PeerState == PeerState_Staging {
			stagingID = id
			break
		}
	}
	if stagingID != "" {
		n.clientMu.Lock()
		n.removePeer(stagingID)
		data, mErr := json.Marshal(n.peersSnapshot())
		if mErr != nil {
			zerolog.Ctx(n.ctx).Error().Err(mErr).Msg("becomeLeader: failed to marshal config")
			n.clientMu.Unlock()
			n.becomeFollower("becomeLeader: staging cleanup could not marshal config")
			return
		}

		// Appended, not waited on. Nothing is replicating yet — startSendLogs is still
		// below us — so a commit-wait here could never be satisfied. If the entry dies
		// with us before committing, rollbackLatestIfTruncated puts the staging peer
		// back and the next leader runs this same cleanup.
		if _, pErr := n.appendEntry(n.ctx, EntryType_Config, data); pErr != nil {
			zerolog.Ctx(n.ctx).Warn().Err(pErr).Msg("becomeLeader: staging peer cleanup failed to append; it may remain")
			n.clientMu.Unlock()
			n.becomeFollower("becomeLeader: staging cleanup append failed")
			return
		}
		n.clientMu.Unlock()
	}

	// Open this term's leadership channel BEFORE the role flips, and flip the role
	// only once every line above has run. Propose gates on role == Leader, and
	// newFuture captures leaderCloseCh — reading a nil one as "not leading" — so
	// setting the role any earlier leaves a window where a proposal is accepted and
	// then either fails instantly with ErrLeadershipLost or registers a future into a
	// list initLeaderTermState is about to replace. In this order, role == Leader
	// always implies a live leaderCloseCh and a fully seeded term.
	n.setLeaderCloseCh()
	n.SetRole(ServerRole_Leader)
	n.SetLeaderID("")

	n.startSendLogs(n.ctx)
}

// -------------------------------------------
// Since being a follower is default role, we only need to start election timeout goroutine when we become follower
// For candidate we need to start election and for leader we need to start sending logs to followers
// find functions for candidate and leader in respective files
// -------------------------------------------

func (n *Node) startElectionOut(ctx context.Context) {
	go func() {
		duration, err := rand.Int(rand.Reader, big.NewInt(int64(n.cfg.ElectionMaxMs-n.cfg.ElectionMinMs)))
		if err != nil {
			zerolog.Ctx(context.Background()).Error().Err(err).Msg("error getting random number for duration")
			return
		}

		timeOut := time.Duration((duration.Int64() + int64(n.cfg.ElectionMinMs)) * int64(time.Millisecond))
		ticker := time.NewTicker(timeOut)

		for {
			select {
			case <-n.electionTimeoutCh:
				ticker.Reset(timeOut)
				continue
			case <-n.timeoutNowCh:
				// TimeoutNow: the leader is handing leadership to us, so skip the
				// rest of the timer and campaign now. Identical to the ticker case
				// on purpose — the point of the transfer is to reach the same
				// place sooner, not to take a different path into the election.
				ticker.Stop()
				n.becomeCandidate("TimeoutNow from the leader")
				return
			case <-ticker.C:
				zerolog.Ctx(ctx).Debug().
					Dur("timeout", timeOut).
					Msg("election timer fired: no contact from a leader")
				ticker.Stop()
				n.becomeCandidate("election timeout elapsed")
				return
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
}

// waitForQuorum blocks until a majority of peers are reachable via the Transport.
// It is called once at startup before the election timer begins, preventing
// spurious elections during the window when containers are starting up and
// connections between peers are not yet established.
//
// Strategy: send a RequestVote with term=0 to each peer. Term 0 is always
// rejected by any peer (since any initialized peer has term >= 1), but a
// rejection is still a valid response — it proves the connection is up.
// A transport error means the peer is not yet reachable.
//
// The function retries every 500ms until majority responds, then returns.
// If the context is cancelled (e.g. server shutdown), it returns immediately.
//
// Note: this is only meaningful on first startup. When startElectionOut is
// called again after a role transition (becomeFollower), the cluster is already
// running so waitForQuorum returns on the first iteration.

func (n *Node) waitForQuorum(ctx context.Context) {
	// Only voters form the quorum an election needs, so wait for a majority of
	// voters to be reachable — a Staging or NonVoter peer being down must not hold
	// up startup. Membership is fixed here (AddMember can't run before we've become
	// leader), so it is safe to read the voter set once.
	// voterIDs is who we ping (peers); voterCount is what the majority is taken
	// over (peers + self). reachable counts responding peers only and is compared
	// against the full-cluster majority, which is deliberately a touch strict —
	// unchanged from before, just no longer relying on majoritySize's implicit +1.
	voterIDs := n.voterPeerIDs()

	if len(voterIDs) < 1 {
		return
	} else if len(voterIDs) == 1 {
		if voterIDs[0] == n.GetID() {
			return
		}
	}

	majority := majoritySize(n.voterCount())

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		reachable := 0
		for _, peerID := range voterIDs {
			// ping each peer with a real RequestVote — if it responds (even rejection) the connection is up
			_, err := n.transport.RequestVote(ctx, peerID, RequestVoteArgs{
				CandidateID: n.ID,
				Term:        0, // term 0 — always rejected but proves connectivity
			})
			if err == nil {
				reachable++
			}
		}

		if reachable >= majority {
			zerolog.Ctx(ctx).Info().
				Int("reachable", reachable).
				Int("majority", majority).
				Msg("quorum reachable, starting election timer")
			return
		}

		zerolog.Ctx(ctx).Debug().
			Int("reachable", reachable).
			Int("majority", majority).
			Msg("waiting for quorum...")

		time.Sleep(500 * time.Millisecond)
	}
}
