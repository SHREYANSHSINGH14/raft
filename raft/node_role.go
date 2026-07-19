package raft

import (
	"context"
	"crypto/rand"
	"math/big"
	"time"

	"github.com/rs/zerolog"
)

// -------------------------------------------
// Role transition functions
// These functions are called when we want to transition from one role to another role
// They also starts the necessary goroutines for that role like election timeout for follower and send logs for leader
// -------------------------------------------

func (n *Node) becomeFollower() {
	zerolog.Ctx(n.ctx).Info().Msg("becoming follower")
	n.SetRole(ServerRole_Follower)
	n.startElectionOut(n.ctx)
}

func (n *Node) becomeCandidate() {
	zerolog.Ctx(n.ctx).Info().Msg("becoming candidate")
	n.SetRole(ServerRole_Candidate)
	n.startElection(n.ctx)
}

func (n *Node) becomeLeader() {
	zerolog.Ctx(n.ctx).Info().Msg("becoming leader")
	n.SetRole(ServerRole_Leader)
	n.SetLeaderID("")

	lastIndex, err := n.store.GetLastLogIndex(n.ctx)
	if err != nil {
		zerolog.Ctx(n.ctx).Error().Err(err).Msg("error getting latest log index")
		n.becomeFollower()
		return
	}

	n.mu.Lock()
	for id, peer := range n.cfg.Peers {
		peer.NextIndex = lastIndex + 1
		peer.MatchIndex = 0
		n.cfg.Peers[id] = peer
	}
	n.mu.Unlock()

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
			case <-ticker.C:
				ticker.Stop()
				n.becomeCandidate()
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
	majority := (len(n.cfg.Peers)+1)/2 + 1

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		reachable := 0
		for _, peerID := range n.peerIDs() {
			// ping each peer with a real RequestVote — if it responds (even rejection) the connection is up
			_, err := n.transport.RequestVote(peerID, RequestVoteArgs{
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
