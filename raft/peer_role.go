package raft

import (
	"context"
	"crypto/rand"
	"math/big"
	"time"

	"github.com/SHREYANSHSINGH14/raft/types"
	"github.com/rs/zerolog"
)

// -------------------------------------------
// Role transition functions
// These functions are called when we want to transition from one role to another role
// They also starts the necessary goroutines for that role like election timeout for follower and send logs for leader
// -------------------------------------------

func (p *Peer) becomeFollower() {
	zerolog.Ctx(p.ctx).Info().Msg("becoming follower")
	p.SetRole(ServerRole_Follower)
	p.startElectionOut(p.ctx)
}

func (p *Peer) becomeCandidate() {
	zerolog.Ctx(p.ctx).Info().Msg("becoming candidate")
	p.SetRole(ServerRole_Candidate)
	p.startElection(p.ctx)
}

func (p *Peer) becomeLeader() {
	zerolog.Ctx(p.ctx).Info().Msg("becoming leader")
	p.SetRole(ServerRole_Leader)
	p.SetLeaderID("")
	p.peerIndexes = make(map[string]PeerIndexes)

	lastIndex, err := p.store.GetLastLogIndex(p.ctx)
	if err != nil {
		zerolog.Ctx(p.ctx).Error().Err(err).Msg("error getting latest log index")
		p.becomeFollower()
		return
	}

	for id := range p.ServerIDRpcUrlMap {
		p.peerIndexes[id] = PeerIndexes{
			nextIndex:  lastIndex + 1,
			matchIndex: 0,
		}
	}
	p.startSendLogs(p.ctx)
}

// -------------------------------------------
// Since being a follower is default role, we only need to start election timeout goroutine when we become follower
// For candidate we need to start election and for leader we need to start sending logs to followers
// find functions for candidate and leader in respective files
// -------------------------------------------

func (p *Peer) startElectionOut(ctx context.Context) {
	go func() {
		duration, err := rand.Int(rand.Reader, big.NewInt(int64(p.cfg.ElectionDurationMs)))
		if err != nil {
			zerolog.Ctx(context.Background()).Error().Err(err).Msg("error getting random number for duration")
			return
		}

		timeOut := time.Duration((duration.Int64() + int64(p.cfg.ElectionMinMs)) * int64(time.Millisecond))
		ticker := time.NewTicker(timeOut)

		for {
			select {
			case <-p.electionTimeoutCh:
				ticker.Reset(timeOut)
				continue
			case <-ticker.C:
				ticker.Stop()
				p.becomeCandidate()
				return
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
}

// waitForQuorum blocks until a majority of peers are reachable over gRPC.
// It is called once at startup before the election timer begins, preventing
// spurious elections during the window when containers are starting up and
// gRPC connections between peers are not yet established.
//
// Strategy: send a RequestVote with term=0 to each peer. Term 0 is always
// rejected by any peer (since any initialized peer has term >= 1), but a
// rejection is still a valid gRPC response — it proves the connection is up.
// A timeout or connection error means the peer is not yet reachable.
//
// The function retries every 500ms until majority responds, then returns.
// If the context is cancelled (e.g. server shutdown), it returns immediately.
//
// Note: this is only meaningful on first startup. When startElectionOut is
// called again after a role transition (becomeFollower), the cluster is already
// running so waitForQuorum returns on the first iteration.

func (p *Peer) waitForQuorum(ctx context.Context) {
	majority := (len(p.ServerIDRpcUrlMap)+1)/2 + 1

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		reachable := 0
		for _, client := range p.ServerIDRpcUrlMap {
			// ping each peer with a real RequestVote — if it responds (even rejection) the connection is up
			rpcCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
			_, err := client.RequestVote(rpcCtx, &types.RequestVoteArgs{
				CandidateId: p.ID,
				Term:        0, // term 0 — always rejected but proves connectivity
			})
			cancel()
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
