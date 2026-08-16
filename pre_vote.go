package raft

import (
	"context"
	"fmt"
	"strings"

	"github.com/rs/zerolog"
)

// HandlePreVote answers the pre-vote probe a candidate sends before it commits
// to an election (Ongaro §9.6). It answers one question: "if I ran for election
// at this term, would you vote for me?" — and answers it *without changing any
// state*.
//
// That side-effect freedom is the entire point of the RPC, and it is what makes
// this function different from HandleRequestVote despite the near-identical
// checks. HandlePreVote must never:
//
//   - persist a term via SetCurrentTerm — a partitioned node that keeps timing
//     out is exactly the disruption pre-vote exists to contain. If its probes
//     could raise our term, it would still depose a healthy leader on rejoin,
//     and pre-vote would buy nothing.
//   - write votedFor — nothing is being promised here, so nothing is spent.
//     The real RequestVote that follows still gets a free vote in that term.
//   - signal electionTimeoutCh — a probe is not evidence of a live candidate,
//     and resetting our timer on one would let a disruptive node hold the
//     cluster in follower state indefinitely.
//
// NOTE: This method is thread safe and can be called concurrently by multiple
// callers. It takes clientMu for the same reason HandleRequestVote does: the
// checks below are a check-then-act over term, configuration and the log, and
// serializing them here means callers like server/rpc.go need no lock of their
// own.
func (n *Node) HandlePreVote(ctx context.Context, args PreVoteArgs) (PreVoteResponse, error) {
	n.clientMu.Lock()
	defer n.clientMu.Unlock()

	if strings.TrimSpace(args.CandidateID) == "" {
		err := fmt.Errorf("candidate id is empty")
		zerolog.Ctx(ctx).Error().Err(err).Msg("candidate id is empty")
		return PreVoteResponse{}, err
	}

	currentTerm, err := n.store.GetCurrentTerm(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("pre vote db err: %s", err.Error())
		return PreVoteResponse{}, err
	}

	respTerm := uint64(currentTerm)
	if args.Term > uint64(currentTerm) {
		respTerm = args.Term
	}

	reject := func() (PreVoteResponse, error) {
		return PreVoteResponse{Term: respTerm, VoteGranted: false}, nil
	}

	// A cluster that has no configuration yet is bootstrapping — there is nobody
	// to recognise the candidate as a member, so membership can't be a reason to
	// refuse. Both membership checks below are skipped in that case.
	peer, inConfig, configSize := n.lookupPeer(args.CandidateID)

	if configSize > 0 && !inConfig {
		zerolog.Ctx(ctx).Warn().Msgf("rejecting pre vote from %s: not in the latest configuration", args.CandidateID)
		return reject()
	}

	// A node that already believes in a leader refuses to encourage a challenger.
	// This is the leader-sticky half of pre-vote: without it, a node whose own
	// election timer is short would happily pre-vote for anyone and the probe
	// would succeed even though the cluster is healthy.
	if leaderID := n.GetLeaderID(); leaderID != "" && leaderID != args.CandidateID {
		zerolog.Ctx(ctx).Warn().Msgf("rejecting pre vote from %s: we already have leader %s", args.CandidateID, leaderID)
		return reject()
	}

	// A candidate probing a term we have already passed cannot win it.
	if args.Term < uint64(currentTerm) {
		return reject()
	}

	// A peer that is in the configuration but is not a Voter (Staging, still
	// catching up, or NonVoter) can never win an election, so its probe is
	// refused regardless of how good its log is. This is the case a node
	// demoted from Voter to NonVoter would otherwise slip through.
	if configSize > 0 && peer.PeerState != PeerState_Voter {
		zerolog.Ctx(ctx).Warn().Msgf("rejecting pre vote from %s: peer is not a voter", args.CandidateID)
		return reject()
	}

	// Index and term come from two calls, not one entry: after compaction the last
	// index can live in the snapshot rather than the log, and GetLogByIndex would not
	// find it. lastIndex applies the snapshot fallback, logTermAt resolves the term
	// from whichever of the two holds it.
	lastLogIndex, err := n.lastIndex(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("pre vote db err: %s", err.Error())
		return PreVoteResponse{}, err
	}

	lastLogTerm, _, err := n.logTermAt(ctx, uint64(lastLogIndex))
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("pre vote db err: %s", err.Error())
		return PreVoteResponse{}, err
	}

	// Same up-to-date test as the real vote: the log with the later last term
	// wins; on equal terms the longer log wins. An empty log (Index 0) can lose
	// to nobody, so the comparison is skipped.
	if lastLogIndex > 0 {
		if args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < uint64(lastLogIndex)) {
			return reject()
		}
	}

	return PreVoteResponse{
		Term:        respTerm,
		VoteGranted: true,
	}, nil
}
