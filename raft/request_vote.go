package raft

import (
	"context"
	"fmt"
	"strings"

	"github.com/rs/zerolog"
)

func (n *Node) HandleRequestVote(ctx context.Context, args RequestVoteArgs) (RequestVoteResponse, error) {
	if strings.TrimSpace(args.CandidateID) == "" {
		err := fmt.Errorf("candidate id is empty")
		zerolog.Ctx(ctx).Error().Err(err).Msg("candidate id is empty")
		return RequestVoteResponse{}, err
	}

	currentTerm, err := n.store.GetCurrentTerm(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("request vote db err: %s", err.Error())
		return RequestVoteResponse{}, err
	}

	if args.Term < uint64(currentTerm) {
		return RequestVoteResponse{
			Term:        uint64(currentTerm),
			VoteGranted: false,
		}, nil
	}

	// Here we are not keeping check for equal term because even if candidate's term is equal to current term
	// then it would mean this is probably a double request from same candidate or from different candidate but in same term
	// In both cases we can just ignore the request and return false because we have already voted for some candidate in this term
	// that happens at voteFor check below where we check if votedFor is empty or candidateId
	if args.Term > uint64(currentTerm) {
		err := n.store.SetCurrentTerm(ctx, uint(args.Term))
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msgf("request vote db err: %s", err.Error())
			return RequestVoteResponse{}, err
		}

		currentTerm = uint(args.Term)

		err = n.store.SetVotedFor(ctx, "")
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msgf("request vote db err: %s", err.Error())
			return RequestVoteResponse{}, err
		}
	}

	votedFor, err := n.store.GetVotedFor(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("request vote db err: %s", err.Error())
		return RequestVoteResponse{}, err
	}

	if votedFor != "" && votedFor != args.CandidateID {
		return RequestVoteResponse{
			Term:        uint64(currentTerm),
			VoteGranted: false,
		}, nil
	}

	lastLog, err := n.store.GetLastLogEntry(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("request vote db err: %s", err.Error())
		return RequestVoteResponse{}, err
	}

	// Raft determines which of two logs is more up-to-date
	// by comparing the index and term of the last entries in the
	// logs. If the logs have last entries with different terms, then
	// the log with the later term is more up-to-date. If the logs
	// end with the same term, then whichever log is longer is
	// more up-to-date.
	if lastLog.Index > 0 {
		if args.LastLogTerm < lastLog.Term || (args.LastLogTerm == lastLog.Term && args.LastLogIndex < lastLog.Index) {
			return RequestVoteResponse{
				Term:        uint64(currentTerm),
				VoteGranted: false,
			}, nil
		}
	}

	err = n.store.SetVotedFor(ctx, args.CandidateID)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("request vote db err: %s", err.Error())
		return RequestVoteResponse{}, err
	}

	// reset election timeout because we have voted for a candidate in current term so we can be sure that there is an active candidate
	// in current term and we can reset our election timeout to avoid unnecessary elections until we hear from the leader of this term or
	// until election timeout happens again in which case we will start new election for next term
	n.electionTimeoutCh <- struct{}{}

	return RequestVoteResponse{
		Term:        uint64(currentTerm),
		VoteGranted: true,
	}, nil
}
