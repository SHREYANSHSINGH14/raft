package raft

import (
	"context"
	"fmt"
	"strings"

	"github.com/SHREYANSHSINGH14/raft/types"
	"github.com/rs/zerolog"
)

func (p *Server) RequestVote(ctx context.Context, args *types.RequestVoteArgs) (*types.RequestVoteResponse, error) {
	p.raftMu.Lock()
	defer p.raftMu.Unlock()

	if strings.TrimSpace(args.CandidateId) == "" {
		err := fmt.Errorf("candidate id is empty")
		zerolog.Ctx(ctx).Error().Err(err).Msg("candidate id is empty")
		return nil, err
	}

	currentTerm, err := p.store.GetCurrentTerm(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("request vote db err: %s", err.Error())
		return nil, err
	}

	if args.Term < uint64(currentTerm) {
		return &types.RequestVoteResponse{
			Term:        uint64(currentTerm),
			VoteGranted: false,
		}, nil
	}

	// Here we are not keeping check for equal term because even if candidate's term is equal to current term
	// then it would mean this is probably a double request from same candidate or from different candidate but in same term
	// In both cases we can just ignore the request and return false because we have already voted for some candidate in this term
	// that happens at voteFor check below where we check if votedFor is empty or candidateId
	if args.Term > uint64(currentTerm) {
		err := p.store.SetCurrentTerm(ctx, uint(args.Term))
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msgf("request vote db err: %s", err.Error())
			return nil, err
		}

		currentTerm = uint(args.Term)

		err = p.store.SetVotedFor(ctx, "")
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msgf("request vote db err: %s", err.Error())
			return nil, err
		}
	}

	votedFor, err := p.store.GetVotedFor(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("request vote db err: %s", err.Error())
		return nil, err
	}

	if votedFor != "" && votedFor != args.CandidateId {
		return &types.RequestVoteResponse{
			Term:        uint64(currentTerm),
			VoteGranted: false,
		}, nil
	}

	lastLog, err := p.store.GetLastLogEntry(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("request vote db err: %s", err.Error())
		return nil, err
	}

	// Raft determines which of two logs is more up-to-date
	// by comparing the index and term of the last entries in the
	// logs. If the logs have last entries with different terms, then
	// the log with the later term is more up-to-date. If the logs
	// end with the same term, then whichever log is longer is
	// more up-to-date.
	if lastLog != nil {
		if args.LastLogTerm < uint64(lastLog.Term) || (args.LastLogTerm == uint64(lastLog.Term) && args.LastLogIndex < lastLog.Index) {
			return &types.RequestVoteResponse{
				Term:        uint64(currentTerm),
				VoteGranted: false,
			}, nil
		}
	}

	err = p.store.SetVotedFor(ctx, args.CandidateId)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("request vote db err: %s", err.Error())
		return nil, err
	}

	return &types.RequestVoteResponse{
		Term:        uint64(currentTerm),
		VoteGranted: true,
	}, nil
}
