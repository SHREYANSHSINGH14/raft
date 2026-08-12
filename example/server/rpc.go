package server

import (
	"context"

	"github.com/SHREYANSHSINGH14/raft"
	"github.com/SHREYANSHSINGH14/raft/example/types"
)

func (s *Server) RequestVote(ctx context.Context, args *types.RequestVoteArgs) (*types.RequestVoteResponse, error) {
	resp, err := s.Node.HandleRequestVote(ctx, raft.RequestVoteArgs{
		Term:         args.Term,
		CandidateID:  args.CandidateId,
		LastLogIndex: args.LastLogIndex,
		LastLogTerm:  args.LastLogTerm,
	})
	if err != nil {
		return nil, err
	}
	return &types.RequestVoteResponse{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
	}, nil
}

// PreVote routes to HandlePreVote, never to HandleRequestVote. The two RPCs carry
// identical fields, which makes the wrong target an easy typo and a silent one:
// HandleRequestVote persists a term and spends a vote, and routing probes there
// would give away exactly what pre-vote exists to protect.
func (s *Server) PreVote(ctx context.Context, args *types.PreVoteArgs) (*types.PreVoteResponse, error) {
	resp, err := s.Node.HandlePreVote(ctx, raft.PreVoteArgs{
		Term:         args.Term,
		CandidateID:  args.CandidateId,
		LastLogIndex: args.LastLogIndex,
		LastLogTerm:  args.LastLogTerm,
	})
	if err != nil {
		return nil, err
	}
	return &types.PreVoteResponse{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
	}, nil
}

func (s *Server) TimeoutNow(ctx context.Context, args *types.TimeoutNowArgs) (*types.TimeoutNowResponse, error) {
	resp, err := s.Node.HandleTimeoutNow(ctx, raft.TimeoutNowArgs{
		Term:     args.Term,
		LeaderID: args.LeaderId,
	})
	if err != nil {
		return nil, err
	}
	return &types.TimeoutNowResponse{
		Term:    resp.Term,
		Success: resp.Success,
	}, nil
}

func (s *Server) AppendEntries(ctx context.Context, args *types.AppendEntriesArgs) (*types.AppendEntriesResponse, error) {
	resp, err := s.Node.HandleAppendEntries(ctx, raft.AppendEntriesArgs{
		Term:         args.Term,
		LeaderID:     args.LeaderId,
		PrevLogIndex: args.PrevLogIndex,
		PrevLogTerm:  args.PrevLogTerm,
		Entries:      types.LogEntriesToRaft(args.Entries),
		LeaderCommit: args.LeaderCommit,
	})
	if err != nil {
		return nil, err
	}
	return &types.AppendEntriesResponse{
		Term:    resp.Term,
		Success: resp.Success,
	}, nil
}
