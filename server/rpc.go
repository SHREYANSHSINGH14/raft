package server

import (
	"context"

	"github.com/SHREYANSHSINGH14/raft/raft"
	"github.com/SHREYANSHSINGH14/raft/types"
)

func (s *Server) RequestVote(ctx context.Context, args *types.RequestVoteArgs) (*types.RequestVoteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

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

func (s *Server) AppendEntries(ctx context.Context, args *types.AppendEntriesArgs) (*types.AppendEntriesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries := make([]raft.LogEntry, len(args.Entries))
	for i, e := range args.Entries {
		entries[i] = raft.LogEntry{
			Index: e.Index,
			Term:  e.Term,
			Data:  e.Data,
		}
	}

	resp, err := s.Node.HandleAppendEntries(ctx, raft.AppendEntriesArgs{
		Term:         args.Term,
		LeaderID:     args.LeaderId,
		PrevLogIndex: args.PrevLogIndex,
		PrevLogTerm:  args.PrevLogTerm,
		Entries:      entries,
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
