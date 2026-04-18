package server

import (
	"context"

	"github.com/SHREYANSHSINGH14/raft/types"
)

func (s *Server) RequestVote(ctx context.Context, args *types.RequestVoteArgs) (*types.RequestVoteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.Peer.HandleRequestVote(ctx, args)
}

func (s *Server) AppendEntries(ctx context.Context, args *types.AppendEntriesArgs) (*types.AppendEntriesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.Peer.HandleAppendEntries(ctx, args)
}
