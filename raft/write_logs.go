package raft

import (
	"context"

	"github.com/SHREYANSHSINGH14/raft/types"
)

func (p *Peer) HandleWriteLogs(ctx context.Context, args *types.WriteLogRequest) (*types.WriteLogResponse, error) {
	if p.Role != ServerRole_Leader {
		return &types.WriteLogResponse{
			Success:  false,
			LeaderId: p.GetLeaderID(),
			ErrorMsg: "not the leader",
		}, nil
	}

	lastLogIndex, err := p.store.GetLastLogIndex(ctx)
	if err != nil {
		return &types.WriteLogResponse{
			Success:  false,
			LeaderId: p.GetLeaderID(),
			ErrorMsg: "failed to get last log index: " + err.Error(),
		}, nil
	}

	currentTerm, err := p.store.GetCurrentTerm(ctx)
	if err != nil {
		return &types.WriteLogResponse{
			Success:  false,
			LeaderId: p.GetLeaderID(),
			ErrorMsg: "failed to get current term: " + err.Error(),
		}, nil
	}

	for i := range args.Entries {
		args.Entries[i].Index = uint64(lastLogIndex) + uint64(i+1)
		args.Entries[i].Term = uint64(currentTerm)
	}
	err = p.store.AppendLogs(ctx, args.Entries)
	if err != nil {
		return &types.WriteLogResponse{
			Success:  false,
			LeaderId: p.GetLeaderID(),
			ErrorMsg: "failed to append logs: " + err.Error(),
		}, nil
	}

	return &types.WriteLogResponse{
		Success:  true,
		LeaderId: p.GetLeaderID(),
		ErrorMsg: "",
	}, nil
}
