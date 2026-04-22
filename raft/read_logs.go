package raft

import (
	"context"
	"log"

	"github.com/SHREYANSHSINGH14/raft/types"
)

func (p *Peer) HandleReadLogs(ctx context.Context, args *types.ReadLogRequest) (*types.ReadLogResponse, error) {
	if args.StartIndex < 0 {
		return &types.ReadLogResponse{
			Entries:  nil,
			LeaderId: p.GetLeaderID(),
			ErrorMsg: "start index cannot be negative",
		}, nil
	}

	startIdx := uint(args.StartIndex)
	logs, err := p.store.GetLogs(ctx, &startIdx, nil)
	if err != nil {
		log.Printf("failed to get logs from index %d: %v", args.StartIndex, err)
		return &types.ReadLogResponse{
			Entries:  nil,
			LeaderId: p.GetLeaderID(),
			ErrorMsg: "failed to get logs: " + err.Error(),
		}, nil
	}

	return &types.ReadLogResponse{
		Entries:  logs,
		LeaderId: p.GetLeaderID(),
		ErrorMsg: "",
	}, nil
}
