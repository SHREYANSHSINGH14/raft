package raft

import (
	"context"
	"log"
)

// GetLogs returns log entries starting at startIndex. Intended for debug use.
func (n *Node) GetLogs(ctx context.Context, startIndex uint64) ([]LogEntry, error) {
	if startIndex < 0 {
		return nil, nil
	}

	startIdx := uint(startIndex)
	logs, err := n.store.GetLogs(ctx, &startIdx, nil)
	if err != nil {
		log.Printf("failed to get logs from index %d: %v", startIndex, err)
		return nil, err
	}

	return logs, nil
}
