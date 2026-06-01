package raft

import (
	"context"
	"fmt"
)

// Propose appends data as a new log entry on the leader. Returns an error if
// this node is not the current leader.
func (n *Node) Propose(ctx context.Context, data []byte) error {
	if n.GetRole() != ServerRole_Leader {
		return fmt.Errorf("not the leader: current leader is %q", n.GetLeaderID())
	}

	lastLogIndex, err := n.store.GetLastLogIndex(ctx)
	if err != nil {
		return fmt.Errorf("propose: failed to get last log index: %w", err)
	}

	currentTerm, err := n.store.GetCurrentTerm(ctx)
	if err != nil {
		return fmt.Errorf("propose: failed to get current term: %w", err)
	}

	entry := LogEntry{
		Index: uint64(lastLogIndex + 1),
		Term:  uint64(currentTerm),
		Data:  data,
	}

	if err := n.store.AppendLogs(ctx, []LogEntry{entry}); err != nil {
		return fmt.Errorf("propose: failed to append log: %w", err)
	}

	n.commitCond.L.Lock()
	for n.commitIndex < uint(entry.Index) && ctx.Err() == nil {
		n.commitCond.Wait()
	}
	if ctx.Err() != nil {
		n.commitCond.L.Unlock()
		return fmt.Errorf("propose: context cancelled before commit")
	}
	n.commitCond.L.Unlock()
	return nil
}
