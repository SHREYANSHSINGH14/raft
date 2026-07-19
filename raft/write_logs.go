package raft

import (
	"context"
	"fmt"
)

// Propose appends data as a new log entry on the leader. Returns an error if
// this node is not the current leader.
// NOTE: This method is thread safe and can be called concurrently by multiple callers

// TODO: return a future struct that is handled by the library and caller waits on that future struct for the result of the propose instead of blocking in Propose method until the log entry is committed, this way we can avoid blocking the caller and allow them to do other work while waiting for the log entry to be committed, but for simplicity we are blocking in Propose method for now
func (n *Node) Propose(ctx context.Context, entryType EntryType, data []byte) error {
	n.clientMu.Lock()
	if n.GetRole() != ServerRole_Leader {
		n.clientMu.Unlock()
		return fmt.Errorf("not the leader: current leader is %q", n.GetLeaderID())
	}

	lastLogIndex, err := n.store.GetLastLogIndex(ctx)
	if err != nil {
		n.clientMu.Unlock()
		return fmt.Errorf("propose: failed to get last log index: %w", err)
	}

	currentTerm, err := n.store.GetCurrentTerm(ctx)
	if err != nil {
		n.clientMu.Unlock()
		return fmt.Errorf("propose: failed to get current term: %w", err)
	}

	entry := LogEntry{
		Index: uint64(lastLogIndex + 1),
		Term:  uint64(currentTerm),
		Type:  entryType,
		Data:  data,
	}

	if err := n.store.AppendLogs(ctx, []LogEntry{entry}); err != nil {
		n.clientMu.Unlock()
		return fmt.Errorf("propose: failed to append log: %w", err)
	}
	n.clientMu.Unlock()

	// TODO: add leaderCloseCh and select on that channel in case leader steps down while waiting for commit so that we can return early instead of waiting for commit indefinitely in that case

	// TODO: we can optimize this by appending the log entry to store before acquiring the lock and then just waiting for commit after acquiring the lock, this way we can reduce the time we are holding the lock and allow other concurrent calls to Propose and HandleAppendEntries and HandleRequestVote to proceed without waiting for the log entry to be appended to store which can be a slow operation, but for simplicity we are doing it in this way for now
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
