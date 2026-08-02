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

	entry, err := n.appendEntry(ctx, entryType, data)
	if err != nil {
		n.clientMu.Unlock()
		return fmt.Errorf("propose: %w", err)
	}
	n.clientMu.Unlock()

	// TODO: we can optimize this by appending the log entry to store before acquiring the lock and then just waiting for commit after acquiring the lock, this way we can reduce the time we are holding the lock and allow other concurrent calls to Propose and HandleAppendEntries and HandleRequestVote to proceed without waiting for the log entry to be appended to store which can be a slow operation, but for simplicity we are doing it in this way for now
	if err := n.waitForCommit(ctx, uint(entry.Index)); err != nil {
		return fmt.Errorf("propose: %w", err)
	}
	return nil
}

// waitForCommit blocks until commitIndex reaches index, the leadership term ends,
// or ctx is cancelled. It returns nil once committed, ctx.Err() if the caller went
// away first, and ErrLeadershipLost if we stopped being leader with the entry
// still uncommitted. Callers decide what to do with the error — a proposer
// surfaces it; a best-effort rollback ignores it. This is the single place the
// commitCond wait loop lives; Propose and every AddMember commit-wait go through it.
func (n *Node) waitForCommit(ctx context.Context, index uint) error {
	// Read before commitMu is taken. getLeaderCloseCh needs mu, and taking mu
	// while holding commitMu would create a lock ordering that exists nowhere
	// else. A local copy is all we need: within a leadership term the channel is
	// only ever closed, never swapped, and a step-down after this read still wakes
	// us via the broadcast in clearLeaderCloseCh.
	leaderCh := n.getLeaderCloseCh()

	n.commitCond.L.Lock()
	defer n.commitCond.L.Unlock()
	for n.commitIndex < index && ctx.Err() == nil && !leadershipEnded(leaderCh) {
		n.commitCond.Wait()
	}

	if n.commitIndex >= index {
		// Committed. This wins over a step-down or a cancelled context that landed
		// in the same wakeup: the entry is committed either way, so reporting a
		// failure would be a lie the caller might act on by retrying.
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return ErrLeadershipLost
}

// leadershipEnded reports whether ch says this node is no longer leading: closed
// (we stepped down) or nil (we were not leader when the wait began). Both mean
// nothing will advance commitIndex on our behalf, so waiting is pointless.
func leadershipEnded(ch <-chan struct{}) bool {
	if ch == nil {
		return true
	}
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// appendEntry builds a LogEntry with the next log index and the current term,
// appends it to the store, and returns it. Callers must hold clientMu.
func (n *Node) appendEntry(ctx context.Context, entryType EntryType, data []byte) (LogEntry, error) {
	lastLogIndex, err := n.store.GetLastLogIndex(ctx)
	if err != nil {
		return LogEntry{}, fmt.Errorf("failed to get last log index: %w", err)
	}

	currentTerm, err := n.store.GetCurrentTerm(ctx)
	if err != nil {
		return LogEntry{}, fmt.Errorf("failed to get current term: %w", err)
	}

	entry := LogEntry{
		Index: uint64(lastLogIndex + 1),
		Term:  uint64(currentTerm),
		Type:  entryType,
		Data:  data,
	}

	if err := n.store.AppendLogs(ctx, []LogEntry{entry}); err != nil {
		return LogEntry{}, fmt.Errorf("failed to append log: %w", err)
	}

	return entry, nil
}
