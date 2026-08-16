package raft

import (
	"context"
	"fmt"
)

// Propose appends data as a new log entry on the leader. It returns an error if
// this node is not the current leader.
//
// It returns as soon as the entry is in the log — it does NOT wait for commit.
// The returned Future is how the caller waits: Future.Wait blocks until the entry
// commits, the leadership term ends, or the caller's context is cancelled. A
// caller that does not care can ignore it; the entry replicates either way.
//
// The zero Future returned alongside an error is not a live one — waiting on it
// reports ErrLeadershipLost rather than blocking forever.
//
// NOTE: This method is thread safe and can be called concurrently by multiple callers
func (n *Node) Propose(ctx context.Context, entryType EntryType, data []byte) (Future, error) {
	n.clientMu.Lock()
	if !n.IsLeader() {
		n.clientMu.Unlock()
		return Future{}, fmt.Errorf("not the leader: current leader is %q", n.GetLeaderID())
	}

	// Admission before the append, never after: a rejection that follows a durable
	// entry tells the caller the proposal failed while the entry goes on to commit.
	// See admitProposal.
	if err := n.admitProposal(entryType); err != nil {
		n.clientMu.Unlock()
		return Future{}, fmt.Errorf("propose: %w", err)
	}

	entry, err := n.appendEntry(ctx, entryType, data)
	if err != nil {
		n.clientMu.Unlock()
		return Future{}, fmt.Errorf("propose: %w", err)
	}

	// Registered under the same clientMu hold as the append, deliberately: futureList
	// is drained as a prefix sorted by index, and append order only equals index order
	// while the two happen together. See newFuture.
	//
	// TODO: clientMu is held across a disk write, so a slow store blocks every other
	// caller-facing entry point (Propose, HandleAppendEntries, HandleRequestVote).
	// Fixing that means giving out log indexes without holding the lock for the write,
	// which is a bigger change than it looks — the index handed out has to stay in
	// order with futureList. Left as is for now.
	future := n.newFuture(entry.Index, make(chan error, 1))
	n.clientMu.Unlock()

	return *future, nil
}

// appendEntry builds a LogEntry with the next log index and the current term,
// appends it to the store, and returns it. Callers must hold clientMu.
func (n *Node) appendEntry(ctx context.Context, entryType EntryType, data []byte) (LogEntry, error) {
	lastLogIndex, err := n.store.GetLastIndex(ctx)
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

	// Record which log index produced the live configuration. The follower does
	// this in processConfigurationLogEntry; the leader has to do it here, because
	// it mutates configurations.latest directly (addPeer/removePeer/SetPeerState)
	// and would otherwise leave latestIndex at 0 for its whole life — making
	// "has the entry behind latest committed yet?" answer yes to everything.
	//
	// latest itself is already correct: AddMember/RemoveMember mutate it under
	// clientMu before calling Propose, and we still hold clientMu here.
	if entryType == EntryType_Config {
		n.setLatestConfiguration(n.peersSnapshot(), entry.Index)
	}

	return entry, nil
}
