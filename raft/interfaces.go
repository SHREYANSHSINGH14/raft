package raft

import (
	"context"
	"io"
)

// Transport is implemented by the caller. The library calls out to it when it
// needs to send RPCs to peers. The caller owns all network concerns — addresses,
// connection pooling, retries, and timeouts.
type Transport interface {
	AppendEntries(ctx context.Context, peerID string, args AppendEntriesArgs) (AppendEntriesResponse, error)
	RequestVote(ctx context.Context, peerID string, args RequestVoteArgs) (RequestVoteResponse, error)
	InstallSnapshot(ctx context.Context, peerID string, args InstallSnapshotArgs) (InstallSnapshotResponse, error)
}

// StateMachine is implemented by the caller. The library calls Apply after a
// log entry reaches commit index and is ready to be applied to the caller's state.
type StateMachine interface {
	// Implementation has to be idempotent and durable otherwise it can diverge
	// lastApplied index
	Apply(ctx context.Context, entries []LogEntry) error
	Snapshot(ctx context.Context) (Snapshot, error)
	Restore(ctx context.Context, snapshot io.ReadCloser) error
}

type Snapshot interface {
	Persist(ctx context.Context, writer io.Writer) error
	Release() error
}

// Storage is implemented by the caller. The library calls it for all persistence
// operations — term, votedFor, and log entries.
type Storage interface {
	SetCurrentTerm(ctx context.Context, term uint) error
	GetCurrentTerm(ctx context.Context) (uint, error)

	SetVotedFor(ctx context.Context, id string) error
	GetVotedFor(ctx context.Context) (string, error)

	SetLastApplied(ctx context.Context, term uint) error
	GetLastApplied(ctx context.Context) (uint, error)

	AppendLogs(ctx context.Context, entries []LogEntry) error
	// GetLogs must return entries in ascending Index order.
	GetLogs(ctx context.Context, start, end *uint) ([]LogEntry, error)
	GetLogByIndex(ctx context.Context, idx uint) (LogEntry, error)
	// TruncateLogs removes all log entries with index >= fromIdx (used for conflict resolution).
	TruncateLogs(ctx context.Context, fromIdx uint) error
	// CompactLogs removes all log entries with index <= upToIdx (used after snapshotting).
	CompactLogs(ctx context.Context, upToIdx uint) error

	GetLastLogEntry(ctx context.Context) (LogEntry, error)
	GetLastLogIndex(ctx context.Context) (uint, error)
	GetLastLogTerm(ctx context.Context) (uint, error)

	GetFirstLogEntry(ctx context.Context) (LogEntry, error)
}
