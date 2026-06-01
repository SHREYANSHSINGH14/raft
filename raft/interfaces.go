package raft

import "context"

// Transport is implemented by the caller. The library calls out to it when it
// needs to send RPCs to peers. The caller owns all network concerns — addresses,
// connection pooling, retries, and timeouts.
type Transport interface {
	AppendEntries(peerID string, args AppendEntriesArgs) (AppendEntriesResponse, error)
	RequestVote(peerID string, args RequestVoteArgs) (RequestVoteResponse, error)
}

// StateMachine is implemented by the caller. The library calls Apply after a
// log entry reaches commit index and is ready to be applied to the caller's state.
type StateMachine interface {
	// Implementation has to be idempotent and durable otherwise it can diverge
	// lastApplied index
	Apply(entries []LogEntry) error
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
	GetLogs(ctx context.Context, start, end *uint) ([]LogEntry, error)
	GetLogByIndex(ctx context.Context, idx uint) (LogEntry, error)
	TruncateLogs(ctx context.Context, fromIdx uint) error

	GetLastLogEntry(ctx context.Context) (LogEntry, error)
	GetLastLogIndex(ctx context.Context) (uint, error)
	GetLastLogTerm(ctx context.Context) (uint, error)
}
