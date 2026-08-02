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
	// PreVote sends the pre-vote probe that precedes a real election. It is a
	// separate RPC from RequestVote because it must not be allowed to change any
	// persistent state on the peer — see HandlePreVote.
	PreVote(ctx context.Context, peerID string, args PreVoteArgs) (PreVoteResponse, error)
	// TimeoutNow hands leadership to peerID: it tells that peer to campaign
	// immediately rather than wait out its election timer. See HandleTimeoutNow.
	TimeoutNow(ctx context.Context, peerID string, args TimeoutNowArgs) (TimeoutNowResponse, error)
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
	// GetLogs returns every log entry whose index lies in the half-open range
	// [start, end): start is inclusive, end is exclusive. Bounds are pointers
	// (rather than a 0 sentinel like DeleteLogs) so a nil bound means "no bound
	// on this side". Note the asymmetry with DeleteLogs, whose range is fully
	// inclusive — here the end is exclusive. So:
	//   - GetLogs(ctx, &from, nil)  returns the suffix index >= from.
	//   - GetLogs(ctx, nil, &to)    returns the prefix index <  to.
	//   - GetLogs(ctx, nil, nil)    returns all log entries.
	// Entries must be returned in ascending Index order.
	GetLogs(ctx context.Context, start, end *uint) ([]LogEntry, error)
	GetLogByIndex(ctx context.Context, idx uint) (LogEntry, error)
	// DeleteLogs removes every log entry whose index lies in the inclusive
	// range [fromIdx, toIdx]. A bound of 0 is open-ended: because valid log
	// indices start at 1, 0 is never a real entry and instead means "no bound
	// on this side". So:
	//   - DeleteLogs(ctx, fromIdx, 0)  removes the suffix index >= fromIdx
	//     (conflict resolution — the old TruncateLogs).
	//   - DeleteLogs(ctx, 0, toIdx)    removes the prefix index <= toIdx
	//     (compaction after snapshotting — the old CompactLogs).
	//   - DeleteLogs(ctx, 0, 0)        removes all log entries.
	DeleteLogs(ctx context.Context, fromIdx, toIdx uint) error

	GetLastLogEntry(ctx context.Context) (LogEntry, error)

	// Note: returns 0 and error nil when no logs are present
	GetLastLogIndex(ctx context.Context) (uint, error)
	// Note: returns 0 and error nil when no logs are present
	GetLastLogTerm(ctx context.Context) (uint, error)

	// GetFirstLogEntry returns the first log entry in the log i.e. the log entry with the smallest
	// index. This is used to determine if a snapshot is needed when a follower's nextIndex is less than the
	// first log entry index. In that case, the follower needs to install a snapshot to catch up.
	GetFirstLogEntry(ctx context.Context) (LogEntry, error)
}
