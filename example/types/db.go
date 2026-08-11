package types

import (
	"context"
	"errors"
)

// ErrNotFound is returned by RaftDBInterface implementations when a key does not exist.
// Callers check for this sentinel to distinguish "not found" from other DB errors.
var ErrNotFound = errors.New("not found")

type RaftDBInterface interface {
	// Current Term
	SetCurrentTerm(context.Context, uint) error
	GetCurrentTerm(context.Context) (uint, error)

	// VotedFor ("" means no vote)
	SetVotedFor(context.Context, string) error
	GetVotedFor(context.Context) (string, error)

	// Logs
	AppendLogs(context.Context, []*LogEntry) error
	GetLogs(ctx context.Context, startIdx, endIdx *uint) ([]*LogEntry, error)
	GetLogByIndex(ctx context.Context, idx uint) (*LogEntry, error)
	GetLogsByTerm(ctx context.Context, term uint) ([]*LogEntry, error)
	// Removes all the logs from startIdx
	TruncateLogs(ctx context.Context, startIdx uint) error

	// Log metadata
	GetLastLogTerm(context.Context) (uint, error)
	GetLastLogIndex(context.Context) (uint, error)
	GetLastLogEntry(context.Context) (*LogEntry, error)
}
