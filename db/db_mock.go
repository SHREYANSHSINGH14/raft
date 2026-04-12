package db

import (
	"context"

	"github.com/SHREYANSHSINGH14/raft/types"
	"github.com/stretchr/testify/mock"
)

type MockStore struct {
	mock.Mock
}

var _ types.RaftDBInterface = &MockStore{}

// ── Current Term ─────────────────────────────────────────────────────────────

func (m *MockStore) SetCurrentTerm(ctx context.Context, term uint) error {
	args := m.Called(ctx, term)
	return args.Error(0)
}

func (m *MockStore) GetCurrentTerm(ctx context.Context) (uint, error) {
	args := m.Called(ctx)
	return args.Get(0).(uint), args.Error(1)
}

// ── VotedFor ─────────────────────────────────────────────────────────────────

func (m *MockStore) SetVotedFor(ctx context.Context, nodeID string) error {
	args := m.Called(ctx, nodeID)
	return args.Error(0)
}

func (m *MockStore) GetVotedFor(ctx context.Context) (string, error) {
	args := m.Called(ctx)
	return args.String(0), args.Error(1)
}

// ── Logs ─────────────────────────────────────────────────────────────────────

func (m *MockStore) AppendLogs(ctx context.Context, logs []*types.LogEntry) error {
	args := m.Called(ctx, logs)
	return args.Error(0)
}

func (m *MockStore) GetLogs(ctx context.Context, startIdx, endIdx *uint) ([]*types.LogEntry, error) {
	args := m.Called(ctx, startIdx, endIdx)
	val := args.Get(0)
	if val == nil {
		return nil, args.Error(1)
	}
	return val.([]*types.LogEntry), args.Error(1)
}

func (m *MockStore) GetLogByIndex(ctx context.Context, idx uint) (*types.LogEntry, error) {
	args := m.Called(ctx, idx)
	val := args.Get(0)
	if val == nil {
		return nil, args.Error(1)
	}
	return val.(*types.LogEntry), args.Error(1)
}

func (m *MockStore) GetLogsByTerm(ctx context.Context, term uint) ([]*types.LogEntry, error) {
	args := m.Called(ctx, term)
	val := args.Get(0)
	if val == nil {
		return nil, args.Error(1)
	}
	return val.([]*types.LogEntry), args.Error(1)
}

func (m *MockStore) TruncateLogs(ctx context.Context, startIdx uint) error {
	args := m.Called(ctx, startIdx)
	return args.Error(0)
}

// ── Log Metadata ─────────────────────────────────────────────────────────────

func (m *MockStore) GetLastLogTerm(ctx context.Context) (uint, error) {
	args := m.Called(ctx)
	return args.Get(0).(uint), args.Error(1)
}

func (m *MockStore) GetLastLogIndex(ctx context.Context) (uint, error) {
	args := m.Called(ctx)
	return args.Get(0).(uint), args.Error(1)
}

func (m *MockStore) GetLastLogEntry(ctx context.Context) (*types.LogEntry, error) {
	args := m.Called(ctx)
	val := args.Get(0)
	if val == nil {
		return nil, args.Error(1)
	}
	return val.(*types.LogEntry), args.Error(1)
}
