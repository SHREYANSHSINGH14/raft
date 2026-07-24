package raft

import (
	"context"
	"sync"

	"github.com/stretchr/testify/mock"
)

// MockStorage is a mock implementation of Storage using testify.
// Use it to test rpc handlers in isolation — set expectations for each call.
type MockStorage struct {
	mock.Mock
	mu sync.Mutex
}

var _ Storage = &MockStorage{}

func (m *MockStorage) SetCurrentTerm(ctx context.Context, term uint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx, term)
	return args.Error(0)
}

func (m *MockStorage) GetCurrentTerm(ctx context.Context) (uint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx)
	return args.Get(0).(uint), args.Error(1)
}

func (m *MockStorage) SetVotedFor(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx, id)
	return args.Error(0)
}

func (m *MockStorage) GetVotedFor(ctx context.Context) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx)
	return args.String(0), args.Error(1)
}

func (m *MockStorage) SetLastApplied(ctx context.Context, term uint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx, term)
	return args.Error(0)
}

func (m *MockStorage) GetLastApplied(ctx context.Context) (uint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx)
	return args.Get(0).(uint), args.Error(1)
}

func (m *MockStorage) AppendLogs(ctx context.Context, entries []LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx, entries)
	return args.Error(0)
}

func (m *MockStorage) GetLogs(ctx context.Context, start, end *uint) ([]LogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx, start, end)
	val := args.Get(0)
	if val == nil {
		return nil, args.Error(1)
	}
	return val.([]LogEntry), args.Error(1)
}

func (m *MockStorage) GetLogByIndex(ctx context.Context, idx uint) (LogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx, idx)
	return args.Get(0).(LogEntry), args.Error(1)
}

func (m *MockStorage) DeleteLogs(ctx context.Context, fromIdx, toIdx uint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx, fromIdx, toIdx)
	return args.Error(0)
}

func (m *MockStorage) GetLastLogEntry(ctx context.Context) (LogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx)
	return args.Get(0).(LogEntry), args.Error(1)
}

func (m *MockStorage) GetLastLogIndex(ctx context.Context) (uint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx)
	return args.Get(0).(uint), args.Error(1)
}

func (m *MockStorage) GetLastLogTerm(ctx context.Context) (uint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx)
	return args.Get(0).(uint), args.Error(1)
}

func (m *MockStorage) GetFirstLogEntry(ctx context.Context) (LogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx)
	return args.Get(0).(LogEntry), args.Error(1)
}

// MemStorage is a stateful in-memory implementation of Storage used in tests.
// Use it when tests need real state across multiple method calls.
type MemStorage struct {
	mu          sync.RWMutex
	currentTerm uint
	votedFor    string
	lastApplied uint
	logs        []LogEntry // maintained in ascending Index order
}

var _ Storage = &MemStorage{}

func NewMemStorage() *MemStorage {
	return &MemStorage{}
}

func (m *MemStorage) SetCurrentTerm(_ context.Context, term uint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentTerm = term
	return nil
}

func (m *MemStorage) GetCurrentTerm(_ context.Context) (uint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.currentTerm, nil
}

func (m *MemStorage) SetLastApplied(_ context.Context, term uint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastApplied = term
	return nil
}

func (m *MemStorage) GetLastApplied(_ context.Context) (uint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastApplied, nil
}

func (m *MemStorage) SetVotedFor(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.votedFor = id
	return nil
}

func (m *MemStorage) GetVotedFor(_ context.Context) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.votedFor, nil
}

func (m *MemStorage) AppendLogs(_ context.Context, entries []LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(m.logs, entries...)
	return nil
}

func (m *MemStorage) GetLogs(_ context.Context, start, end *uint) ([]LogEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []LogEntry
	for _, e := range m.logs {
		if start != nil && e.Index < uint64(*start) {
			continue
		}
		if end != nil && e.Index >= uint64(*end) {
			break
		}
		result = append(result, e)
	}
	return result, nil
}

func (m *MemStorage) GetLogByIndex(_ context.Context, idx uint) (LogEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for i := len(m.logs) - 1; i >= 0; i-- {
		if uint(m.logs[i].Index) == idx {
			return m.logs[i], nil
		}
	}
	return LogEntry{}, ErrNotFound
}

func (m *MemStorage) DeleteLogs(_ context.Context, fromIdx, toIdx uint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	kept := make([]LogEntry, 0, len(m.logs))
	for _, e := range m.logs {
		idx := uint(e.Index)
		inRange := (fromIdx == 0 || idx >= fromIdx) && (toIdx == 0 || idx <= toIdx)
		if !inRange {
			kept = append(kept, e)
		}
	}
	m.logs = kept
	return nil
}

func (m *MemStorage) GetLastLogEntry(_ context.Context) (LogEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.logs) == 0 {
		return LogEntry{}, nil
	}
	return m.logs[len(m.logs)-1], nil
}

func (m *MemStorage) GetLastLogIndex(_ context.Context) (uint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.logs) == 0 {
		return 0, nil
	}
	return uint(m.logs[len(m.logs)-1].Index), nil
}

func (m *MemStorage) GetLastLogTerm(_ context.Context) (uint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.logs) == 0 {
		return 0, nil
	}
	return uint(m.logs[len(m.logs)-1].Term), nil
}

func (m *MemStorage) GetFirstLogEntry(_ context.Context) (LogEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.logs) == 0 {
		return LogEntry{}, nil
	}
	return m.logs[0], nil
}
