package raft

import (
	"context"
	"fmt"
	"sync"

	"github.com/stretchr/testify/mock"
)

// MockStorage is a mock implementation of Storage using testify.
// Use it to test rpc handlers in isolation — set expectations for each call.
type MockStorage struct {
	mock.Mock

	// ApplyErr / CloseErr drive the batch commit and discard paths — see Apply.
	ApplyErr error
	CloseErr error
	mu       sync.Mutex
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

func (m *MockStorage) GetLastIndex(ctx context.Context) (uint, error) {
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

func (m *MockStorage) GetFirstIndex(ctx context.Context) (uint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx)
	return args.Get(0).(uint), args.Error(1)
}

// NewBatch returns the mock itself, which already satisfies Batch. Staged calls
// therefore land on the same expectations as direct ones, so a test that batches
// needs no extra setup — it asserts on SetCurrentTerm/SetVotedFor/DeleteLogs exactly
// as before, plus Commit. A mock cannot model rollback anyway; use MemStorage when a
// test needs an uncommitted batch to actually leave state untouched.
func (m *MockStorage) NewBatch() Batch {
	return m
}

// Apply and Close are driven by fields rather than expectations. NewBatch returns the
// mock itself, so a staged SetCurrentTerm/SetVotedFor/DeleteLogs has already landed on
// the usual .On(...) expectation by the time Apply runs — making Apply itself mockable
// would force every test that touches a batched write to declare an expectation for a
// call it does not care about. Set ApplyErr/CloseErr to exercise the failure paths.
func (m *MockStorage) Apply(_ Batch) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ApplyErr
}

func (m *MockStorage) Close(_ Batch) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.CloseErr
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

func (m *MemStorage) GetLastIndex(_ context.Context) (uint, error) {
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

func (m *MemStorage) GetFirstIndex(_ context.Context) (uint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.logs) == 0 {
		return 0, nil
	}
	return uint(m.logs[0].Index), nil
}

// memBatch is a real staging batch: mutations are recorded and applied to the parent
// only on Commit, so a batch that is never committed leaves the store untouched. That
// is the property the batch exists for — it turns "revert on failure" into "do not
// commit" — and it is why MemStorage models it rather than applying eagerly.
type memBatch struct {
	s   *MemStorage
	ops []func()
}

// NewBatch stages onto the parent. The batch is not safe for concurrent use; the
// parent's lock is taken once, at Commit.
func (m *MemStorage) NewBatch() Batch {
	return &memBatch{s: m}
}

// Apply runs every staged mutation under one lock hold, so a reader never sees half a
// batch. A batch that is never handed to Apply leaves the store untouched — that is
// the whole point of staging, and what turns "revert on failure" into "do not apply".
func (m *MemStorage) Apply(b Batch) error {
	mb, ok := b.(*memBatch)
	if !ok {
		return fmt.Errorf("MemStorage.Apply: foreign batch type %T", b)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, op := range mb.ops {
		op()
	}
	mb.ops = nil
	return nil
}

// Close discards a batch without applying it. Dropping the staged ops is the whole
// of it — nothing has touched the store yet, which is the property that makes "never
// apply" a valid way to unwind a half-built sequence.
func (m *MemStorage) Close(b Batch) error {
	mb, ok := b.(*memBatch)
	if !ok {
		return fmt.Errorf("MemStorage.Close: foreign batch type %T", b)
	}
	mb.ops = nil
	return nil
}

func (b *memBatch) SetCurrentTerm(_ context.Context, term uint) error {
	b.ops = append(b.ops, func() { b.s.currentTerm = term })
	return nil
}

func (b *memBatch) SetVotedFor(_ context.Context, id string) error {
	b.ops = append(b.ops, func() { b.s.votedFor = id })
	return nil
}

func (b *memBatch) DeleteLogs(_ context.Context, fromIdx, toIdx uint) error {
	b.ops = append(b.ops, func() {
		kept := make([]LogEntry, 0, len(b.s.logs))
		for _, e := range b.s.logs {
			idx := uint(e.Index)
			inRange := (fromIdx == 0 || idx >= fromIdx) && (toIdx == 0 || idx <= toIdx)
			if !inRange {
				kept = append(kept, e)
			}
		}
		b.s.logs = kept
	})
	return nil
}
