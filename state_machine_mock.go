package raft

import (
	"context"
	"io"
	"sync"

	"github.com/stretchr/testify/mock"
)

// MockStateMachine is a mock implementation of StateMachine using testify.
// Use it to test apply logic in isolation — set expectations for each call.
type MockStateMachine struct {
	mock.Mock
	mu sync.Mutex
}

var _ StateMachine = &MockStateMachine{}

func (m *MockStateMachine) Apply(ctx context.Context, entries []LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx, entries)
	return args.Error(0)
}

func (m *MockStateMachine) Snapshot(ctx context.Context) (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx)
	snap := args.Get(0)
	if snap == nil {
		return nil, args.Error(1)
	}
	return snap.(Snapshot), args.Error(1)
}

func (m *MockStateMachine) Restore(ctx context.Context, snapshot io.ReadCloser) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx, snapshot)
	return args.Error(0)
}

type MockSnapshot struct {
	mock.Mock
}

var _ Snapshot = &MockSnapshot{}

func (m *MockSnapshot) Persist(ctx context.Context, w io.Writer) error {
	args := m.Called(ctx, w)
	return args.Error(0)
}

func (m *MockSnapshot) Release() error {
	args := m.Called()
	return args.Error(0)
}
