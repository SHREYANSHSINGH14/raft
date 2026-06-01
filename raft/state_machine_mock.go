package raft

import (
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

func (m *MockStateMachine) Apply(entries []LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(entries)
	return args.Error(0)
}
