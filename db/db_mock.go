package db

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"

	"github.com/SHREYANSHSINGH14/raft/types"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"
)

// MockStore is a mock implementation of the RaftDBInterface for testing purposes.
// This was used to test rpc calls and their conditional checks in raft package without worrying about the actual db implementation and its correctness.
// But this was a bit narrow visioned to be honest because we only tested the rpc calls and their conditional checks, now when thinks come to testing
// the behaviour in an actual cluster with multiple goroutines mimicing different nodes then we need a stateful mock which can actually store the current term,
// votedFor and logs in memory and return them when asked, otherwise we would have to set expectations for every single rpc call which would be a nightmare to maintain

// So now we'll create a new mock store which will be stateful

// Not removing this since the tests written using this are still useful to test the rpc calls and their conditional checks in isolation without worrying about the actual db
// implementation and its correctness, also this is first time working with testify mock and it was a good learning experience to understand how it works and how to use it effectively in our tests
// so we can keep this as a reference for future when we need to write tests for rpc calls in isolation without worrying about the actual db implementation and its correctness

type MockStore struct {
	mock.Mock
	mu sync.Mutex
}

var _ types.RaftDBInterface = &MockStore{}

// ── Current Term ─────────────────────────────────────────────────────────────

func (m *MockStore) SetCurrentTerm(ctx context.Context, term uint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx, term)
	return args.Error(0)
}

func (m *MockStore) GetCurrentTerm(ctx context.Context) (uint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx)
	return args.Get(0).(uint), args.Error(1)
}

// ── VotedFor ─────────────────────────────────────────────────────────────────

func (m *MockStore) SetVotedFor(ctx context.Context, nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx, nodeID)
	return args.Error(0)
}

func (m *MockStore) GetVotedFor(ctx context.Context) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx)
	return args.String(0), args.Error(1)
}

// ── Logs ─────────────────────────────────────────────────────────────────────

func (m *MockStore) AppendLogs(ctx context.Context, logs []*types.LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx, logs)
	return args.Error(0)
}

func (m *MockStore) GetLogs(ctx context.Context, startIdx, endIdx *uint) ([]*types.LogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx, startIdx, endIdx)
	val := args.Get(0)
	if val == nil {
		return nil, args.Error(1)
	}
	return val.([]*types.LogEntry), args.Error(1)
}

func (m *MockStore) GetLogByIndex(ctx context.Context, idx uint) (*types.LogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx, idx)
	val := args.Get(0)
	if val == nil {
		return nil, args.Error(1)
	}
	return val.(*types.LogEntry), args.Error(1)
}

func (m *MockStore) GetLogsByTerm(ctx context.Context, term uint) ([]*types.LogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx, term)
	val := args.Get(0)
	if val == nil {
		return nil, args.Error(1)
	}
	return val.([]*types.LogEntry), args.Error(1)
}

func (m *MockStore) TruncateLogs(ctx context.Context, startIdx uint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx, startIdx)
	return args.Error(0)
}

// ── Log Metadata ─────────────────────────────────────────────────────────────

func (m *MockStore) GetLastLogTerm(ctx context.Context) (uint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx)
	return args.Get(0).(uint), args.Error(1)
}

func (m *MockStore) GetLastLogIndex(ctx context.Context) (uint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx)
	return args.Get(0).(uint), args.Error(1)
}

func (m *MockStore) GetLastLogEntry(ctx context.Context) (*types.LogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx)
	val := args.Get(0)
	if val == nil {
		return nil, args.Error(1)
	}
	return val.(*types.LogEntry), args.Error(1)
}

// MockStore is an in-memory implementation of types.RaftDBInterface.
// Keys are stored in the same raw byte encoding as the real pebble Store,
// so sort.Strings() on log keys produces the same order as pebble's
// lexicographic comparator (big-endian guarantees this).
type MockKVStore struct {
	mu   sync.RWMutex
	data map[string][]byte // raw key → raw value
}

var _ types.RaftDBInterface = &MockKVStore{}

func NewMockKVStore() *MockKVStore {
	return &MockKVStore{
		data: make(map[string][]byte),
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

func (m *MockKVStore) set(key []byte, val []byte) {
	m.data[string(key)] = val
}

func (m *MockKVStore) get(key []byte) ([]byte, bool) {
	v, ok := m.data[string(key)]
	return v, ok
}

// sortedLogKeys returns all log: keys in ascending lexicographic order.
// Because keys are big-endian encoded this equals ascending index order.
func (m *MockKVStore) sortedLogKeys() []string {
	prefix := LogPrefix
	var keys []string
	for k := range m.data {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys) // lex == numeric for big-endian keys
	return keys
}

// indexFromLogKey reverses logKey() — strips the prefix and decodes uint64.
func indexFromLogKey(k string) (uint64, error) {
	raw := k[len(LogPrefix):]
	if len(raw) != 8 {
		return 0, fmt.Errorf("malformed log key")
	}
	return binary.BigEndian.Uint64([]byte(raw)), nil
}

// ── Current Term ─────────────────────────────────────────────────────────────

func (m *MockKVStore) SetCurrentTerm(_ context.Context, term uint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.set([]byte(CurrentTermKey), uintToBytes(term))
	return nil
}

func (m *MockKVStore) GetCurrentTerm(_ context.Context) (uint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.get([]byte(CurrentTermKey))
	if !ok {
		return 0, nil
	}
	return bytesToUint(v)
}

// ── VotedFor ─────────────────────────────────────────────────────────────────

func (m *MockKVStore) SetVotedFor(_ context.Context, nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.set([]byte(VotedForKey), []byte(nodeID))
	return nil
}

func (m *MockKVStore) GetVotedFor(_ context.Context) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.get([]byte(VotedForKey))
	if !ok {
		return "", nil
	}
	return string(v), nil
}

// ── Log metadata ─────────────────────────────────────────────────────────────

func (m *MockKVStore) GetLastLogIndex(_ context.Context) (uint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys := m.sortedLogKeys()
	if len(keys) == 0 {
		return 0, nil
	}
	idx, err := indexFromLogKey(keys[len(keys)-1])
	return uint(idx), err
}

func (m *MockKVStore) GetLastLogTerm(ctx context.Context) (uint, error) {
	entry, err := m.GetLastLogEntry(ctx)
	if err != nil || entry == nil {
		return 0, err
	}
	return uint(entry.Term), nil
}

func (m *MockKVStore) GetLastLogEntry(_ context.Context) (*types.LogEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys := m.sortedLogKeys()
	if len(keys) == 0 {
		return nil, nil
	}
	return m.unmarshalEntry([]byte(keys[len(keys)-1]))
}

// ── Log CRUD ──────────────────────────────────────────────────────────────────

func (m *MockKVStore) AppendLogs(_ context.Context, logs []*types.LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, l := range logs {
		val, err := proto.Marshal(l)
		if err != nil {
			return err
		}
		m.set(logKey(uint64(l.Index)), val)
	}
	return nil
}

func (m *MockKVStore) GetLogByIndex(_ context.Context, idx uint) (*types.LogEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := logKey(uint64(idx))
	v, ok := m.get(key)
	if !ok {
		return &types.LogEntry{
			Index: 0,
			Term:  0,
			Data:  []byte{},
			Type:  types.EntryType_ENTRY_TYPE_NO_OP,
		}, nil
	}
	var entry types.LogEntry
	if err := proto.Unmarshal(v, &entry); err != nil {
		return nil, err
	}
	return &entry, nil
}

// GetLogs returns entries in [startIdx, endIdx).
// endIdx == nil means "to the end".
func (m *MockKVStore) GetLogs(_ context.Context, startIdx, endIdx *uint) ([]*types.LogEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	startKey := string(logKey(uint64(*startIdx)))
	var endKey string
	if endIdx == nil {
		endKey = string(upperBound([]byte(LogPrefix)))
	} else {
		endKey = string(logKey(uint64(*endIdx)))
	}

	keys := m.sortedLogKeys()
	var logs []*types.LogEntry

	for _, k := range keys {
		if k < startKey || k >= endKey { // mirrors pebble's [lower, upper) range
			continue
		}
		entry, err := m.unmarshalEntry([]byte(k))
		if err != nil {
			return nil, err
		}
		logs = append(logs, entry)
	}
	return logs, nil
}

func (m *MockKVStore) GetLogsByTerm(_ context.Context, term uint) ([]*types.LogEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keys := m.sortedLogKeys()
	var logs []*types.LogEntry

	for _, k := range keys {
		entry, err := m.unmarshalEntry([]byte(k))
		if err != nil {
			return nil, err
		}
		if uint(entry.Term) == term {
			logs = append(logs, entry)
		}
	}
	return logs, nil
}

func (m *MockKVStore) TruncateLogs(_ context.Context, startIdx uint) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	startKey := string(logKey(uint64(startIdx)))
	endKey := string(upperBound([]byte(LogPrefix)))

	for k := range m.data {
		if k >= startKey && k < endKey {
			delete(m.data, k)
		}
	}
	return nil
}

// ── internal ─────────────────────────────────────────────────────────────────

func (m *MockKVStore) unmarshalEntry(key []byte) (*types.LogEntry, error) {
	v, ok := m.get(key)
	if !ok {
		return nil, fmt.Errorf("key not found: %q", key)
	}
	var entry types.LogEntry
	if err := proto.Unmarshal(v, &entry); err != nil {
		return nil, err
	}
	return &entry, nil
}
