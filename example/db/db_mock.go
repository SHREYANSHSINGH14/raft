package db

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"

	"github.com/SHREYANSHSINGH14/raft"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"

	"github.com/SHREYANSHSINGH14/raft/example/types"
)

// MockStore is a mock implementation of raft.Storage for testing purposes.
// Use it to test rpc calls and their conditional checks in isolation without
// worrying about actual db implementation correctness.
type MockStore struct {
	mock.Mock
	mu sync.Mutex
}

var _ raft.Storage = &MockStore{}

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

// ── LastApplied ───────────────────────────────────────────────────────────────

func (m *MockStore) SetLastApplied(ctx context.Context, term uint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx, term)
	return args.Error(0)
}

func (m *MockStore) GetLastApplied(ctx context.Context) (uint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx)
	return args.Get(0).(uint), args.Error(1)
}

// ── Logs ─────────────────────────────────────────────────────────────────────

func (m *MockStore) AppendLogs(ctx context.Context, logs []raft.LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx, logs)
	return args.Error(0)
}

func (m *MockStore) GetLogs(ctx context.Context, startIdx, endIdx *uint) ([]raft.LogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx, startIdx, endIdx)
	val := args.Get(0)
	if val == nil {
		return nil, args.Error(1)
	}
	return val.([]raft.LogEntry), args.Error(1)
}

func (m *MockStore) GetLogByIndex(ctx context.Context, idx uint) (raft.LogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx, idx)
	return args.Get(0).(raft.LogEntry), args.Error(1)
}

func (m *MockStore) DeleteLogs(ctx context.Context, fromIdx, toIdx uint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx, fromIdx, toIdx)
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

func (m *MockStore) GetLastLogEntry(ctx context.Context) (raft.LogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx)
	return args.Get(0).(raft.LogEntry), args.Error(1)
}

func (m *MockStore) GetFirstLogEntry(ctx context.Context) (raft.LogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx)
	return args.Get(0).(raft.LogEntry), args.Error(1)
}

// MockKVStore is a stateful in-memory implementation of raft.Storage.
// Keys are stored in the same raw byte encoding as the real pebble Store,
// so sort.Strings() on log keys produces the same order as pebble's
// lexicographic comparator (big-endian guarantees this).
type MockKVStore struct {
	mu   sync.RWMutex
	data map[string][]byte // raw key → raw value
}

var _ raft.Storage = &MockKVStore{}

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

// ── LastApplied ───────────────────────────────────────────────────────────────

func (m *MockKVStore) SetLastApplied(_ context.Context, term uint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.set([]byte(LastAppliedKey), uintToBytes(term))
	return nil
}

func (m *MockKVStore) GetLastApplied(_ context.Context) (uint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.get([]byte(LastAppliedKey))
	if !ok {
		return 0, nil
	}
	return bytesToUint(v)
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
	if err != nil {
		return 0, err
	}
	return uint(entry.Term), nil
}

func (m *MockKVStore) GetLastLogEntry(_ context.Context) (raft.LogEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys := m.sortedLogKeys()
	if len(keys) == 0 {
		return raft.LogEntry{}, nil
	}
	return m.unmarshalEntry([]byte(keys[len(keys)-1]))
}

func (m *MockKVStore) GetFirstLogEntry(_ context.Context) (raft.LogEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys := m.sortedLogKeys()
	if len(keys) == 0 {
		return raft.LogEntry{}, nil
	}
	return m.unmarshalEntry([]byte(keys[0]))
}

// ── Log CRUD ──────────────────────────────────────────────────────────────────

func (m *MockKVStore) AppendLogs(_ context.Context, logs []raft.LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, l := range logs {
		val, err := proto.Marshal(types.LogEntryFromRaft(l))
		if err != nil {
			return err
		}
		m.set(logKey(uint64(l.Index)), val)
	}
	return nil
}

func (m *MockKVStore) GetLogByIndex(_ context.Context, idx uint) (raft.LogEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := logKey(uint64(idx))
	_, ok := m.get(key)
	if !ok {
		return raft.LogEntry{}, raft.ErrNotFound
	}
	return m.unmarshalEntry(key)
}

// GetLogs returns entries in [startIdx, endIdx).
// endIdx == nil means "to the end".
func (m *MockKVStore) GetLogs(_ context.Context, startIdx, endIdx *uint) ([]raft.LogEntry, error) {
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
	var logs []raft.LogEntry

	for _, k := range keys {
		if k < startKey || k >= endKey {
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

func (m *MockKVStore) DeleteLogs(_ context.Context, fromIdx, toIdx uint) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// [startKey, endKey) — startKey inclusive, endKey exclusive.
	startKey := string(logKey(uint64(fromIdx))) // fromIdx==0 -> below every real entry
	var endKey string
	if toIdx == 0 {
		endKey = string(upperBound([]byte(LogPrefix))) // past the last log key
	} else {
		endKey = string(logKey(uint64(toIdx) + 1)) // +1 so toIdx itself is included
	}

	for k := range m.data {
		if k >= startKey && k < endKey {
			delete(m.data, k)
		}
	}
	return nil
}

// ── internal ─────────────────────────────────────────────────────────────────

func (m *MockKVStore) unmarshalEntry(key []byte) (raft.LogEntry, error) {
	v, ok := m.get(key)
	if !ok {
		return raft.LogEntry{}, fmt.Errorf("key not found: %q", key)
	}
	var entry types.LogEntry
	if err := proto.Unmarshal(v, &entry); err != nil {
		return raft.LogEntry{}, err
	}
	return types.LogEntryToRaft(&entry), nil
}
