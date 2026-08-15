package db

import (
	"context"
	"errors"

	"github.com/SHREYANSHSINGH14/raft"
	"github.com/SHREYANSHSINGH14/raft/example/types"
	"github.com/cockroachdb/pebble"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"
)

const (
	CurrentTermKey = "current_term"
	VotedForKey    = "voted_for"
	LastAppliedKey = "last_applied"
	LogPrefix      = "log:"
)

type Store struct {
	db *pebble.DB
}

func NewStore(ctx context.Context, dirPath string) (*Store, error) {
	db, err := pebble.Open(dirPath, &pebble.Options{})
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("error while initializing DB")
		return nil, err
	}

	return &Store{
		db: db,
	}, nil
}

func (s *Store) DB() *pebble.DB {
	return s.db
}

var _ raft.Storage = &Store{}

// LastApplied
//
// Durability note: applyEntries calls sm.Apply *before* SetLastApplied, so a crash
// between the two re-applies those entries on restart rather than skipping them.
// That is the safe direction, and it is why this needs no shared batch with the
// state machine — but it does mean StateMachine.Apply must genuinely be idempotent.
func (s *Store) SetLastApplied(ctx context.Context, idx uint) error {
	key := []byte(LastAppliedKey)
	val := uintToBytes(idx)

	err := s.db.Set(key, val, pebble.Sync)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("error while setting last applied")
		return err
	}
	return nil
}

// GetLastApplied returns 0 (not ErrNotFound) on a fresh store: nothing has been
// applied yet, which is a fact, not a failure. Both callers treat any error as
// fatal — startApplyLoop kills the apply loop on one, so returning a sentinel
// here would stop a brand-new node from ever applying anything.
func (s *Store) GetLastApplied(ctx context.Context) (uint, error) {
	key := []byte(LastAppliedKey)

	data, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}
		zerolog.Ctx(ctx).Error().Err(err).Msg("error while getting last applied")
		return 0, err
	}
	defer closer.Close()

	val, err := bytesToUint(data)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("error while converting byte data to uint")
		return 0, err
	}

	return val, nil
}

// Current Term
func (s *Store) SetCurrentTerm(ctx context.Context, term uint) error {
	key := []byte(CurrentTermKey)
	val := uintToBytes(term)

	err := s.db.Set(key, val, pebble.Sync)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("error while setting current term")
		return err
	}
	return nil
}

func (s *Store) GetCurrentTerm(ctx context.Context) (uint, error) {
	key := []byte(CurrentTermKey)

	data, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, raft.ErrNotFound
		}
		zerolog.Ctx(ctx).Error().Err(err).Msg("error while getting current term")
		return 0, err
	}

	val, err := bytesToUint(data)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("error while converting byte data to uint")
		return 0, err
	}

	err = closer.Close()
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("error while closing")
		return 0, err
	}

	return val, nil
}

// Voted For
func (s *Store) SetVotedFor(ctx context.Context, nodeID string) error {
	key := []byte(VotedForKey)
	val := []byte(nodeID)

	err := s.db.Set(key, val, pebble.Sync)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("error while setting voted for")
		return err
	}
	return nil
}

func (s *Store) GetVotedFor(ctx context.Context) (string, error) {
	key := []byte(VotedForKey)

	data, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return "", raft.ErrNotFound
		}
		zerolog.Ctx(ctx).Error().Err(err).Msg("error while getting voted for")
		return "", err
	}

	val := string(data)

	err = closer.Close()
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("error while closing")
		return "", err
	}

	return val, nil
}

// Logs
func (s *Store) GetLastLogIndex(ctx context.Context) (uint, error) {
	iterOptions := &pebble.IterOptions{
		LowerBound: []byte(LogPrefix),
		UpperBound: upperBound([]byte(LogPrefix)),
	}

	iter, err := s.db.NewIter(iterOptions)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil // no logs yet
		}
		return 0, err
	}
	defer iter.Close()

	if !iter.Last() {
		return 0, nil // no logs yet
	}

	key := iter.Key()
	indexBytes := key[len(LogPrefix):] // strip prefix
	return bytesToUint(indexBytes)
}

func (s *Store) GetLastLogTerm(ctx context.Context) (uint, error) {
	lastIdx, err := s.GetLastLogIndex(ctx)
	if err != nil {
		return 0, err
	}

	if lastIdx == 0 {
		return 0, nil // no logs yet
	}

	key := logKey(uint64(lastIdx))

	val, closer, err := s.db.Get(key)
	if err != nil {
		return 0, err
	}

	var log types.LogEntry

	err = proto.Unmarshal(val, &log)
	if err != nil {
		return 0, err
	}

	err = closer.Close()
	if err != nil {
		return 0, err
	}

	return uint(log.Term), nil
}

func (s *Store) GetLastLogEntry(ctx context.Context) (raft.LogEntry, error) {
	lastIdx, err := s.GetLastLogIndex(ctx)
	if err != nil {
		return raft.LogEntry{}, err
	}

	if lastIdx == 0 {
		return raft.LogEntry{}, nil // no logs yet
	}

	key := logKey(uint64(lastIdx))

	val, closer, err := s.db.Get(key)
	if err != nil {
		return raft.LogEntry{}, err
	}

	var log types.LogEntry

	err = proto.Unmarshal(val, &log)
	if err != nil {
		return raft.LogEntry{}, err
	}

	err = closer.Close()
	if err != nil {
		return raft.LogEntry{}, err
	}

	return types.LogEntryToRaft(&log), nil
}

func (s *Store) GetFirstLogEntry(ctx context.Context) (raft.LogEntry, error) {
	iterOptions := &pebble.IterOptions{
		LowerBound: []byte(LogPrefix),
		UpperBound: upperBound([]byte(LogPrefix)),
	}

	iter, err := s.db.NewIter(iterOptions)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return raft.LogEntry{}, nil // no logs yet
		}
		return raft.LogEntry{}, err
	}
	defer iter.Close()

	if !iter.First() {
		return raft.LogEntry{}, nil // no logs yet
	}

	var log types.LogEntry
	if err := proto.Unmarshal(iter.Value(), &log); err != nil {
		return raft.LogEntry{}, err
	}

	return types.LogEntryToRaft(&log), nil
}

// Note: we leave the entry.Index and index key check to business logic
// this layer is just supposed set them in db
func (s *Store) AppendLogs(ctx context.Context, logs []raft.LogEntry) error {
	batch := s.db.NewBatch()

	for i := range logs {
		key := logKey(uint64(logs[i].Index))
		val, err := proto.Marshal(types.LogEntryFromRaft(logs[i]))
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msg("error marshaling")
			return err
		}
		if err := batch.Set(key, val, nil); err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msg("error setting log")
			return err
		}
	}

	return batch.Commit(pebble.Sync)
}

// Gets logs from startIdx upto endIdx (excluding the endIdx)
// endIdx is optional if not provided then get all the logs from startIdx
func (s *Store) GetLogs(ctx context.Context, startIdx, endIdx *uint) ([]raft.LogEntry, error) {
	iteroptions := pebble.IterOptions{
		LowerBound: logKey(uint64(*startIdx)),
	}

	if endIdx == nil {
		iteroptions.UpperBound = upperBound([]byte(LogPrefix))
	} else {
		iteroptions.UpperBound = logKey(uint64(*endIdx))
	}

	iter, err := s.db.NewIter(&iteroptions)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("error get new iterator")
		return nil, err
	}

	defer iter.Close()

	var logs []raft.LogEntry

	for iter.First(); iter.Valid(); iter.Next() {
		value := iter.Value()

		var log types.LogEntry

		err := proto.Unmarshal(value, &log)
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msg("error unmarshalling log")
			return nil, err
		}

		logs = append(logs, types.LogEntryToRaft(&log))
	}

	return logs, nil
}

func (s *Store) GetLogByIndex(ctx context.Context, idx uint) (raft.LogEntry, error) {
	key := logKey(uint64(idx))
	val, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return raft.LogEntry{}, raft.ErrNotFound
		}
		zerolog.Ctx(ctx).Error().Err(err).Msgf("error getting log for index: %d", idx)
		return raft.LogEntry{}, err
	}

	var log types.LogEntry

	err = proto.Unmarshal(val, &log)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("error unmarshalling log for index: %d", idx)
		return raft.LogEntry{}, err
	}

	err = closer.Close()
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("error closing value for index: %d", idx)
		return raft.LogEntry{}, err
	}

	return types.LogEntryToRaft(&log), nil
}

// Since in this KV pebble DB we cannot put secondary index on term
// without complicating writes by managing a secondary index we just
// scan the whole thing Time Complexity: O(N) N is number of entries
func (s *Store) GetLogsByTerm(ctx context.Context, term uint) ([]raft.LogEntry, error) {
	iterOptions := pebble.IterOptions{
		LowerBound: []byte(LogPrefix),
		UpperBound: upperBound([]byte(LogPrefix)),
	}

	iter, err := s.db.NewIter(&iterOptions)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("error get new iterator")
		return nil, err
	}
	defer iter.Close()

	var logs []raft.LogEntry

	for iter.First(); iter.Valid(); iter.Next() {
		val := iter.Value()
		var log types.LogEntry

		err := proto.Unmarshal(val, &log)
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msg("error unmarshalling")
			return nil, err
		}

		if log.Term == uint64(term) {
			logs = append(logs, types.LogEntryToRaft(&log))
		}
	}

	return logs, nil
}

// DeleteLogs removes every log entry whose index lies in the inclusive range
// [fromIdx, toIdx]. A bound of 0 is open-ended (log indices start at 1):
// fromIdx==0 has no lower bound, toIdx==0 has no upper bound. See the
// raft.Storage interface for the full contract.
func (s *Store) DeleteLogs(ctx context.Context, fromIdx, toIdx uint) error {
	// DeleteRange deletes [lower, upper) — lower inclusive, upper exclusive.
	lower := logKey(uint64(fromIdx)) // fromIdx==0 -> logKey(0), below every real entry
	var upper []byte
	if toIdx == 0 {
		upper = upperBound([]byte(LogPrefix)) // past the last log key
	} else {
		upper = logKey(uint64(toIdx) + 1) // +1 so toIdx itself is included
	}
	err := s.db.DeleteRange(lower, upper, nil)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("error while deleting logs [%d, %d]", fromIdx, toIdx)
		return err
	}
	return nil
}

// upperBound takes a key prefix and returns the smallest key that is
// guaranteed to be greater than all keys sharing that prefix.
//
// Example: prefix "log:" → upper bound "log;"
// This lets PebbleDB iterate over exactly all "log:..." keys and no more.
func upperBound(prefix []byte) []byte {

	// Create a new byte slice of the same length as prefix.
	// We don't want to modify the original prefix, so we make a copy.
	// e.g. "log:" → allocates [0, 0, 0, 0]
	upper := make([]byte, len(prefix))

	// Copy all bytes from prefix into upper.
	// e.g. upper is now [108, 111, 103, 58] which is "log:"
	copy(upper, prefix)

	// Increment the last byte by 1.
	// e.g. last byte ':' (ASCII 58) becomes ';' (ASCII 59)
	// upper is now [108, 111, 103, 59] which is "log;"
	//
	// Since PebbleDB compares keys byte by byte left to right,
	// any "log:XXXX" key will always have 58 in position 3,
	// which is less than 59 — so "log;" is strictly greater
	// than every possible "log:..." key.
	upper[len(upper)-1]++

	return upper
}
