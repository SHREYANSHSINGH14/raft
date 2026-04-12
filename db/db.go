package db

import (
	"context"
	"errors"

	"github.com/SHREYANSHSINGH14/raft/types"
	"github.com/cockroachdb/pebble"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"
)

const (
	CurrentTermKey = "current_term"
	VotedForKey    = "voted_for"
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

var _ types.RaftDBInterface = &Store{}

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
		zerolog.Ctx(ctx).Error().Err(err).Msg("error while getting current term")
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
		UpperBound: upperBound([]byte(LogPrefix)), //
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

func (s *Store) GetLastLogEntry(ctx context.Context) (*types.LogEntry, error) {
	lastIdx, err := s.GetLastLogIndex(ctx)
	if err != nil {
		return nil, err
	}

	if lastIdx == 0 {
		return &types.LogEntry{
			Index: 0,
			Term:  0,
			Data:  []byte{},
			Type:  types.EntryType_ENTRY_TYPE_NO_OP,
		}, nil // no logs yet
	}

	key := logKey(uint64(lastIdx))

	val, closer, err := s.db.Get(key)
	if err != nil {
		return nil, err
	}

	var log types.LogEntry

	err = proto.Unmarshal(val, &log)
	if err != nil {
		return nil, err
	}

	err = closer.Close()
	if err != nil {
		return nil, err
	}

	return &log, nil
}

// Note: we leave the entry.Index and index key check to business logic
// this layer is just supposed set them in db
func (s *Store) AppendLogs(ctx context.Context, logs []*types.LogEntry) error {
	batch := s.db.NewBatch()

	for i := range logs {
		key := logKey(uint64(logs[i].Index))
		val, err := proto.Marshal(logs[i])
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
func (s *Store) GetLogs(ctx context.Context, startIdx, endIdx *uint) ([]*types.LogEntry, error) {
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

	var logs []*types.LogEntry

	for iter.First(); iter.Valid(); iter.Next() {
		value := iter.Value()

		var log types.LogEntry

		err := proto.Unmarshal(value, &log)
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msg("error unmarshalling log")
			return nil, err
		}

		logs = append(logs, &log)
	}

	return logs, nil
}

func (s *Store) GetLogByIndex(ctx context.Context, idx uint) (*types.LogEntry, error) {
	key := logKey(uint64(idx))
	val, closer, err := s.db.Get(key)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("error getting log for index: %d", idx)
		return nil, err
	}

	var log types.LogEntry

	err = proto.Unmarshal(val, &log)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("error unmarshalling log for index: %d", idx)
		return nil, err
	}

	err = closer.Close()
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("error closing value for index: %d", idx)
		return nil, err
	}

	return &log, nil
}

// Since in this KV pebble DB we cannot put secondary index on term
// without complicating writes by managing a secondary index we just
// scan the whole thing Time Complexity: O(N) N is number of entries
func (s *Store) GetLogsByTerm(ctx context.Context, term uint) ([]*types.LogEntry, error) {
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

	var logs []*types.LogEntry

	for iter.First(); iter.Valid(); iter.Next() {
		val := iter.Value()
		var log types.LogEntry

		err := proto.Unmarshal(val, &log)
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msg("error unmarshalling")
			return nil, err
		}

		if log.Term == uint64(term) {
			logs = append(logs, &log)
		}
	}

	return logs, nil
}

// Deletes everything from the provided startIndex to lastIndex
// the reason is to maintain consistency throughout the logs
func (s *Store) TruncateLogs(ctx context.Context, startIdx uint) error {
	err := s.db.DeleteRange(logKey(uint64(startIdx)), upperBound([]byte(LogPrefix)), nil)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("error while deleting startIdx: %d", startIdx)
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
