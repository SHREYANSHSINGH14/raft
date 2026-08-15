package statemachine

import (
	"context"
	"encoding/binary"
	"io"

	"github.com/SHREYANSHSINGH14/raft"
	"github.com/cockroachdb/pebble"
)

type Snapshot struct {
	snap *pebble.Snapshot
}

var _ raft.Snapshot = &Snapshot{}

func (s *Snapshot) Persist(ctx context.Context, w io.Writer) error {
	// Bounded to our own namespace. The Raft log, current term and votedFor share
	// this database; shipping those to another node would overwrite its Raft state.
	iter, err := s.snap.NewIterWithContext(ctx, &pebble.IterOptions{
		LowerBound: []byte(StatePrefix),
		UpperBound: upperBound([]byte(StatePrefix)),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		// The prefix is a storage detail — the snapshot carries user keys, so the
		// format survives a change to StatePrefix.
		key := userKey(iter.Key())
		value := iter.Value()

		err := writeData(w, key, value)
		if err != nil {
			return err
		}
	}

	return iter.Error()
}

func (s *Snapshot) Release() error {
	return s.snap.Close()
}

func (s *StateMachine) Snapshot(ctx context.Context) (raft.Snapshot, error) {
	snap := Snapshot{
		snap: s.store.DB().NewSnapshot(),
	}
	return &snap, nil
}

func writeData(w io.Writer, key, value []byte) error {
	if err := binary.Write(w, binary.LittleEndian, uint32(len(key))); err != nil {
		return err
	}
	if _, err := w.Write(key); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(value))); err != nil {
		return err
	}
	if _, err := w.Write(value); err != nil {
		return err
	}
	return nil
}
