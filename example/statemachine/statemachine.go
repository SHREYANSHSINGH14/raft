package statemachine

import (
	"context"
	"errors"
	"slices"

	"github.com/SHREYANSHSINGH14/raft"
	"github.com/SHREYANSHSINGH14/raft/example/db"
	"github.com/cockroachdb/pebble"
)

type StateMachine struct {
	store               *db.Store
	commandResultBuffer *CommandResultBuffer
}

var _ raft.StateMachine = &StateMachine{}

func NewStateMachine(ctx context.Context, store *db.Store) *StateMachine {
	return &StateMachine{
		store:               store,
		commandResultBuffer: NewCommandResultBuffer(ctx),
	}
}

// Register creates the waiter for id. Call it before Propose — see
// CommandResultBuffer.Register for why the order is not negotiable.
func (s *StateMachine) Register(ctx context.Context, id string) {
	s.commandResultBuffer.Register(ctx, id)
}

// Forget drops the waiter for id. Defer it immediately after Register, so it runs
// even on the path where Propose itself fails.
func (s *StateMachine) Forget(id string) {
	s.commandResultBuffer.Forget(id)
}

// WaitForResult blocks until the command applies, its caller gives up, or the node
// shuts down. It answers "what did applying it produce" — Future.Wait answers "did it
// commit", and a caller needs both.
func (s *StateMachine) WaitForResult(id string) error {
	return s.commandResultBuffer.WaitForResult(id)
}

// Sweep drops waiters whose callers have gone away. Backstop for a missed Forget.
func (s *StateMachine) Sweep() int {
	return s.commandResultBuffer.Sweep()
}

// Get reads a key directly out of the local database, without going through the log.
//
// This is a STALE read: it returns whatever this node has applied so far. On a
// follower, or on a leader that has been deposed and does not know it yet, that can
// lag or contradict the cluster. A linearizable read needs a log entry or a ReadIndex
// round trip; neither exists here.
func (s *StateMachine) Get(key string) ([]byte, error) {
	val, closer, err := s.store.DB().Get(stateKey([]byte(key)))
	if err != nil {
		// Translate pebble's sentinel to the library's, so callers do not have to
		// import pebble to recognise a missing key.
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, raft.ErrNotFound
		}
		return nil, err
	}
	defer closer.Close()
	return slices.Clone(val), nil
}
