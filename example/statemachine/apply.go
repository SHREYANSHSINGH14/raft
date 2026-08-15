package statemachine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/SHREYANSHSINGH14/raft"
	"github.com/cockroachdb/pebble"
	"github.com/rs/zerolog"
)

// failedCommand pairs a waiter with the reason its command did not take effect.
type failedCommand struct {
	result CommandResult
	err    error
}

// Apply stages every entry into one batch and commits it atomically.
//
// The error it returns is fatal: the library's apply loop calls setFatal and stops
// applying for the life of the process. So only node-level failures — storage, I/O,
// corruption — may be returned. A command that fails on its own terms (a CAS that
// did not match, an op we do not recognise) is a deterministic outcome every replica
// reaches identically; it is reported to the waiting client and apply continues.
// ErrCommandFailed is what separates the two.
//
// Waiters are notified only after DB.Apply succeeds. Notifying inside the loop would
// hand a success receipt to clients whose writes are still sitting in a batch that a
// later entry may cause us to discard.
func (s *StateMachine) Apply(ctx context.Context, entries []raft.LogEntry) error {
	batch := s.store.DB().NewIndexedBatch()
	defer batch.Close()

	var succeeded []CommandResult
	var failed []failedCommand

	for _, log := range entries {
		var cmd Command
		if err := cmd.Unmarshal(log.Data); err != nil {
			// Every replica sees these same bytes and skips this entry identically,
			// so skipping does not diverge us from the cluster.
			zerolog.Ctx(ctx).Error().Err(err).Msg("apply: skipping unparseable command entry")
			continue
		}

		cmdErr := s.processCommand(&cmd, batch)
		if cmdErr != nil && !errors.Is(cmdErr, ErrCommandFailed) {
			return fmt.Errorf("applying command %s: %w", cmd.ID, cmdErr)
		}

		// A miss is the normal case on a follower and on any node that restarted
		// since the proposal: only the node that took the client request registered
		// this ID. Do the state-machine work, skip the notification.
		result, ok := s.commandResultBuffer.Lookup(cmd.ID)
		if !ok {
			continue
		}
		if cmdErr != nil {
			failed = append(failed, failedCommand{result: result, err: cmdErr})
			continue
		}
		succeeded = append(succeeded, result)
	}

	if err := s.store.DB().Apply(batch, pebble.Sync); err != nil {
		return err
	}

	dispatchResults(succeeded, failed)
	return nil
}

// dispatchResults releases every waiter for the batch that just committed. It runs
// after DB.Apply returns nil, never before — see Apply.
func dispatchResults(succeeded []CommandResult, failed []failedCommand) {
	for _, result := range succeeded {
		if result.abandoned() {
			continue
		}
		close(result.res)
	}
	for _, f := range failed {
		if f.result.abandoned() {
			continue
		}
		// Non-blocking anyway: the caller can give up between the check above and
		// this send. A blocking send here would wedge the apply loop on a client that
		// walked away.
		select {
		case f.result.err <- f.err:
		default:
		}
	}
}

// processCommand stages one command into batch. It never touches CommandResult —
// reporting is Apply's job, once the batch is durable.
//
// Errors wrapping ErrCommandFailed describe the command; anything else describes
// this node and is fatal to it.
func (s *StateMachine) processCommand(cmd *Command, batch *pebble.Batch) error {
	// Batch writes take their durability from the pebble.Sync on DB.Apply; the
	// WriteOptions on the staging calls are ignored.
	switch cmd.Op {
	case OpsTypeSet:
		return batch.Set(stateKey([]byte(cmd.Key)), cmd.Value, nil)
	case OpsTypeDelete:
		return batch.Delete(stateKey([]byte(cmd.Key)), nil)
	case OpsTypeCAS:
		return processCAS(cmd, batch)
	default:
		return fmt.Errorf("%w: unknown op %q", ErrCommandFailed, cmd.Op)
	}
}

// processCAS reads through the indexed batch, so it sees writes staged earlier in
// this same Apply call as well as the committed database.
func processCAS(cmd *Command, batch *pebble.Batch) error {
	val, closer, err := batch.Get(stateKey([]byte(cmd.Key)))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return fmt.Errorf("%w: cas: key %q does not exist", ErrCommandFailed, cmd.Key)
		}
		return err
	}

	// val points into pebble's buffer and is only valid until closer.Close().
	current := slices.Clone(val)
	if err := closer.Close(); err != nil {
		return err
	}

	if !bytes.Equal(current, cmd.ExpectedValue) {
		return fmt.Errorf("%w: cas: value mismatch for key %q", ErrCommandFailed, cmd.Key)
	}
	return batch.Set(stateKey([]byte(cmd.Key)), cmd.Value, nil)
}
