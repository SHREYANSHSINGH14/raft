package statemachine

import (
	"context"
	"encoding/binary"
	"errors"
	"io"

	"github.com/cockroachdb/pebble"
)

type KVPair struct {
	key string
	val []byte
}

const (
	_  = iota
	KB = 1 << (10 * iota) // 1024
	MB                    // 1048576
	GB
)

// Restore replaces the state machine with the contents of snapshot.
//
// Replaces, not merges. A node that fell far enough behind to need an
// InstallSnapshot may hold keys the leader has since deleted, and those keys are not
// mentioned anywhere in the snapshot — there is nothing in the stream to undo them.
// Clearing the namespace first is what makes "the state machine becomes the
// snapshot" true rather than "the state machine gains the snapshot's keys".
//
// The writes are flushed in bounded batches rather than one, so a crash midway can
// leave the clear and part of the restore committed. That is safe, and not by
// accident: the snapshot file is already on disk, SetLastApplied runs only once
// Restore has returned nil, and startup replays that same file from the beginning.
// Nothing downstream can observe the partial state, and re-running over it converges.
//
// Ordering survives the split. Within a batch later operations win, and later batches
// sort after earlier ones, so the DeleteRange staged first never eats the Sets that
// follow it — in this batch or in any that comes after.
func (s *StateMachine) Restore(ctx context.Context, snapshot io.ReadCloser) error {
	batch := s.store.DB().NewBatch()
	defer batch.Close()

	// Bounded to our own namespace — the Raft log and metadata live in this same
	// database and must survive a state machine restore untouched.
	if err := batch.DeleteRange([]byte(StatePrefix), upperBound([]byte(StatePrefix)), nil); err != nil {
		return err
	}

	kvChan := make(chan KVPair, 100)
	errChan := make(chan error, 1)
	doneCh := make(chan struct{})
	consCh := make(chan struct{}, 1)

	go func() {
		readData(ctx, snapshot, kvChan, errChan, consCh)
		doneCh <- struct{}{}
	}()
	defer func() {
		<-doneCh
	}()

	for {
		select {
		case kv, ok := <-kvChan:
			if !ok {
				return s.store.DB().Apply(batch, pebble.Sync)
			}
			// 512KB is bracketed by two pebble constants: under batchMaxRetainedSize
			// (1MB), so Reset keeps the buffer rather than dropping it for GC, and
			// well under largeBatchThreshold (MemTableSize/2, so 2MB by default),
			// past which pebble diverts the batch out of the memtable into its own
			// flushable and pays for the extra flush.
			//
			// NoSync here, Sync only on the final Apply above. A crash partway leaves
			// the DeleteRange and some writes committed, but the snapshot file is
			// already on disk and SetLastApplied has not run, so startup replays this
			// same file from the beginning — nothing downstream can observe the
			// partial state. An fsync every 512KB would buy durability we discard.
			if batch.Len() >= 512*KB {
				err := s.store.DB().Apply(batch, pebble.NoSync)
				if err != nil {
					consCh <- struct{}{}
					return err
				}
				batch.Reset()
			}
			if err := batch.Set(stateKey([]byte(kv.key)), kv.val, nil); err != nil {
				consCh <- struct{}{}
				return err
			}
		case err := <-errChan:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func readData(ctx context.Context, r io.Reader, kvChan chan<- KVPair, errCh chan<- error, consCh <-chan struct{}) {
	var keyLen, valLen uint32
	var key, val []byte
	for {
		if err := decode(r, &keyLen, &key); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			errCh <- err
			return
		}
		if err := decode(r, &valLen, &val); err != nil {
			errCh <- err
			return
		}
		select {
		case kvChan <- KVPair{
			key: string(key),
			val: val,
		}:
		case <-consCh:
			return
		case <-ctx.Done():
			return
		}
	}
	close(kvChan)
	return
}

func prefixLength(r io.Reader, dst *uint32) error {
	return binary.Read(r, binary.LittleEndian, dst)
}

func decode(r io.Reader, lenDst *uint32, dst *[]byte) error {
	err := prefixLength(r, lenDst)
	if err != nil {
		return err
	}
	lenBuf := make([]byte, *lenDst)
	_, err = io.ReadFull(r, lenBuf)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	*dst = lenBuf
	return nil
}
