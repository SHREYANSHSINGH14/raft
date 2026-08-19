package raft

import (
	"context"
	"fmt"
	"slices"

	"github.com/rs/zerolog"
)

// startApplyLoop applies committed log entries to the state machine.
//
// One goroutine, waiting on commitCh. The inner loop guards against spurious
// wake-ups — if the signal arrives and commitIndex has not advanced past
// lastApplied, it waits again. The outer loop handles commitIndex advancing during
// slow work (DB reads, sm.Apply): on reacquire the condition check catches it
// immediately, with no signal needed. lastApplied is tracked locally to keep a DB
// read out of the hot path.
//
// The wait must be lock-neutral, and that is the thing to preserve here. Unlock,
// receive, lock — so the condition at the top of the inner loop is always evaluated
// with commitMu held no matter which way it was reached, and the loop leaves the
// lock exactly as it found it. sync.Cond.Wait gave that property for free; a channel
// receive does not, and every rearrangement of these three lines that has been tried
// produced either a self-deadlock on the non-reentrant Lock or an unlock of an
// unlocked mutex. See JOURNEY.md.
//
// The ctx.Done() case returns having released the lock, which is correct precisely
// because it exits before the reacquire.
func (n *Node) startApplyLoop(ctx context.Context) {
	go func() {
		// read once at startup — tracked locally after that, no DB call in hot path
		lastApplied := n.GetLastApplied()
		n.commitMu.Lock()

		for ctx.Err() == nil {
			// inner loop guards against spurious wakeups
			for n.shouldWaitForApply(lastApplied) && ctx.Err() == nil {
				n.commitMu.Unlock() // unlock before slow work
				select {
				case <-ctx.Done():
					return
				case <-n.commitCh:
					// Wake up to re-check the floor after a snapshot is done
				}
				n.commitMu.Lock() // reacquire before next condition check
			}

			if ctx.Err() != nil {
				n.commitMu.Unlock()
				return
			}

			commitIdx := n.commitIndex
			n.commitMu.Unlock() // unlock before slow work

			zerolog.Ctx(ctx).Debug().
				Uint("from", lastApplied+1).
				Uint("to", commitIdx).
				Msg("applying committed entries")

			if err := n.applyEntries(ctx, lastApplied, commitIdx); err != nil {
				zerolog.Ctx(ctx).Error().Err(err).Msg("startApplyLoop error")
				// The entries up to commitIdx are committed and cannot be taken back, and
				// nothing will hand them to Apply again, so giving up here leaves the state
				// machine permanently short of the log. Tell the caller before we go — see
				// Fatal. A cancelled context is ordinary shutdown, not that.
				if ctx.Err() == nil {
					n.setFatal(ctx, fmt.Errorf("applying entries up to %d: %w", commitIdx, err))
				}
				return
			}

			// The final hop: committed -> applied. A waiter released by processFutures
			// has only been told the entry is durable; it is this line that means the
			// state machine can answer for it.
			zerolog.Ctx(ctx).Debug().
				Uint("last_applied", commitIdx).
				Msg("entries applied")

			lastApplied = commitIdx
			n.commitMu.Lock() // reacquire before next condition check
		}
		n.commitMu.Unlock()
	}()
}

func (n *Node) shouldWaitForApply(lastApplied uint) bool {
	return n.commitIndex <= lastApplied || n.snapShotInProgress.Load()
}

func (n *Node) applyEntries(ctx context.Context, lastApplied, commitIdx uint) error {
	startIdx := lastApplied + 1
	endIdx := commitIdx + 1
	logs, err := n.store.GetLogs(ctx, &startIdx, &endIdx)
	if err != nil {
		return err
	}

	slices.SortFunc(logs, func(a, b LogEntry) int {
		return int(a.Index) - int(b.Index)
	})

	var commandEntries []LogEntry
	for _, log := range logs {
		if log.Type != EntryType_Command {
			continue
		}
		commandEntries = append(commandEntries, log)
	}
	// Hand the entries over in chunks of ApplyBatchSize. lastApplied advances after
	// each chunk rather than once at the end: a chunk that has applied is durable, and
	// leaving lastApplied behind it would re-apply it if a later chunk failed.
	batchSize := n.cfg.ApplyBatchSize
	if batchSize <= 0 {
		batchSize = len(commandEntries)
	}

	for start := 0; start < len(commandEntries); start += batchSize {
		end := min(start+batchSize, len(commandEntries))
		chunk := commandEntries[start:end]

		if err = n.sm.Apply(ctx, chunk); err != nil {
			return err
		}
		n.SetLastApplied(uint(chunk[len(chunk)-1].Index))
	}

	// Past the last command entry there may be no-op or config entries the state
	// machine never sees; commitIdx accounts for them.
	n.SetLastApplied(commitIdx)
	return nil
}
