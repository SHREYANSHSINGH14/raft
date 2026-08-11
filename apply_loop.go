package raft

import (
	"context"
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
		lastApplied, err := n.store.GetLastApplied(ctx)
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msg("startApplyLoop db error: error getting lastapplied")
			return
		}

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

			if err := n.applyEntries(ctx, lastApplied, commitIdx); err != nil {
				zerolog.Ctx(ctx).Error().Err(err).Msg("startApplyLoop error")
				return
			}

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

	if err = n.sm.Apply(ctx, logs); err != nil {
		return err
	}

	return n.store.SetLastApplied(ctx, commitIdx)
}
