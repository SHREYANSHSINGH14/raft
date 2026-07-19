package raft

import (
	"context"
	"slices"

	"github.com/rs/zerolog"
)

// startApplyLoop applies committed log entries to the state machine.
//
// Earlier design: a bridge goroutine translated commitCond broadcasts into a
// buffered channel, and a separate goroutine consumed that channel with an
// inFlight flag to avoid concurrent applies. This required drain logic to
// prevent buffer fill, a re-signal on completion to avoid missing commits that
// arrived while inFlight=true, and a non-blocking send to avoid deadlocking
// with the lock held. Three mechanisms solving problems created by each other.
//
// Current design: one goroutine, sync.Cond directly. The inner for loop guards
// against spurious wakeups — if Wait() returns and commitIndex hasn't advanced
// past lastApplied, it sleeps again. The outer for loop handles the case where
// commitIndex advances during slow work (DB reads, sm.Apply) — on reacquire,
// the condition check catches it immediately without needing a signal.
// lastApplied is tracked locally to avoid a DB read on every iteration.
// The lock is released only during slow work and reacquired immediately after,
// ensuring no broadcast can be missed between condition check and Wait().
func (n *Node) startApplyLoop(ctx context.Context) {
	go func() {
		// read once at startup — tracked locally after that, no DB call in hot path
		lastApplied, err := n.store.GetLastApplied(ctx)
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msg("startApplyLoop db error: error getting lastapplied")
			return
		}

		n.commitCond.L.Lock()

		for ctx.Err() == nil {
			// inner loop guards against spurious wakeups
			for n.shouldWaitForApply(lastApplied) && ctx.Err() == nil {
				n.commitCond.Wait()
			}

			if ctx.Err() != nil {
				n.commitCond.L.Unlock()
				return
			}

			commitIdx := n.commitIndex
			n.commitCond.L.Unlock() // unlock before slow work

			if err := n.applyEntries(ctx, lastApplied, commitIdx); err != nil {
				zerolog.Ctx(ctx).Error().Err(err).Msg("startApplyLoop error")
				return
			}

			lastApplied = commitIdx
			n.commitCond.L.Lock() // reacquire before next condition check
		}
		n.commitCond.L.Unlock()
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
