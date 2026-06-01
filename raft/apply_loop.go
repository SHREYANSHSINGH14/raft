package raft

import (
	"context"

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
			for n.commitIndex <= lastApplied && ctx.Err() == nil {
				n.commitCond.Wait()
			}

			if ctx.Err() != nil {
				n.commitCond.L.Unlock()
				return
			}

			commitIdx := n.commitIndex
			n.commitCond.L.Unlock() // unlock before slow work

			startIdx := lastApplied + 1
			endIdx := commitIdx + 1
			logs, err := n.store.GetLogs(ctx, &startIdx, &endIdx)
			if err != nil {
				zerolog.Ctx(ctx).Error().Err(err).Msg("startApplyLoop db error: error getting logs")
				return
			}

			err = n.sm.Apply(logs)
			if err != nil {
				zerolog.Ctx(ctx).Error().Err(err).Msg("startApplyLoop stateMachine error: error applying logs")
				return
			}

			err = n.store.SetLastApplied(ctx, commitIdx)
			if err != nil {
				zerolog.Ctx(ctx).Error().Err(err).Msg("startApplyLoop db error: error setting lastapplied")
				return
			}

			lastApplied = commitIdx
			n.commitCond.L.Lock() // reacquire before next condition check
		}
		n.commitCond.L.Unlock()
	}()
}
