package raft

import (
	"context"
	"fmt"
)

type Future struct {
	idx uint64
	// doneCh is closed when the future is completed. It is used to signal waiters.
	doneCh chan struct{}
	// if leaderClose is closed, the future is aborted because the leader has stepped down. It is used to signal waiters.
	leaderClose chan struct{}
	// err is set when the future is completed. It is used to signal waiters.
	errCh chan error
}

// newFuture registers a waiter for the entry at idx.
//
// Callers must hold clientMu, right next to appendEntry. futureList is drained as
// a sorted prefix, and append order only equals index order if the two happen
// together — two racing Proposes that append after unlocking can land out of
// order, and then processFutures stops at the wrong place and strands a waiter.
//
// leaderCloseCh is captured here rather than read at wait time so a step-down
// stays O(1): it closes one channel and every future in flight sees it, with no
// window in which a future registered between a drain and the role flip is missed.
// A nil value means we were not leading when this was registered, which the waiter
// has to treat exactly like closed.
func (n *Node) newFuture(idx uint64, errCh chan error) *Future {
	// Read this before taking commitMu. getLeaderCloseCh needs mu, and commitMu ->
	// mu is a lock ordering this codebase does not otherwise have — initFutureList
	// keeps out of it from the other side, by running before initLeaderTermState
	// takes mu.
	leaderCh := n.getLeaderCloseCh()

	n.commitMu.Lock()
	defer n.commitMu.Unlock()

	future := &Future{
		idx:         idx,
		doneCh:      make(chan struct{}),
		leaderClose: leaderCh,
		errCh:       errCh,
	}
	n.futureList = append(n.futureList, future)
	return future
}

// futureListInitialCap is the capacity a fresh leadership term starts with. It is
// preallocation, NOT a limit — append grows past it. The limit is
// Config.MaxPendingProposals, enforced by admitProposal.
const futureListInitialCap = 1024

// DefaultMaxPendingProposals bounds entries appended but not yet committed when
// Config.MaxPendingProposals is unset.
const DefaultMaxPendingProposals = 1024

// admitProposal reports whether another proposal may be appended, and must be
// called BEFORE appendEntry — under the same clientMu hold, so the count cannot
// rise between the two. Nothing else can append while we hold clientMu, and
// processFutures only removes, so a passing check stays true until we register.
//
// The ordering is the whole point. Rejecting after the append would leave an entry
// that is already durable, will replicate, and will very likely commit, while its
// caller was told the proposal failed — a false negative with a live entry behind
// it, which is worse than the unbounded list this limit exists to prevent.
//
// EntryType_Config is exempt. The list only fills when commitIndex has stopped
// moving, which usually means quorum is lost — and AddMember/RemoveMember propose
// through this same path. Capping them uniformly would reject the membership
// change most likely to restore quorum at exactly the moment it is needed.
// Config entries are also bounded by other means: Raft permits one membership
// change at a time, and hasStagingPeer enforces it.
func (n *Node) admitProposal(entryType EntryType) error {
	if entryType == EntryType_Config {
		return nil
	}

	limit := n.cfg.MaxPendingProposals
	if limit <= 0 {
		limit = DefaultMaxPendingProposals
	}

	n.commitMu.Lock()
	defer n.commitMu.Unlock()
	if len(n.futureList) >= limit {
		return fmt.Errorf("%w: %d awaiting commit", ErrTooManyPendingProposals, len(n.futureList))
	}
	return nil
}

// initFutureList gives the new leadership term an empty list, and clearFutureList
// drops the one the term that just ended was using.
//
// Both take commitMu, the lock newFuture and processFutures hold when they touch
// futureList — a write from a role transition is no less a write for happening
// once. They are deliberately not folded into initLeaderTermState/becomeFollower's
// own critical sections: initLeaderTermState holds mu, and taking commitMu inside
// it would introduce a mu -> commitMu ordering that closes the cycle newFuture
// avoids from the other side (see its comment on reading leaderCloseCh early).
// Called outside those holds, the two locks are simply never held together.
func (n *Node) initFutureList() {
	n.commitMu.Lock()
	defer n.commitMu.Unlock()
	n.futureList = make([]*Future, 0, futureListInitialCap)
}

// clearFutureList abandons every future the ended term registered. It does not
// close them: clearLeaderCloseCh has already closed the channel each one captured,
// so their waiters are woken and answer for themselves — committed ones return nil,
// the rest ErrLeadershipLost. Callers must clear the leadership channel first.
func (n *Node) clearFutureList() {
	n.commitMu.Lock()
	defer n.commitMu.Unlock()
	n.futureList = nil
}

// processFutures closes every waiter whose entry has now committed.
//
// Call it AFTER SetCommitIndex returns, never from inside it: SetCommitIndex holds
// commitMu for its whole body, and Go mutexes are not reentrant.
//
// The whole read-close-trim is one critical section. Splitting it lets a
// concurrent newFuture append — possibly reallocating the backing array — between
// the count and the trim, leaving the count indexing a list it was not computed
// against. Closing a channel is non-blocking and calls back into nothing, so
// holding commitMu across it costs nothing.
func (n *Node) processFutures(commitIndex uint64) {
	n.commitMu.Lock()
	defer n.commitMu.Unlock()

	// cut counts the futures closed, so the survivors begin at index cut. Ranging a
	// nil futureList is a no-op, so it needs no guard.
	cut := 0
	for _, future := range n.futureList {
		if future.idx > commitIndex {
			break // sorted by idx, so nothing after this qualifies either
		}
		close(future.doneCh)
		cut++
	}
	if cut == 0 {
		return
	}

	// Shift the survivors down rather than re-slicing. Re-slicing shrinks cap on
	// every drain, so the slice keeps sliding rightward and reallocating; copying
	// down reuses one array forever. Nil the vacated tail so the drained futures are
	// collectable — they hold channels, and slots past len but inside cap stay
	// reachable through the backing array.
	rest := copy(n.futureList, n.futureList[cut:])
	for j := rest; j < len(n.futureList); j++ {
		n.futureList[j] = nil
	}
	n.futureList = n.futureList[:rest]
}

// committed reports whether this entry has committed, without blocking. A closed
// doneCh is processFutures' record of the idx <= commitIndex comparison it already
// made, so this asks the question rather than redoing it — no commitMu taken, no
// reading commitIndex behind the drain loop's back.
//
// Every exit that is about to report a failure calls this first. Committed outranks
// a step-down, a delivered error, and a cancelled context that land in the same
// wakeup: the entry is in the log for good either way, and reporting failure invites
// a retry that appends it a second time. A nil doneCh — the zero Future handed back
// on a rejected proposal — falls through to default, so this is safe on one.
func (f *Future) committed() bool {
	select {
	case <-f.doneCh:
		return true
	default:
		return false
	}
}

// Wait blocks until the entry commits, an error is delivered, the leadership term
// ends, or ctx is cancelled.
//
// The select decides only when to stop sleeping; what to report is re-derived
// underneath it in priority order. That split is the point: a select chooses
// uniformly at random among ready cases, so the case that fires says which signal
// arrived, not which fact outranks the others.
func (f *Future) Wait(ctx context.Context) error {
	// A nil leaderClose means we were not leading when this was registered. It can
	// still have been completed since — processFutures closes doneCh without
	// consulting the role — so the commit check comes first here too.
	if f.leaderClose == nil {
		if f.committed() {
			return nil
		}
		return ErrLeadershipLost
	}

	select {
	case <-f.doneCh:
		return nil
	case <-f.leaderClose:
		if f.committed() {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return ErrLeadershipLost
	case err := <-f.errCh:
		// Return err even if ctx has also expired: the receive consumed it, and it is
		// the more specific answer — a context deadline says the caller stopped
		// waiting, this says why the entry failed.
		if f.committed() {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	case <-ctx.Done():
		if f.committed() {
			return nil
		}
		return ctx.Err()
	}
}

func (f *Future) Index() uint64 {
	return f.idx
}
