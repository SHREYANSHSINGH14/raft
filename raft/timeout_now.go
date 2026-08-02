package raft

import (
	"context"
	"fmt"
	"strings"

	"github.com/rs/zerolog"
)

// HandleTimeoutNow accepts a leadership transfer (Ongaro §3.10). The leader sends
// it to a successor whose log it has already brought up to date; the successor
// campaigns at once instead of waiting out its election timer, so the handover
// costs one election round-trip rather than a full election timeout.
//
// The handler itself does not transition. It signals timeoutNowCh, which the
// election-timeout goroutine is selecting on, and that goroutine calls
// becomeCandidate — the same path a fired ticker takes. That is the invariant
// from CLAUDE.md: only the goroutine that owns a lifecycle may end it. A handler
// calling becomeCandidate directly would leave the timer goroutine alive
// alongside the new candidate.
//
// Deliberately absent, in contrast to HandlePreVote:
//
//   - no leader-stickiness check. Pre-vote refuses to encourage a challenger
//     while a leader exists; TimeoutNow is that leader asking, so the whole point
//     is to bypass it.
//   - no term bump. Nothing is persisted here. The recipient raises its term when
//     it actually campaigns, through the normal election path.
//   - no log up-to-date check. The leader only sends this after replicating up to
//     its own last index, and the recipient cannot verify that claim locally
//     anyway — the vote it then requests is what really decides the election.
//
// NOTE: This method is thread safe and can be called concurrently by multiple
// callers. It takes clientMu like the other caller-facing handlers.
func (n *Node) HandleTimeoutNow(ctx context.Context, args TimeoutNowArgs) (TimeoutNowResponse, error) {
	n.clientMu.Lock()
	defer n.clientMu.Unlock()

	if strings.TrimSpace(args.LeaderID) == "" {
		err := fmt.Errorf("leader id is empty")
		zerolog.Ctx(ctx).Error().Err(err).Msg("leader id is empty")
		return TimeoutNowResponse{}, err
	}

	currentTerm, err := n.store.GetCurrentTerm(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("timeout now db err: %s", err.Error())
		return TimeoutNowResponse{}, err
	}

	reject := func() (TimeoutNowResponse, error) {
		return TimeoutNowResponse{Term: uint64(currentTerm), Success: false}, nil
	}

	// A deposed leader's in-flight transfer must not start elections after the
	// cluster has moved on.
	if args.Term < uint64(currentTerm) {
		zerolog.Ctx(ctx).Warn().Msgf("rejecting timeout now from %s: stale term %d < %d", args.LeaderID, args.Term, currentTerm)
		return reject()
	}

	// Only the leader we currently recognise may hand us leadership. Without this
	// any peer could force elections at will, which is a disruption in its own
	// right — the opposite of what pre-vote is for. An empty leaderID means we
	// have not heard from anyone yet, so there is nobody to contradict.
	if leaderID := n.GetLeaderID(); leaderID != "" && leaderID != args.LeaderID {
		zerolog.Ctx(ctx).Warn().Msgf("rejecting timeout now from %s: our leader is %s", args.LeaderID, leaderID)
		return reject()
	}

	// Already leading — there is nothing to transfer. Campaigning here would
	// depose us in favour of ourselves at a higher term, for nothing.
	if n.GetRole() == ServerRole_Leader {
		zerolog.Ctx(ctx).Warn().Msgf("rejecting timeout now from %s: we are already the leader", args.LeaderID)
		return reject()
	}

	// Hand it to whoever owns the election timer. If we are a candidate right now
	// there is no timer goroutine listening, so the signal sits in the size-1
	// buffer and fires the moment becomeFollower restarts one — which is the right
	// outcome either way: a candidate is already trying to become leader, and if
	// it loses, the pending signal makes it retry at once instead of waiting out
	// another randomized timeout.
	n.signalTimeoutNow()

	return TimeoutNowResponse{
		Term:    uint64(currentTerm),
		Success: true,
	}, nil
}
