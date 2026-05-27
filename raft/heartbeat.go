package raft

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/rs/zerolog"
)

// responsible for starting goroutines for each peer, those routines will heartbeats to each node
// so each node gets it's own orchestrator and this also starts a routine that updates commitIndex
// based on matchIndexes periodically
func (n *Node) startSendLogs(ctx context.Context) {
	heartbeatCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// BUG (fixed): previously, sendLogs called n.becomeFollower() directly when it saw a higher
	// term in an AppendEntries response. that caused two problems:
	//   1. startSendLogs never got the signal to exit — heartbeatCtx was never cancelled,
	//      so all sendLogsPerPeer goroutines kept running as zombies even after the node
	//      transitioned to follower.
	//   2. becomeFollower started a new election timer, but the heartbeat goroutines were
	//      still alive, still sending RPCs as leader. the node was simultaneously follower
	//      and leader.
	//   3. next election win called becomeLeader again, spawning a second set of heartbeat
	//      goroutines on top of the existing ones. goroutines accumulated every leadership
	//      cycle — a leak that grew unboundedly.
	//
	// FIX: same pattern used in election.go — child goroutines never drive role transitions
	// directly. instead, sendLogs signals stepDownCh and returns. startSendLogs (the
	// orchestrator) reads that signal, cancels heartbeatCtx (which stops all sendLogsPerPeer
	// goroutines via ctx.Done()), then calls becomeFollower exactly once. clean shutdown,
	// single owner of the transition.
	//
	// why buffer size = len(peers)?
	// context cancellation doesn't preempt running code — it only fires on the next select
	// or ctx check. so after startSendLogs reads the first signal and cancels heartbeatCtx,
	// other sendLogs goroutines that are already past sendAppendLogs and sitting at the
	// higher-term check can still reach the stepDownCh send before noticing cancellation.
	// worst case: all peers respond with higher term simultaneously — len(peers) senders.
	// buffering all of them means no sender ever blocks. the unread signals are GC'd when
	// startSendLogs returns and the channel goes out of scope.
	stepDownCh := make(chan struct{}, len(n.cfg.Peers))
	updateCommitIndexCh := make(chan struct{}, len(n.cfg.Peers))

	started := make(chan struct{}, len(n.cfg.Peers))

	for _, k := range n.cfg.Peers {
		go func(id string) {
			started <- struct{}{}
			n.sendLogsPerPeer(heartbeatCtx, id, stepDownCh, updateCommitIndexCh)
		}(k)
	}

	// drain and count — blocks until all goroutines have actually started
	for i := 0; i < len(n.cfg.Peers); i++ {
		<-started
	}

	go n.startCommitIndexUpdater(heartbeatCtx, updateCommitIndexCh)

	select {
	case <-stepDownCh:
		// a peer responded with a higher term — we are no longer the legitimate leader.
		// cancel heartbeatCtx first so all sendLogsPerPeer goroutines stop, then
		// transition. order matters: cancel before becomeFollower so no zombie heartbeats
		// race against the new follower state.
		cancel()
		n.becomeFollower()
		return
	case <-n.electionTimeoutCh:
		cancel()
		n.becomeFollower()
		return
	case <-ctx.Done():
		cancel()
		return
	}
}

func (n *Node) startCommitIndexUpdater(ctx context.Context, updateCommitCh <-chan struct{}) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-updateCommitCh:
			for len(updateCommitCh) > 0 {
				<-updateCommitCh
			}
			lastLogIndex, err := n.store.GetLastLogIndex(ctx)
			if err != nil {
				zerolog.Ctx(ctx).Error().Err(err).Msgf("commit index updater db error: %s", err.Error())

				continue
			}

			commitIndex := getMajorityMatchIndex(n.nodeIdxs, lastLogIndex)
			if commitIndex == 0 {

				continue
			}

			commitIndexLog, err := n.store.GetLogByIndex(ctx, commitIndex)
			if err != nil {
				zerolog.Ctx(ctx).Error().Err(err).Msgf("commit index updater db error: %s", err.Error())

				continue
			}

			currentTerm, err := n.store.GetCurrentTerm(ctx)
			if err != nil {
				zerolog.Ctx(ctx).Error().Err(err).Msgf("commit index updater db error: %s", err.Error())

				continue
			}

			if commitIndexLog.Term == uint64(currentTerm) {
				n.SetCommitIndex(commitIndex)
			}
		}
	}
}

// per peer heartbeat orchestrator
func (n *Node) sendLogsPerPeer(ctx context.Context, peerID string, stepDownCh, updateCommitIndexCh chan<- struct{}) {
	heartBeatTime := time.Duration(n.cfg.HeartbeatMs) * time.Millisecond
	ticker := time.NewTicker(heartBeatTime)
	sendLogErrChan := make(chan error, 1)
	sendLogCtx, cancel := context.WithCancel(ctx)
	inFlight := false
	for {
		select {
		case <-ticker.C:
			if !inFlight {
				go n.sendLogs(sendLogCtx, peerID, sendLogErrChan, stepDownCh, updateCommitIndexCh)
				inFlight = true
			}
		case err := <-sendLogErrChan:
			if err != nil {
				zerolog.Ctx(ctx).Error().Err(err).Msgf("send logs to peer %s failed, will retry on next heartbeat", peerID)
			}
			inFlight = false
		case <-ctx.Done():
			cancel()
			return
		}
	}
}

func (n *Node) sendLogs(ctx context.Context, peerID string, errChan chan<- error, stepDownCh, updateCommitIndexCh chan<- struct{}) {
	currentTerm, err := n.store.GetCurrentTerm(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("send logs db err: %s", err.Error())
		errChan <- err
		return
	}

	nextIdx := n.GetPeerIndex(peerID).nextIndex

	var prevLog LogEntry
	if nextIdx > 1 {
		prevLog, err = n.store.GetLogByIndex(ctx, nextIdx-1)
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msgf("send logs db err at index:%d : %s", nextIdx-1, err.Error())
			errChan <- err
			return
		}
	}
	// nextIdx <= 1: prevLog is zero value {Index:0, Term:0} — correct for the first entry

	logs, err := n.store.GetLogs(ctx, &nextIdx, nil)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("send logs db err: %s", err.Error())
		errChan <- err
		return
	}

	peerLogLen := uint(len(logs))

	res, err := n.sendAppendLogs(ctx, peerID, currentTerm, uint(prevLog.Term), uint(prevLog.Index), n.commitIndex, logs)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("error in append logs rpc response from peer %s", peerID)
		errChan <- err
		return
	}

	fmt.Printf("\nCurrentTerm: %d\nRes Term: %d", currentTerm, res.Term)
	if uint(res.Term) > currentTerm {
		// BUG (fixed): we used to call n.becomeFollower() directly here.
		// that meant a child goroutine was driving the role transition, bypassing
		// the orchestrator entirely. heartbeatCtx was never cancelled, leaving all
		// sendLogsPerPeer goroutines running as zombies. see startSendLogs comment
		// for the full failure chain.
		//
		// now we just signal and return. startSendLogs owns the transition.
		stepDownCh <- struct{}{}
		errChan <- nil
		return
	}

	if res.Success {
		if peerLogLen > 0 {
			currentNext := n.GetPeerIndex(peerID).nextIndex
			n.SetMatchPeerIndex(peerID, currentNext+peerLogLen-1)
			n.SetNextPeerIndex(peerID, currentNext+peerLogLen)
			updateCommitIndexCh <- struct{}{}
		}
		// peerLogLen == 0 means pure heartbeat — follower is consistent, nothing to advance
	} else {
		currentNext := n.GetPeerIndex(peerID).nextIndex
		if currentNext > 1 {
			n.SetNextPeerIndex(peerID, currentNext-1)
		}
	}

	errChan <- nil
	return
}

func (n *Node) sendAppendLogs(ctx context.Context, peerID string, currentTerm, prevLogTerm, prevLogIndex, leaderCommit uint, logs []LogEntry) (AppendEntriesResponse, error) {
	rpcReq := AppendEntriesArgs{
		Term:         uint64(currentTerm),
		LeaderID:     n.ID,
		PrevLogIndex: uint64(prevLogIndex),
		PrevLogTerm:  uint64(prevLogTerm),
		Entries:      logs,
		LeaderCommit: uint64(leaderCommit),
	}

	return n.transport.AppendEntries(peerID, rpcReq)
}

// Sort all matchIndexes (peers + self) in descending order.
// In the sorted array, matchIndexes[i] means at least i+1 servers have replicated up to that index,
// because all servers at positions 0..i have matchIndex >= matchIndexes[i].
// So matchIndexes[majorityCount-1] is the highest index replicated on a majority of servers.
// Example 1 (no duplicates): n2:5, n3:3, n4:4, n5:6, self:7 → sorted [7,6,5,4,3], majorityCount=3 → matchIndexes[2]=5
// Example 2 (with duplicates): n2:6, n3:5, n4:6, n5:5, self:7 → sorted [7,6,6,5,5], majorityCount=3 → matchIndexes[2]=6
func getMajorityMatchIndex(nodeIdxs map[string]nodeIndexes, selfLastIndex uint) uint {
	var matchIndexes []uint

	for _, peer := range nodeIdxs {
		matchIndexes = append(matchIndexes, peer.matchIndex)
	}

	matchIndexes = append(matchIndexes, selfLastIndex)

	slices.SortFunc(matchIndexes, func(a, b uint) int {
		if a > b {
			return -1
		} else if a < b {
			return 1
		}
		return 0
	})

	majorityCount := (len(nodeIdxs)+1)/2 + 1 // +1 is for self
	return matchIndexes[majorityCount-1]
}
