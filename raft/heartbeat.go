package raft

import (
	"context"
	"slices"
	"time"

	"github.com/SHREYANSHSINGH14/raft/config"
	"github.com/SHREYANSHSINGH14/raft/types"
	"github.com/rs/zerolog"
)

// responsible for starting goroutines for each peer, those routines will heartbeats to each node
// so each node gets it's own orchestrator and this also starts a routine that updates commitIndex
// based on matchIndexes periodically
func (p *Peer) startSendLogs(ctx context.Context) {
	heartbeatCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	started := make(chan struct{}, len(p.ServerIDRpcUrlMap))

	for k := range p.ServerIDRpcUrlMap {
		go func(id string) {
			started <- struct{}{}
			p.sendLogsPerPeer(heartbeatCtx, id)
		}(k)
	}

	// drain and count — blocks until all goroutines have actually started
	for i := 0; i < len(p.ServerIDRpcUrlMap); i++ {
		<-started
	}

	go p.startCommitIndexUpdater(heartbeatCtx)

	select {
	case <-p.electionTimeoutCh:
		cancel()
		p.becomeFollower(ctx)
		return
	case <-ctx.Done():
		cancel()
		return
	}
}

func (p *Peer) startCommitIndexUpdater(ctx context.Context) {
	sleepDuration := time.Duration(config.GetConfig().CommitIndexUpdaterSleepS) * time.Second

	sleep := func() bool {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(sleepDuration):
			return true
		}
	}

	for {
		lastLogIndex, err := p.store.GetLastLogIndex(ctx)
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msgf("commit index updater db error: %s", err.Error())
			if !sleep() {
				return
			}
			continue
		}

		commitIndex := getMajorityMatchIndex(p.peerIndexes, lastLogIndex)
		if commitIndex == 0 {
			if !sleep() {
				return
			}
			continue
		}

		commitIndexLog, err := p.store.GetLogByIndex(ctx, commitIndex)
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msgf("commit index updater db error: %s", err.Error())
			if !sleep() {
				return
			}
			continue
		}

		currentTerm, err := p.store.GetCurrentTerm(ctx)
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msgf("commit index updater db error: %s", err.Error())
			if !sleep() {
				return
			}
			continue
		}

		if commitIndexLog.Term == uint64(currentTerm) {
			p.SetCommitIndex(commitIndex)
		}

		if !sleep() {
			return
		}
	}
}

// per peer heartbeat orchestrator
func (p *Peer) sendLogsPerPeer(ctx context.Context, peerID string) {
	heartBeatTime := time.Duration(time.Duration(config.GetConfig().HeartbeatMs) * time.Millisecond)
	ticker := time.NewTicker(heartBeatTime)
	sendLogErrChan := make(chan error, 1)
	sendLogCtx, cancel := context.WithCancel(ctx)
	inFlight := false
	for {
		select {
		case <-ticker.C:
			if !inFlight {
				go p.sendLogs(sendLogCtx, peerID, sendLogErrChan)
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

func (p *Peer) sendLogs(ctx context.Context, peerID string, errChan chan<- error) {
	currentTerm, err := p.store.GetCurrentTerm(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("send logs db err: %s", err.Error())
		errChan <- err
		return
	}

	client := p.ServerIDRpcUrlMap[peerID]
	nextIdx := p.GetPeerIndex(peerID).nextIndex

	var prevLog *types.LogEntry
	if nextIdx > 1 {
		prevLog, err = p.store.GetLogByIndex(ctx, nextIdx-1)
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msgf("send logs db err at index:%d : %s", nextIdx-1, err.Error())
			errChan <- err
			return
		}
	} else {
		prevLog = &types.LogEntry{
			Index: 0,
			Term:  0,
			Data:  []byte{},
			Type:  types.EntryType_ENTRY_TYPE_NO_OP,
		}
	}

	logs, err := p.store.GetLogs(ctx, &nextIdx, nil)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("send logs db err: %s", err.Error())
		errChan <- err
		return
	}

	peerLogLen := uint(len(logs))

	res, err := sendAppendLogs(ctx, p.GetID(), peerID, client, currentTerm, uint(prevLog.Term), uint(prevLog.Index), p.commitIndex, logs)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("error in append logs rpc response from peer %s", peerID)
		errChan <- err
		return

	}

	if uint(res.Term) > currentTerm {
		p.becomeFollower(ctx)
		errChan <- nil
		return
	}

	if res.Success {
		p.SetMatchPeerIndex(peerID, p.peerIndexes[peerID].nextIndex+peerLogLen-1)
		p.SetNextPeerIndex(peerID, p.peerIndexes[peerID].nextIndex+peerLogLen)
	} else {
		p.SetNextPeerIndex(peerID, p.peerIndexes[peerID].nextIndex-1)
	}

	errChan <- nil
	return
}

func sendAppendLogs(ctx context.Context, leaderID, peerID string, client types.RaftRpcClient, currentTerm, prevLogTerm, prevLogIndex, leaderCommit uint, logs []*types.LogEntry) (*types.AppendEntriesResponse, error) {
	rpcCtx, cancel := context.WithTimeout(ctx, time.Duration(config.GetConfig().RPCTimeoutMs)*time.Millisecond)
	defer cancel()

	rpcReq := types.AppendEntriesArgs{
		Term:         uint64(currentTerm),
		LeaderId:     leaderID,
		PrevLogIndex: uint64(prevLogIndex),
		PrevLogTerm:  uint64(prevLogTerm),
		Entries:      logs,
		LeaderCommit: uint64(leaderCommit),
	}

	return client.AppendEntries(rpcCtx, &rpcReq)
}

// Intution behind this function
// We need to find the log index which is replicated on majority of servers, because we can only commit logs which are replicated on majority of servers
// So the lowest match index would always be replicated to all so its safe to take that as the commit index, but that is sub optimal
// Lets take an example, n2: 5, n3: 3, n4: 4, n5: 6, self: 7 (these are match index per node)
// here replication frequency per matchIndex would look like 3: 5, 4: 4, 5: 3, 6: 2, 7: 1
// so if we take the lowest match index which is 3, then we can only commit logs till index 3, but we can actually commit logs till index 5 because logs till index 5 are replicated on majority of servers (n2, n3, n4)
// so we need to find the highest log index which is replicated on majority of servers and that would be our commit index
// To do that we take all the matchIndex and sort them in descending order which would look like [7, 6, 5, 4, 3], and there replication frequency would look like 7: 1, 6: 2, 5: 3, 4: 4, 3: 5
// lets take another example where matchIndexes are [7,6,6,5,5,5,4,3] and there replication frequency would be 7: 1, 6: 3, 5: 6, 4: 7, 3: 8
// here we can see a pattern that a matchIndex's replication frequency is always equal to its last occurence index + 1 in descending sorted matchIndex array
// we create a map of matchIndex to its replication frequency using this pattern and then we iterate over the matchIndexes (without duplicates) in descending order and check if its replication frequency is greater than
// or equal to majority count, if it is then we return that matchIndex as the commit index because that would be the highest log index which is replicated on majority of servers
func getMajorityMatchIndex(peerIndexes map[string]PeerIndexes, selfLastIndex uint) uint {
	var matchIndexes []uint

	for _, peer := range peerIndexes {
		matchIndexes = append(matchIndexes, peer.matchIndex)
	}

	matchIndexes = append(matchIndexes, selfLastIndex)

	// sort the match indexes in descending order
	slices.Sort(matchIndexes)
	slices.Reverse(matchIndexes)

	matchIndexesFrequency := make(map[uint]int)

	for i, idx := range matchIndexes {
		matchIndexesFrequency[idx] = i + 1
	}

	matchIndexes = slices.Compact(matchIndexes)

	majorutyCount := (len(peerIndexes)+1)/2 + 1 // +1 is for self

	for _, idx := range matchIndexes {
		if matchIndexesFrequency[idx] >= majorutyCount {
			return idx
		}
	}

	return 0
}
