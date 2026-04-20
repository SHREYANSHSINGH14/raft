package raft

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/SHREYANSHSINGH14/raft/types"
	"github.com/rs/zerolog"
)

func (p *Peer) startSendLogs(ctx context.Context) {
	heartBeatTime := time.Duration(500 * time.Millisecond) // TODO: replace with config value
	_ = heartBeatTime
	ticker := time.NewTicker(heartBeatTime)
	sendLogCtx, cancel := context.WithCancel(ctx)
	sendLogErrChan := make(chan error, 1)
	for {
		select {
		case <-p.electionTimeoutCh:
			cancel()
			p.becomeFollower(ctx)
			return
		case <-ticker.C:
			// this needs to be non blocking call because if it gets blocked then it won't be able to send logs to peers at regular interval
			// and if there is a bug in sendLogs function which is causing it to get blocked then it will also block the server indefinitely
			// and we won't be able to fix the bug because we won't be able to see the logs of sendLogs function

			// it won't be able to check for electionTimeoutCh while it is blocked in sendLogs function and if there is a bug in sendLogs, then
			// it won't become a follower and it will keep trying to send logs to peers and it will keep getting blocked in sendLogs function and it will keep getting blocked indefinitely
			go p.sendLogs(sendLogCtx, sendLogErrChan)
		case err := <-sendLogErrChan:
			if err != nil {
				zerolog.Ctx(ctx).Error().Err(err).Msg("error sending logs to peers")
			}
		case <-ctx.Done():
			cancel()
			return
		}
	}
}

func (p *Peer) sendLogs(ctx context.Context, errChan chan<- error) {
	currentTerm, err := p.store.GetCurrentTerm(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("send logs db err: %s", err.Error())
		errChan <- err
		return
	}

	var wg sync.WaitGroup
	responseCh := make(chan ResponseAppendLogs, len(p.ServerIDRpcUrlMap))
	peerLogLen := make(map[string]uint)

	for id, client := range p.ServerIDRpcUrlMap {
		nextIdx := p.GetPeerIndex(id).nextIndex

		var prevLog *types.LogEntry
		if nextIdx > 1 {
			prevLog, err = p.store.GetLogByIndex(ctx, nextIdx-1)
			if err != nil {
				zerolog.Ctx(ctx).Error().Err(err).Msgf("send logs db err: %s", err.Error())
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

		peerLogLen[id] = uint(len(logs))

		wg.Add(1)
		go sendAppendLogs(ctx, &wg, p.GetID(), id, client, currentTerm, uint(prevLog.Term), uint(prevLog.Index), p.commitIndex, logs, responseCh)

	}

	wg.Wait()

	for _ = range len(p.ServerIDRpcUrlMap) {
		res := <-responseCh

		if res.err != nil {
			zerolog.Ctx(ctx).Error().Err(res.err).Msgf("error in append logs rpc response from peer %s", res.id)
			continue
		}

		if uint(res.rpcRes.Term) > currentTerm {
			p.becomeFollower(ctx)
			errChan <- nil
			return
		}

		if res.rpcRes.Success {
			p.SetMatchPeerIndex(res.id, p.peerIndexes[res.id].nextIndex+peerLogLen[res.id]-1)
			p.SetNextPeerIndex(res.id, p.peerIndexes[res.id].nextIndex+peerLogLen[res.id])
		} else {
			p.SetNextPeerIndex(res.id, p.peerIndexes[res.id].nextIndex-1)
		}
	}

	lastLogIndex, err := p.store.GetLastLogIndex(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("send logs db err: %s", err.Error())
		errChan <- err
		return
	}

	newCommitIndex := getMajorityMatchIndex(p.peerIndexes, lastLogIndex)
	newCommitLog, err := p.store.GetLogByIndex(ctx, newCommitIndex)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("send logs db err: %s", err.Error())
		errChan <- err
		return
	}

	if newCommitIndex > p.commitIndex && newCommitLog.Term >= uint64(currentTerm) {
		p.commitIndex = newCommitIndex
	}

	errChan <- nil
	return
}

type ResponseAppendLogs struct {
	rpcRes *types.AppendEntriesResponse
	id     string
	err    error
}

func sendAppendLogs(ctx context.Context, wg *sync.WaitGroup, leaderID, peerID string, client types.RaftRpcClient, currentTerm, prevLogTerm, prevLogIndex, leaderCommit uint, logs []*types.LogEntry, responseCh chan<- ResponseAppendLogs) {
	defer wg.Done()

	rpcCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond) // TODO: Replace with config
	defer cancel()

	rpcReq := types.AppendEntriesArgs{
		Term:         uint64(currentTerm),
		LeaderId:     leaderID,
		PrevLogIndex: uint64(prevLogIndex),
		PrevLogTerm:  uint64(prevLogTerm),
		Entries:      logs,
		LeaderCommit: uint64(leaderCommit),
	}

	var res ResponseAppendLogs

	rpcRes, err := client.AppendEntries(rpcCtx, &rpcReq)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("error sending append logs rpc to peer %s: %s", peerID, err.Error())
		res.err = err
		res.id = peerID

		responseCh <- res
		return
	}

	res.rpcRes = rpcRes
	res.id = peerID

	responseCh <- res
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
