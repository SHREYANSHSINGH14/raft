package raft

import (
	"context"
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

	// update commit index
	// check which log index has been replicated on majority of servers, using the match index of peers
	matchIndexes := make(map[uint]uint, len(p.ServerIDRpcUrlMap))

	for _, peer := range p.peerIndexes {
		matchIndexes[peer.matchIndex]++
	}

	for idx, count := range matchIndexes {
		idxLog, err := p.store.GetLogByIndex(ctx, idx)
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msgf("error getting log by index %d: %s", idx, err.Error())
			continue
		}

		// only update commit index if the log has been replicated on majority of servers and the log is from current term
		// this is important to make sure that we don't commit any log from previous term which might not be present on the new leader after a leader change
		// read 5.4.2 of raft thesis for more details
		if count >= uint(len(p.ServerIDRpcUrlMap)/2+1) && idx > p.commitIndex && idxLog.Term == uint64(currentTerm) {
			p.SetCommitIndex(idx)
		}
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

	res.rpcRes.Term = rpcRes.Term
	res.rpcRes.Success = rpcRes.Success
	res.id = peerID

	responseCh <- res
}
