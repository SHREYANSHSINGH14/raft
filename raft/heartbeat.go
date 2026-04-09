package raft

import (
	"context"
	"sync"
	"time"

	"github.com/SHREYANSHSINGH14/raft/types"
	"github.com/rs/zerolog"
)

func (p *Server) startSendLogs(ctx context.Context) {
	heartBeatTime := time.Duration(500 * time.Millisecond) // TODO: replace with config value
	_ = heartBeatTime
	ticker := time.NewTicker(heartBeatTime)
	sendLogCtx, cancel := context.WithCancel(ctx)
	for {
		select {
		case <-p.electionTimeoutCh:
			cancel()
			p.becomeFollower(ctx)
			return
		case <-ticker.C:
			err := p.sendLogs(sendLogCtx)
			if err != nil {
				zerolog.Ctx(ctx).Error().Err(err).Msg("error sending logs")
			}
		}
	}
}

func (p *Server) sendLogs(ctx context.Context) error {
	currentTerm, err := p.store.GetCurrentTerm(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("send logs db err: %+w", err)
		return err
	}

	var wg sync.WaitGroup
	responseCh := make(chan ResponseAppendLogs, len(p.ServerIDRpcUrlMap))
	peerLogLen := make(map[string]uint)

	for id, client := range p.ServerIDRpcUrlMap {
		nextIdx := p.getPeerIndex(id).nextIndex

		prevLog, err := p.store.GetLogByIndex(ctx, nextIdx-1)
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msgf("send logs db err: %+w", err)
			return err
		}

		logs, err := p.store.GetLogs(ctx, &nextIdx, nil)
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msgf("send logs db err: %+w", err)
			return err
		}

		peerLogLen[id] = uint(len(logs))

		wg.Add(1)
		go sendAppendLogs(ctx, &wg, p.getID(), id, client, currentTerm, uint(prevLog.Term), uint(prevLog.Index), p.commitIndex, logs, responseCh)

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
			return nil
		}

		if res.rpcRes.Success {
			p.setMatchPeerIndex(res.id, p.peerIndexes[res.id].nextIndex+peerLogLen[res.id]-1)
			p.setNextPeerIndex(res.id, p.peerIndexes[res.id].nextIndex+peerLogLen[res.id])
		} else {
			p.setNextPeerIndex(res.id, p.peerIndexes[res.id].nextIndex-1)
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
			zerolog.Ctx(ctx).Error().Err(err).Msgf("error getting log by index %d: %+w", idx, err)
			continue
		}

		// only update commit index if the log has been replicated on majority of servers and the log is from current term
		// this is important to make sure that we don't commit any log from previous term which might not be present on the new leader after a leader change
		// read 5.4.2 of raft thesis for more details
		if count >= uint(len(p.ServerIDRpcUrlMap)/2+1) && idx > p.commitIndex && idxLog.Term == uint64(currentTerm) {
			p.setCommitIndex(idx)
		}
	}

	return nil
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
