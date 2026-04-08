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

	for id, url := range p.ServerIDRpcUrlMap {
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
		go sendAppendLogs(ctx, &wg, p.getID(), id, url, currentTerm, uint(prevLog.Term), uint(prevLog.Index), p.commitIndex, logs, responseCh)

	}

	wg.Wait()

	for _ = range len(p.ServerIDRpcUrlMap) {
		res := <-responseCh
		if res.term > currentTerm {
			p.becomeFollower(ctx)
			return nil
		}

		if res.success {
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
		if count >= uint(len(p.ServerIDRpcUrlMap)/2+1) && idx > p.commitIndex {
			p.setCommitIndex(idx)
		}
	}

	return nil
}

type ResponseAppendLogs struct {
	term    uint
	success bool
	id      string
}

func sendAppendLogs(ctx context.Context, wg *sync.WaitGroup, leaderID, peerID, url string, currentTerm, prevLogTerm, prevLogIndex, leaderCommit uint, logs []*types.LogEntry, responseCh chan<- ResponseAppendLogs) {
	defer wg.Done()
	// send append entries
	var res ResponseAppendLogs
	res.term = 1
	res.success = true
	res.id = peerID

	responseCh <- res
}
