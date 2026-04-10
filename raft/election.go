package raft

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/SHREYANSHSINGH14/raft/types"
	"github.com/rs/zerolog"
)

type ElectionResponse struct {
	transitonRole ServerRole
	err           error
}

func (p *Server) startElection(ctx context.Context) {
	go func() {
		electionTime := time.Duration(500 * time.Millisecond) // TODO: Replace with config
		ticker := time.NewTicker(electionTime)

		electionResChan := make(chan ElectionResponse, 1)

		electionContext, cancel := context.WithCancel(ctx)
		go p.election(electionContext, electionResChan)
		for {
			select {
			// if we receive any message on election timeout channel then that means
			// either we received a log from leader or we received a vote response from peer
			// in both cases we should reset the election timeout and start waiting for next timeout
			case <-p.electionTimeoutCh:
				cancel() // cancel the previous election context to stop the previous election goroutine
				p.becomeFollower(ctx)
				return

			// if duration of election elapses without reaching a decision
			// then we cancel the previous election goroutine and start a new election
			// this can happen when there is a network partition and we are not able to reach majority of servers to win the election
			// or when there is a bug in election code and we are not able to reach a decision
			// in both cases we should start a new election to try to reach a decision
			// if there is a bug in election code then starting a new election will not solve the problem but at least it will not block the server indefinitely
			// and we can fix the bug by looking at the logs of multiple election attempts
			case <-ticker.C:
				cancel()
				electionContext, cancel = context.WithCancel(ctx)
				go p.election(electionContext, electionResChan)

			// if we receive a message on election result channel then that means we have reached a decision in current election
			// and we should transition to the role which is decided by election result
			case res := <-electionResChan:
				if res.err != nil {
					zerolog.Ctx(ctx).Error().Err(res.err).Msg("election error")
				}
				switch res.transitonRole {
				case ServerRole_Leader:
					p.becomeLeader(ctx)
				case ServerRole_Follower:
					p.becomeFollower(ctx)
				case ServerRole_Candidate:
					p.becomeCandidate(ctx)
				}
			case <-ctx.Done():
				cancel()
				return
			}
		}
	}()
}

func (p *Server) election(ctx context.Context, resCh chan ElectionResponse) {
	var electionRes ElectionResponse
	if p.getRole() != ServerRole_Candidate {
		err := fmt.Errorf("server is not a candidate cannot start election")
		zerolog.Ctx(ctx).Error().Err(err).Msg(err.Error())
		electionRes.transitonRole = p.getRole()
		electionRes.err = err

		resCh <- electionRes

		return
	}

	currentTerm, err := p.store.GetCurrentTerm(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("election db error: %s", err.Error())
		electionRes.transitonRole = p.getRole()
		electionRes.err = err

		resCh <- electionRes

		return
	}

	newTerm := currentTerm + 1
	err = p.store.SetCurrentTerm(ctx, newTerm)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("election db error: %s", err.Error())
		electionRes.transitonRole = p.getRole()
		electionRes.err = err

		resCh <- electionRes

		return
	}

	err = p.store.SetVotedFor(ctx, p.ID)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("election db error: %s", err.Error())
		electionRes.transitonRole = p.getRole()
		electionRes.err = err

		resCh <- electionRes

		return
	}

	lastLogIndex, err := p.store.GetLastLogIndex(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("election db error: %s", err.Error())
		electionRes.transitonRole = p.getRole()
		electionRes.err = err

		resCh <- electionRes

		return
	}

	lastLog, err := p.store.GetLogByIndex(ctx, lastLogIndex)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("election db error: %s", err.Error())
		electionRes.transitonRole = p.getRole()
		electionRes.err = err

		resCh <- electionRes

		return
	}

	requestVoteResponses := make(chan ResponseRequestVote, len(p.ServerIDRpcUrlMap))
	defer close(requestVoteResponses)

	var wg sync.WaitGroup

	for id, client := range p.ServerIDRpcUrlMap {
		wg.Add(1)
		go sendRequestVote(ctx, &wg, p.getID(), id, client, uint64(newTerm), lastLog.Index, lastLog.Term, requestVoteResponses)
	}

	wg.Wait()

	responseReceived := 0
	var majority int = (len(p.ServerIDRpcUrlMap) / 2) + 1
	votesReceived := 0

	for _ = range len(p.ServerIDRpcUrlMap) {
		res := <-requestVoteResponses
		responseReceived++

		if res.err != nil {
			zerolog.Ctx(ctx).Error().Err(res.err).Msgf("error in request vote rpc response from peer %s", res.id)
			continue
		}

		if uint(res.rpcRes.Term) > newTerm {
			electionRes.transitonRole = ServerRole_Follower
			electionRes.err = nil

			resCh <- electionRes

			return
		}

		if res.rpcRes.VoteGranted && p.getRole() == ServerRole_Candidate {
			votesReceived++
		}

		if votesReceived >= majority {
			electionRes.transitonRole = ServerRole_Leader
			electionRes.err = nil

			resCh <- electionRes

			return
		}
	}

	electionRes.transitonRole = p.getRole()
	electionRes.err = nil

	resCh <- electionRes
}

type ResponseRequestVote struct {
	rpcRes *types.RequestVoteResponse
	id     string
	err    error
}

func sendRequestVote(ctx context.Context, wg *sync.WaitGroup, candidateID, peerID string, client types.RaftRpcClient, newTerm, lastLogIndex, lastLogTerm uint64, responseCh chan<- ResponseRequestVote) { // TODO: change this simple type with proto type
	defer wg.Done()

	rpcCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond) // TODO: Replace with config
	defer cancel()

	rpcReq := &types.RequestVoteArgs{
		Term:         newTerm,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		CandidateId:  candidateID,
	}

	var res ResponseRequestVote

	rpcRes, err := client.RequestVote(rpcCtx, rpcReq)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("error sending request vote rpc to peer %s: %s", peerID, err.Error())
		res.err = err
		res.id = peerID

		responseCh <- res

		return
	}

	res.rpcRes.VoteGranted = rpcRes.VoteGranted
	res.rpcRes.Term = rpcRes.Term
	res.id = peerID

	responseCh <- res
}
