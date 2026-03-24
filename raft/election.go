package raft

import (
	"context"
	"fmt"
	"sync"
	"time"

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
			case <-p.electionTimeoutCh:
				cancel()
				// becomefollower
				return
			case <-ticker.C:
				cancel()
				electionContext, cancel = context.WithCancel(ctx)
				go p.election(electionContext, electionResChan)
			case res := <-electionResChan:
				if res.err != nil {

				}
				switch res.transitonRole {
				case ServerRole_Leader:

				case ServerRole_Follower:

				case ServerRole_Candidate:
				}
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
	_ = lastLogIndex

	requestVoteResponses := make(chan RequestResponse, len(p.ServerIDRpcUrlMap))
	defer close(requestVoteResponses)

	var wg sync.WaitGroup

	for id, url := range p.ServerIDRpcUrlMap {
		wg.Add(1)
		go sendRequestVote(ctx, &wg, id, url, requestVoteResponses)
	}

	wg.Wait()

	responseReceived := 0
	var majority int = (len(p.ServerIDRpcUrlMap) / 2) + 1
	votesReceived := 0

	for _ = range len(p.ServerIDRpcUrlMap) {
		res := <-requestVoteResponses
		responseReceived++

		if res.term > newTerm {
			electionRes.transitonRole = ServerRole_Follower
			electionRes.err = nil

			resCh <- electionRes

			return
		}

		if res.voteGranted && p.getRole() == ServerRole_Candidate {
			votesReceived++
		}

		if votesReceived >= majority {
			electionRes.transitonRole = ServerRole_Leader
			electionRes.err = nil

			resCh <- electionRes

			return
		}
	}
}

type RequestResponse struct {
	voteGranted bool
	term        uint
	id          string
}

func sendRequestVote(ctx context.Context, wg *sync.WaitGroup, peerID string, url string, responseCh chan<- RequestResponse) { // TODO: change this simple type with proto type
	defer wg.Done()
	//  send RPC request
	var res RequestResponse
	res.voteGranted = true
	res.term = 1 // change with actual value
	res.id = peerID

	responseCh <- res
}
