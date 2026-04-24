package raft

import (
	"context"
	"fmt"
	"time"

	"github.com/SHREYANSHSINGH14/raft/config"
	"github.com/SHREYANSHSINGH14/raft/types"
	"github.com/rs/zerolog"
)

type ElectionResponse struct {
	transitonRole ServerRole
	err           error
}

func (p *Peer) startElection(ctx context.Context) {
	go func() {
		electionTime := time.Duration(config.GetConfig().ElectionDurationMs) * time.Millisecond // TODO: Replace with config
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
				cancel()
				switch res.transitonRole {
				case ServerRole_Leader:
					p.becomeLeader(ctx)
				case ServerRole_Follower:
					p.becomeFollower(ctx)
				case ServerRole_Candidate:
					p.becomeCandidate(ctx)
				}
				return
			case <-ctx.Done():
				cancel()
				return
			}
		}
	}()
}

func (p *Peer) election(ctx context.Context, resCh chan ElectionResponse) {
	var electionRes ElectionResponse
	if p.GetRole() != ServerRole_Candidate {
		err := fmt.Errorf("server is not a candidate cannot start election")
		zerolog.Ctx(ctx).Error().Err(err).Msg(err.Error())
		electionRes.transitonRole = p.GetRole()
		electionRes.err = err

		resCh <- electionRes

		return
	}

	currentTerm, err := p.store.GetCurrentTerm(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("election db error: %s", err.Error())
		electionRes.transitonRole = p.GetRole()
		electionRes.err = err

		resCh <- electionRes

		return
	}

	newTerm := currentTerm + 1
	err = p.store.SetCurrentTerm(ctx, newTerm)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("election db error: %s", err.Error())
		electionRes.transitonRole = p.GetRole()
		electionRes.err = err

		resCh <- electionRes

		return
	}

	err = p.store.SetVotedFor(ctx, p.ID)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("election db error: %s", err.Error())
		electionRes.transitonRole = p.GetRole()
		electionRes.err = err

		resCh <- electionRes

		return
	}

	lastLogIndex, err := p.store.GetLastLogIndex(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("election db error: %s", err.Error())
		electionRes.transitonRole = p.GetRole()
		electionRes.err = err

		resCh <- electionRes

		return
	}

	var lastLog *types.LogEntry
	if lastLogIndex > 0 {
		lastLog, err = p.store.GetLogByIndex(ctx, lastLogIndex)
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msgf("election db error: %s", err.Error())
			electionRes.transitonRole = p.GetRole()
			electionRes.err = err

			resCh <- electionRes

			return
		}
	} else {
		lastLog = &types.LogEntry{
			Index: 0,
			Term:  0,
			Data:  []byte{},
			Type:  types.EntryType_ENTRY_TYPE_NO_OP,
		}
	}

	requestVoteResponses := make(chan ResponseRequestVote, len(p.ServerIDRpcUrlMap))
	// defer close(requestVoteResponses)
	// 1. closing is the sender's responsibility, and there are multiple senders (one goroutine
	//    per peer) — no single goroutine can safely close without coordinating with others,
	//    and the receiver closing it causes "send on closed channel" panic when remaining
	//    goroutines try to send after election returns early (e.g. on majority reached)
	// 2. the buffer is sized exactly to len(ServerIDRpcUrlMap), so every goroutine can send
	//    without blocking even if election() has already returned and nobody is reading
	// 3. every sendRequestVote goroutine exits within RPCTimeoutMs (50ms default) via
	//    context timeout — there is no indefinite block, so no goroutine leak
	// 4. once all goroutines finish sending and election() returns, the channel has no
	//    remaining references and is garbage collected automatically

	// var wg sync.WaitGroup
	// wg was used to wait for all sendRequestVote goroutines to finish before processing responses.
	// this caused a critical bug: wg.Wait() has no awareness of context cancellation, so when startElection
	// called cancel() and spawned a new election goroutine (on ticker fire or electionTimeoutCh), the old
	// election goroutine was still alive — blocked at wg.Wait() until all RPCs timed out naturally.
	// since electionResChan has a buffer of 1, when the stale goroutine eventually unblocked and tried to
	// send its result, the channel was already full from the newer election — causing the goroutine to block
	// permanently and leak. this happened on every election timeout cycle, leading to millions of leaked
	// goroutines, term inflation (each stale goroutine incremented the term via SetCurrentTerm), and
	// eventually an OOM crash.
	// the fix is to remove wg entirely and process responses as they arrive using a select loop with
	// ctx.Done(). this way, when cancel() is called, the goroutine exits immediately from the select
	// without ever trying to send on resCh — so no leak, no blocking, no cascading term explosion.

	for id, client := range p.ServerIDRpcUrlMap {
		// wg.Add(1)
		go sendRequestVote(ctx, p.GetID(), id, client, uint64(newTerm), lastLog.Index, lastLog.Term, requestVoteResponses)
	}

	// wg.Wait()

	// responseReceived := 0
	responsesPending := len(p.ServerIDRpcUrlMap)
	var majority int = ((len(p.ServerIDRpcUrlMap) + 1) / 2) + 1 // +1 for counting self vote, /2 for getting majority and +1 to round up in case of even number of servers
	votesReceived := 1                                          // we have already voted for ourselves so we start with 1 vote

	for responsesPending > 0 {
		select {
		case <-ctx.Done():
			return // clean exit, no send needed — caller already moved on

		case res := <-requestVoteResponses:
			responsesPending--
			if res.err != nil {
				continue
			}
			if uint(res.rpcRes.Term) > newTerm {
				resCh <- ElectionResponse{transitonRole: ServerRole_Follower}
				return
			}
			if res.rpcRes.VoteGranted && p.GetRole() == ServerRole_Candidate {
				votesReceived++
				if votesReceived >= majority {
					resCh <- ElectionResponse{transitonRole: ServerRole_Leader}
					return
				}
			}
		}
	}

	resCh <- ElectionResponse{transitonRole: p.GetRole()}
}

type ResponseRequestVote struct {
	rpcRes *types.RequestVoteResponse
	id     string
	err    error
}

func sendRequestVote(ctx context.Context, candidateID, peerID string, client types.RaftRpcClient, newTerm, lastLogIndex, lastLogTerm uint64, responseCh chan<- ResponseRequestVote) { // TODO: change this simple type with proto type
	// defer wg.Done()

	rpcCtx, cancel := context.WithTimeout(ctx, time.Duration(config.GetConfig().RPCTimeoutMs)*time.Millisecond) // TODO: Replace with config
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

	fmt.Printf("Received RequestVote response from peer %s: VoteGranted=%v, Term=%d\n", peerID, rpcRes.VoteGranted, rpcRes.Term)

	res.rpcRes = rpcRes
	res.id = peerID

	responseCh <- res
}
