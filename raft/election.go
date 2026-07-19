package raft

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"
)

type ElectionResponse struct {
	transitonRole ServerRole
	err           error
}

func (n *Node) startElection(ctx context.Context) {
	go func() {
		electionTime := time.Duration(n.cfg.ElectionMaxMs-n.cfg.ElectionMinMs) * time.Millisecond
		ticker := time.NewTicker(electionTime)

		electionResChan := make(chan ElectionResponse, 1)

		electionContext, cancel := context.WithCancel(ctx)
		go n.election(electionContext, electionResChan)
		for {
			select {
			// if we receive any message on election timeout channel then that means
			// either we received a log from leader or we received a vote response from peer
			// in both cases we should reset the election timeout and start waiting for next timeout
			case <-n.electionTimeoutCh:
				cancel() // cancel the previous election context to stop the previous election goroutine
				n.becomeFollower()
				return

			// if duration of election elapses without reaching a decision then we turn back to follower
			case <-ticker.C:
				cancel()
				n.becomeFollower()
				return

			// if we receive a message on election result channel then that means we have reached a decision in current election
			// and we should transition to the role which is decided by election result
			case res := <-electionResChan:
				if res.err != nil {
					zerolog.Ctx(ctx).Error().Err(res.err).Msg("election error")
				}
				cancel()
				switch res.transitonRole {
				case ServerRole_Leader:
					n.becomeLeader()
				case ServerRole_Follower:
					n.becomeFollower()
				}
				return
			case <-ctx.Done():
				cancel()
				return
			}
		}
	}()
}

func (n *Node) election(ctx context.Context, resCh chan ElectionResponse) {
	var electionRes ElectionResponse
	if n.GetRole() != ServerRole_Candidate {
		err := fmt.Errorf("server is not a candidate cannot start election")
		zerolog.Ctx(ctx).Error().Err(err).Msg(err.Error())
		electionRes.transitonRole = ServerRole_Follower
		electionRes.err = err

		resCh <- electionRes

		return
	}

	currentTerm, err := n.store.GetCurrentTerm(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("election db error: %s", err.Error())
		electionRes.transitonRole = ServerRole_Follower
		electionRes.err = err

		resCh <- electionRes

		return
	}

	newTerm := currentTerm + 1
	err = n.store.SetCurrentTerm(ctx, newTerm)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("election db error: %s", err.Error())
		electionRes.transitonRole = ServerRole_Follower
		electionRes.err = err

		resCh <- electionRes

		return
	}

	err = n.store.SetVotedFor(ctx, n.ID)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("election db error: %s", err.Error())
		electionRes.transitonRole = ServerRole_Follower
		electionRes.err = err

		resCh <- electionRes

		return
	}

	lastLogIndex, err := n.store.GetLastLogIndex(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("election db error: %s", err.Error())
		electionRes.transitonRole = ServerRole_Follower
		electionRes.err = err

		resCh <- electionRes

		return
	}

	var lastLogTerm uint64
	if lastLogIndex > 0 {
		lastLog, err := n.store.GetLogByIndex(ctx, lastLogIndex)
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msgf("election db error: %s", err.Error())
			electionRes.transitonRole = ServerRole_Follower
			electionRes.err = err

			resCh <- electionRes

			return
		}
		lastLogTerm = lastLog.Term
	}

	peerIDs := n.peerIDs()

	requestVoteResponses := make(chan responseRequestVote, len(peerIDs))
	// defer close(requestVoteResponses)
	// 1. closing is the sender's responsibility, and there are multiple senders (one goroutine
	//    per peer) — no single goroutine can safely close without coordinating with others,
	//    and the receiver closing it causes "send on closed channel" panic when remaining
	//    goroutines try to send after election returns early (e.g. on majority reached)
	// 2. the buffer is sized exactly to len(Peers), so every goroutine can send
	//    without blocking even if election() has already returned and nobody is reading
	// 3. every sendRequestVote goroutine exits when the transport returns — there is no
	//    indefinite block, so no goroutine leak
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

	for _, id := range peerIDs {
		// wg.Add(1)
		go n.sendRequestVote(ctx, id, uint64(newTerm), uint64(lastLogIndex), lastLogTerm, requestVoteResponses)
	}

	// wg.Wait()

	// responseReceived := 0
	responsesPending := len(peerIDs)
	var majority int = ((len(peerIDs) + 1) / 2) + 1 // +1 for counting self vote, /2 for getting majority and +1 to round up in case of even number of servers
	votesReceived := 1                                  // we have already voted for ourselves so we start with 1 vote

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
			if res.rpcRes.VoteGranted && n.GetRole() == ServerRole_Candidate {
				votesReceived++
			}
		}
	}

	if votesReceived >= majority {
		resCh <- ElectionResponse{transitonRole: ServerRole_Leader}
		return
	}
	resCh <- ElectionResponse{transitonRole: ServerRole_Follower}
	return
}

type responseRequestVote struct {
	rpcRes RequestVoteResponse
	id     string
	err    error
}

func (n *Node) sendRequestVote(ctx context.Context, peerID string, newTerm, lastLogIndex, lastLogTerm uint64, responseCh chan<- responseRequestVote) {
	rpcReq := RequestVoteArgs{
		Term:         newTerm,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		CandidateID:  n.ID,
	}

	var res responseRequestVote

	rpcRes, err := n.transport.RequestVote(peerID, rpcReq)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("error sending request vote rpc to peer %s: %s", peerID, err.Error())
		res.err = err
		res.id = peerID

		responseCh <- res

		return
	}

	res.rpcRes = rpcRes
	res.id = peerID

	responseCh <- res
}
