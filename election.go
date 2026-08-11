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

	// The log state is read before the term bump because the pre-vote round needs
	// it too — it asks the same up-to-date question the real vote does. Reading it
	// once and reusing it for both rounds also guarantees the two rounds describe
	// the same log, which a re-read between them would not.
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

	// voterPeers is who we ask (everyone but us); voterCount is what a majority is
	// taken over (including us). A node the cluster has removed is absent from
	// voterCount and must not campaign at all — it cannot win, and trying would
	// disturb a cluster it is no longer part of.
	voterPeers := n.voterPeerIDs()
	voterCount := n.voterCount()

	if !n.isVoter() {
		zerolog.Ctx(ctx).Warn().Msg("not a voter in the live configuration, abandoning election")
		electionRes.transitonRole = ServerRole_Follower

		resCh <- electionRes

		return
	}

	newTerm := currentTerm + 1

	// Pre-vote round (Ongaro §9.6) — the gate in front of everything below.
	//
	// Ask whether we *could* win at newTerm before doing anything that costs the
	// cluster something. Everything after this point is irreversible in the sense
	// that matters: SetCurrentTerm persists a higher term, and the RequestVote
	// fan-out spreads it to every peer, deposing a perfectly healthy leader. A
	// node partitioned away from the cluster would otherwise loop through that
	// sequence on every election timeout, inflating the term the whole time, and
	// disrupt the cluster the moment it rejoined.
	//
	// Losing the pre-vote is a normal outcome, not an error: we simply go back to
	// being a follower, having changed nothing — no term written, no vote spent,
	// and no peer's state touched. The randomized election timer then decides when
	// to try again.
	if !n.preVote(ctx, voterPeers, voterCount, uint64(newTerm), uint64(lastLogIndex), lastLogTerm) {
		zerolog.Ctx(ctx).Debug().Msgf("pre-vote for term %d not granted by a majority, staying follower", newTerm)
		electionRes.transitonRole = ServerRole_Follower

		resCh <- electionRes

		return
	}

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
	requestVoteResponses := make(chan responseRequestVote, len(voterPeers))
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

	for _, id := range voterPeers {
		// wg.Add(1)
		go n.sendRequestVote(ctx, id, uint64(newTerm), uint64(lastLogIndex), lastLogTerm, requestVoteResponses)
	}

	// wg.Wait()

	// responseReceived := 0
	responsesPending := len(voterPeers)
	majority := majoritySize(voterCount) // voters incl. self, which is in the configuration
	votesReceived := 1                   // we have already voted for ourselves so we start with 1 vote

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

// preVote runs the pre-vote round: it asks every voting peer whether it would
// grant a vote at nextTerm and reports whether a majority would. Nothing here
// writes to the store or changes any peer's state — that is the entire point of
// the round, and it is what makes losing one free.
//
// The shape mirrors the real fan-out below it deliberately: one goroutine per
// voter, responses collected on a channel buffered to exactly the number of
// senders, and no wg.Wait. Same reasons, spelled out at the requestVoteResponses
// comment — a WaitGroup ignores context cancellation and couples every peer to
// the slowest one, which is what leaked goroutines and inflated terms in Bug 1.
//
// Self is counted as a granted pre-vote (we would obviously vote for ourselves),
// matching majoritySize's assumption that the caller is itself a voter. A cluster
// with no voting peers therefore passes on our own vote alone, exactly as the
// real election does.
func (n *Node) preVote(ctx context.Context, voterPeers []string, voterCount int, nextTerm, lastLogIndex, lastLogTerm uint64) bool {
	preVoteResponses := make(chan responsePreVote, len(voterPeers))

	for _, id := range voterPeers {
		go n.sendPreVote(ctx, id, nextTerm, lastLogIndex, lastLogTerm, preVoteResponses)
	}

	responsesPending := len(voterPeers)
	majority := majoritySize(voterCount)
	granted := 1 // ourselves

	if granted >= majority {
		return true // single-voter cluster: nobody to ask
	}

	for responsesPending > 0 {
		select {
		case <-ctx.Done():
			// The election was cancelled out from under us (timer fired, or the
			// node is shutting down). Abandoning here is free precisely because
			// the round changed nothing.
			return false

		case res := <-preVoteResponses:
			responsesPending--
			if res.err != nil {
				// Unreachable peer. Treated as a withheld pre-vote rather than a
				// failure: a minority being down must not stop a legitimate
				// candidate, and a majority being down means we could not have won
				// the real election either.
				continue
			}

			// A peer answering with a term beyond the one we are probing knows
			// something we do not — we cannot win at nextTerm. Note we do NOT adopt
			// that term: a pre-vote response is not authority to move, and
			// following it would reintroduce the disruption pre-vote prevents. We
			// just stop, and learn the real term from the next AppendEntries.
			//
			// This is a fast path, not a guarantee: a majority can grant and trip
			// the early return below before a higher-term response lands. That is
			// fine — the real election checks terms again, and a peer being ahead
			// does not stop a candidate a majority already agreed to.
			if res.rpcRes.Term > nextTerm {
				zerolog.Ctx(ctx).Debug().Msgf("pre-vote: peer %s reports term %d beyond our probe of %d", res.id, res.rpcRes.Term, nextTerm)
				return false
			}

			if res.rpcRes.VoteGranted {
				granted++
				if granted >= majority {
					// Enough. The outstanding goroutines still finish and send into
					// the buffered channel, which is sized for all of them, so
					// returning early strands nobody.
					return true
				}
			}
		}
	}

	return false
}

type responsePreVote struct {
	rpcRes PreVoteResponse
	id     string
	err    error
}

func (n *Node) sendPreVote(ctx context.Context, peerID string, nextTerm, lastLogIndex, lastLogTerm uint64, responseCh chan<- responsePreVote) {
	rpcReq := PreVoteArgs{
		Term:         nextTerm,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		CandidateID:  n.ID,
	}

	var res responsePreVote
	deadLineCtx, cancel := context.WithTimeout(ctx, time.Duration(n.cfg.RPCTimeoutMs)*time.Millisecond)
	defer cancel()

	rpcRes, err := n.transport.PreVote(deadLineCtx, peerID, rpcReq)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("error sending pre vote rpc to peer %s: %s", peerID, err.Error())
		res.err = err
		res.id = peerID

		responseCh <- res

		return
	}

	res.rpcRes = rpcRes
	res.id = peerID

	responseCh <- res
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
	deadLineCtx, cancel := context.WithTimeout(ctx, time.Duration(n.cfg.RPCTimeoutMs)*time.Millisecond)
	defer cancel()

	rpcRes, err := n.transport.RequestVote(deadLineCtx, peerID, rpcReq)
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
