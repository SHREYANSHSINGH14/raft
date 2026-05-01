package raft

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/SHREYANSHSINGH14/raft/config"
	"github.com/SHREYANSHSINGH14/raft/db"
	"github.com/SHREYANSHSINGH14/raft/types"
	"github.com/cockroachdb/pebble"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ServerRole string

const (
	ServerRole_Follower  ServerRole = "FOLLOWER"
	ServerRole_Candidate ServerRole = "CANDIDATE"
	ServerRole_Leader    ServerRole = "LEADER"
)

type PeerIndexes struct {
	nextIndex  uint
	matchIndex uint
}

type Peer struct {
	ID                string
	Role              ServerRole
	ServerIDRpcUrlMap map[string]types.RaftRpcClient
	LeaderID          string

	store types.RaftDBInterface

	commitIndex uint
	lastApplied uint

	// below fields will be bootstrapped as nil
	// only gets initialized when role is LEADER
	peerIndexes map[string]PeerIndexes

	mu sync.Mutex

	// This second mutex is to protect from concurrent calls at RequestVote and AppendEntries rpc handlers, since both of these handlers can be called concurrently and
	// they both read and write to the same state variables like current term, voted for, leader id etc so to avoid any race conditions we need to have a separate mutex for these rpc handlers
	// we can't use the same mutex for both client calls and internal state changes because it will lead to deadlocks in case of any long running operations in the rpc handlers like db calls
	// or network calls to other servers, so we need to have a separate mutex for these rpc handlers to avoid any deadlocks

	// This will make calls sequential to RequestVote and AppendEntries handlers, which will strip away concurrency advantage of grpc but for this specific implementation we value correctness
	// and that is btw the protocol requirement of raft that the server should handle rpc calls sequentially to avoid any race conditions

	// Other ways like erlang actors model can also be used to handle concurrent calls to rpc handlers sequentially without using mutex but for simplicity we are using mutex here
	// PHASE 2: Use actors model to handle concurrent calls to rpc handlers sequentially without using mutex, this will improve the performance of the server by allowing concurrent calls to rpc
	// handlers without blocking each other and also it will make the code more clean and easy to understand by avoiding the use of mutex and locks (DISCARDED)

	// Mutex also does the same thing of handling concurrent calls to rpc handlers sequentially, by putting goroutines to a FIFO queue and allowing only one goroutine to access the critical section
	// at a time, since in Raft we'd only be having small number of peers so advantage of handling backpressure, cancellation, queue depth in actors model over mutex will not be significant in this case
	// raftMu sync.Mutex (DEPRECATED) moved the rpc server handling to a separate server struct to avoid the need of this mutex, since now the rpc handlers will be called on the server struct which will have its
	// own mutex to handle concurrent calls to rpc handlers sequentially without blocking the main peer struct which will handle the internal state changes and role transitions, this will avoid any deadlocks (DEPRECATED)

	ctx context.Context

	// this channel is called whenever a log is received from leader or a vote is granted to reset election timeout
	// election timeout triggers role transition from follower to candidate so if we receive a log or grant vote then we should reset the election timeout
	// by passing an empty struct to this channel, the election timeout goroutine will reset the timer and start waiting for next timeout
	electionTimeoutCh chan struct{}
}

func NewPeer(ctx context.Context, cfg config.Config) (*Peer, error) {
	store, err := db.NewStore(ctx, cfg.DBDir)
	if err != nil {
		fmt.Println("error while initializing db store")
		return nil, err
	}

	var srv Peer
	srv.ID = cfg.ID
	srv.Role = ServerRole_Follower
	srv.store = store
	srv.commitIndex = 0
	srv.lastApplied = 0
	srv.peerIndexes = nil
	srv.LeaderID = ""

	// buffered channel to avoid blocking in case of multiple logs received in short time, 2 is just to be safe, 1 should be enought since we
	// will get logs from leader one by one and we just need to reset the election timeout for that log, if we receive multiple logs in short time then it means there is some issue with the leader
	// and in that case we can just reset the election timeout for the first log and ignore the rest of the logs because if there is some issue with the leader then it will be removed in next election
	// and we will get a new leader
	srv.electionTimeoutCh = make(chan struct{}, 2)
	srv.ctx = ctx
	srv.ctx = srv.ctx

	dialOptions := []grpc.DialOption{}
	dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))

	// initialize rpc clients for all other servers
	srv.ServerIDRpcUrlMap = make(map[string]types.RaftRpcClient)
	for id, url := range cfg.ServerIDS {
		if id == cfg.ID {
			continue
		}
		conn, err := grpc.NewClient("dns:///"+url, dialOptions...)
		if err != nil {
			return nil, fmt.Errorf("error creating grpc client for server %s: %w", id, err)
		}
		srv.ServerIDRpcUrlMap[id] = types.NewRaftRpcClient(conn)
	}

	return &srv, nil
}

func (p *Peer) Start() {
	_, err := p.store.GetCurrentTerm(p.ctx)
	if err != nil {
		if !errors.Is(pebble.ErrNotFound, err) {
			zerolog.Ctx(p.ctx).Error().Err(err).Msg("error getting current term")
			return
		}
		err := p.store.SetCurrentTerm(p.ctx, 0)
		if err != nil {
			zerolog.Ctx(p.ctx).Error().Err(err).Msg("error initializing current term")
			return
		}
	}

	_, err = p.store.GetVotedFor(p.ctx)
	if err != nil {
		if !errors.Is(pebble.ErrNotFound, err) {
			zerolog.Ctx(p.ctx).Error().Err(err).Msg("error getting vote for")
			return
		}
		err := p.store.SetVotedFor(p.ctx, "")
		if err != nil {
			zerolog.Ctx(p.ctx).Error().Err(err).Msg("error initializing voted for")
			return
		}
	}

	zerolog.Ctx(p.ctx).Debug().Msg("Waiting for peers to up")
	p.waitForQuorum(p.ctx)

	p.startElectionOut(p.ctx)

	// Since startElectionOut is a separate goroutine, we need to block the main goroutine to keep the server running,
	// we can block it by waiting for the server context to be done, which will happen when the server is stopped

	// We run startElectionOut in a separate goroutine and block here on ctx.Done() to keep the server alive.
	// Alternative: run startElectionOut directly on the main goroutine (no goroutine inside it) — it would
	// block forever in its select loop, keeping the process alive without needing <-p.ctx.Done().
	// We prefer the goroutine approach because it keeps Start() extensible — any future work after
	// startElectionOut (e.g. metrics, health checks) would be unreachable if main goroutine was blocked there.

	<-p.ctx.Done()
}

// -------------------------------------------
// Role transition functions
// These functions are called when we want to transition from one role to another role
// They also starts the necessary goroutines for that role like election timeout for follower and send logs for leader
// -------------------------------------------

func (p *Peer) becomeFollower() {
	zerolog.Ctx(p.ctx).Info().Msg("becoming follower")
	p.SetRole(ServerRole_Follower)
	p.startElectionOut(p.ctx)
}

func (p *Peer) becomeCandidate() {
	zerolog.Ctx(p.ctx).Info().Msg("becoming candidate")
	p.SetRole(ServerRole_Candidate)
	p.startElection(p.ctx)
}

func (p *Peer) becomeLeader() {
	zerolog.Ctx(p.ctx).Info().Msg("becoming leader")
	p.SetRole(ServerRole_Leader)
	p.SetLeaderID("")
	p.peerIndexes = make(map[string]PeerIndexes)

	lastIndex, err := p.store.GetLastLogIndex(p.ctx)
	if err != nil {
		zerolog.Ctx(p.ctx).Error().Err(err).Msg("error getting latest log index")
		p.becomeFollower()
		return
	}

	for id, _ := range p.ServerIDRpcUrlMap {
		p.peerIndexes[id] = PeerIndexes{
			nextIndex:  lastIndex + 1,
			matchIndex: 0,
		}
	}
	p.startSendLogs(p.ctx)
}

// -------------------------------------------
// Since being a follower is default role, we only need to start election timeout goroutine when we become follower
// For candidate we need to start election and for leader we need to start sending logs to followers
// find functions for candidate and leader in respective files
// -------------------------------------------

func (p *Peer) startElectionOut(ctx context.Context) {
	go func() {
		duration, err := rand.Int(rand.Reader, big.NewInt(int64(config.GetConfig().ElectionDurationMs)))
		if err != nil {
			zerolog.Ctx(context.Background()).Error().Err(err).Msg("error getting random number for duration")
			return
		}

		timeOut := time.Duration((duration.Int64() + int64(config.GetConfig().ElectionMinMs)) * int64(time.Millisecond))
		ticker := time.NewTicker(timeOut)

		for {
			select {
			case <-p.electionTimeoutCh:
				ticker.Reset(timeOut)
				continue
			case <-ticker.C:
				ticker.Stop()
				p.becomeCandidate()
				return
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
}

// waitForQuorum blocks until a majority of peers are reachable over gRPC.
// It is called once at startup before the election timer begins, preventing
// spurious elections during the window when containers are starting up and
// gRPC connections between peers are not yet established.
//
// Strategy: send a RequestVote with term=0 to each peer. Term 0 is always
// rejected by any peer (since any initialized peer has term >= 1), but a
// rejection is still a valid gRPC response — it proves the connection is up.
// A timeout or connection error means the peer is not yet reachable.
//
// The function retries every 500ms until majority responds, then returns.
// If the context is cancelled (e.g. server shutdown), it returns immediately.
//
// Note: this is only meaningful on first startup. When startElectionOut is
// called again after a role transition (becomeFollower), the cluster is already
// running so waitForQuorum returns on the first iteration.

func (p *Peer) waitForQuorum(ctx context.Context) {
	majority := (len(p.ServerIDRpcUrlMap)+1)/2 + 1

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		reachable := 0
		for _, client := range p.ServerIDRpcUrlMap {
			// ping each peer with a real RequestVote — if it responds (even rejection) the connection is up
			rpcCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
			_, err := client.RequestVote(rpcCtx, &types.RequestVoteArgs{
				CandidateId: p.ID,
				Term:        0, // term 0 — always rejected but proves connectivity
			})
			cancel()
			if err == nil {
				reachable++
			}
		}

		if reachable >= majority {
			zerolog.Ctx(ctx).Info().
				Int("reachable", reachable).
				Int("majority", majority).
				Msg("quorum reachable, starting election timer")
			return
		}

		zerolog.Ctx(ctx).Debug().
			Int("reachable", reachable).
			Int("majority", majority).
			Msg("waiting for quorum...")

		time.Sleep(500 * time.Millisecond)
	}
}

// -------------------------------------------
// Below are some helper functions to get and set server state like role, peer indexes, commit index etc
// These functions are thread safe and should be used whenever we want to read or write these state variables
// -------------------------------------------

func (p *Peer) SetRole(role ServerRole) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Role = role
	return
}

func (p *Peer) GetRole() ServerRole {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.Role
}

func (p *Peer) GetID() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ID
}

func (p *Peer) GetPeerIndex(id string) PeerIndexes {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.peerIndexes[id]
}

func (p *Peer) SetNextPeerIndex(id string, idx uint) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// map returns a copy of value so if we do
	// p.peerIndexes[id].nextIndex = idx
	// it won't work coz we change value of copy
	// not the original thing so to change the
	// actual value assign a new struct
	// Better to use pointers if frequent change
	// but for learning we keep it like this
	peer, ok := p.peerIndexes[id] // copy
	if !ok {
		peer = PeerIndexes{}
	}
	peer.nextIndex = idx     // modify
	p.peerIndexes[id] = peer // write back
}

func (p *Peer) SetMatchPeerIndex(id string, idx uint) {
	p.mu.Lock()
	defer p.mu.Unlock()

	peer, ok := p.peerIndexes[id]
	if !ok {
		peer = PeerIndexes{}
	}
	peer.matchIndex = idx
	p.peerIndexes[id] = peer
}

func (p *Peer) SetCommitIndex(idx uint) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if idx < p.commitIndex {
		return
	}
	p.commitIndex = idx
}

func (p *Peer) GetCommitIndex() uint {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.commitIndex
}

func (p *Peer) SetLeaderID(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.LeaderID = id
}

func (p *Peer) GetLeaderID() string {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.LeaderID
}

func (p *Peer) GetCurrentTerm(ctx context.Context) (uint, error) {
	return p.store.GetCurrentTerm(ctx)
}
