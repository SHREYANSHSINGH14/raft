package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/SHREYANSHSINGH14/raft/types"
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
	cfg   RaftConfig

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

func NewPeer(ctx context.Context, cfg RaftConfig, store types.RaftDBInterface) (*Peer, error) {
	var srv Peer
	srv.ID = cfg.ID
	srv.cfg = cfg
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

	dialOptions := []grpc.DialOption{}
	dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))

	// initialize rpc clients for all other servers
	srv.ServerIDRpcUrlMap = make(map[string]types.RaftRpcClient)
	for id, url := range cfg.Peers {
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
		if !errors.Is(types.ErrNotFound, err) {
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
		if !errors.Is(types.ErrNotFound, err) {
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

	// We run startElectionOut in a separate goroutine and block here on ctx.Done() to keep the server alive.
	// Alternative: run startElectionOut directly on the main goroutine (no goroutine inside it) — it would
	// block forever in its select loop, keeping the process alive without needing <-p.ctx.Done().
	// We prefer the goroutine approach because it keeps Start() extensible — any future work after
	// startElectionOut (e.g. metrics, health checks) would be unreachable if main goroutine was blocked there.

	<-p.ctx.Done()
}
