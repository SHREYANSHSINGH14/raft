package raft

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"math/big"
	"net"
	"os"
	"sync"
	"time"

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
	raftMu sync.Mutex

	ctx        context.Context
	cancelFunc context.CancelFunc

	// this channel is called whenever a log is received from leader or a vote is granted to reset election timeout
	// election timeout triggers role transition from follower to candidate so if we receive a log or grant vote then we should reset the election timeout
	// by passing an empty struct to this channel, the election timeout goroutine will reset the timer and start waiting for next timeout
	electionTimeoutCh chan struct{}

	// embedding the unimplemented server to make sure if we add any new rpc in future then we will get compile error if we forget to implement that rpc
	types.UnimplementedRaftRpcServer

	url string
}

var _ types.RaftRpcServer = &Peer{}

func NewPeer(ctx context.Context, cfg Config) (*Peer, error) {
	store, err := db.NewStore(ctx, cfg.DBDir)
	if err != nil {
		fmt.Println("error while initializing db store")
		return nil, err
	}

	logLevel := getLogLevel(cfg.LogLevel)
	logger := zerolog.New(os.Stdout).With().Timestamp().Caller().Logger()
	zerolog.DefaultContextLogger = &logger
	zerolog.SetGlobalLevel(logLevel)

	var srv Peer
	srv.ID = cfg.ID
	srv.Role = ServerRole_Follower
	srv.store = store
	srv.commitIndex = 0
	srv.lastApplied = 0
	srv.peerIndexes = nil
	srv.LeaderID = ""
	srv.url = fmt.Sprintf("http://%s:%s", cfg.BaseURL, cfg.Port)

	// buffered channel to avoid blocking in case of multiple logs received in short time, 2 is just to be safe, 1 should be enought since we
	// will get logs from leader one by one and we just need to reset the election timeout for that log, if we receive multiple logs in short time then it means there is some issue with the leader
	// and in that case we can just reset the election timeout for the first log and ignore the rest of the logs because if there is some issue with the leader then it will be removed in next election
	// and we will get a new leader
	srv.electionTimeoutCh = make(chan struct{}, 2)
	srv.ctx, srv.cancelFunc = context.WithCancel(ctx)
	srv.ctx = logger.WithContext(srv.ctx)

	dialOptions := []grpc.DialOption{}
	dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))

	// initialize rpc clients for all other servers
	srv.ServerIDRpcUrlMap = make(map[string]types.RaftRpcClient)
	for id, url := range cfg.ServerIDS {
		if id == cfg.ID {
			continue
		}
		conn, err := grpc.NewClient(url, dialOptions...)
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

	// Start grpc server
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", 3001))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	types.RegisterRaftRpcServer(grpcServer, p)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	// This goroutine will wait for the context to be done and then gracefully stop the grpc server and cancel the server context to stop all other goroutines
	// IMPORTANT to gracefully stop the grpc server to avoid any panics in case of any ongoing rpc calls when the server is stopped
	// Also canceling the server context will stop all other goroutines like election timeout and send logs goroutine to avoid any
	// unwanted role transitions or rpc calls when the server is stopped

	// This is an important concept to remeber that whenever we have some long running goroutines in our server then we should always have a way to stop those goroutines gracefully when
	// the server is stopped to avoid any unwanted behavior and also to free up the resources used by those goroutines
	go func() {
		<-p.ctx.Done()
		grpcServer.GracefulStop()
	}()

	p.startElectionOut(p.ctx)

	// Since startElectionOut is a separate goroutine, we need to block the main goroutine to keep the server running,
	// we can block it by waiting for the server context to be done, which will happen when the server is stopped

	// We run startElectionOut in a separate goroutine and block here on ctx.Done() to keep the server alive.
	// Alternative: run startElectionOut directly on the main goroutine (no goroutine inside it) — it would
	// block forever in its select loop, keeping the process alive without needing <-p.ctx.Done().
	// We prefer the goroutine approach because it keeps Start() extensible — any future work after
	// startElectionOut (e.g. metrics, health checks) would be unreachable if main goroutine was blocked there.

	zerolog.Ctx(p.ctx).Debug().Str("id", p.ID).Str("role", string(p.Role)).Str("listen_address", p.url).Msg("server started")

	<-p.ctx.Done()
}

// -------------------------------------------
// Role transition functions
// These functions are called when we want to transition from one role to another role
// They also starts the necessary goroutines for that role like election timeout for follower and send logs for leader
// -------------------------------------------

func (p *Peer) becomeFollower(ctx context.Context) {
	p.setRole(ServerRole_Follower)
	p.startElectionOut(ctx)
}

func (p *Peer) becomeCandidate(ctx context.Context) {
	p.setRole(ServerRole_Candidate)
	p.startElection(ctx)
}

func (p *Peer) becomeLeader(ctx context.Context) {
	p.setRole(ServerRole_Leader)
	p.peerIndexes = make(map[string]PeerIndexes)

	lastIndex, err := p.store.GetLastLogIndex(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("error getting latest log index")
		return
	}

	for id, _ := range p.ServerIDRpcUrlMap {
		p.peerIndexes[id] = PeerIndexes{
			nextIndex:  lastIndex + 1,
			matchIndex: 0,
		}
	}
	p.startSendLogs(ctx)
}

// -------------------------------------------
// Since being a follower is default role, we only need to start election timeout goroutine when we become follower
// For candidate we need to start election and for leader we need to start sending logs to followers
// find functions for candidate and leader in respective files
// -------------------------------------------

func (p *Peer) startElectionOut(ctx context.Context) {
	go func() {
		duration, err := rand.Int(rand.Reader, big.NewInt(300))
		if err != nil {
			zerolog.Ctx(context.Background()).Error().Err(err).Msg("error getting random number for duration")
			return
		}

		timeOut := time.Duration((duration.Int64() + 150) * int64(time.Millisecond))
		ticker := time.NewTicker(timeOut)

		for {
			select {
			case <-p.electionTimeoutCh:
				ticker.Reset(timeOut)
				continue
			case <-ticker.C:
				ticker.Stop()
				p.becomeCandidate(ctx)
				return
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
}

// -------------------------------------------
// Below are some helper functions to get and set server state like role, peer indexes, commit index etc
// These functions are thread safe and should be used whenever we want to read or write these state variables
// -------------------------------------------

func (p *Peer) setRole(role ServerRole) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Role = role
	return
}

func (p *Peer) getRole() ServerRole {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.Role
}

func (p *Peer) getID() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ID
}

func (p *Peer) getPeerIndex(id string) PeerIndexes {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.peerIndexes[id]
}

func (p *Peer) setNextPeerIndex(id string, idx uint) {
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

func (p *Peer) setMatchPeerIndex(id string, idx uint) {
	p.mu.Lock()
	defer p.mu.Unlock()

	peer, ok := p.peerIndexes[id]
	if !ok {
		peer = PeerIndexes{}
	}
	peer.matchIndex = idx
	p.peerIndexes[id] = peer
}

func (p *Peer) setCommitIndex(idx uint) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.commitIndex = idx
}

func (p *Peer) getCommitIndex() uint {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.commitIndex
}

func (p *Peer) setLeaderID(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.LeaderID = id
}

func (p *Peer) getLeaderID() string {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.LeaderID
}

func getLogLevel(level string) zerolog.Level {
	switch level {
	case "info":
		return zerolog.InfoLevel
	case "debug":
		return zerolog.DebugLevel
	case "warn":
		return zerolog.WarnLevel
	case "error":
		return zerolog.ErrorLevel
	case "fatal":
		return zerolog.FatalLevel
	case "panic":
		return zerolog.PanicLevel
	case "disable":
		return zerolog.Disabled
	default:
		return zerolog.DebugLevel
	}
}
