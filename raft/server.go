package raft

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/SHREYANSHSINGH14/raft/db"
	"github.com/SHREYANSHSINGH14/raft/types"
	"github.com/cockroachdb/pebble"
	"github.com/rs/zerolog"
)

type ServerRole string

const (
	ServerRole_Follower  ServerRole = "FOLLOWER"
	ServerRole_Candidate ServerRole = "CANDIDATE"
	ServerRole_Leader    ServerRole = "LEADER"
)

type Config struct {
	ID        string
	ServerIDS map[string]string
	DBDir     string
	LogLevel  string
}

type Server struct {
	ID                string
	Role              ServerRole
	ServerIDRpcUrlMap map[string]string

	store types.RaftDBInterface

	commitIndex uint
	lastApplied uint

	// below fields will be bootstrapped as nil
	// only gets initialized when role is LEADER
	nextIndex  map[string]uint
	matchIndex map[string]uint

	mu sync.Mutex

	peerIndexes map[string]PeerIndexes

	mu         sync.Mutex
	ctx        context.Context
	cancelFunc context.CancelFunc

	// this channel is called whenever a log is received from leader or a vote is granted to reset election timeout
	// election timeout triggers role transition from follower to candidate so if we receive a log or grant vote then we should reset the election timeout
	// by passing an empty struct to this channel, the election timeout goroutine will reset the timer and start waiting for next timeout
	electionTimeoutCh chan struct{}
}

func NewServer(ctx context.Context, cfg Config) (*Server, error) {
	store, err := db.NewStore(ctx, cfg.DBDir)
	if err != nil {
		fmt.Println("error while initializing db store")
		return nil, err
	}

	logLevel := getLogLevel(cfg.LogLevel)
	zerolog.SetGlobalLevel(logLevel)

	var srv Server
	srv.ID = cfg.ID
	srv.Role = ServerRole_Follower
	srv.ServerIDRpcUrlMap = cfg.ServerIDS
	srv.store = store
	srv.commitIndex = 0
	srv.lastApplied = 0
	srv.peerIndexes = nil
	srv.electionTimeoutCh = make(chan struct{})
	srv.ctx, srv.cancelFunc = context.WithCancel(ctx)

	return &srv, nil
}

func (p *Server) Start(ctx context.Context) {
	_, err := p.store.GetCurrentTerm(ctx)
	if err != nil {
		if !errors.Is(pebble.ErrNotFound, err) {
			zerolog.Ctx(ctx).Error().Err(err).Msg("error getting current term")
			return
		}
		err := p.store.SetCurrentTerm(ctx, 0)
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msg("error initializing current term")
			return
		}
	}

	_, err = p.store.GetVotedFor(ctx)
	if err != nil {
		if !errors.Is(pebble.ErrNotFound, err) {
			zerolog.Ctx(ctx).Error().Err(err).Msg("error getting vote for")
			return
		}
		err := p.store.SetVotedFor(ctx, "")
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msg("error initializing voted for")
			return
		}
	}

	p.startElectionOut(ctx)
}

// -------------------------------------------
// Role transition functions
// These functions are called when we want to transition from one role to another role
// They also starts the necessary goroutines for that role like election timeout for follower and send logs for leader
// -------------------------------------------

func (p *Server) becomeFollower(ctx context.Context) {
	p.setRole(ServerRole_Follower)
	p.startElectionOut(ctx)
}

func (p *Server) becomeCandidate(ctx context.Context) {
	p.setRole(ServerRole_Candidate)
	p.startElection(ctx)
}

func (p *Server) becomeLeader(ctx context.Context) {
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

func (p *Server) startElectionOut(ctx context.Context) {
	go func() {
		duration, err := rand.Int(rand.Reader, big.NewInt(100))
		if err != nil {
			zerolog.Ctx(context.Background()).Error().Err(err).Msg("error getting random number for duration")
			return
		}

		timeOut := time.Duration(duration.Int64() * int64(time.Millisecond))
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
			}
		}
	}()
}

// -------------------------------------------
// Below are some helper functions to get and set server state like role, peer indexes, commit index etc
// These functions are thread safe and should be used whenever we want to read or write these state variables
// -------------------------------------------

func (p *Server) setRole(role ServerRole) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Role = role
	return
}

func (p *Server) getRole() ServerRole {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.Role
}

func (p *Server) getID() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ID
}

func (p *Server) getPeerIndex(id string) PeerIndexes {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.peerIndexes[id]
}

func (p *Server) setNextPeerIndex(id string, idx uint) {
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

func (p *Server) setMatchPeerIndex(id string, idx uint) {
	p.mu.Lock()
	defer p.mu.Unlock()

	peer, ok := p.peerIndexes[id]
	if !ok {
		peer = PeerIndexes{}
	}
	peer.matchIndex = idx
	p.peerIndexes[id] = peer
}

func (p *Server) setCommitIndex(idx uint) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.commitIndex = idx
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
