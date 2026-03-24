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
	srv.nextIndex = nil
	srv.matchIndex = nil
	srv.electionTimeoutCh = make(chan struct{})

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

	p.startElectionOut()
}

func (p *Server) becomeFollower() {
	p.setRole(ServerRole_Follower)
	p.startElectionOut()
}

func (p *Server) becomeCandidate(ctx context.Context) {
	p.setRole(ServerRole_Candidate)
	p.startElection(ctx)
}

func (p *Server) startElectionOut() {
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
				// startElection
				return
			}
		}
	}()
}

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
