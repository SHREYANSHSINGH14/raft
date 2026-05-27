package raft

import (
	"context"
	"errors"
	"sync"

	"github.com/rs/zerolog"
)

type ServerRole string

const (
	ServerRole_Follower  ServerRole = "FOLLOWER"
	ServerRole_Candidate ServerRole = "CANDIDATE"
	ServerRole_Leader    ServerRole = "LEADER"
)

type nodeIndexes struct {
	nextIndex  uint
	matchIndex uint
}

// Node is the library entry point. Create one with NewNode, then call Start.
type Node struct {
	ID       string
	Role     ServerRole
	LeaderID string

	transport Transport
	sm        StateMachine
	store     Storage
	cfg       Config

	commitIndex uint
	lastApplied uint

	// only populated when Role == Leader
	nodeIdxs map[string]nodeIndexes

	mu sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc

	// signals the election-timeout goroutine to reset its timer when a valid
	// leader heartbeat or granted vote is received.
	electionTimeoutCh chan struct{}
}

func NewNode(cfg Config, storage Storage, transport Transport, sm StateMachine) *Node {
	return &Node{
		ID:                cfg.ID,
		Role:              ServerRole_Follower,
		transport:         transport,
		sm:                sm,
		store:             storage,
		cfg:               cfg,
		commitIndex:       0,
		lastApplied:       0,
		nodeIdxs:          nil,
		LeaderID:          "",
		electionTimeoutCh: make(chan struct{}, 2),
	}
}

// Start initialises persistent state if missing, waits for quorum, then begins
// the election timer. It blocks until ctx is cancelled.
func (n *Node) Start(ctx context.Context) {
	n.ctx, n.cancel = context.WithCancel(ctx)

	_, err := n.store.GetCurrentTerm(n.ctx)
	if err != nil {
		if !errors.Is(err, ErrNotFound) {
			zerolog.Ctx(n.ctx).Error().Err(err).Msg("error getting current term")
			return
		}
		if err := n.store.SetCurrentTerm(n.ctx, 0); err != nil {
			zerolog.Ctx(n.ctx).Error().Err(err).Msg("error initialising current term")
			return
		}
	}

	_, err = n.store.GetVotedFor(n.ctx)
	if err != nil {
		if !errors.Is(err, ErrNotFound) {
			zerolog.Ctx(n.ctx).Error().Err(err).Msg("error getting voted for")
			return
		}
		if err := n.store.SetVotedFor(n.ctx, ""); err != nil {
			zerolog.Ctx(n.ctx).Error().Err(err).Msg("error initialising voted for")
			return
		}
	}

	zerolog.Ctx(n.ctx).Debug().Msg("Waiting for peers to be up")
	n.waitForQuorum(n.ctx)

	n.startElectionOut(n.ctx)

	<-n.ctx.Done()
}

// Stop cancels the internal context, causing all goroutines to exit cleanly.
func (n *Node) Stop() {
	if n.cancel != nil {
		n.cancel()
	}
}
