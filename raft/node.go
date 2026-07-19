package raft

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog"
)

type ServerRole string

const (
	ServerRole_Follower  ServerRole = "FOLLOWER"
	ServerRole_Candidate ServerRole = "CANDIDATE"
	ServerRole_Leader    ServerRole = "LEADER"
)

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

	mu sync.Mutex
	// commitMu guards commitIndex reads/writes in the apply loop and Propose waiters.
	// Kept separate from mu because sync.Cond.Wait() holds its lock while sleeping —
	// if commitCond used mu, the apply loop sleeping in Wait() would block every
	// internal goroutine that needs mu (election, heartbeat, role transitions).
	// SetCommitIndex updates commitIndex under mu, then broadcasts on commitCond
	// without holding mu — the two mutexes are intentionally independent.
	commitMu sync.Mutex

	// clientMu guards exposed methods that may be called concurrently by users of the library, like Propose, HandleAppendEntries, and HandleRequestVote. This is separate from mu because it doesn't need to be held for internal operations like the election timer or apply loop, and we want to avoid unnecessary blocking of those.
	clientMu sync.Mutex

	// commitCond notifies the apply loop and Propose waiters when commitIndex advances.
	// Wait() must be called with commitMu held. Internally, sync.Cond maintains a list
	// of blocked goroutines — shared memory. Before sleeping, a goroutine must register
	// itself on that list, which is the "ticket". The lock ensures ticket assignment and
	// lock release are atomic — if they weren't, a Signal() or Broadcast() could fire
	// after the goroutine decided to wait but before it got its ticket. The goroutine
	// would be on no list, Signal() or Broadcast() would walk the list and miss it,
	// and it would sleep forever with nothing to wake it. By holding the lock during
	// ticket assignment, we guarantee: by the time Signal() or Broadcast() walk the
	// waiter list, any goroutine that decided to wait is already registered on it.
	// Signal() and Broadcast() themselves require no lock — they only read the list.
	commitCond sync.Cond

	ctx    context.Context
	cancel context.CancelFunc

	// signals the election-timeout goroutine to reset its timer when a valid
	// leader heartbeat or granted vote is received.
	electionTimeoutCh chan struct{}

	// when statemachine is taking a snapshot, this flag is set to prevent apply loop from applying new entries and
	//potentially diverging lastApplied index from the snapshot index
	snapShotInProgress atomic.Bool
}

func NewNode(cfg Config, storage Storage, transport Transport, sm StateMachine) *Node {
	node := Node{
		ID:                cfg.ID,
		Role:              ServerRole_Follower,
		transport:         transport,
		sm:                sm,
		store:             storage,
		cfg:               cfg,
		commitIndex:       0,
		lastApplied:       0,
		LeaderID:          "",
		electionTimeoutCh: make(chan struct{}, 2),
		mu:                sync.Mutex{},
		commitMu:          sync.Mutex{},
		clientMu:          sync.Mutex{},
	}

	node.commitCond = *sync.NewCond(&node.commitMu)
	return &node
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
	n.startApplyLoop(n.ctx)

	<-n.ctx.Done()
}

// Stop cancels the internal context, causing all goroutines to exit cleanly.
func (n *Node) Stop() {
	if n.cancel != nil {
		n.cancel()
	}
}
