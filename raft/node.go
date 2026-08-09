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

const DefaultCatchingUpIdx int64 = -1

// Node is the library entry point. Create one with NewNode, then call Start.
type Node struct {
	ID       string
	Role     ServerRole
	LeaderID string

	transport Transport
	sm        StateMachine
	store     Storage
	cfg       Config

	// configurations holds the latest/committed membership views. `latest` is the
	// live operating configuration and supersedes cfg.Peers, which is now only the
	// bootstrap seed read once in NewNode. Guarded by mu. See raft_config.go.
	configurations configurations

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

	// timeoutNowCh carries the TimeoutNow signal (Ongaro §3.10, leadership
	// transfer): the election-timeout goroutine selects on it and campaigns at
	// once instead of waiting out its timer. Deliberately a signal rather than a
	// context cancellation — the timer goroutine returns on its own and
	// becomeFollower restarts it normally, whereas a cancelled context would stay
	// cancelled and the node could never time out again.
	//
	// Buffered 1 so a sender never blocks; a second signal while one is pending is
	// dropped, because the timer only needs to fire once.
	timeoutNowCh chan struct{}

	// leaderCloseCh is open for exactly as long as this node is leader:
	// becomeLeader creates it, becomeFollower closes it and sets it back to nil.
	// Every Future registered during the term captures it, so a step-down fails the
	// waiters with ErrLeadershipLost instead of leaving them blocked until their
	// contexts expire — the entries we appended may never commit under the new
	// leader, so there is nothing left to wait for. One close answers all of them.
	//
	// Guarded by mu; nil means "not leader", which Future.Wait treats the same as
	// closed. clearLeaderCloseCh is the only correct way to close it.
	leaderCloseCh chan struct{}

	memberAddedCh   chan string
	memberRemovedCh map[string]chan struct{}

	// futureList holds one entry per proposal appended but not yet committed, kept
	// sorted by log index so processFutures can drain it as a prefix.
	//
	// Guarded by commitMu, NOT mu — it is read and written next to commitIndex, by
	// newFuture and processFutures. The role transitions that create and drop it go
	// through initFutureList/clearFutureList for the same reason.
	futureList []*Future

	// when statemachine is taking a snapshot, this flag is set to prevent apply loop from applying new entries and
	//potentially diverging lastApplied index from the snapshot index
	snapShotInProgress atomic.Bool

	// Set every time a snapshot is taken (leader) or installed (follower): the
	// last-included index and term of the latest snapshot. The term is what lets
	// HandleAppendEntries accept prevLogIndex == snapshotLatestIndex as a valid
	// consistency anchor even though that entry is compacted (see logTermAt).
	snapshotLatestIndex uint
	snapshotLatestTerm  uint

	// catchingUpIdx is the retain floor a catching-up member publishes: while it is
	// not DefaultCatchingUpIdx, the compactor must not delete logs at or above it.
	// catchUpSignal wakes the snapshot goroutine parked in waitForCatchUpFloor each
	// time the floor changes (see setCatchingUpIdx). Buffered size 1: a pending wake
	// is enough because the waiter always re-Loads the latest floor.
	catchingUpIdx atomic.Int64
	catchUpSignal chan struct{}
}

func NewNode(cfg Config, storage Storage, transport Transport, sm StateMachine) *Node {
	node := Node{
		ID:                  cfg.ID,
		Role:                ServerRole_Follower,
		transport:           transport,
		sm:                  sm,
		store:               storage,
		cfg:                 cfg,
		commitIndex:         0,
		lastApplied:         0,
		LeaderID:            "",
		electionTimeoutCh:   make(chan struct{}, 2),
		timeoutNowCh:        make(chan struct{}, 1),
		mu:                  sync.Mutex{},
		commitMu:            sync.Mutex{},
		clientMu:            sync.Mutex{},
		snapshotLatestIndex: 0,
		catchUpSignal:       make(chan struct{}, 1),
	}

	// No member is catching up at startup. The zero value of atomic.Int64 is 0,
	// which is a valid log index, so it must be set to the inactive sentinel
	// explicitly — otherwise the compactor would think a floor at index 0 is held.
	node.catchingUpIdx.Store(DefaultCatchingUpIdx)

	node.commitCond = *sync.NewCond(&node.commitMu)

	// Seed both configuration views from the caller-supplied bootstrap peers at
	// index 0. Until the first config entry is appended, latest == committed.
	//
	// cfg.Peers is the caller's view of the OTHER servers — server/ builds it from
	// the peer file with its own id skipped — but configurations.latest holds the
	// whole membership, so this node is added here if the caller left it out. It
	// joins as a Voter: a bootstrapping server always votes, and anything else is
	// reached through AddMember.
	bootstrap := clonePeers(cfg.Peers)
	if _, present := bootstrap[cfg.ID]; !present {
		bootstrap[cfg.ID] = Peer{PeerState: PeerState_Voter}
	}

	node.configurations = configurations{
		latest:         clonePeers(bootstrap),
		latestIndex:    0,
		committed:      clonePeers(bootstrap),
		committedIndex: 0,
	}

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
	n.startSnapshotLoop(n.ctx)

	<-n.ctx.Done()
}

// Stop cancels the internal context, causing all goroutines to exit cleanly.
func (n *Node) Stop() {
	if n.cancel != nil {
		n.cancel()
	}
}
