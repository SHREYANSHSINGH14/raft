package raft

import (
	"context"
	"encoding/json"
	"errors"
	"os"
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
	// commitMu guards commitIndex and futureList. Kept separate from mu because the
	// apply loop holds it while evaluating its wait condition, and a shared lock
	// would block every internal goroutine that needs mu (election, heartbeat, role
	// transitions) for as long as the loop had nothing to do. The two mutexes are
	// intentionally independent, and neither is ever taken while holding the other.
	commitMu sync.Mutex

	// clientMu guards exposed methods that may be called concurrently by users of the library, like Propose, HandleAppendEntries, and HandleRequestVote. This is separate from mu because it doesn't need to be held for internal operations like the election timer or apply loop, and we want to avoid unnecessary blocking of those.
	clientMu sync.Mutex

	// commitCh wakes the apply loop when commitIndex advances or a snapshot ends.
	// This replaced a sync.Cond: a Cond cannot be selected on, so shutdown needed a
	// broadcast on every cancellation path, whereas the loop can now select over
	// commitCh and ctx.Done() together.
	//
	// **Buffered 1, and written only through signalCommit, which never blocks.** The
	// signal is a level, not a queue — it says "state moved, go look", and the loop
	// re-reads commitIndex and snapShotInProgress when it wakes, so a second queued
	// signal would tell it nothing the first did not. One slot is therefore enough,
	// and dropping a signal into a full buffer loses nothing. Sizing it larger only
	// buys spurious wake-ups; making the send block reintroduces the deadlock in
	// signalCommit's comment.
	commitCh chan struct{}

	// fatalCh is closed exactly once, by setFatal, when this node has hit a local
	// failure that leaves its state machine permanently behind the log. fatalErr
	// holds the cause and is guarded by mu.
	//
	// Closed rather than sent on, so every waiter sees it and a caller that arrives
	// late still does — the same reason context.Done() is a closed channel. The
	// library takes no action of its own beyond stopping the apply loop; deciding
	// what a broken replica should do is the caller's, and Fatal documents why.
	fatalCh   chan struct{}
	fatalOnce sync.Once
	fatalErr  error

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

	node.commitCh = make(chan struct{}, 1)
	node.fatalCh = make(chan struct{})

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

	err = n.restore(n.ctx)
	if err != nil {
		zerolog.Ctx(n.ctx).Error().Err(err).Msg("error restoring state machine from snapshot")
		return
	}

	zerolog.Ctx(n.ctx).Debug().Msg("Waiting for peers to be up")
	n.waitForQuorum(n.ctx)

	zerolog.Ctx(n.ctx).Debug().MsgFunc(func() string {
		peers := n.peersSnapshot()
		ids := make([]string, 0, len(peers))
		for id := range peers {
			ids = append(ids, id)
		}
		// Sorted, because map order is random and two boots of the same cluster
		// should produce diffable output.
		slices.Sort(ids)

		str := "Bootstrapped peers:\n"
		for _, id := range ids {
			p := peers[id]
			str += fmt.Sprintf("  %-12s state=%-8s next=%d match=%d\n", id, p.PeerState, p.NextIndex, p.MatchIndex)
		}
		return str
	})
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

// restore rebuilds the state that is derived from disk but not itself persisted:
// commitIndex, the snapshot boundary, and the two configuration views.
//
// It deliberately does NOT restore the state machine in the normal case. This
// library assumes a durable state machine, so its state is already on disk — not
// only everything below the snapshot index, but every entry appended after it and
// applied since, because applyEntries calls Apply before SetLastApplied and the
// store keeps both. Calling sm.Restore here would roll the state machine back to
// the snapshot index while the apply loop resumed from lastApplied, silently
// dropping everything applied since the last snapshot.
//
// The one exception is an interrupted InstallSnapshot — see below.
func (n *Node) restore(ctx context.Context) error {
	lastApplied, err := n.store.GetLastApplied(ctx)
	if err != nil {
		return err
	}

	dir, meta, err := n.readLatestSnapshotMeta(ctx)
	if err != nil {
		return err
	}

	if dir != "" {
		if err := n.seedConfigurationFromSnapshot(ctx, meta); err != nil {
			return err
		}
		n.SetSnapshotLatest(meta.Index, meta.Term)

		// lastApplied is normally >= the snapshot index, because our own snapshots
		// are taken at the applied index. Lower means a leader-pushed
		// InstallSnapshot was interrupted: it writes the snapshot to disk before
		// calling sm.Restore and SetLastApplied, so a crash anywhere in that window
		// leaves a snapshot the state machine may never have taken.
		//
		// It does not matter where in that window it died. Restore is a wholesale
		// replace, so re-running it when the state machine is already at meta.Index
		// changes nothing; the crash point stops being a case to distinguish.
		if meta.Index > lastApplied {
			zerolog.Ctx(ctx).Warn().Msgf(
				"snapshot at index %d is ahead of lastApplied %d; finishing interrupted install",
				meta.Index, lastApplied)
			if err := n.installSnapshotFromDisk(ctx, dir, meta); err != nil {
				return err
			}
			lastApplied = meta.Index
		}
	}

	// Only committed entries are ever applied, so lastApplied is a safe lower bound
	// for commitIndex. The real value may have been higher when we crashed; the
	// leader's next AppendEntries carries it, and SetCommitIndex ignores decreases.
	n.SetCommitIndex(lastApplied)

	return n.replayConfigurations(ctx, meta.Index, lastApplied)
}

// readLatestSnapshotMeta decodes meta.json from the newest snapshot directory.
// Having no snapshot is the ordinary case, not an error, and returns "".
func (n *Node) readLatestSnapshotMeta(ctx context.Context) (string, SnapshotMeta, error) {
	latestDir, err := getLatestSnapshotDir(n.cfg.SnapshotDir)
	if errors.Is(err, ErrNoSnapshot) {
		// Also the state of every node in an embedding that configures no
		// SnapshotDir. Start aborts on an error, so returning one here would stop
		// the node booting.
		zerolog.Ctx(ctx).Debug().Msg("no snapshot to restore from")
		return "", SnapshotMeta{}, nil
	}
	if err != nil {
		return "", SnapshotMeta{}, err
	}

	metaFile, err := os.Open(n.cfg.SnapshotDir + "/" + latestDir + "/" + metaFileName)
	if err != nil {
		return "", SnapshotMeta{}, err
	}
	defer metaFile.Close()

	var meta SnapshotMeta
	if err := json.NewDecoder(metaFile).Decode(&meta); err != nil {
		return "", SnapshotMeta{}, err
	}
	return latestDir, meta, nil
}

// seedConfigurationFromSnapshot sets both configuration views to the membership
// the snapshot captured. replayConfigurations then applies whatever the log holds
// above it.
func (n *Node) seedConfigurationFromSnapshot(ctx context.Context, meta SnapshotMeta) error {
	members := make(map[string]Peer, len(meta.MemberConfig))
	for id, state := range meta.MemberConfig {
		members[id] = Peer{
			PeerState: state,
		}
	}
	n.setLatestConfiguration(members, uint64(meta.Index))
	n.setCommittedConfiguration(members, uint64(meta.Index))

	lastIndex, err := n.store.GetLastIndex(ctx)
	if err != nil {
		return err
	}
	if lastIndex < meta.Index {
		// The log is missing entries the snapshot says were committed — we crashed
		// after snapshotting but before compacting. DeleteLogs is idempotent, so
		// drop everything up to the snapshot index.
		if err := n.store.DeleteLogs(ctx, 0, meta.Index); err != nil {
			return err
		}
	}
	return nil
}

// installSnapshotFromDisk finishes an InstallSnapshot that crashed partway. No
// lock is taken because restore runs before startApplyLoop, so nothing else is
// touching the state machine yet.
func (n *Node) installSnapshotFromDisk(ctx context.Context, dir string, meta SnapshotMeta) error {
	snapshotFile, err := os.Open(n.cfg.SnapshotDir + "/" + dir + "/" + snapshotFileName)
	if err != nil {
		return err
	}
	defer snapshotFile.Close()

	if err := n.sm.Restore(ctx, snapshotFile); err != nil {
		return err
	}
	return n.store.SetLastApplied(ctx, meta.Index)
}

// replayConfigurations walks the log above the snapshot index and re-applies every
// configuration entry in order, reproducing the latest/committed split the node
// held before it restarted.
//
// This is the only way membership survives a restart: cfg.Peers is a bootstrap
// seed and configurations lives in memory, so without the replay a node that
// restarts after an AddMember reverts to the seed and operates on a cluster that
// no longer exists.
//
// advanceCommittedConfiguration runs after each entry rather than once at the end.
// Once at the end would only ever consider the final config entry, so a committed
// change followed by an uncommitted one would strand committed at the snapshot's
// view instead of at the committed change.
func (n *Node) replayConfigurations(ctx context.Context, snapshotIdx, commitIndex uint) error {
	start := snapshotIdx + 1
	logs, err := n.store.GetLogs(ctx, &start, nil)
	if err != nil {
		return err
	}

	replayed := 0
	for _, entry := range logs {
		if entry.Type != EntryType_Config {
			continue
		}
		if err := n.processConfigurationLogEntry(entry); err != nil {
			return err
		}
		n.advanceCommittedConfiguration(uint64(commitIndex))
		replayed++
	}

	if replayed > 0 {
		zerolog.Ctx(ctx).Debug().Msgf("replayed %d configuration entries above index %d", replayed, snapshotIdx)
	}
	return nil
}
