package raft

// DefaultInstallSnapshotBaseMs is the fixed half of the InstallSnapshot deadline:
// enough for the receiver's fsyncs, directory rename and state machine Restore
// before any size-proportional time is added.
const DefaultInstallSnapshotBaseMs = 5000

type Config struct {
	ID    string
	Peers map[string]Peer // peer IDs only; addresses are the Transport's concern

	RPCTimeoutMs  int
	HeartbeatMs   int
	ElectionMinMs int
	ElectionMaxMs int

	// InstallSnapshot RPC deadline. Unlike every other RPC this one does not start
	// from RPCTimeoutMs, and that is the whole point: RPCTimeoutMs is pinned below
	// HeartbeatMs because a heartbeat that outlives its interval piles up goroutines.
	// An InstallSnapshot has nothing to do with that budget — the receiver has to
	// fsync the snapshot file, fsync its metadata, rename the directory, run a full
	// state machine Restore and commit a compaction batch. That fixed cost is
	// hundreds of milliseconds and is independent of how big the snapshot is, so
	// borrowing the heartbeat budget as a base fails a 2KB transfer as reliably as a
	// 2MB one.
	//
	//   deadline = InstallSnapshotBaseMs
	//              + (snapshotBytes / InstallSnapshotDeadlineScaleSizeByte)
	//                * InstallSnapshotDeadlineScaleTimeMs
	//
	// Zero means DefaultInstallSnapshotBaseMs.
	InstallSnapshotBaseMs                int
	InstallSnapshotDeadlineScaleSizeByte int
	InstallSnapshotDeadlineScaleTimeMs   int

	// AppendEntries RPC deadline scaling. A batch of log entries can be large
	// (e.g. the whole retained tail sent to a catching-up member), so the RPC
	// deadline is RPCTimeoutMs plus AppendEntriesDeadlineScaleTimeMs for every
	// AppendEntriesDeadlineScaleCount entries in the batch. Entries are handed to
	// the Transport as structs — they are not serialized at this layer — so the
	// batch is measured in entry count, not bytes. Set ScaleCount to 0 to disable
	// scaling and use a flat RPCTimeoutMs.
	AppendEntriesDeadlineScaleCount  int
	AppendEntriesDeadlineScaleTimeMs int

	// ApplyBatchSize caps how many command entries are handed to StateMachine.Apply
	// in one call. Zero (the default) means no cap: everything newly committed goes
	// in a single call, which is the fewest state machine commits and the fewest
	// fsyncs.
	//
	// Set it to 1 when a caller waits on the result of its own command. Apply is
	// all-or-nothing per call and results are only released once the call returns, so
	// with one big call every waiter is held until the slowest entry in the range has
	// applied. One entry per call releases each waiter as soon as its own entry is
	// durable, at the cost of a commit per entry.
	//
	// It changes batching only. The library still applies entries in index order and
	// still advances lastApplied monotonically, so no state machine can tell the
	// difference except in latency and commit count.
	ApplyBatchSize int

	SnapshotDir string
	// will only be used by snapshot loop
	SnapshotInterval  uint // in seconds
	SnapshotThreshold uint // in number of log entries

	// MaxPendingProposals bounds how many entries may be appended and awaiting
	// commit at once. Past it, Propose rejects with ErrTooManyPendingProposals
	// having written nothing. Zero means DefaultMaxPendingProposals.
	//
	// The list only grows without bound when commitIndex stops moving, so this is
	// really a bound on how long a stalled leader may keep accepting work. Size it
	// against what the application can have in flight, not against throughput —
	// under healthy replication the list drains continuously and never approaches
	// the limit.
	MaxPendingProposals int
}

type PeerState int

const (
	PeerState_Unknown PeerState = iota
	PeerState_Staging
	PeerState_Voter
	PeerState_NonVoter
)

// String makes PeerState printable with %s and %v. Without it every log line that
// formats a Peer renders the state as %!s(raft.PeerState=2), which is both unreadable
// and one enum renumbering away from being wrong.
func (p PeerState) String() string {
	switch p {
	case PeerState_Staging:
		return "STAGING"
	case PeerState_Voter:
		return "VOTER"
	case PeerState_NonVoter:
		return "NONVOTER"
	default:
		return "UNKNOWN"
	}
}

type Peer struct {
	PeerState PeerState `json:"peer_state"`
	// only populated when node is leader
	NextIndex  uint `json:"next_index"`
	MatchIndex uint `json:"match_index"`
}

// configurations tracks cluster membership as two views, mirroring the
// hashicorp/raft model:
//
//   - latest    — the most recent configuration that appears in the log, whether
//     or not it has committed. This is the OPERATING configuration: the peer
//     index helpers, election, heartbeat and quorum math all read from it. It
//     replaces the old single cfg.Peers map.
//   - committed — the most recent configuration that has actually committed. It
//     is the safe fallback: while resolving a log conflict we may truncate a
//     suffix that carried an as-yet-uncommitted config entry, and when we do,
//     latest must roll back to committed (see rollbackLatestIfTruncated).
//
// latestIndex / committedIndex record the log index of the config entry that
// produced each view, so a truncation at index X can tell whether it invalidates
// the current latest configuration (X <= latestIndex means it does).
//
// Access is guarded by Node.mu, the same lock that protects the peer maps.
type configurations struct {
	committed      map[string]Peer
	committedIndex uint64

	latest      map[string]Peer
	latestIndex uint64
}

// clonePeers returns a deep copy of a peer map so that mutating one configuration
// view (latest) can never alias another (committed).
func clonePeers(src map[string]Peer) map[string]Peer {
	dst := make(map[string]Peer, len(src))
	for id, peer := range src {
		dst[id] = peer
	}
	return dst
}
