package raft

type Config struct {
	ID    string
	Peers map[string]Peer // peer IDs only; addresses are the Transport's concern

	RPCTimeoutMs  int
	HeartbeatMs   int
	ElectionMinMs int
	ElectionMaxMs int

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

	SnapshotDir string
	// will only be used by snapshot loop
	SnapshotInterval  uint // in seconds
	SnapshotThreshold uint // in number of log entries
}

type PeerState int

const (
	PeerState_Unknown PeerState = iota
	PeerState_Staging
	PeerState_Voter
	PeerState_NonVoter
)

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
