package raft

type Config struct {
	ID    string
	Peers map[string]Peer // peer IDs only; addresses are the Transport's concern

	RPCTimeoutMs  int
	HeartbeatMs   int
	ElectionMinMs int
	ElectionMaxMs int

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
	PeerState PeerState
	// only populated when node is leader
	NextIndex  uint
	MatchIndex uint
}
