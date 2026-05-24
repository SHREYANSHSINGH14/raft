package raft

type RaftConfig struct {
	ID    string
	Peers map[string]string // peer id -> rpc url

	RPCTimeoutMs       int
	HeartbeatMs        int
	ElectionMinMs      int
	ElectionDurationMs int // ElectionMaxMs - ElectionMinMs
}
