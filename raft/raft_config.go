package raft

type Config struct {
	ID    string
	Peers []string // peer IDs only; addresses are the Transport's concern

	RPCTimeoutMs  int
	HeartbeatMs   int
	ElectionMinMs int
	ElectionMaxMs int
}
