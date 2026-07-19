package raft

import (
	"errors"
	"io"
	"time"
)

// ErrNotFound is returned by Storage implementations when a key does not exist.
var ErrNotFound = errors.New("not found")

// EntryType distinguishes the kind of payload a LogEntry carries, so the raft
// layer itself (not just the state machine) can react differently to entries
// like cluster membership changes.
type EntryType int

const (
	EntryType_Command EntryType = iota // normal state-machine command
	EntryType_NoOp                     // leader no-op written on election win
	EntryType_Config                   // cluster membership change
	EntryType_Barrier                  // force a commit-index advance
)

type LogEntry struct {
	Index uint64
	Term  uint64
	Type  EntryType
	Data  []byte
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesResponse struct {
	Term    uint64
	Success bool
}

type RequestVoteArgs struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

type SnapshotMetadata struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	TimeStamp         time.Time
	MemberConfig      map[string]PeerState
}

type InstallSnapshotRequest struct {
	Term             uint64
	LeaderID         string
	SnapshotMetadata SnapshotMetadata
	SnapshotSize     uint64
	Reader           io.Reader
}

type InstallSnapshotResponse struct {
	Term    uint64
	Success bool
}
