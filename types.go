package raft

import (
	"errors"
	"io"
	"time"
)

// ErrNotFound is returned by Storage implementations when a key does not exist.
var ErrNotFound = errors.New("not found")

// ErrNoSnapshot is returned by getLatestSnapshotDir when there is no snapshot to
// find — no SnapshotDir configured, the directory does not exist yet, or it holds
// no parseable snapshot. It is separate from a read failure because startup
// recovery treats it as the normal state of a node that has never snapshotted,
// while callInstallSnapshot treats it as a genuine failure.
var ErrNoSnapshot = errors.New("no snapshot found")

// ErrLeadershipLost is returned by Propose when this node stopped being leader
// while the proposed entry was still uncommitted. The entry stays in the local
// log — the new leader decides whether it survives — so the caller must treat the
// proposal as failed but not assume it was discarded.
var ErrLeadershipLost = errors.New("leadership lost before commit")

// ErrTooManyPendingProposals is returned by Propose when too many entries are
// already appended and waiting to commit. Nothing was written to the log, so
// unlike ErrLeadershipLost this one is unambiguous: the proposal did not happen
// and retrying it is safe.
//
// It means commitIndex has stopped moving — lost quorum, or a partitioned leader
// that has not noticed yet — because that is the only situation in which the
// pending list has no drain. Backpressure is deliberately pushed to the caller,
// which is the only layer that can decide between retrying, shedding, and
// telling its own client.
var ErrTooManyPendingProposals = errors.New("too many proposals awaiting commit")

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

// PreVoteArgs is the §9.6 pre-vote probe. It carries exactly what
// RequestVoteArgs carries, but Term is the term the candidate *would* run in
// (currentTerm + 1) — it is a hypothetical, not a term the candidate has
// entered. Nobody persists it.
type PreVoteArgs struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

// PreVoteResponse mirrors RequestVoteResponse. Term is the responder's view of
// the term the vote was judged against — see HandlePreVote for why that is not
// always the responder's own currentTerm.
type PreVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

// TimeoutNowArgs is the leadership-transfer RPC (Ongaro §3.10). The leader sends
// it to the peer it has chosen as successor, once that peer's log has caught up.
// Term is the leader's term; LeaderID identifies the sender so the recipient can
// refuse a transfer from anyone that is not its current leader.
type TimeoutNowArgs struct {
	Term     uint64
	LeaderID string
}

// TimeoutNowResponse reports whether the recipient accepted the transfer and will
// campaign. Success is false when it declined (stale term, wrong sender, already
// leader) — the transfer failed and the leader keeps leadership.
type TimeoutNowResponse struct {
	Term    uint64
	Success bool
}

type SnapshotMetadata struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	TimeStamp         time.Time
	MemberConfig      map[string]PeerState
}

type InstallSnapshotArgs struct {
	Term             uint64
	LeaderID         string
	SnapshotMetadata SnapshotMetadata
	SnapshotSize     uint64
	Reader           io.ReadCloser
}

type InstallSnapshotResponse struct {
	Term    uint64
	Success bool
}
