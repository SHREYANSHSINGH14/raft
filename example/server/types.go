package server

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/SHREYANSHSINGH14/raft"
	"github.com/SHREYANSHSINGH14/raft/example/statemachine"
)

// KV request/response types for the debug server. Values are carried as raw JSON so
// whatever goes in comes back out unchanged.

type KVSetRequest struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

type KVDeleteRequest struct {
	Key string `json:"key"`
}

type KVCASRequest struct {
	Key      string          `json:"key"`
	Expected json.RawMessage `json:"expected"`
	Value    json.RawMessage `json:"value"`
}

type KVResponse struct {
	Success bool            `json:"success"`
	Key     string          `json:"key,omitempty"`
	Value   json.RawMessage `json:"value,omitempty"`

	// Where the answering node stood. /kv/get is a stale local read, so a value —
	// or a "not found" — only means anything alongside the position it was read at:
	// a node whose commit_index trails the leader's is expected to disagree.
	NodeID      string `json:"node_id,omitempty"`
	Role        string `json:"role,omitempty"`
	CommitIndex uint   `json:"commit_index"`

	// CommandID and Index identify the entry a write produced, so a proposal can be
	// found again in /logs/get.
	CommandID string `json:"command_id,omitempty"`
	Index     uint64 `json:"index,omitempty"`

	ErrorMsg string `json:"error_msg,omitempty"`
	LeaderID string `json:"leader_id,omitempty"`
}

type GetLogsDebugResponse struct {
	// Which node answered, and where it was when it did. A log read is only
	// interpretable against the node's own commit/snapshot position — the same
	// query against two nodes legitimately returns different things.
	NodeID      string `json:"node_id,omitempty"`
	Role        string `json:"role,omitempty"`
	Term        uint   `json:"term,omitempty"`
	CommitIndex uint   `json:"commit_index"`

	StartIndex uint64           `json:"start_index"`
	Count      int              `json:"count"`
	Entries    []*LogEntryDebug `json:"entries"`
	ErrorMsg   string           `json:"error_msg,omitempty"`
	LeaderID   string           `json:"leader_id,omitempty"`
}

type LogEntryDebug struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	// Type as a name, because the numeric EntryType is an iota whose meaning is
	// invisible in a JSON dump — and the proto enum is offset by one, so the raw
	// number is actively misleading if you compare the two.
	Type      string `json:"type"`
	TypeCode  int    `json:"type_code"`
	Committed bool   `json:"committed"`
	DataSize  int    `json:"data_size"`

	// Data when it parses as JSON, DataText when it does not. Config entries and
	// state machine commands are both JSON; a no-op carries nothing.
	Data     json.RawMessage `json:"data,omitempty"`
	DataText string          `json:"data_text,omitempty"`

	// Command is the decoded payload of an EntryType_Command, so a log dump shows
	// what the entry actually does rather than an opaque blob.
	Command *CommandDebug `json:"command,omitempty"`
}

type CommandDebug struct {
	ID            string          `json:"id"`
	Op            string          `json:"op"`
	Key           string          `json:"key"`
	Value         json.RawMessage `json:"value,omitempty"`
	ExpectedValue json.RawMessage `json:"expected_value,omitempty"`
}

type StatusDebugResponse struct {
	ID       string `json:"id"`
	Role     string `json:"role"`
	IsLeader bool   `json:"is_leader"`
	Term     uint   `json:"term"`
	LeaderID string `json:"leader_id"`

	CommitIndex  uint   `json:"commit_index"`
	LastLogIndex uint64 `json:"last_log_index"`

	SnapshotIndex uint `json:"snapshot_index"`
	SnapshotTerm  uint `json:"snapshot_term"`

	// Peers is the live configuration as this node sees it — configurations.latest,
	// not the bootstrap seed. NextIndex/MatchIndex are only meaningful on a leader.
	Peers map[string]*PeerDebug `json:"peers,omitempty"`

	ErrorMsg string `json:"error_msg,omitempty"`
}

type PeerDebug struct {
	PeerState  string `json:"peer_state"`
	NextIndex  uint   `json:"next_index"`
	MatchIndex uint   `json:"match_index"`
}

// entryTypeName maps the library's EntryType to a readable name. The library has
// no String method and this is a presentation concern, so it lives here.
func entryTypeName(t raft.EntryType) string {
	switch t {
	case raft.EntryType_Command:
		return "COMMAND"
	case raft.EntryType_NoOp:
		return "NO_OP"
	case raft.EntryType_Config:
		return "CONFIG"
	case raft.EntryType_Barrier:
		return "BARRIER"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", int(t))
	}
}

func peerStateName(s raft.PeerState) string {
	switch s {
	case raft.PeerState_Staging:
		return "STAGING"
	case raft.PeerState_Voter:
		return "VOTER"
	case raft.PeerState_NonVoter:
		return "NONVOTER"
	default:
		return "UNKNOWN"
	}
}

func toDebugEntries(entries []raft.LogEntry, commitIndex uint) []*LogEntryDebug {
	result := make([]*LogEntryDebug, 0, len(entries))
	for _, e := range entries {
		d := &LogEntryDebug{
			Index:     e.Index,
			Term:      e.Term,
			Type:      entryTypeName(e.Type),
			TypeCode:  int(e.Type),
			Committed: e.Index <= uint64(commitIndex),
			DataSize:  len(e.Data),
		}

		if json.Valid(e.Data) {
			d.Data = json.RawMessage(e.Data)
		} else if len(e.Data) > 0 {
			d.DataText = string(e.Data)
		}

		if e.Type == raft.EntryType_Command {
			var cmd statemachine.Command
			if err := cmd.Unmarshal(e.Data); err == nil {
				d.Command = &CommandDebug{
					ID:            cmd.ID,
					Op:            string(cmd.Op),
					Key:           cmd.Key,
					Value:         rawIfValid(cmd.Value),
					ExpectedValue: rawIfValid(cmd.ExpectedValue),
				}
			}
		}

		result = append(result, d)
	}
	return result
}

// rawIfValid passes bytes through as JSON only when they are JSON, so an
// unparseable value cannot make the whole response invalid.
func rawIfValid(b []byte) json.RawMessage {
	if len(b) > 0 && json.Valid(b) {
		return json.RawMessage(b)
	}
	return nil
}

// Cluster membership requests for the debug server.

type ClusterAddRequest struct {
	ID string `json:"id"`
	// RPCUrl is unavoidable here. The library tracks membership by ID and never
	// learns addresses — that is the Transport's concern — so the caller supplies
	// the address the same way peers.yaml does at startup.
	RPCUrl string `json:"rpc_url"`
	// PeerState is VOTER or NONVOTER; empty defaults to VOTER. STAGING is not
	// accepted — it is the transient state AddMember moves through on its own.
	PeerState string `json:"peer_state"`
}

type ClusterRemoveRequest struct {
	ID string `json:"id"`
}

type ClusterResponse struct {
	Success  bool                  `json:"success"`
	ID       string                `json:"id,omitempty"`
	NodeID   string                `json:"node_id,omitempty"`
	Role     string                `json:"role,omitempty"`
	Peers    map[string]*PeerDebug `json:"peers,omitempty"`
	ErrorMsg string                `json:"error_msg,omitempty"`
	LeaderID string                `json:"leader_id,omitempty"`
}

// errPeerState is returned for a peer_state the caller may not ask for.
var errPeerState = errors.New("peer_state must be VOTER or NONVOTER")
