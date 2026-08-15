package server

import (
	"encoding/json"

	"github.com/SHREYANSHSINGH14/raft"
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
	Success  bool            `json:"success"`
	Value    json.RawMessage `json:"value,omitempty"`
	ErrorMsg string          `json:"error_msg,omitempty"`
	LeaderID string          `json:"leader_id,omitempty"`
}

type AppendLogsDebugRequest struct {
	Data string `json:"data"`
}

type AppendLogsDebugResponse struct {
	Success  bool   `json:"success"`
	ErrorMsg string `json:"error_msg,omitempty"`
	LeaderID string `json:"leader_id,omitempty"`
}

type GetLogsDebugResponse struct {
	Entries  []*LogEntryDebug `json:"entries"`
	ErrorMsg string           `json:"error_msg,omitempty"`
	LeaderID string           `json:"leader_id,omitempty"`
}

type LogEntryDebug struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Data  string `json:"data"`
}

type StatusDebugResponse struct {
	ID          string `json:"id"`
	Role        string `json:"role"`
	Term        uint   `json:"term"`
	CommitIndex uint   `json:"commit_index"`
	LeaderID    string `json:"leader_id"`
}

func toDebugEntries(entries []raft.LogEntry) []*LogEntryDebug {
	result := make([]*LogEntryDebug, 0, len(entries))
	for _, e := range entries {
		result = append(result, &LogEntryDebug{
			Index: e.Index,
			Term:  e.Term,
			Data:  string(e.Data),
		})
	}
	return result
}
