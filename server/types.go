package server

import "github.com/SHREYANSHSINGH14/raft/types"

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

func toDebugEntries(entries []*types.LogEntry) []*LogEntryDebug {
	result := make([]*LogEntryDebug, 0, len(entries))
	for _, e := range entries {
		result = append(result, &LogEntryDebug{
			Index: e.Index,
			Term:  uint64(e.Term),
			Data:  string(e.Data),
		})
	}
	return result
}
