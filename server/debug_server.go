// server/debug_server.go
package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/SHREYANSHSINGH14/raft/types"
	"github.com/rs/zerolog"
)

type DebugServer struct {
	server *Server
}

func NewDebugServer(server *Server) *DebugServer {
	return &DebugServer{server: server}
}

func (d *DebugServer) Start(port string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/logs/append", d.handleAppendLogs)
	mux.HandleFunc("/logs/get", d.handleGetLogs)
	mux.HandleFunc("/status", d.handleStatus)

	go func() {
		zerolog.Ctx(d.server.ctx).Debug().Str("port", port).Msg("debug server started")
		if err := http.ListenAndServe(":"+port, mux); err != nil {
			zerolog.Ctx(d.server.ctx).Error().Err(err).Msg("debug server error")
		}
	}()
}

// POST /logs/append
// body: {"data": "set x=5"}
func (d *DebugServer) handleAppendLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req AppendLogsDebugRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, AppendLogsDebugResponse{
			Success:  false,
			ErrorMsg: "invalid request body: " + err.Error(),
		})
		return
	}

	resp, err := d.server.Peer.HandleWriteLogs(context.Background(), &types.WriteLogRequest{
		Entries: []*types.LogEntry{
			{
				Data: []byte(req.Data),
			},
		},
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, AppendLogsDebugResponse{
			Success:  false,
			ErrorMsg: err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, AppendLogsDebugResponse{
		Success:  resp.Success,
		ErrorMsg: resp.ErrorMsg,
		LeaderID: resp.LeaderId,
	})
}

// GET /logs/get?start=1&end=10
func (d *DebugServer) handleGetLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")

	start, err := strconv.ParseUint(startStr, 10, 64)
	if err != nil {
		start = 1
	}

	end, _ := strconv.ParseUint(endStr, 10, 64) // 0 means fetch all from start

	resp, err := d.server.Peer.HandleReadLogs(context.Background(), &types.ReadLogRequest{
		StartIndex: start,
		EndIndex:   end,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, GetLogsDebugResponse{
			ErrorMsg: err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, GetLogsDebugResponse{
		Entries:  toDebugEntries(resp.Entries),
		ErrorMsg: resp.ErrorMsg,
		LeaderID: resp.LeaderId,
	})
}

// GET /status
func (d *DebugServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	currentTerm, _ := d.server.Peer.GetCurrentTerm(context.Background())

	writeJSON(w, http.StatusOK, StatusDebugResponse{
		ID:          d.server.Peer.GetID(),
		Role:        string(d.server.Peer.GetRole()),
		Term:        currentTerm,
		CommitIndex: d.server.Peer.GetCommitIndex(),
		LeaderID:    d.server.Peer.GetLeaderID(),
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
