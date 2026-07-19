// server/debug_server.go
package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/SHREYANSHSINGH14/raft/raft"
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

	err := d.server.Node.Propose(context.Background(), raft.EntryType_Command, []byte(req.Data))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, AppendLogsDebugResponse{
			Success:  false,
			ErrorMsg: err.Error(),
			LeaderID: d.server.Node.GetLeaderID(),
		})
		return
	}

	writeJSON(w, http.StatusOK, AppendLogsDebugResponse{Success: true})
}

// GET /logs/get?start=1&end=10
func (d *DebugServer) handleGetLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	startStr := r.URL.Query().Get("start")

	start, err := strconv.ParseUint(startStr, 10, 64)
	if err != nil {
		start = 1
	}

	logs, err := d.server.Node.GetLogs(context.Background(), start)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, GetLogsDebugResponse{
			ErrorMsg: err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, GetLogsDebugResponse{
		Entries: toDebugEntries(logs),
	})
}

// GET /status
func (d *DebugServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	currentTerm, _ := d.server.Node.GetCurrentTerm(context.Background())

	writeJSON(w, http.StatusOK, StatusDebugResponse{
		ID:          d.server.Node.GetID(),
		Role:        string(d.server.Node.GetRole()),
		Term:        currentTerm,
		CommitIndex: d.server.Node.GetCommitIndex(),
		LeaderID:    d.server.Node.GetLeaderID(),
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
