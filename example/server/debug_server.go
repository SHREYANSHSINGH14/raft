// server/debug_server.go
package server

import (
	"encoding/json"
	"net/http"
	"strconv"

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
	mux.HandleFunc("/logs/get", d.handleGetLogs)
	mux.HandleFunc("/status", d.handleStatus)
	mux.HandleFunc("/kv/set", d.handleKVSet)
	mux.HandleFunc("/kv/delete", d.handleKVDelete)
	mux.HandleFunc("/kv/cas", d.handleKVCAS)
	mux.HandleFunc("/kv/get", d.handleKVGet)
	mux.HandleFunc("/cluster/add", d.handleClusterAdd)
	mux.HandleFunc("/cluster/remove", d.handleClusterRemove)

	go func() {
		zerolog.Ctx(d.server.ctx).Debug().Str("port", port).Msg("debug server started")
		if err := http.ListenAndServe(":"+port, mux); err != nil {
			zerolog.Ctx(d.server.ctx).Error().Err(err).Msg("debug server error")
		}
	}()
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

	ctx := r.Context()
	logs, err := d.server.Node.GetLogs(ctx, start)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, GetLogsDebugResponse{
			ErrorMsg: err.Error(),
		})
		return
	}

	commitIndex := d.server.Node.GetCommitIndex()
	term, _ := d.server.Node.GetCurrentTerm(ctx)

	writeJSON(w, http.StatusOK, GetLogsDebugResponse{
		NodeID:      d.server.Node.GetID(),
		Role:        string(d.server.Node.GetRole()),
		Term:        term,
		CommitIndex: commitIndex,
		LeaderID:    d.server.Node.GetLeaderID(),
		StartIndex:  start,
		Count:       len(logs),
		Entries:     toDebugEntries(logs, commitIndex),
	})
}

// GET /status
func (d *DebugServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx := r.Context()
	currentTerm, _ := d.server.Node.GetCurrentTerm(ctx)

	resp := StatusDebugResponse{
		ID:             d.server.Node.GetID(),
		Role:           string(d.server.Node.GetRole()),
		IsLeader:       d.server.Node.IsLeader(),
		Term:           currentTerm,
		LeaderID:       d.server.Node.GetLeaderID(),
		CommitIndex:    d.server.Node.GetCommitIndex(),
		SnapshotIndex:  d.server.Node.GetSnapshotLatestIndex(),
		SnapshotTerm:   d.server.Node.GetSnapshotLatestTerm(),
		SnapshotCaller: d.server.Node.GetSnapshotSetCaller(),
		LastApplied:    d.server.Node.GetLastApplied(),
		Peers:          map[string]*PeerDebug{},
	}

	// The tail of the log, so "how far behind is this node" is answerable from one
	// call: last_log_index is what it has, commit_index is what it may apply,
	// last_applied is what the state machine has actually consumed.
	//
	// Asked of the node rather than derived from GetLogs(ctx, 1). Scanning from
	// index 1 reports 0 on any node whose log has been compacted — the entries are
	// in the snapshot, not the log — which reads as "this node has nothing" for a
	// node that is perfectly caught up. GetLastIndex applies the snapshot fallback.
	if lastLogIndex, err := d.server.Node.GetLastIndex(ctx); err == nil {
		resp.LastLogIndex = uint64(lastLogIndex)
	}

	for _, id := range d.server.trackedPeerIDs() {
		peer := d.server.Node.GetPeerIndex(id)
		resp.Peers[id] = &PeerDebug{
			PeerState:  peerStateName(peer.PeerState),
			NextIndex:  peer.NextIndex,
			MatchIndex: peer.MatchIndex,
		}
	}

	writeJSON(w, http.StatusOK, resp)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
