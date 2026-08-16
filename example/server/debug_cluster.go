// server/debug_cluster.go — membership changes over HTTP.
//
// These are the two operations the library cannot drive alone. It tracks membership by
// ID and deliberately knows nothing about addresses, so introducing a peer is always
// two steps — teach the Transport where it lives, then tell Raft it exists — and only
// the embedding can sequence them.
package server

import (
	"net/http"

	"github.com/SHREYANSHSINGH14/raft"
)

// POST /cluster/add
// body: {"id": "peer6", "rpc_url": "peer6:50056", "peer_state": "VOTER"}
//
// Leader-only, and one addition at a time — the library rejects a second while one is
// staging.
func (d *DebugServer) handleClusterAdd(w http.ResponseWriter, r *http.Request) {
	var req ClusterAddRequest
	if !decodeClusterRequest(w, r, &req) {
		return
	}
	if req.ID == "" || req.RPCUrl == "" {
		d.writeCluster(w, http.StatusBadRequest, req.ID, "id and rpc_url are required")
		return
	}
	if req.ID == d.server.Node.GetID() {
		d.writeCluster(w, http.StatusBadRequest, req.ID, "cannot add this node to itself")
		return
	}

	peerState, err := parsePeerState(req.PeerState)
	if err != nil {
		d.writeCluster(w, http.StatusBadRequest, req.ID, err.Error())
		return
	}

	// Dial first. AddMember catches the new member up over this transport, so a peer
	// that is not addressable yet cannot be reached, catch-up fails, and the addition
	// rolls itself back — "add to the map once Raft says success" can never succeed,
	// because success depends on the map.
	if err := d.server.transport.AddPeer(req.ID, req.RPCUrl); err != nil {
		d.writeCluster(w, http.StatusInternalServerError, req.ID, err.Error())
		return
	}

	if err := d.server.Node.AddMember(r.Context(), req.ID, peerState); err != nil {
		// Undo the dial, so a rejected member does not leave a client behind.
		d.server.transport.RemovePeer(req.ID)
		d.writeCluster(w, http.StatusInternalServerError, req.ID, err.Error())
		return
	}

	d.server.trackPeer(req.ID)
	d.writeCluster(w, http.StatusOK, req.ID, "")
}

// POST /cluster/remove
// body: {"id": "peer6"}
//
// A leader may remove itself. It goes on replicating the new configuration without
// counting itself toward the majority, then steps down — so this can return while the
// cluster is mid-handover, and the node answering may no longer be the leader.
func (d *DebugServer) handleClusterRemove(w http.ResponseWriter, r *http.Request) {
	var req ClusterRemoveRequest
	if !decodeClusterRequest(w, r, &req) {
		return
	}
	if req.ID == "" {
		d.writeCluster(w, http.StatusBadRequest, "", "id is required")
		return
	}

	if err := d.server.Node.RemoveMember(r.Context(), req.ID); err != nil {
		d.writeCluster(w, http.StatusInternalServerError, req.ID, err.Error())
		return
	}

	// Drop the connection only after Raft has committed the removal. The reverse of
	// AddMember: there the transport has to lead, here it has to follow, because the
	// removal is replicated over the very link being closed.
	d.server.transport.RemovePeer(req.ID)
	d.server.untrackPeer(req.ID)
	d.writeCluster(w, http.StatusOK, req.ID, "")
}

// writeCluster reports the resulting configuration alongside the outcome, so a caller
// can see what the change actually did without a second request to /status.
func (d *DebugServer) writeCluster(w http.ResponseWriter, status int, id, errMsg string) {
	resp := ClusterResponse{
		Success:  errMsg == "",
		ID:       id,
		NodeID:   d.server.Node.GetID(),
		Role:     string(d.server.Node.GetRole()),
		ErrorMsg: errMsg,
		Peers:    map[string]*PeerDebug{},
	}
	if errMsg != "" {
		resp.LeaderID = d.server.Node.GetLeaderID()
	}
	for _, peerID := range d.server.trackedPeerIDs() {
		peer := d.server.Node.GetPeerIndex(peerID)
		resp.Peers[peerID] = &PeerDebug{
			PeerState:  peerStateName(peer.PeerState),
			NextIndex:  peer.NextIndex,
			MatchIndex: peer.MatchIndex,
		}
	}
	writeJSON(w, status, resp)
}

// parsePeerState accepts only the two states a caller may ask for. STAGING is the
// transient state AddMember moves through while catching a member up; asking for it
// directly would stage a member nothing will ever promote.
func parsePeerState(s string) (raft.PeerState, error) {
	switch s {
	case "", "VOTER", "voter":
		return raft.PeerState_Voter, nil
	case "NONVOTER", "nonvoter":
		return raft.PeerState_NonVoter, nil
	default:
		return raft.PeerState_Unknown, errPeerState
	}
}

func decodeClusterRequest(w http.ResponseWriter, r *http.Request, dst any) bool {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return false
	}
	if err := decodeJSON(r, dst); err != nil {
		writeJSON(w, http.StatusBadRequest, ClusterResponse{ErrorMsg: "invalid request body: " + err.Error()})
		return false
	}
	return true
}
