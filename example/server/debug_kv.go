// server/debug_kv.go — the client half of a proposal, over HTTP.
package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/SHREYANSHSINGH14/raft"
	"github.com/SHREYANSHSINGH14/raft/example/statemachine"
	"github.com/rs/zerolog"
)

// POST /kv/set
// body: {"key": "a", "value": {"any": "json"}}
func (d *DebugServer) handleKVSet(w http.ResponseWriter, r *http.Request) {
	var req KVSetRequest
	if !decodeKVRequest(w, r, &req) {
		return
	}
	d.runCommand(w, r, req.Key, statemachine.OpsTypeSet, req.Value, nil)
}

// POST /kv/delete
// body: {"key": "a"}
func (d *DebugServer) handleKVDelete(w http.ResponseWriter, r *http.Request) {
	var req KVDeleteRequest
	if !decodeKVRequest(w, r, &req) {
		return
	}
	d.runCommand(w, r, req.Key, statemachine.OpsTypeDelete, nil, nil)
}

// POST /kv/cas
// body: {"key": "a", "expected": 1, "value": 2}
func (d *DebugServer) handleKVCAS(w http.ResponseWriter, r *http.Request) {
	var req KVCASRequest
	if !decodeKVRequest(w, r, &req) {
		return
	}
	d.runCommand(w, r, req.Key, statemachine.OpsTypeCAS, req.Value, req.Expected)
}

// GET /kv/get?key=a
//
// A stale local read — it never touches the log, so it answers from whatever this
// node has applied. See StateMachine.Get.
func (d *DebugServer) handleKVGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		writeJSON(w, http.StatusBadRequest, KVResponse{ErrorMsg: "missing key"})
		return
	}

	resp := KVResponse{
		Key:         key,
		NodeID:      d.server.Node.GetID(),
		Role:        string(d.server.Node.GetRole()),
		CommitIndex: d.server.Node.GetCommitIndex(),
		LeaderID:    d.server.Node.GetLeaderID(),
	}

	val, err := d.server.SM.Get(key)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, raft.ErrNotFound) {
			status = http.StatusNotFound
		}
		resp.ErrorMsg = err.Error()
		writeJSON(w, status, resp)
		return
	}

	resp.Success = true
	resp.Value = val
	writeJSON(w, http.StatusOK, resp)
}

// runCommand builds a command, drives it through the log, and reports what applying
// it produced.
//
// The ordering here is the part that matters. Register must happen before Propose:
// the caller picks the id, and on a single-node cluster majority is one, so the entry
// can commit and apply before Propose has even returned. Registering afterwards would
// lose that race every time on a single node and almost never on three, which is the
// worse of the two failure modes.
func (d *DebugServer) runCommand(w http.ResponseWriter, r *http.Request, key string, op statemachine.OpsType, value, expected json.RawMessage) {
	if key == "" {
		writeJSON(w, http.StatusBadRequest, KVResponse{ErrorMsg: "missing key"})
		return
	}

	id, err := newCommandID()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, KVResponse{ErrorMsg: err.Error()})
		return
	}

	cmd, err := statemachine.NewCommand(id, key, op, rawOrNil(value), rawOrNil(expected))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, KVResponse{ErrorMsg: err.Error()})
		return
	}

	idx, err := d.propose(r.Context(), cmd)
	if err != nil {
		// A command the state machine evaluated and refused — a CAS that did not
		// match, an unknown op — is a client error, not a node failure.
		status := http.StatusInternalServerError
		if errors.Is(err, statemachine.ErrCommandFailed) {
			status = http.StatusConflict
		}
		writeJSON(w, status, KVResponse{
			Key:         key,
			CommandID:   cmd.ID,
			Index:       idx,
			NodeID:      d.server.Node.GetID(),
			Role:        string(d.server.Node.GetRole()),
			CommitIndex: d.server.Node.GetCommitIndex(),
			ErrorMsg:    err.Error(),
			LeaderID:    d.server.Node.GetLeaderID(),
		})
		return
	}

	writeJSON(w, http.StatusOK, KVResponse{
		Success:     true,
		Key:         key,
		CommandID:   cmd.ID,
		Index:       idx,
		NodeID:      d.server.Node.GetID(),
		Role:        string(d.server.Node.GetRole()),
		CommitIndex: d.server.Node.GetCommitIndex(),
	})
}

// propose returns the log index the entry was appended at, which stays useful on the
// error path too — a command that failed still occupies an index, and that is where
// /logs/get will show it.
func (d *DebugServer) propose(ctx context.Context, cmd *statemachine.Command) (uint64, error) {
	// Each stage is logged separately because they fail for unrelated reasons, and a
	// single "propose failed" line cannot tell you which one you are looking at:
	// appended-but-not-committed is a leadership problem, committed-but-refused is a
	// state machine verdict, and they want opposite responses from the caller.
	log := zerolog.Ctx(ctx).With().
		Str("command_id", cmd.ID).
		Str("op", string(cmd.Op)).
		Str("key", cmd.Key).
		Logger()

	data, err := cmd.Marshal()
	if err != nil {
		log.Error().Err(err).Msg("propose: could not marshal command")
		return 0, err
	}

	// Deferred immediately, so it also runs on the path where Propose itself fails.
	d.server.SM.Register(ctx, cmd.ID)
	defer d.server.SM.Forget(cmd.ID)

	start := time.Now()
	future, err := d.server.Node.Propose(ctx, raft.EntryType_Command, data)
	if err != nil {
		log.Warn().Err(err).Msg("propose: not appended")
		return 0, err
	}
	idx := future.Index()
	log.Debug().Uint64("index", idx).Msg("propose: appended, waiting for commit")

	// Two waits, answering two different questions. The future answers "did it
	// commit"; WaitForResult answers "what did applying it produce". A future that
	// fails — ErrLeadershipLost, a cancelled request — must not fall through to the
	// second, because nothing will ever complete that waiter.
	if err := future.Wait(ctx); err != nil {
		// The entry is in this node's log either way. Not committed means we cannot
		// say whether it ever will be — a new leader may commit it or truncate it —
		// so this is the one outcome a caller must not retry blindly.
		log.Warn().Err(err).
			Uint64("index", idx).
			Dur("waited", time.Since(start)).
			Msg("propose: NOT committed")
		return idx, err
	}
	committedAt := time.Since(start)
	log.Debug().Uint64("index", idx).Dur("commit_ms", committedAt).Msg("propose: committed")

	if err := d.server.SM.WaitForResult(cmd.ID); err != nil {
		// Committed and applied — the command itself was refused, or its caller went
		// away. The log entry stands regardless.
		log.Info().Err(err).
			Uint64("index", idx).
			Dur("total", time.Since(start)).
			Msg("propose: committed, but applying it did not succeed")
		return idx, err
	}

	log.Info().
		Uint64("index", idx).
		Dur("commit", committedAt).
		Dur("total", time.Since(start)).
		Msg("propose: committed and applied")
	return idx, nil
}

// rawOrNil keeps an absent JSON field absent. NewCommand marshals any non-nil value,
// and a nil json.RawMessage inside a non-nil interface would marshal to "null"
// rather than being skipped.
func rawOrNil(raw json.RawMessage) any {
	if len(raw) == 0 {
		return nil
	}
	return raw
}

func newCommandID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func decodeKVRequest(w http.ResponseWriter, r *http.Request, dst any) bool {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return false
	}
	if err := decodeJSON(r, dst); err != nil {
		writeJSON(w, http.StatusBadRequest, KVResponse{ErrorMsg: "invalid request body: " + err.Error()})
		return false
	}
	return true
}

func decodeJSON(r *http.Request, dst any) error {
	return json.NewDecoder(r.Body).Decode(dst)
}
