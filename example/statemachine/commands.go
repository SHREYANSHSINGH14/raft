package statemachine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// ErrCommandFailed marks an error as a property of the command rather than of this
// node: a deterministic outcome that every replica reaches identically from the same
// log. Apply reports these to the waiting client and keeps going. Anything not
// wrapping it is a node-level failure and stops the apply loop for good.
var ErrCommandFailed = errors.New("command failed")

// ErrNoWaiter is returned by WaitForResult for an id that was never registered.
var ErrNoWaiter = errors.New("no waiter registered for command")

type OpsType string

const (
	OpsTypeUnspecified OpsType = "UNSPECIFIED"
	OpsTypeSet         OpsType = "SET"
	OpsTypeDelete      OpsType = "DELETE"
	OpsTypeCAS         OpsType = "CAS"
)

type Command struct {
	ID            string  `json:"id"`
	Op            OpsType `json:"op"`
	Key           string  `json:"key"`
	Value         []byte  `json:"value"`
	ExpectedValue []byte  `json:"expected_value"`
}

func (cmd *Command) Marshal() ([]byte, error) {
	return json.Marshal(cmd)
}

func (cmd *Command) Unmarshal(data []byte) error {
	return json.Unmarshal(data, cmd)
}

func NewCommand(cmdID, key string, ops OpsType, value, expected interface{}) (*Command, error) {
	var val, expectedVal []byte
	var err error
	cmd := Command{
		ID:  cmdID,
		Op:  ops,
		Key: key,
	}
	if value != nil {
		val, err = json.Marshal(value)
		if err != nil {
			return nil, err
		}
		cmd.Value = val
	}
	if expected != nil {
		expectedVal, err = json.Marshal(expected)
		if err != nil {
			return nil, err
		}
		cmd.ExpectedValue = expectedVal
	}

	return &cmd, nil
}

// CommandResult is the waiter for one in-flight command.
//
// ctx is the *request's* context, captured at Register. Storing a context in a struct
// is normally wrong, but this struct is request-scoped and dies with the request, and
// holding it here is what makes an entry self-describing: an entry whose ctx is done
// is provably garbage, so a Forget that never ran is detectable rather than
// indistinguishable from a live waiter. It does not replace Forget — it makes missing
// one survivable.
type CommandResult struct {
	ctx context.Context
	res chan struct{}
	err chan error
}

// abandoned reports whether the caller has given up, so there is no point completing
// this waiter.
func (cr CommandResult) abandoned() bool {
	return cr.ctx.Err() != nil
}

// CommandResultBuffer maps in-flight command ids to their waiters. Register and
// Forget run on client goroutines while Apply reads from the apply loop, so every
// access takes mu.
type CommandResultBuffer struct {
	ctx    context.Context
	mu     sync.RWMutex
	buffer map[string]CommandResult
}

func NewCommandResultBuffer(ctx context.Context) *CommandResultBuffer {
	buffer := make(map[string]CommandResult, 1024)
	return &CommandResultBuffer{
		ctx:    ctx,
		buffer: buffer,
	}
}

// Register creates the waiter for id. It must be called *before* Propose: the caller
// chooses the id, so registering first closes the window in which a fast commit —
// certain on a single-node cluster, where majority is one — applies the entry before
// the waiter exists.
//
// ctx must be the request's context, not the node's. Pair every Register with a
// deferred Forget, including on the path where Propose itself fails.
func (cb *CommandResultBuffer) Register(ctx context.Context, id string) {
	commandResult := CommandResult{
		ctx: ctx,
		res: make(chan struct{}, 1),
		err: make(chan error, 1),
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.buffer[id] = commandResult
}

func (cb *CommandResultBuffer) Forget(id string) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	delete(cb.buffer, id)
}

// Lookup reports whether anyone on this node is waiting on id. A miss is ordinary:
// followers apply commands they never registered.
func (cb *CommandResultBuffer) Lookup(id string) (CommandResult, bool) {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	result, ok := cb.buffer[id]
	return result, ok
}

func (cb *CommandResultBuffer) WaitForResult(id string) error {
	// Resolve once: re-reading the map inside the select would take the lock twice
	// and, on a miss, select over nil channels and block until the context dies.
	result, ok := cb.Lookup(id)
	if !ok {
		return fmt.Errorf("%w: %s", ErrNoWaiter, id)
	}
	// Both contexts, because they are unrelated: a gRPC request context is not a
	// child of the node's, so cancelling one does not cancel the other. Waking on
	// only one of them leaves the waiter stuck on the other's event.
	select {
	case <-result.res:
		return nil
	case err := <-result.err:
		return err
	case <-result.ctx.Done():
		return result.ctx.Err()
	case <-cb.ctx.Done():
		return cb.ctx.Err()
	}
}

// Sweep drops every entry whose caller has gone away. Cleanup is Forget's job; this
// is the backstop that keeps a missed Forget from growing the map for the life of the
// process.
func (cb *CommandResultBuffer) Sweep() int {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	var dropped int
	for id, result := range cb.buffer {
		if result.abandoned() {
			delete(cb.buffer, id)
			dropped++
		}
	}
	return dropped
}
