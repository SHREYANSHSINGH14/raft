package raft

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const methodTimeoutNow = "TimeoutNow"

// newRemoveTestNode wires a leader whose Propose returns immediately: a large
// pre-set commitIndex satisfies waitForCommit without any replication, so the
// RemoveMember flow runs synchronously. All four peers are Voters, which
// NewNodeMock does not do by default.
func newRemoveTestNode(t *testing.T, store Storage) (*Node, *MockTransport) {
	t.Helper()
	transport := NewMockTransport()
	node := NewNodeMock(store, nil)
	node.transport = transport
	node.Role = ServerRole_Leader
	node.setLeaderCloseCh() // RemoveMember proposes; waitForCommit needs a live one
	node.SetCommitIndex(1000)

	for _, id := range []string{"node-2", "node-3", "node-4", "node-5"} {
		node.addPeer(id, Peer{PeerState: PeerState_Voter})
	}
	return node, transport
}

// storeForPropose stubs the reads and the append that Propose makes.
func storeForPropose() *MockStorage {
	store := new(MockStorage)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(2), nil)
	store.On(methodGetLastLogIndex, mock.Anything).Return(uint(10), nil)
	store.On(methodAppendLogs, mock.Anything, mock.Anything).Return(nil)
	return store
}

// ── guards ────────────────────────────────────────────────────────────────────

func TestRemoveMember_NotLeader(t *testing.T) {
	node, _ := newRemoveTestNode(t, new(MockStorage))
	node.Role = ServerRole_Follower

	err := node.RemoveMember(context.Background(), "node-2")

	assert.ErrorContains(t, err, "not the leader")
}

func TestRemoveMember_MembershipChangeInProgress(t *testing.T) {
	node, _ := newRemoveTestNode(t, new(MockStorage))
	node.addPeer("node-88", Peer{PeerState: PeerState_Staging})

	err := node.RemoveMember(context.Background(), "node-2")

	assert.ErrorContains(t, err, "already in progress")
}

func TestRemoveMember_UnknownPeer(t *testing.T) {
	node, _ := newRemoveTestNode(t, new(MockStorage))

	err := node.RemoveMember(context.Background(), "node-404")

	assert.ErrorContains(t, err, "not a member")
}

// Removing the last voter would leave a cluster that can never commit anything
// again — including the entry that would undo the removal.
func TestRemoveMember_LastVoter_Refused(t *testing.T) {
	node, _ := newRemoveTestNode(t, new(MockStorage))
	for _, id := range []string{"node-2", "node-3", "node-4", "node-5"} {
		node.removePeer(id)
	}
	assert.Equal(t, 1, node.voterCount(), "only this node should be left")

	err := node.RemoveMember(context.Background(), node.GetID())

	assert.ErrorContains(t, err, "only voter left")
	assert.True(t, node.isVoter(), "a refused removal must not have mutated the config")
}

// ── the ordinary case: removing someone else ──────────────────────────────────

func TestRemoveMember_RemovesPeerAndReplicatesConfig(t *testing.T) {
	store := storeForPropose()
	node, _ := newRemoveTestNode(t, store)

	err := node.RemoveMember(context.Background(), "node-3")

	assert.NoError(t, err)
	_, present := node.configurations.latest["node-3"]
	assert.False(t, present, "removed peer should be gone from the live configuration")
	assert.Equal(t, 4, node.voterCount(), "5 voters minus one")
	assert.True(t, node.isVoter(), "removing someone else does not remove us")
}

// The replicated entry carries the whole membership including this node, which is
// what lets a follower tell "leader removed" from "config unchanged".
func TestRemoveMember_ConfigEntryCarriesWholeMembershipInclSelf(t *testing.T) {
	store := storeForPropose()
	var appended []LogEntry
	store.ExpectedCalls = nil
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(2), nil)
	store.On(methodGetLastLogIndex, mock.Anything).Return(uint(10), nil)
	store.On(methodAppendLogs, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { appended = args.Get(1).([]LogEntry) }).
		Return(nil)

	node, _ := newRemoveTestNode(t, store)

	assert.NoError(t, node.RemoveMember(context.Background(), "node-5"))

	assert.Len(t, appended, 1)
	assert.Equal(t, EntryType_Config, appended[0].Type)

	var members map[string]Peer
	assert.NoError(t, json.Unmarshal(appended[0].Data, &members))
	assert.Contains(t, members, "node-1", "the author must be in its own configuration entry")
	assert.NotContains(t, members, "node-5", "the removed peer must be absent")
	assert.Len(t, members, 4)
}

// A failed Propose must leave the live configuration exactly as it was, or it
// would disagree with the log.
func TestRemoveMember_ProposeFails_PeerIsRestored(t *testing.T) {
	store := new(MockStorage)
	store.On(methodGetCurrentTerm, mock.Anything).Return(uint(2), nil)
	store.On(methodGetLastLogIndex, mock.Anything).Return(uint(10), nil)
	store.On(methodAppendLogs, mock.Anything, mock.Anything).Return(errors.New("db error"))

	node, _ := newRemoveTestNode(t, store)

	err := node.RemoveMember(context.Background(), "node-3")

	assert.Error(t, err)
	restored, present := node.configurations.latest["node-3"]
	assert.True(t, present, "peer should be restored after a failed propose")
	assert.Equal(t, PeerState_Voter, restored.PeerState, "and restored with its old state")
	assert.Equal(t, 5, node.voterCount())
}

// ── self-removal ──────────────────────────────────────────────────────────────

// The payoff of keeping self in configurations.latest: once the entry removing us
// is in place we stop counting ourselves, with nothing having to special-case it.
func TestRemoveMember_Self_StopsCountingItselfInQuorum(t *testing.T) {
	store := storeForPropose()
	node, transport := newRemoveTestNode(t, store)
	transport.On(methodTimeoutNow, mock.Anything, mock.Anything).
		Return(TimeoutNowResponse{Success: true, Term: 2}, nil)

	assert.Equal(t, 5, node.voterCount())
	assert.Equal(t, 3, majoritySize(node.voterCount()))

	assert.NoError(t, node.RemoveMember(context.Background(), node.GetID()))

	assert.False(t, node.isVoter(), "we are no longer a voting member")
	assert.Equal(t, 4, node.voterCount(), "and no longer counted")
	assert.Equal(t, 3, majoritySize(node.voterCount()), "majority is now over the remaining four")
}

// TimeoutNow goes to the voter with the highest MatchIndex — the one needing the
// least catching up, so likeliest to win the election it is about to start.
func TestRemoveMember_Self_HandsOffToMostCaughtUpVoter(t *testing.T) {
	store := storeForPropose()
	node, transport := newRemoveTestNode(t, store)

	node.SetMatchPeerIndex("node-2", 3)
	node.SetMatchPeerIndex("node-3", 9) // furthest ahead
	node.SetMatchPeerIndex("node-4", 7)
	node.SetMatchPeerIndex("node-5", 1)

	transport.On(methodTimeoutNow, "node-3", mock.MatchedBy(func(args TimeoutNowArgs) bool {
		return args.LeaderID == "node-1" && args.Term == 2
	})).Return(TimeoutNowResponse{Success: true, Term: 2}, nil)

	assert.NoError(t, node.RemoveMember(context.Background(), node.GetID()))

	transport.AssertExpectations(t)
	transport.AssertNotCalled(t, methodTimeoutNow, "node-4", mock.Anything)
}

// Option B: a failed handoff does not keep us leader. The committed configuration
// says we are not a member, so staying is not on offer — we step down and let the
// ordinary election timer close the gap.
func TestRemoveMember_Self_HandoffRPCFails_StepsDownAnyway(t *testing.T) {
	store := storeForPropose()
	node, transport := newRemoveTestNode(t, store)
	transport.On(methodTimeoutNow, mock.Anything, mock.Anything).
		Return(TimeoutNowResponse{}, errors.New("connection refused"))

	assert.NoError(t, node.RemoveMember(context.Background(), node.GetID()),
		"a failed handoff is not a failed removal — the config change committed")

	assertStepDownRequested(t, node)
}

// Same when the target answers but declines.
func TestRemoveMember_Self_HandoffDeclined_StepsDownAnyway(t *testing.T) {
	store := storeForPropose()
	node, transport := newRemoveTestNode(t, store)
	transport.On(methodTimeoutNow, mock.Anything, mock.Anything).
		Return(TimeoutNowResponse{Success: false, Term: 2}, nil)

	assert.NoError(t, node.RemoveMember(context.Background(), node.GetID()))

	assertStepDownRequested(t, node)
}

// mostCaughtUpVoter finding nobody is what drives the no-handoff branch. It is
// not reachable through RemoveMember — the last-voter guard already guarantees a
// second voter exists — so it is exercised here directly. NonVoters are not
// candidates for a handoff no matter how far ahead their logs are.
func TestMostCaughtUpVoter_NoOtherVoters(t *testing.T) {
	node := NewNodeMock(new(MockStorage), nil)
	for _, id := range []string{"node-2", "node-3", "node-4", "node-5"} {
		node.addPeer(id, Peer{PeerState: PeerState_NonVoter, MatchIndex: 99})
	}

	_, ok := node.mostCaughtUpVoter()

	assert.False(t, ok, "non-voters must never be handoff targets")
}

func TestMostCaughtUpVoter_PicksHighestMatchIndex(t *testing.T) {
	node := NewNodeMock(new(MockStorage), nil)
	node.addPeer("node-2", Peer{PeerState: PeerState_Voter, MatchIndex: 4})
	node.addPeer("node-3", Peer{PeerState: PeerState_Voter, MatchIndex: 11})
	node.addPeer("node-4", Peer{PeerState: PeerState_NonVoter, MatchIndex: 99}) // ineligible
	node.addPeer("node-5", Peer{PeerState: PeerState_Voter, MatchIndex: 6})

	target, ok := node.mostCaughtUpVoter()

	assert.True(t, ok)
	assert.Equal(t, "node-3", target)
}

// Removing someone else must not trigger a handoff or a step-down.
func TestRemoveMember_Other_DoesNotStepDown(t *testing.T) {
	store := storeForPropose()
	node, transport := newRemoveTestNode(t, store)

	assert.NoError(t, node.RemoveMember(context.Background(), "node-3"))

	transport.AssertNotCalled(t, methodTimeoutNow, mock.Anything, mock.Anything)
	assert.Empty(t, node.electionTimeoutCh, "no step-down should have been requested")
}

// assertStepDownRequested checks that leadership was relinquished through the
// heartbeat orchestrator rather than by flipping the role directly — the Bug 3
// invariant that only the goroutine owning a lifecycle may end it.
func assertStepDownRequested(t *testing.T, node *Node) {
	t.Helper()
	select {
	case <-node.electionTimeoutCh:
	case <-time.After(time.Second):
		t.Fatal("no step-down was signalled to the heartbeat orchestrator")
	}
	assert.Equal(t, ServerRole_Leader, node.GetRole(),
		"RemoveMember must not flip the role itself; startSendLogs owns that transition")
}
