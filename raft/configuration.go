package raft

import (
	"encoding/json"
	"fmt"
)

// -------------------------------------------
// Mutators for the latest/committed configuration views. All of these lock mu
// internally, so callers must NOT already hold mu. See the configurations type in
// raft_config.go for the meaning of the two views.
// -------------------------------------------

// setLatestConfiguration overwrites the latest configuration and the index that
// produced it. The peers map is deep-copied so callers can pass in another view
// (e.g. committed during a rollback) without the two aliasing afterwards.
func (n *Node) setLatestConfiguration(peers map[string]Peer, index uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.configurations.latest = clonePeers(peers)
	n.configurations.latestIndex = index
}

// setCommittedConfiguration advances the committed view once a config entry has
// committed.
func (n *Node) setCommittedConfiguration(peers map[string]Peer, index uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.configurations.committed = clonePeers(peers)
	n.configurations.committedIndex = index
}

// advanceCommittedConfiguration promotes latest into committed once the log entry
// that produced latest has committed. Called by the commit-index updater every
// time commitIndex moves.
//
// Both halves are read under ONE mu hold. Reading latestIndex and then snapshotting
// latest separately can tear: a config entry appended in between would pair a new
// map with an old index, which is exactly the (index, map) invariant the
// configurations type documents.
//
// committedIndex becomes latestIndex, NOT commitIndex. They are different facts —
// commitIndex is merely where the log has got to, while latestIndex is where this
// configuration came from, and it is the latter that rollbackLatestIfTruncated
// compares truncation points against.
func (n *Node) advanceCommittedConfiguration(commitIndex uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.configurations.latestIndex > commitIndex {
		return // the entry behind latest has not committed yet
	}
	if n.configurations.latestIndex <= n.configurations.committedIndex {
		return // already promoted; nothing new has committed
	}

	n.configurations.committed = clonePeers(n.configurations.latest)
	n.configurations.committedIndex = n.configurations.latestIndex
}

// rollbackLatestIfTruncated rolls the latest configuration back to the committed
// one when a log suffix starting at fromIndex is truncated during conflict
// resolution. If that truncation removes the entry that produced the latest
// configuration (fromIndex <= latestIndex), latest is no longer backed by the
// log and must revert to the last configuration we know committed.
//
// The read of latestIndex and the write of latest happen under a single mu hold
// so a concurrent reader can never observe a torn (index, map) pair.
func (n *Node) rollbackLatestIfTruncated(fromIndex uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if fromIndex > n.configurations.latestIndex {
		return
	}
	n.configurations.latest = clonePeers(n.configurations.committed)
	n.configurations.latestIndex = n.configurations.committedIndex
}

// processConfigurationLogEntry applies a freshly-appended EntryType_Config entry
// to the latest configuration. The entry carries the WHOLE cluster configuration
// as a JSON map[string]Peer (see AddMember, which marshals n.peersSnapshot()), so
// applying it is a straight replace: latest becomes the decoded map and
// latestIndex becomes entry.Index.
//
// It does NOT touch committed — that only advances once the entry commits
// (setCommittedConfiguration, wired from the apply loop; see STATE.md).
//
// NOTE: this runs on the follower path (HandleAppendEntries). The decoded Peers
// carry the leader's NextIndex/MatchIndex, which are leader-only replication
// bookkeeping; a follower never reads them, and becomeLeader re-initialises them
// on promotion, so adopting them here is harmless.
func (n *Node) processConfigurationLogEntry(entry LogEntry) error {
	var peers map[string]Peer
	if err := json.Unmarshal(entry.Data, &peers); err != nil {
		return fmt.Errorf("processConfigurationLogEntry: unmarshal config at index %d: %w", entry.Index, err)
	}

	n.setLatestConfiguration(peers, entry.Index)
	return nil
}

func (n *Node) getLatestConfigurationIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.configurations.latestIndex
}

func (n *Node) getCommittedConfigurationIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.configurations.committedIndex
}
