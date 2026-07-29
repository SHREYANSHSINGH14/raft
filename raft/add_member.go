package raft

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/rs/zerolog"
)

func (n *Node) AddMember(ctx context.Context, peerID string, peerState PeerState) error {
	n.clientMu.Lock()
	if n.GetRole() != ServerRole_Leader {
		n.clientMu.Unlock()
		return fmt.Errorf("not the leader: current leader is %q", n.GetLeaderID())
	}

	if n.hasStagingPeer() {
		n.clientMu.Unlock()
		return fmt.Errorf("addMember: one member addition already in progress")
	}

	n.addPeer(peerID, Peer{
		PeerState:  PeerState_Staging,
		NextIndex:  0,
		MatchIndex: 0,
	})

	memberConfigs := n.peersSnapshot()

	data, err := json.Marshal(memberConfigs)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("addMember: failed to marshal member info")
	}

	// 1. Append the member additon log
	entry, err := n.appendEntry(ctx, EntryType_Config, data)
	if err != nil {
		n.clientMu.Unlock()
		return fmt.Errorf("addMember: %w", err)
	}
	n.clientMu.Unlock()

	// 2. Wait for it to commit
	n.commitCond.L.Lock()
	for n.commitIndex < uint(entry.Index) && ctx.Err() == nil {
		n.commitCond.Wait()
	}
	if ctx.Err() != nil {
		n.commitCond.L.Unlock()
		return fmt.Errorf("addMember: context cancelled before commit")
	}
	n.commitCond.L.Unlock()

	n.clientMu.Lock()
	// 3. Send snapshot
	// 4. Start catching up the node
	// 5. Send log of state change to Voter/Non-voter (Here what you are saying about appendEntries can be true)
	return nil
}
