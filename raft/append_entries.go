package raft

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/SHREYANSHSINGH14/raft/types"
	"github.com/cockroachdb/pebble"
	"github.com/rs/zerolog"
)

func (p *Server) AppendEntries(ctx context.Context, args *types.AppendEntriesArgs) (*types.AppendEntriesResponse, error) {
	p.raftMu.Lock()
	defer p.raftMu.Unlock()

	if strings.TrimSpace(args.LeaderId) == "" {
		err := fmt.Errorf("leader id is empty")
		zerolog.Ctx(ctx).Error().Err(err).Msg("leader id is empty")
		return nil, err
	}

	currentTerm, err := p.store.GetCurrentTerm(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("append entries db err: %s", err.Error())
		return nil, err
	}

	if args.Term < uint64(currentTerm) {
		return &types.AppendEntriesResponse{
			Term:    uint64(currentTerm),
			Success: false,
		}, nil
	}

	if args.Term > uint64(currentTerm) {
		err := p.store.SetCurrentTerm(ctx, uint(args.Term))
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msgf("append entries db err: %s", err.Error())
			return nil, err
		}
		currentTerm = uint(args.Term)

		err = p.store.SetVotedFor(ctx, "")
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msgf("append entries db err: %s", err.Error())
			return nil, err
		}
	}

	prevLog, err := p.store.GetLogByIndex(ctx, uint(args.PrevLogIndex))
	if err != nil {
		if !errors.Is(err, pebble.ErrNotFound) {
			zerolog.Ctx(ctx).Error().Err(err).Msgf("append entries db err: %s", err.Error())
			return nil, err
		}
		if args.PrevLogIndex != 0 {
			// If prevLogIndex is not 0 and we are getting ErrNotFound then it means there is log inconsistency because it means leader is expecting some log at prevLogIndex but follower doesn't have that log
			// This can happen when there is a new leader and it is trying to replicate its logs to the followers but some followers are lagging behind and they don't have the logs that leader has,
			// in that case we should just return false and let the leader handle the log inconsistency in next append entries call by sending the logs from nextIndex to end of log to that follower
			return &types.AppendEntriesResponse{
				Term:    uint64(currentTerm),
				Success: false,
			}, nil
		}
	}

	if prevLog != nil && prevLog.Term != args.PrevLogTerm {
		return &types.AppendEntriesResponse{
			Term:    uint64(currentTerm),
			Success: false,
		}, nil
	}

	// Here we are not checking for log consistency and directly appending the logs because in this implementation we are not supporting log replication optimization
	// and we are just sending all the logs from nextIndex to end of log to the follower and if there is any inconsistency then it will be resolved in next append entries
	// call when leader will again send all the logs from nextIndex to end of log and in that case follower will just overwrite the inconsistent log
	// with the correct log from leader

	// Append Logs Optimization Phases

	// Phase 1 - Leader sends the new logs to the followers and they append the new logs to their log store
	// leader will keep decrementing the nextIndex for that follower until it finds the right log index from which
	// it should send the logs to that follower and once it finds that index then it will update the nextIndex for
	// that follower to be that index + 1 and then in next append entries call it will send the logs from that index + 1 to end of log (DONE)

	// Phase 2 - We response is unsuccessful because of log inconsistency, then follower will also send the conflicting log index in response
	// follower will skip to starting of term of that conflicting log index, then leader will update the nextIndex for that follower to be that conflicting log index
	// and then in next append entries call it will send the logs from that conflicting log index to end of log (TODO)

	// If we are here then it means the logs are consistent till prevLogIndex and prevLogTerm, so we can safely truncate the logs from prevLogIndex + 1 and append the new logs
	// This is because if there is any inconsistency then it will be in the logs after prevLogIndex and prevLogTerm, so we can just truncate the logs from there and append the new logs from leader which are correct
	// This is simple way to resolve log inconsistency without having to find the exact conflicting log index and it is also efficient because in real world scenario we won't have many inconsistent logs and even if
	// we have many inconsistent logs then it means there is some issue with the leader and in that case we can just truncate all the logs after prevLogIndex and append the new logs from leader which are correct
	err = p.store.TruncateLogs(ctx, uint(args.PrevLogIndex)+1)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("append entries db err: %s", err.Error())
		return nil, err
	}

	// Append the new logs to the log store
	if len(args.Entries) > 0 {
		err = p.store.AppendLogs(ctx, args.Entries)
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msgf("append entries db err: %s", err.Error())
			return nil, err
		}
	}

	if args.LeaderCommit > uint64(p.getCommitIndex()) {
		lastLogIdx, err := p.store.GetLastLogIndex(ctx)
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msgf("append entries db err: %s", err.Error())
			return nil, err
		}

		minCommitIndex := min(args.LeaderCommit, uint64(lastLogIdx))
		p.setCommitIndex(uint(minCommitIndex))
	}

	if p.getLeaderID() != args.LeaderId {
		p.setLeaderID(args.LeaderId)
	}

	p.electionTimeoutCh <- struct{}{} // reset election timeout

	return &types.AppendEntriesResponse{
		Term:    uint64(currentTerm),
		Success: true,
	}, nil
}
