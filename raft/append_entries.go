package raft

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/rs/zerolog"
)

// NOTE: This method is thread safe and can be called concurrently by multiple callers
func (n *Node) HandleAppendEntries(ctx context.Context, args AppendEntriesArgs) (AppendEntriesResponse, error) {
	n.clientMu.Lock()
	defer n.clientMu.Unlock()

	if strings.TrimSpace(args.LeaderID) == "" {
		err := fmt.Errorf("leader id is empty")
		zerolog.Ctx(ctx).Error().Err(err).Msg("leader id is empty")
		return AppendEntriesResponse{}, err
	}

	currentTerm, err := n.store.GetCurrentTerm(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("append entries db err: %s", err.Error())
		return AppendEntriesResponse{}, err
	}

	if args.Term < uint64(currentTerm) {
		return AppendEntriesResponse{
			Term:    uint64(currentTerm),
			Success: false,
		}, nil
	}

	if args.Term > uint64(currentTerm) {
		err := n.store.SetCurrentTerm(ctx, uint(args.Term))
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msgf("append entries db err: %s", err.Error())
			return AppendEntriesResponse{}, err
		}
		currentTerm = uint(args.Term)

		err = n.store.SetVotedFor(ctx, "")
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msgf("append entries db err: %s", err.Error())
			return AppendEntriesResponse{}, err
		}
	}

	prevLog, err := n.store.GetLogByIndex(ctx, uint(args.PrevLogIndex))
	if err != nil {
		if !errors.Is(err, ErrNotFound) {
			zerolog.Ctx(ctx).Error().Err(err).Msgf("append entries db err: %s", err.Error())
			return AppendEntriesResponse{}, err
		}
		if args.PrevLogIndex != 0 {
			// If prevLogIndex is not 0 and we are getting ErrNotFound then it means there is log inconsistency because it means leader is expecting some log at prevLogIndex but follower doesn't have that log
			// This can happen when there is a new leader and it is trying to replicate its logs to the followers but some followers are lagging behind and they don't have the logs that leader has,
			// in that case we should just return false and let the leader handle the log inconsistency in next append entries call by sending the logs from nextIndex to end of log to that follower
			return AppendEntriesResponse{
				Term:    uint64(currentTerm),
				Success: false,
			}, nil
		}
		// prevLogIndex == 0 and ErrNotFound — fresh follower, no prior log to check, skip term check
	}

	if prevLog.Term != args.PrevLogTerm {
		return AppendEntriesResponse{
			Term:    uint64(currentTerm),
			Success: false,
		}, nil
	}

	// The leader-side replication fallback is still the naive one: on a failed
	// AppendEntries it decrements nextIndex by one and retries (Phase 1 below).
	// The follower side (this function), however, now does proper §5.3 conflict
	// resolution rather than blindly truncating everything after prevLogIndex.

	// Append Logs Optimization Phases

	// Phase 1 - Leader sends the new logs to the followers and they append the new logs to their log store
	// leader will keep decrementing the nextIndex for that follower until it finds the right log index from which
	// it should send the logs to that follower and once it finds that index then it will update the nextIndex for
	// that follower to be that index + 1 and then in next append entries call it will send the logs from that index + 1 to end of log (DONE)

	// Phase 2 - We response is unsuccessful because of log inconsistency, then follower will also send the conflicting log index in response
	// follower will skip to starting of term of that conflicting log index, then leader will update the nextIndex for that follower to be that conflicting log index
	// and then in next append entries call it will send the logs from that conflicting log index to end of log (TODO)

	// Resolve conflicts and append new entries (Raft §5.3).
	//
	// We deliberately do NOT blindly truncate everything after prevLogIndex and
	// re-append. A delayed or duplicated AppendEntries could otherwise wipe out
	// entries the leader still considers committed. Instead we walk the incoming
	// entries against what we already have:
	//   - an incoming entry we already hold with the SAME term is a no-op (skip it),
	//   - the first incoming index we don't have yet marks where the new suffix
	//     begins,
	//   - the first TERM CONFLICT (same index, different term) is where we delete
	//     our suffix and take the leader's version from that index onward.
	// Only that conflict case truncates, and only from the conflicting index.
	lastLogIdx, err := n.store.GetLastLogIndex(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("append entries db err: %s", err.Error())
		return AppendEntriesResponse{}, err
	}

	var newEntries []LogEntry
	for i, entry := range args.Entries {
		if entry.Index > uint64(lastLogIdx) {
			// We don't have this index yet — it and everything after is new.
			newEntries = args.Entries[i:]
			break
		}

		storeEntry, err := n.store.GetLogByIndex(ctx, uint(entry.Index))
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msgf("append entries db err: %s", err.Error())
			return AppendEntriesResponse{}, err
		}

		if entry.Term != storeEntry.Term {
			// Conflict: our entry at this index came from a different term. Delete it
			// and the whole suffix, then take the leader's entries from here onward.
			zerolog.Ctx(ctx).Warn().
				Uint64("from", entry.Index).
				Uint("to", lastLogIdx).
				Msg("clearing conflicting log suffix")
			if err := n.store.DeleteLogs(ctx, uint(entry.Index), 0); err != nil {
				zerolog.Ctx(ctx).Error().Err(err).Msgf("append entries db err: %s", err.Error())
				return AppendEntriesResponse{}, err
			}

			// If the truncation removed the entry that produced our latest cluster
			// configuration, latest is no longer backed by the log and must revert
			// to the last committed configuration.
			n.rollbackLatestIfTruncated(entry.Index)

			newEntries = args.Entries[i:]
			break
		}
	}

	if len(newEntries) > 0 {
		if err := n.store.AppendLogs(ctx, newEntries); err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msgf("append entries db err: %s", err.Error())
			return AppendEntriesResponse{}, err
		}

		// React to any membership changes carried by the newly appended entries.
		// processConfigurationLogEntry is a no-op today; the wiring lives here so
		// the config path is already exercised once it learns to mutate latest.
		for _, entry := range newEntries {
			if entry.Type == EntryType_Config {
				if err := n.processConfigurationLogEntry(entry); err != nil {
					zerolog.Ctx(ctx).Warn().Err(err).
						Uint64("index", entry.Index).
						Msg("failed to process configuration entry")
					return AppendEntriesResponse{}, err
				}
			}
		}
	}

	if args.LeaderCommit >= uint64(n.GetCommitIndex()) {
		lastLogIdx, err := n.store.GetLastLogIndex(ctx)
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msgf("append entries db err: %s", err.Error())
			return AppendEntriesResponse{}, err
		}

		minCommitIndex := min(args.LeaderCommit, uint64(lastLogIdx))
		n.SetCommitIndex(uint(minCommitIndex))
	}

	if n.GetLeaderID() != args.LeaderID {
		n.SetLeaderID(args.LeaderID)
	}

	n.electionTimeoutCh <- struct{}{} // reset election timeout

	return AppendEntriesResponse{
		Term:    uint64(currentTerm),
		Success: true,
	}, nil
}
