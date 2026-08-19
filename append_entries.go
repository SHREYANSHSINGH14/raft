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
		zerolog.Ctx(ctx).Debug().
			Str("leader", args.LeaderID).
			Uint64("leader_term", args.Term).
			Uint("our_term", currentTerm).
			Msg("appendEntries rejected: stale term")
		return AppendEntriesResponse{
			Term:    uint64(currentTerm),
			Success: false,
		}, nil
	}

	// The receiving half of convergence. One line per AppendEntries says what the
	// leader offered and from what anchor; the matching "accepted" line below says
	// where this node ended up. Together they are the follower's side of the story
	// that the leader's match-index and commit-index lines tell from the other end.
	zerolog.Ctx(ctx).Debug().
		Str("leader", args.LeaderID).
		Uint64("term", args.Term).
		Uint64("prev_log_index", args.PrevLogIndex).
		Uint64("prev_log_term", args.PrevLogTerm).
		Int("entries", len(args.Entries)).
		Uint64("leader_commit", args.LeaderCommit).
		Uint("our_commit", n.GetCommitIndex()).
		Msg("appendEntries received")

	if args.Term > uint64(currentTerm) {
		// Adopting the term and clearing the vote is one write — see setTermAndVote.
		if err := n.setTermAndVote(ctx, uint(args.Term), ""); err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msgf("append entries db err: %s", err.Error())
			return AppendEntriesResponse{}, err
		}
		currentTerm = uint(args.Term)
	}

	// prevLog consistency check. logTermAt returns the term at prevLogIndex,
	// treating the snapshot boundary as a valid anchor: right after an
	// InstallSnapshot the leader's next AppendEntries carries prevLogIndex equal to
	// our snapshot's last-included index, whose entry is compacted — but the
	// snapshot metadata proves we agree at that index. ok == false means we have
	// neither the entry nor an anchor there, i.e. a log inconsistency: reply false
	// and let the leader back off (or fall back to InstallSnapshot).
	prevTerm, ok, err := n.logTermAt(ctx, args.PrevLogIndex)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msgf("append entries db err: %s", err.Error())
		return AppendEntriesResponse{}, err
	}
	if !ok || prevTerm != args.PrevLogTerm {
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
	lastLogIdx, err := n.store.GetLastIndex(ctx)
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

		// React to any membership changes carried by the newly appended entries. A
		// config entry carries the whole membership, so applying it replaces
		// configurations.latest outright — the follower's half of what the leader does
		// in appendEntry.
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
		lastLogIdx, err := n.store.GetLastIndex(ctx)
		if err != nil {
			zerolog.Ctx(ctx).Error().Err(err).Msgf("append entries db err: %s", err.Error())
			return AppendEntriesResponse{}, err
		}

		minCommitIndex := min(args.LeaderCommit, uint64(lastLogIdx))
		if prev := n.GetCommitIndex(); uint(minCommitIndex) > prev {
			// Capped by our own last index, not the leader's: we can only commit what
			// we actually hold. That cap is why a follower's commit can trail the
			// leader's for a round even when nothing is wrong.
			zerolog.Ctx(ctx).Debug().
				Uint("from", prev).
				Uint64("to", minCommitIndex).
				Uint64("leader_commit", args.LeaderCommit).
				Uint("our_last_log", lastLogIdx).
				Msg("follower commit index advanced")
		}
		n.SetCommitIndex(uint(minCommitIndex))
	}

	if n.GetLeaderID() != args.LeaderID {
		n.SetLeaderID(args.LeaderID)
	}

	n.signalElectionTimeout() // a live leader has spoken; hold off on campaigning

	// last_log_index is derived, not read: a log line must not add a store call.
	// On the success path the log ends exactly where the leader said it would.
	zerolog.Ctx(ctx).Debug().
		Str("leader", args.LeaderID).
		Int("entries", len(args.Entries)).
		Uint64("last_log_index", args.PrevLogIndex+uint64(len(args.Entries))).
		Uint("commit_index", n.GetCommitIndex()).
		Msg("appendEntries accepted")

	return AppendEntriesResponse{
		Term:    uint64(currentTerm),
		Success: true,
	}, nil
}

// logTermAt returns the term of the log entry at index for the prevLog
// consistency check, treating the log floor as a valid anchor even when the entry
// itself has been compacted into a snapshot:
//   - index 0                      → term 0 (the empty-log floor).
//   - index == snapshotLatestIndex → the snapshot's term; the entry lives inside
//     the latest snapshot and its metadata is the proof we agree at that index.
//   - otherwise                    → the stored entry's term.
//
// ok is false when there is neither a stored entry nor an anchor at index, which
// the caller treats as a log inconsistency (reply false; the leader backs off).
func (n *Node) logTermAt(ctx context.Context, index uint64) (term uint64, ok bool, err error) {
	if index == 0 {
		return 0, true, nil
	}
	if snapIdx := n.GetSnapshotLatestIndex(); snapIdx != 0 && index == uint64(snapIdx) {
		return uint64(n.GetSnapshotLatestTerm()), true, nil
	}
	entry, err := n.store.GetLogByIndex(ctx, uint(index))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return 0, false, nil
		}
		return 0, false, err
	}
	return entry.Term, true, nil
}
