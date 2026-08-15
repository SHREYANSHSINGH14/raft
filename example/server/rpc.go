package server

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/SHREYANSHSINGH14/raft"
	"github.com/SHREYANSHSINGH14/raft/example/types"
	"google.golang.org/grpc"
)

func (s *Server) RequestVote(ctx context.Context, args *types.RequestVoteArgs) (*types.RequestVoteResponse, error) {
	resp, err := s.Node.HandleRequestVote(ctx, raft.RequestVoteArgs{
		Term:         args.Term,
		CandidateID:  args.CandidateId,
		LastLogIndex: args.LastLogIndex,
		LastLogTerm:  args.LastLogTerm,
	})
	if err != nil {
		return nil, err
	}
	return &types.RequestVoteResponse{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
	}, nil
}

// PreVote routes to HandlePreVote, never to HandleRequestVote. The two RPCs carry
// identical fields, which makes the wrong target an easy typo and a silent one:
// HandleRequestVote persists a term and spends a vote, and routing probes there
// would give away exactly what pre-vote exists to protect.
func (s *Server) PreVote(ctx context.Context, args *types.PreVoteArgs) (*types.PreVoteResponse, error) {
	resp, err := s.Node.HandlePreVote(ctx, raft.PreVoteArgs{
		Term:         args.Term,
		CandidateID:  args.CandidateId,
		LastLogIndex: args.LastLogIndex,
		LastLogTerm:  args.LastLogTerm,
	})
	if err != nil {
		return nil, err
	}
	return &types.PreVoteResponse{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
	}, nil
}

func (s *Server) TimeoutNow(ctx context.Context, args *types.TimeoutNowArgs) (*types.TimeoutNowResponse, error) {
	resp, err := s.Node.HandleTimeoutNow(ctx, raft.TimeoutNowArgs{
		Term:     args.Term,
		LeaderID: args.LeaderId,
	})
	if err != nil {
		return nil, err
	}
	return &types.TimeoutNowResponse{
		Term:    resp.Term,
		Success: resp.Success,
	}, nil
}

func (s *Server) AppendEntries(ctx context.Context, args *types.AppendEntriesArgs) (*types.AppendEntriesResponse, error) {
	resp, err := s.Node.HandleAppendEntries(ctx, raft.AppendEntriesArgs{
		Term:         args.Term,
		LeaderID:     args.LeaderId,
		PrevLogIndex: args.PrevLogIndex,
		PrevLogTerm:  args.PrevLogTerm,
		Entries:      types.LogEntriesToRaft(args.Entries),
		LeaderCommit: args.LeaderCommit,
	})
	if err != nil {
		return nil, err
	}
	return &types.AppendEntriesResponse{
		Term:    resp.Term,
		Success: resp.Success,
	}, nil
}

func (s *Server) InstallSnapshot(stream grpc.ClientStreamingServer[types.InstallSnapshotArgs, types.InstallSnapshotResponse]) error {
	ctx := stream.Context()
	metaReceived := false

	var req raft.InstallSnapshotArgs
	pr, pw := io.Pipe()
	defer func() {
		pw.Close()
	}()

	type handleInstallResp struct {
		res *raft.InstallSnapshotResponse
		err error
	}
	handleInstallRespCh := make(chan *handleInstallResp, 1)
	bytesWritten := 0

	for {
		arg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		switch arg.Payload.(type) {
		case *types.InstallSnapshotArgs_SnapshotMeta:
			if metaReceived {
				return fmt.Errorf("meta already received")
			}
			snapshotMeta := arg.GetSnapshotMeta()

			memberConfig := make(map[string]raft.PeerState, len(snapshotMeta.MemberConfig))
			for _, conf := range snapshotMeta.MemberConfig {
				memberConfig[conf.Id] = protoToRaftPeerState(conf.PeerState)
			}

			req.Term = snapshotMeta.Term
			req.LeaderID = snapshotMeta.LeaderId
			req.SnapshotSize = snapshotMeta.SnapshotSize
			req.SnapshotMetadata = raft.SnapshotMetadata{
				LastIncludedIndex: snapshotMeta.LastIncludedIndex,
				LastIncludedTerm:  snapshotMeta.LastIncludedTerm,
				TimeStamp:         snapshotMeta.Timestamp.AsTime(),
				MemberConfig:      memberConfig,
			}
			req.Reader = pr
			go func() {
				res, err := s.Node.HandleInstallSnapshot(ctx, &req)
				handleInstallRespCh <- &handleInstallResp{
					res: res,
					err: err,
				}
			}()
		case *types.InstallSnapshotArgs_SnapshotChunk:
			if !metaReceived {
				return fmt.Errorf("meta data has to be sent before streaming data")
			}
			snapshotChunk := arg.GetSnapshotChunk()
			n, err := pw.Write(snapshotChunk.Chunk)
			if err != nil && !errors.Is(io.ErrClosedPipe, err) {
				return err
			}
			bytesWritten += n
		}
	}
	select {
	case handleInstallResp := <-handleInstallRespCh:
		res := handleInstallResp.res
		err := handleInstallResp.err
		if err != nil {
			stream.SendAndClose(&types.InstallSnapshotResponse{
				Term:    res.Term,
				Success: res.Success,
			})
			return err
		}
		if res != nil {
			stream.SendAndClose(&types.InstallSnapshotResponse{
				Term:    res.Term,
				Success: res.Success,
			})
			return nil
		}
		stream.SendAndClose(nil)
		return fmt.Errorf("handleInstallSnapshot response is empty")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func protoToRaftPeerState(state types.PeerState) raft.PeerState {
	switch state {
	case types.PeerState_UNKNOWN:
		return raft.PeerState_Unknown
	case types.PeerState_STAGING:
		return raft.PeerState_Staging
	case types.PeerState_VOTER:
		return raft.PeerState_Voter
	case types.PeerState_NONVOTER:
		return raft.PeerState_NonVoter
	default:
		return raft.PeerState_Unknown
	}
}

func raftToProtoPeerState(state raft.PeerState) types.PeerState {
	switch state {
	case raft.PeerState_Unknown:
		return types.PeerState_UNKNOWN
	case raft.PeerState_Staging:
		return types.PeerState_STAGING
	case raft.PeerState_Voter:
		return types.PeerState_VOTER
	case raft.PeerState_NonVoter:
		return types.PeerState_NONVOTER
	default:
		return types.PeerState_UNKNOWN
	}
}
