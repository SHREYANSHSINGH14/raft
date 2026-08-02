package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/SHREYANSHSINGH14/raft/config"
	"github.com/SHREYANSHSINGH14/raft/db"
	"github.com/SHREYANSHSINGH14/raft/raft"
	"github.com/SHREYANSHSINGH14/raft/types"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Server struct {
	Node *raft.Node

	baseUrl   string
	port      string
	debugPort string

	mu sync.Mutex

	ctx        context.Context
	cancelFunc context.CancelFunc

	// embedding the unimplemented server to make sure if we add any new rpc in future then we will get compile error if we forget to implement that rpc
	types.UnimplementedRaftRpcServer
}

var _ types.RaftRpcServer = &Server{}

func NewServer(ctx context.Context, cfg config.Config) (*Server, error) {
	var server Server
	server.ctx, server.cancelFunc = context.WithCancel(ctx)

	// initialize logger here, attach to server context
	logLevel := config.GetLogLevel(cfg.LogLevel)
	logger := zerolog.New(os.Stdout).With().Timestamp().Caller().Logger()
	zerolog.DefaultContextLogger = &logger
	zerolog.SetGlobalLevel(logLevel)
	server.ctx = logger.WithContext(server.ctx)

	store, err := db.NewStore(ctx, cfg.DBDir)
	if err != nil {
		fmt.Println("error while initializing db store")
		return nil, err
	}

	transport, err := newGRPCTransport(cfg.ServerIDS, cfg.RPCTimeoutMs)
	if err != nil {
		return nil, fmt.Errorf("error creating transport: %w", err)
	}

	peers := make(map[string]raft.Peer, len(cfg.ServerIDS))
	for id := range cfg.ServerIDS {
		peers[id] = raft.Peer{PeerState: raft.PeerState_Voter}
	}

	raftCfg := raft.Config{
		ID:            cfg.ID,
		Peers:         peers,
		RPCTimeoutMs:  cfg.RPCTimeoutMs,
		HeartbeatMs:   cfg.HeartbeatMs,
		ElectionMinMs: cfg.ElectionMinMs,
		ElectionMaxMs: cfg.ElectionMaxMs,
	}

	node := raft.NewNode(raftCfg, store, transport, nil)

	server.Node = node
	server.baseUrl = cfg.BaseURL
	server.port = cfg.Port
	server.debugPort = cfg.DebugPort

	return &server, nil
}

func (s *Server) Start() {
	// Start grpc server
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%s", s.baseUrl, s.port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	types.RegisterRaftRpcServer(grpcServer, s)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	// This goroutine will wait for the context to be done and then gracefully stop the grpc server and cancel the server context to stop all other goroutines
	// IMPORTANT to gracefully stop the grpc server to avoid any panics in case of any ongoing rpc calls when the server is stopped
	// Also canceling the server context will stop all other goroutines like election timeout and send logs goroutine to avoid any
	// unwanted role transitions or rpc calls when the server is stopped
	go func() {
		<-s.ctx.Done()
		grpcServer.GracefulStop()
	}()

	zerolog.Ctx(s.ctx).Debug().Str("id", s.Node.GetID()).Str("role", string(s.Node.GetRole())).Str("listen_address", s.baseUrl+":"+s.port).Msg("server started")

	debugServer := NewDebugServer(s)
	debugServer.Start(s.debugPort)

	zerolog.Ctx(s.ctx).Debug().Msgf("starting debug server on port %d", 8080)

	s.Node.Start(s.ctx)
}

// grpcTransport implements raft.Transport using gRPC connections to peers.
type grpcTransport struct {
	clients map[string]types.RaftRpcClient
	timeout time.Duration
}

func newGRPCTransport(peerURLs map[string]string, timeoutMs int) (*grpcTransport, error) {
	clients := make(map[string]types.RaftRpcClient, len(peerURLs))
	for id, url := range peerURLs {
		conn, err := grpc.NewClient(url, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("failed to connect to peer %s at %s: %w", id, url, err)
		}
		clients[id] = types.NewRaftRpcClient(conn)
	}
	return &grpcTransport{
		clients: clients,
		timeout: time.Duration(timeoutMs) * time.Millisecond,
	}, nil
}

func (t *grpcTransport) RequestVote(ctx context.Context, peerID string, args raft.RequestVoteArgs) (raft.RequestVoteResponse, error) {
	client, ok := t.clients[peerID]
	if !ok {
		return raft.RequestVoteResponse{}, fmt.Errorf("unknown peer: %s", peerID)
	}
	ctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()

	resp, err := client.RequestVote(ctx, &types.RequestVoteArgs{
		CandidateId:  args.CandidateID,
		Term:         args.Term,
		LastLogTerm:  args.LastLogTerm,
		LastLogIndex: args.LastLogIndex,
	})
	if err != nil {
		return raft.RequestVoteResponse{}, err
	}
	return raft.RequestVoteResponse{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
	}, nil
}

func (t *grpcTransport) AppendEntries(ctx context.Context, peerID string, args raft.AppendEntriesArgs) (raft.AppendEntriesResponse, error) {
	client, ok := t.clients[peerID]
	if !ok {
		return raft.AppendEntriesResponse{}, fmt.Errorf("unknown peer: %s", peerID)
	}
	ctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()

	entries := make([]*types.LogEntry, len(args.Entries))
	for i, e := range args.Entries {
		entries[i] = &types.LogEntry{
			Index: e.Index,
			Term:  e.Term,
			Data:  e.Data,
		}
	}

	resp, err := client.AppendEntries(ctx, &types.AppendEntriesArgs{
		Term:         args.Term,
		LeaderId:     args.LeaderID,
		PrevLogIndex: args.PrevLogIndex,
		PrevLogTerm:  args.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: args.LeaderCommit,
	})
	if err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	return raft.AppendEntriesResponse{
		Term:    resp.Term,
		Success: resp.Success,
	}, nil
}

// PreVote is a stub, for the same reason as InstallSnapshot below: proto/rpc.proto
// has no PreVote RPC yet, so types.RaftRpcClient can't send one. Wire the client
// call once the proto exists — the receiving side (Node.HandlePreVote) is done.
func (t *grpcTransport) PreVote(ctx context.Context, peerID string, args raft.PreVoteArgs) (raft.PreVoteResponse, error) {
	return raft.PreVoteResponse{}, fmt.Errorf("PreVote not implemented")
}

// InstallSnapshot is a stub: proto/rpc.proto has no InstallSnapshot RPC yet, so
// types.RaftRpcClient can't send one. Wire the streaming client call once the
// proto exists. See STATE.md.
func (t *grpcTransport) InstallSnapshot(ctx context.Context, peerID string, args raft.InstallSnapshotArgs) (raft.InstallSnapshotResponse, error) {
	return raft.InstallSnapshotResponse{}, fmt.Errorf("InstallSnapshot not implemented")
}
