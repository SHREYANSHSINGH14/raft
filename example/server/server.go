package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"net"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/SHREYANSHSINGH14/raft"
	"github.com/SHREYANSHSINGH14/raft/example/config"
	"github.com/SHREYANSHSINGH14/raft/example/db"
	"github.com/SHREYANSHSINGH14/raft/example/statemachine"
	"github.com/SHREYANSHSINGH14/raft/example/types"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Server struct {
	Node *raft.Node
	// SM is the same state machine the node applies into. The debug handlers need it
	// directly: Register/WaitForResult/Forget are the client half of a proposal, and
	// the library never sees them.
	SM *statemachine.StateMachine

	// peerIDs is every member of the cluster including this node. cfg.ServerIDS
	// leaves self out; the debug status wants the whole configuration, and the
	// library's own peerIDs() is unexported.
	peerIDs []string

	// transport is held so membership changes can introduce a peer's address. The
	// library never learns addresses — that is the Transport's concern — so adding a
	// member is two steps that only the embedding can sequence.
	transport *grpcTransport

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

const (
	// defaultSnapshotIntervalS is the fallback when the environment leaves it unset.
	defaultSnapshotIntervalS = 300
	// sweepInterval is how often abandoned command waiters are reaped.
	sweepInterval = 30 * time.Second
)

func NewServer(ctx context.Context, cfg config.Config) (*Server, error) {
	var server Server
	server.ctx, server.cancelFunc = context.WithCancel(ctx)

	// initialize logger here, attach to server context
	logLevel := config.GetLogLevel(cfg.LogLevel)
	logger := zerolog.New(os.Stdout).With().Timestamp().Caller().Logger()
	zerolog.DefaultContextLogger = &logger
	zerolog.SetGlobalLevel(logLevel)
	server.ctx = logger.WithContext(server.ctx)

	store, err := db.NewStore(server.ctx, cfg.DBDir)
	if err != nil {
		fmt.Println("error while initializing db store")
		return nil, err
	}

	transport, err := newGRPCTransport(cfg.ServerIDS)
	if err != nil {
		return nil, fmt.Errorf("error creating transport: %w", err)
	}

	peers := make(map[string]raft.Peer, len(cfg.ServerIDS))
	for id := range cfg.ServerIDS {
		peers[id] = raft.Peer{PeerState: raft.PeerState_Voter}
	}

	// SnapshotInterval feeds time.NewTicker, which panics on a non-positive interval,
	// so a zero from the environment has to be caught here rather than at Start.
	snapshotInterval := cfg.SnapshotInterval
	if snapshotInterval == 0 {
		snapshotInterval = defaultSnapshotIntervalS
	}
	snapshotDir := cfg.SnapshotDir
	if snapshotDir == "" {
		snapshotDir = cfg.DBDir + "/snapshots"
	}

	raftCfg := raft.Config{
		ID:            cfg.ID,
		Peers:         peers,
		RPCTimeoutMs:  cfg.RPCTimeoutMs,
		HeartbeatMs:   cfg.HeartbeatMs,
		ElectionMinMs: cfg.ElectionMinMs,
		ElectionMaxMs: cfg.ElectionMaxMs,

		SnapshotDir:       snapshotDir,
		SnapshotInterval:  snapshotInterval,
		SnapshotThreshold: cfg.SnapshotThreshold,

		InstallSnapshotBaseMs:                cfg.InstallSnapshotBaseMs,
		InstallSnapshotDeadlineScaleSizeByte: cfg.InstallSnapshotDeadlineScaleSizeByte,
		InstallSnapshotDeadlineScaleTimeMs:   cfg.InstallSnapshotDeadlineScaleTimeMs,
	}

	sm := statemachine.NewStateMachine(server.ctx, store)
	node := raft.NewNode(raftCfg, store, transport, sm)

	server.Node = node
	server.SM = sm
	server.peerIDs = append([]string{cfg.ID}, slices.Collect(maps.Keys(cfg.ServerIDS))...)
	slices.Sort(server.peerIDs)
	server.transport = transport
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

	// The apply loop stops for good when it trips Fatal, and nothing else notices:
	// the gRPC server would go on accepting writes and answering reads from a state
	// machine that has permanently stopped tracking the log. Started before
	// Node.Start, which blocks until the context is done.
	go func() {
		select {
		case <-s.Node.Fatal():
			zerolog.Ctx(s.ctx).Error().Err(s.Node.FatalErr()).Msg("node reported fatal failure, shutting down")
			s.cancelFunc()
		case <-s.ctx.Done():
		}
	}()

	// Backstop for a handler that never reached its deferred Forget.
	go func() {
		ticker := time.NewTicker(sweepInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if dropped := s.SM.Sweep(); dropped > 0 {
					zerolog.Ctx(s.ctx).Debug().Int("dropped", dropped).Msg("swept abandoned command waiters")
				}
			case <-s.ctx.Done():
				return
			}
		}
	}()

	zerolog.Ctx(s.ctx).Debug().Str("id", s.Node.GetID()).Str("role", string(s.Node.GetRole())).Str("listen_address", s.baseUrl+":"+s.port).Msg("server started")

	debugServer := NewDebugServer(s)
	debugServer.Start(s.debugPort)

	zerolog.Ctx(s.ctx).Debug().Msgf("starting debug server on port %d", 8080)

	s.Node.Start(s.ctx)
}

// grpcTransport implements raft.Transport using gRPC connections to peers.
//
// It sets no deadlines of its own. The library deadlines every RPC before handing us
// the context, and two of them are scaled to the work: AppendEntries by batch size,
// InstallSnapshot by snapshot size. A context.WithTimeout here would keep whichever
// deadline is earlier, so a flat per-transport timeout would silently override that
// scaling and cap a large catch-up at the budget for a heartbeat.
//
// The client map is mutable: AddMember introduces peers the process did not start
// with, so it is guarded by mu and every method reads it through client().
type grpcTransport struct {
	mu      sync.RWMutex
	clients map[string]peerConn
}

// peerConn keeps the connection alongside the client so RemovePeer can close it.
type peerConn struct {
	client types.RaftRpcClient
	conn   *grpc.ClientConn
}

func newGRPCTransport(peerURLs map[string]string) (*grpcTransport, error) {
	t := &grpcTransport{clients: make(map[string]peerConn, len(peerURLs))}
	for id, url := range peerURLs {
		if err := t.AddPeer(id, url); err != nil {
			return nil, err
		}
	}
	return t, nil
}

// AddPeer dials a peer and makes it addressable.
//
// It must run *before* Node.AddMember, not after it succeeds: AddMember catches the
// new member up over this very transport, so a peer that is not in the map yet cannot
// be reached, catch-up fails, and the addition rolls itself back. Undo with RemovePeer
// if AddMember then fails.
//
// grpc.NewClient does not connect — it builds a lazy client — so dialling a peer that
// is ultimately rejected costs nothing but the map entry.
func (t *grpcTransport) AddPeer(id, url string) error {
	conn, err := grpc.NewClient(url, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to peer %s at %s: %w", id, url, err)
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if existing, ok := t.clients[id]; ok {
		// Replacing an address: close the old connection rather than leaking it.
		_ = existing.conn.Close()
	}
	t.clients[id] = peerConn{client: types.NewRaftRpcClient(conn), conn: conn}
	return nil
}

// RemovePeer drops a peer and closes its connection. Safe to call for an id that was
// never added.
func (t *grpcTransport) RemovePeer(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if pc, ok := t.clients[id]; ok {
		_ = pc.conn.Close()
		delete(t.clients, id)
	}
}

func (t *grpcTransport) client(peerID string) (types.RaftRpcClient, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	pc, ok := t.clients[peerID]
	return pc.client, ok
}

func (t *grpcTransport) RequestVote(ctx context.Context, peerID string, args raft.RequestVoteArgs) (raft.RequestVoteResponse, error) {
	client, ok := t.client(peerID)
	if !ok {
		return raft.RequestVoteResponse{}, fmt.Errorf("unknown peer: %s", peerID)
	}
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
	client, ok := t.client(peerID)
	if !ok {
		return raft.AppendEntriesResponse{}, fmt.Errorf("unknown peer: %s", peerID)
	}
	resp, err := client.AppendEntries(ctx, &types.AppendEntriesArgs{
		Term:         args.Term,
		LeaderId:     args.LeaderID,
		PrevLogIndex: args.PrevLogIndex,
		PrevLogTerm:  args.PrevLogTerm,
		Entries:      types.LogEntriesFromRaft(args.Entries),
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

func (t *grpcTransport) PreVote(ctx context.Context, peerID string, args raft.PreVoteArgs) (raft.PreVoteResponse, error) {
	client, ok := t.client(peerID)
	if !ok {
		return raft.PreVoteResponse{}, fmt.Errorf("unknown peer: %s", peerID)
	}
	resp, err := client.PreVote(ctx, &types.PreVoteArgs{
		Term:         args.Term,
		CandidateId:  args.CandidateID,
		LastLogIndex: args.LastLogIndex,
		LastLogTerm:  args.LastLogTerm,
	})
	if err != nil {
		return raft.PreVoteResponse{}, err
	}
	return raft.PreVoteResponse{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
	}, nil
}

func (t *grpcTransport) TimeoutNow(ctx context.Context, peerID string, args raft.TimeoutNowArgs) (raft.TimeoutNowResponse, error) {
	client, ok := t.client(peerID)
	if !ok {
		return raft.TimeoutNowResponse{}, fmt.Errorf("unknown peer: %s", peerID)
	}
	resp, err := client.TimeoutNow(ctx, &types.TimeoutNowArgs{
		Term:     args.Term,
		LeaderId: args.LeaderID,
	})
	if err != nil {
		return raft.TimeoutNowResponse{}, err
	}
	return raft.TimeoutNowResponse{
		Term:    resp.Term,
		Success: resp.Success,
	}, nil
}

func (t *grpcTransport) InstallSnapshot(ctx context.Context, peerID string, args raft.InstallSnapshotArgs) (raft.InstallSnapshotResponse, error) {
	client, ok := t.client(peerID)
	if !ok {
		return raft.InstallSnapshotResponse{}, fmt.Errorf("unknown peer: %s", peerID)
	}
	stream, err := client.InstallSnapshot(ctx)
	if err != nil {
		return raft.InstallSnapshotResponse{}, err
	}

	var memberConfig []*types.MemberConfig
	for id, state := range args.SnapshotMetadata.MemberConfig {
		memberConfig = append(memberConfig, &types.MemberConfig{
			Id:        id,
			PeerState: raftToProtoPeerState(state),
		})
	}

	err = stream.Send(&types.InstallSnapshotArgs{
		Payload: &types.InstallSnapshotArgs_SnapshotMeta{
			SnapshotMeta: &types.InstallSnapshotMeta{
				Term:              args.Term,
				LeaderId:          args.LeaderID,
				SnapshotSize:      args.SnapshotSize,
				LastIncludedIndex: args.SnapshotMetadata.LastIncludedIndex,
				LastIncludedTerm:  args.SnapshotMetadata.LastIncludedTerm,
				Timestamp:         timestamppb.New(args.SnapshotMetadata.TimeStamp),
				MemberConfig:      memberConfig,
			},
		},
	})
	if err != nil {
		return raft.InstallSnapshotResponse{}, err
	}

	// Chunk at 10% of the snapshot, so a transfer is always ~10 messages whatever its
	// size — the streaming path gets exercised even by a tiny test snapshot, instead
	// of fitting in one message and never proving anything.
	//
	// Both bounds are load-bearing. The floor stops a zero-length buffer, which Read
	// fills with 0 bytes and no error, forever. The ceiling keeps a message under
	// gRPC's 4MB default receive limit — 10% of a large snapshot would otherwise be
	// rejected by the far side rather than sent.
	chunkSize := int(args.SnapshotSize / 10)
	chunkSize = min(max(chunkSize, 4*statemachine.KB), statemachine.MB)

	chunks := make([]byte, chunkSize)
	for {
		n, err := args.Reader.Read(chunks)
		if n > 0 {
			if sendErr := stream.Send(&types.InstallSnapshotArgs{
				Payload: &types.InstallSnapshotArgs_SnapshotChunk{
					SnapshotChunk: &types.SnapshotChunk{Chunk: chunks[:n]},
				},
			}); sendErr != nil {
				break // stream is broken; the real status comes from CloseAndRecv
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return raft.InstallSnapshotResponse{}, err
		}
	}

	res, err := stream.CloseAndRecv()
	if err != nil {
		return raft.InstallSnapshotResponse{}, err
	}
	return raft.InstallSnapshotResponse{
		Term:    res.Term,
		Success: res.Success,
	}, nil
}

// trackedPeerIDs returns the members this server knows to ask the node about. It is
// mutable because AddMember and RemoveMember change the cluster at runtime, and
// cfg.ServerIDS is only the bootstrap seed.
func (s *Server) trackedPeerIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.peerIDs)
}

func (s *Server) trackPeer(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !slices.Contains(s.peerIDs, id) {
		s.peerIDs = append(s.peerIDs, id)
		slices.Sort(s.peerIDs)
	}
}

func (s *Server) untrackPeer(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.peerIDs = slices.DeleteFunc(s.peerIDs, func(p string) bool { return p == id })
}
