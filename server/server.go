package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	"github.com/SHREYANSHSINGH14/raft/config"
	"github.com/SHREYANSHSINGH14/raft/raft"
	"github.com/SHREYANSHSINGH14/raft/types"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
)

type Server struct {
	Peer *raft.Peer

	baseUrl string
	port    string

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

	// pass server.ctx down to Peer — same context, same lifecycle
	peer, err := raft.NewPeer(server.ctx, cfg)
	if err != nil {
		return nil, err
	}

	server.Peer = peer
	server.baseUrl = cfg.BaseURL
	server.port = cfg.Port

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

	// This is an important concept to remeber that whenever we have some long running goroutines in our server then we should always have a way to stop those goroutines gracefully when
	// the server is stopped to avoid any unwanted behavior and also to free up the resources used by those goroutines
	go func() {
		<-s.ctx.Done()
		grpcServer.GracefulStop()
	}()

	zerolog.Ctx(s.ctx).Debug().Str("id", s.Peer.GetID()).Str("role", string(s.Peer.GetRole())).Str("listen_address", s.baseUrl+":"+s.port).Msg("server started")

	s.Peer.Start()
}
