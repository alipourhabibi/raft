package grpc

import (
	"fmt"
	"log/slog"
	"net"

	raftpb "github.com/alipourhabibi/raft/gen/go/raft/v1"
	"github.com/alipourhabibi/raft/internal/config"
	"github.com/alipourhabibi/raft/internal/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type GrpcServer struct {
	port        int
	raftService *raft.Raft
}

func NewGrpcServer(
	config *config.Config,
	raftService *raft.Raft,
) *GrpcServer {
	return &GrpcServer{
		port:        config.Port,
		raftService: raftService,
	}
}

func (g *GrpcServer) Boot() error {
	grpcServer := grpc.NewServer()
	raftpb.RegisterRaftServiceServer(grpcServer, g.raftService)
	reflection.Register(grpcServer)

	slog.Info("Starting grpc server", "port", g.port)
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", g.port))
	if err != nil {
		return err
	}
	return grpcServer.Serve(lis)
}
