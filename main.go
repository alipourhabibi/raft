package main

import (
	"log/slog"
	"os"

	"github.com/alipourhabibi/raft/internal/config"
	"github.com/alipourhabibi/raft/internal/transport/grpc"
)

func main() {
	config, err := config.NewConfig()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewGrpcServer(config)
	if err := grpcServer.Boot(); err != nil {
		slog.Error("failed to boot grpc server", "error", err)
		os.Exit(1)
	}
}
