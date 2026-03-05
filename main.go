package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/alipourhabibi/raft/internal/config"
	"github.com/alipourhabibi/raft/internal/raft"
	"github.com/alipourhabibi/raft/internal/repository/raft/memory"
	"github.com/alipourhabibi/raft/internal/transport/grpc"
	"golang.org/x/sync/errgroup"
)

func main() {
	opts := &slog.HandlerOptions{
		Level: slog.LevelDebug,
		// You can still override time format if you want something custom
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				// Optional: force particular format
				// (but usually not needed – default is good)
				return slog.String(slog.TimeKey, a.Value.Time().Format(time.RFC3339Nano))
			}
			return a
		},
	}
	handler := slog.NewTextHandler(os.Stdout, opts)
	logger := slog.New(handler)
	slog.SetDefault(logger)

	config, err := config.NewConfig()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	memoryRepo := memory.NewMemoryDB()
	r, err := raft.NewRaftService(memoryRepo, config)
	if err != nil {
		slog.Error("failed to create memory db", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		grpcServer := grpc.NewGrpcServer(config, r)
		if err := grpcServer.Boot(); err != nil {
			slog.Error("failed to boot grpc server", "error", err)
			return err
		}
		return nil
	})

	g.Go(func() error {
		err := r.Serve()
		if err != nil {
			slog.Error("failed to serve raft")
			return err
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		slog.Error("failed with", "error", err)
	}
}
