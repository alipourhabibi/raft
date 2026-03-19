package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alipourhabibi/raft/internal/config"
	redisinfra "github.com/alipourhabibi/raft/internal/infrastructure/redis"
	"github.com/alipourhabibi/raft/internal/raft"
	raftrep "github.com/alipourhabibi/raft/internal/repository/raft"
	"github.com/alipourhabibi/raft/internal/repository/raft/memory"
	raftredis "github.com/alipourhabibi/raft/internal/repository/raft/redis"
	staterep "github.com/alipourhabibi/raft/internal/repository/statemachine"
	statemachinememroy "github.com/alipourhabibi/raft/internal/repository/statemachine/memory"
	stateredis "github.com/alipourhabibi/raft/internal/repository/statemachine/redis"
	"github.com/alipourhabibi/raft/internal/statemachine"
	"github.com/alipourhabibi/raft/internal/transport/grpc"
	"golang.org/x/sync/errgroup"
)

func main() {
	opts := &slog.HandlerOptions{
		Level: slog.LevelDebug,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.String(slog.TimeKey, a.Value.Time().Format(time.RFC3339Nano))
			}
			return a
		},
	}
	handler := slog.NewTextHandler(os.Stdout, opts)
	logger := slog.New(handler)
	slog.SetDefault(logger)

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()

	config, err := config.NewConfig()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	redisInfra, err := redisinfra.NewRedis(ctx, config)
	if err != nil {
		slog.Error("failed to init redis inra", "error", err)
		os.Exit(1)
	}

	var stateMachineRepo staterep.StateMachineRepo

	var repo raftrep.RaftRepository
	if config.DBType == "redis" {
		repo, err = raftredis.NewRedisDB(ctx, config, redisInfra)
		if err != nil {
			slog.Error("failed to init redis", "error", err)
			os.Exit(1)
		}
		stateMachineRepo, err = stateredis.NewRedisStateMachine(ctx, redisInfra)
		if err != nil {
			slog.Error("failed to init statemachine redis", "error", err)
			os.Exit(1)
		}
	} else {
		repo = memory.NewMemoryDB(config)
		stateMachineRepo = statemachinememroy.NewMemoryStateMachine(ctx)
	}

	s, err := statemachine.NewStateMachine(config, stateMachineRepo)
	if err != nil {
		slog.Error("failed to init state machine service", "error", err)
		os.Exit(1)
	}

	r, err := raft.NewRaftService(repo, config, s)
	if err != nil {
		slog.Error("failed to create memory db", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		grpcServer := grpc.NewGrpcServer(config, r)
		if err := grpcServer.Boot(ctx); err != nil {
			slog.Error("failed to boot grpc server", "error", err)
			return err
		}
		return nil
	})

	g.Go(func() error {
		err := r.Serve(ctx)
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
