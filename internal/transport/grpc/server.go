package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	raftpb "github.com/alipourhabibi/raft/gen/go/raft/v1"
	"github.com/alipourhabibi/raft/internal/config"
	"github.com/alipourhabibi/raft/internal/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type GrpcServer struct {
	port        int
	raftService *raftServer
}

type raftServer struct {
	raftpb.UnimplementedRaftServiceServer
	raft *raft.Raft
}

// one-way, return Empty

func (s *raftServer) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.Empty, error) {
	msg := raft.RequestVoteRequest{
		Term:         req.Term,
		CandidateID:  raft.NodeID(req.CandidateId),
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	}
	if err := s.raft.Deliver(ctx, raft.NodeID(req.From), msg); err != nil {
		return nil, err
	}
	return &raftpb.Empty{}, nil
}

func (s *raftServer) RequestVoteReply(ctx context.Context, req *raftpb.RequestVoteResponse) (*raftpb.Empty, error) {
	msg := raft.RequestVoteResponse{
		Term:    req.Term,
		Granted: req.Granted,
	}
	if err := s.raft.Deliver(ctx, raft.NodeID(req.From), msg); err != nil {
		return nil, err
	}
	return &raftpb.Empty{}, nil
}

func (s *raftServer) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.Empty, error) {
	msg := raft.AppendEntriesRequest{
		Term:         req.Term,
		LeaderID:     raft.NodeID(req.LeaderId),
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      req.Entries,
		LeaderCommit: req.LeaderCommit,
		Seq:          req.Seq,
	}
	if err := s.raft.Deliver(ctx, raft.NodeID(req.From), msg); err != nil {
		return nil, err
	}
	return &raftpb.Empty{}, nil
}

func (s *raftServer) AppendEntriesReply(ctx context.Context, req *raftpb.AppendEntriesResponse) (*raftpb.Empty, error) {
	msg := raft.AppendEntriesResponse{
		Term:    req.Term,
		Success: req.Success,
		Seq:     req.Seq,
	}
	if err := s.raft.Deliver(ctx, raft.NodeID(req.From), msg); err != nil {
		return nil, err
	}
	return &raftpb.Empty{}, nil
}

// client requests: wait here

func (s *raftServer) Submit(ctx context.Context, req *raftpb.SubmitRequest) (*raftpb.SubmitResponse, error) {
	return call(ctx, s.raft, func(reply func(*raftpb.SubmitResponse, error)) raft.Message {
		return raft.SubmitRequest{Req: req, Reply: reply}
	})
}

func (s *raftServer) Get(ctx context.Context, req *raftpb.GetRequest) (*raftpb.GetResponse, error) {
	return call(ctx, s.raft, func(reply func(*raftpb.GetResponse, error)) raft.Message {
		return raft.GetRequest{Req: req, Reply: reply}
	})
}

func (s *raftServer) ChangeNodes(ctx context.Context, req *raftpb.ChangeNodesRequest) (*raftpb.ChangeNodesResponse, error) {
	return call(ctx, s.raft, func(reply func(*raftpb.ChangeNodesResponse, error)) raft.Message {
		return raft.ChangeNodesRequest{Req: req, Reply: reply}
	})
}

// call puts a client request into Raft and waits for its Reply.
func call[Resp any](ctx context.Context, r *raft.Raft, build func(reply func(Resp, error)) raft.Message) (Resp, error) {
	type result struct {
		resp Resp
		err  error
	}

	// Reply never blocks the Raft goroutine, even if the client left
	ch := make(chan result, 1)
	msg := build(func(resp Resp, err error) { ch <- result{resp, err} })

	var zero Resp
	if err := r.Deliver(ctx, "", msg); err != nil {
		return zero, err
	}
	select {
	case res := <-ch:
		return res.resp, res.err
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}

func NewGrpcServer(
	config *config.Config,
	raftService *raft.Raft,
) *GrpcServer {
	raftServer := &raftServer{
		raft: raftService,
	}
	return &GrpcServer{
		port:        config.Port,
		raftService: raftServer,
	}
}

func (g *GrpcServer) Boot(ctx context.Context) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", g.port))
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	raftpb.RegisterRaftServiceServer(grpcServer, g.raftService)
	reflection.Register(grpcServer)

	go func() {
		<-ctx.Done()
		slog.Info("shutting down grpc server")

		done := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			grpcServer.Stop()
		}
	}()

	slog.Info("Starting grpc server", "port", g.port)
	return grpcServer.Serve(lis)
}
