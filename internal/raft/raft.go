package raft

import (
	"context"

	"github.com/alipourhabibi/raft/gen/go/raft/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Raft struct {
	raft.UnimplementedRaftServiceServer
}

func NewRaftService() *Raft {
	return &Raft{}
}

func (r Raft) RequestVote(context.Context, *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method RequestVote not implemented")
}
