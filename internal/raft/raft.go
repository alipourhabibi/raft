package raft

import (
	"context"

	raftpb "github.com/alipourhabibi/raft/gen/go/raft/v1"
	repository "github.com/alipourhabibi/raft/internal/repository/raft"
)

type Raft struct {
	raftpb.UnimplementedRaftServiceServer

	repository repository.RaftRepository
}

func NewRaftService(
	repository repository.RaftRepository,
) *Raft {
	return &Raft{
		repository: repository,
	}
}

func (r Raft) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	term, err := r.repository.GetCurrentTerm(ctx)
	if err != nil {
		return nil, err
	}

	// current term is higher than candidate's term
	if term > req.Term {
		return &raftpb.RequestVoteResponse{
			Term:    term,
			Granted: false,
		}, nil
	}

	votedFor, err := r.repository.GetIsVotedFor(ctx, term)
	if err != nil {
		return nil, err
	}
	// Already voted in this term and candidate is not the same as voted before
	if votedFor != nil && votedFor != &req.CandidateId {
		return &raftpb.RequestVoteResponse{
			Term:    term,
			Granted: false,
		}, nil
	}

	lastLogIndex, err := r.repository.GetLogIndex(ctx)
	if err != nil {
		return nil, err
	}

	if lastLogIndex > req.LastLogIndex {
		return &raftpb.RequestVoteResponse{
			Term:    term,
			Granted: false,
		}, nil
	}

	return &raftpb.RequestVoteResponse{
		Term:    req.Term,
		Granted: true,
	}, nil
}
