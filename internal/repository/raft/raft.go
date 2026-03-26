package raft

import (
	"context"
	"errors"

	raftpb "github.com/alipourhabibi/raft/gen/go/raft/v1"
)

var ErrIndexOutofRange = errors.New("index out of range")

type RaftRepository interface {
	GetCurrentTerm(context.Context) (uint64, error)
	SetCurrentTerm(context.Context, uint64) error
	IncCurrentTerm(context.Context) (uint64, error) // return new term

	// maybe return ok not the pointer to nodeID
	GetVotedFor(ctx context.Context, term uint64) (*string, error) // returns the id of node
	VoteFor(ctx context.Context, term uint64, node string) error
	GetCommitIndex(ctx context.Context) (uint64, error)

	GetLastAppliedIndex(ctx context.Context) (uint64, error)

	GetMatchIndexByNodeID(ctx context.Context, nodeID string) (uint64, error)
	GetEntryFromIndex(ctx context.Context, index uint64) ([]*raftpb.Entry, error)
	GetEntryAtIndex(ctx context.Context, index uint64) (*raftpb.Entry, error)

	InitLeaderState(ctx context.Context) error

	TruncateAndAppend(ctx context.Context, fromIndex uint64, entries []*raftpb.Entry) error
	SetNextIndex(ctx context.Context, nodeID string, index uint64) error
	SetMatchIndex(ctx context.Context, nodeID string, index uint64) error
	SetCommitIndex(ctx context.Context, index uint64) error
	GetLastLogIndex(ctx context.Context) (uint64, error)
	SetLastApplied(ctx context.Context, index uint64) error

	GetNextIndexByNodeID(ctx context.Context, nodeID string) (uint64, error)

	// Cluster
	GetClusterConfig(ctx context.Context) (*raftpb.ClusterConfig, error)
	SetClusterConfig(ctx context.Context, config *raftpb.ClusterConfig) error

	SetSerialNumber(ctx context.Context, serialNumber string, status bool) error
	GetSerialNumber(ctx context.Context, serialNumber string) (bool, error)
}
