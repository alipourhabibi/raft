package raft

import "context"

type RaftRepository interface {
	GetCurrentTerm(context.Context) (uint64, error)
	// maybe return ok not the pointer to nodeID
	GetIsVotedFor(ctx context.Context, term uint64) (*uint64, error) // returns the id of node
	GetLogIndex(ctx context.Context) (uint64, error)
}
