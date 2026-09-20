package raft

import (
	"context"

	raftpb "github.com/alipourhabibi/raft/gen/go/raft/v1"
)

// Snapshot is a read-only view of this node, for tests and invariants.
type Snapshot struct {
	Term         uint64
	Role         raftpb.Role
	LeaderID     string
	CommitIndex  uint64
	LastApplied  uint64
	LastLogIndex uint64
}

func (r *Raft) Snapshot(ctx context.Context) (Snapshot, error) {
	term, err := r.repository.GetCurrentTerm(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	lastIdx, err := r.repository.GetLastLogIndex(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	return Snapshot{
		Term:         term,
		Role:         r.getRole(),
		LeaderID:     r.getLastKnownLeader(),
		CommitIndex:  r.commitIndex,
		LastApplied:  r.lastApplied,
		LastLogIndex: lastIdx,
	}, nil
}
