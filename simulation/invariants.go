package simulation

import (
	"fmt"

	"github.com/alipourhabibi/detsim/sim"
	raftpb "github.com/alipourhabibi/raft/gen/go/raft/v1"
	"github.com/alipourhabibi/raft/internal/raft"
)

func snapshots(s *sim.Sim) map[raft.NodeID]raft.Snapshot {
	out := map[raft.NodeID]raft.Snapshot{}
	for _, id := range s.Nodes() {
		if !s.Up(id) {
			continue // crashed: no volatile state
		}
		d, ok := s.Handler(id).(*driver)
		if !ok {
			continue
		}
		out[raft.NodeID(d.cfg.ID)] = d.snapshot(s.ReadCtx(id))
	}
	return out
}

func OneLeaderPerTerm(s *sim.Sim) error {
	seen := map[uint64]raft.NodeID{}
	for id, snap := range snapshots(s) {
		if snap.Role != raftpb.Role_LEADER {
			continue
		}
		if other, dup := seen[snap.Term]; dup {
			return fmt.Errorf("two leaders in term %d: %s and %s", snap.Term, other, id)
		}
		seen[snap.Term] = id
	}
	return nil
}

func CommitNotBehindApplied(s *sim.Sim) error {
	for id, snap := range snapshots(s) {
		if snap.LastApplied > snap.CommitIndex {
			return fmt.Errorf("%s applied %d > commit %d", id, snap.LastApplied, snap.CommitIndex)
		}
	}
	return nil
}
