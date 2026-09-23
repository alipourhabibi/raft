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

// CommittedPrefixesAgree is Raft's Log Matching on the committed part:
// two nodes never hold different entries at the same committed index.
func CommittedPrefixesAgree(s *sim.Sim) error {
	logs := committedLogs(s)
	for i := range logs {
		for j := i + 1; j < len(logs); j++ {
			a, b := logs[i], logs[j]
			n := min(len(a.committed), len(b.committed))
			for k := 0; k < n; k++ {
				x, y := a.committed[k], b.committed[k]
				if x.Term != y.Term || x.Command != y.Command || x.SerialNumber != y.SerialNumber {
					return fmt.Errorf("node %d and %d differ at committed index %d: %v vs %v",
						a.id, b.id, k+1, x, y) // k+1: entries[0] is index 1
				}
			}
		}
	}
	return nil
}
