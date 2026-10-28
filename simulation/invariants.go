package simulation

import (
	"fmt"
	"maps"
	"slices"

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

// committedEntry is an entry some node reported committed, kept for the rest
// of the run. Raft never takes a commit back, so later logs are only compared
// against it.
type committedEntry struct {
	entry *raftpb.Entry
	node  raft.NodeID
	term  uint64 // the node's term when it reported the commit
}

// memo is what the invariants remember across events. Looking only at the
// nodes that are up right now misses a leader that crashed before a second
// leader of the same term showed up, or a commit only a dead node had.
type memo struct {
	leaders   map[uint64]raft.NodeID // term -> the leader of that term
	committed map[uint64]committedEntry
}

func newMemo() *memo {
	return &memo{leaders: map[uint64]raft.NodeID{}, committed: map[uint64]committedEntry{}}
}

func sameEntry(a, b *raftpb.Entry) bool {
	return a.Term == b.Term && a.Command == b.Command && a.SerialNumber == b.SerialNumber
}

// check runs after every event:
//
//   - election safety: at most one leader per term, over the whole run
//   - state machine safety: an index that committed never changes, on any
//     node, at any later time. This also covers log matching on the
//     committed part.
//   - leader completeness: a leader has every entry committed in an earlier
//     term
//
// The logs are decoded once here and shared. Decoding them again for each
// invariant was most of the run time.
func (m *memo) check(s *sim.Sim) error {
	type node struct {
		name raft.NodeID
		snap raft.Snapshot
		log  []*raftpb.Entry // log[0] is index 1
	}
	var nodes []node
	for _, id := range s.Nodes() {
		if !s.Up(id) {
			continue
		}
		d, ok := s.Handler(id).(*driver)
		if !ok {
			continue // a client
		}
		ctx := s.ReadCtx(id)
		nodes = append(nodes, node{raft.NodeID(d.cfg.ID), d.snapshot(ctx), d.logEntries(ctx)})
	}

	for _, n := range nodes {
		if n.snap.Role == raftpb.Role_LEADER {
			if other, ok := m.leaders[n.snap.Term]; ok && other != n.name {
				return fmt.Errorf("two leaders in term %d: %s and %s", n.snap.Term, other, n.name)
			}
			m.leaders[n.snap.Term] = n.name
		}

		upTo := min(n.snap.CommitIndex, uint64(len(n.log)))
		for i := uint64(1); i <= upTo; i++ {
			e := n.log[i-1]
			old, ok := m.committed[i]
			if !ok {
				m.committed[i] = committedEntry{entry: e, node: n.name, term: n.snap.Term}
				continue
			}
			if !sameEntry(old.entry, e) {
				return fmt.Errorf("committed index %d changed: %s had %v, %s has %v",
					i, old.node, old.entry, n.name, e)
			}
		}
	}

	for _, n := range nodes {
		if n.snap.Role != raftpb.Role_LEADER {
			continue
		}
		for _, i := range slices.Sorted(maps.Keys(m.committed)) {
			c := m.committed[i]
			if c.term >= n.snap.Term {
				continue // committed in this term or later, not before this leader
			}
			if i > uint64(len(n.log)) || !sameEntry(n.log[i-1], c.entry) {
				return fmt.Errorf("leader %s of term %d is missing index %d (%v), committed in term %d",
					n.name, n.snap.Term, i, c.entry, c.term)
			}
		}
	}
	return nil
}
