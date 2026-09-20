package simulation

import (
	"os"
	"testing"

	"github.com/alipourhabibi/detsim/sim"
	raftpb "github.com/alipourhabibi/raft/gen/go/raft/v1"
	"github.com/alipourhabibi/raft/internal/config"
	"github.com/alipourhabibi/raft/internal/raft"
)

type Cluster struct {
	Sim      *sim.Sim
	NodesInt map[raft.NodeID]int
	Nodes    []*driver
	NodesMap map[raft.NodeID]*driver
}

func Build(cfg sim.Config, ids []raft.NodeID, cfgs map[raft.NodeID]*config.Config) *Cluster {
	nodesMap := map[int]raft.NodeID{}
	nodesInt := map[raft.NodeID]int{}
	simNodes := make([]int, len(ids))

	for id, nodeID := range ids {
		nodesMap[id] = nodeID
		nodesInt[nodeID] = id
		simNodes[id] = id
	}

	drivers := map[int]*driver{}

	factory := func() sim.NodeFactory {
		return func(id int) sim.Handler {
			nodeConfig := cfgs[nodesMap[id]]
			d := &driver{
				turn:     &sim.Turn{},
				cfg:      nodeConfig,
				ids:      nodesInt,
				nodesMap: nodesMap,
			}
			drivers[id] = d
			return d
		}
	}

	s := sim.New(cfg, simNodes, factory())

	c := &Cluster{
		Sim:      s,
		NodesInt: nodesInt,
		Nodes:    make([]*driver, len(ids)),
		NodesMap: map[raft.NodeID]*driver{},
	}
	for i, nodeID := range ids {
		c.Nodes[i] = drivers[i]
		c.NodesMap[nodeID] = drivers[i]
	}
	return c
}

type clusterOpts struct {
	Seed    uint64
	Nodes   []raft.NodeID
	Network sim.NetworkConfig
	Until   sim.Time
}

func defaultOpts(seed uint64) clusterOpts {
	return clusterOpts{
		Seed:  seed,
		Nodes: []raft.NodeID{"node1", "node2", "node3"},
		Network: sim.NetworkConfig{
			Delay: sim.DelaySpec{
				Kind:   sim.DelayUniform,
				Base:   sim.Duration(5),
				Spread: sim.Duration(5),
			},
			LossPPM:        0,
			DuplicationPPM: 0,
			IsReordering:   false,
		},
		Until: 5_000,
	}
}

// nodeConfigs builds one config per node; Nodes holds the peers.
func nodeConfigs(ids []raft.NodeID) map[raft.NodeID]*config.Config {
	out := map[raft.NodeID]*config.Config{}
	for _, n := range ids {
		peers := map[string]string{}
		for _, other := range ids {
			if other != n {
				peers[string(other)] = string(other)
			}
		}
		out[n] = &config.Config{
			ID:                   string(n),
			Host:                 string(n),
			Nodes:                peers,
			TickInterval:         10,
			ElectionTimeoutStart: 150,
			ElectionTimeoutEnd:   300,
			HeartbeatTimeout:     50,
		}
	}
	return out
}

// buildCluster builds the cluster, adds the safety invariants, and starts it.
func buildCluster(t *testing.T, opts clusterOpts) *Cluster {
	t.Helper()

	simCfg := sim.Config{
		Seed:          opts.Seed,
		MaxEvents:     1_000_000,
		TraceLevel:    sim.TraceHashEventsAndState,
		TraceKeep:     sim.KeepAll,
		NetworkConfig: opts.Network,
	}

	cl := Build(simCfg, opts.Nodes, nodeConfigs(opts.Nodes))
	cl.Sim.AddInvariant(OneLeaderPerTerm)
	cl.Sim.AddInvariant(CommitNotBehindApplied)
	cl.Sim.Start()
	return cl
}

// runUntilLeader runs until some node is leader, or the deadline passes.
func runUntilLeader(t *testing.T, cl *Cluster, deadline sim.Time) {
	t.Helper()
	for cl.Sim.Now() < deadline {
		if err := cl.Sim.Step(100); err != nil {
			dump(t, cl)
			t.Fatalf("invariant broken: %v", err)
		}
		// a legal old leader in an older term never fails the test.
		if leaderCount(cl) >= 1 {
			return
		}
	}
	dump(t, cl)
	t.Fatal("no leader before deadline")
}

// leaderCount counts the live nodes that believe they are leader.
func leaderCount(cl *Cluster) int {
	n := 0
	for _, snap := range snapshots(cl.Sim) {
		if snap.Role == raftpb.Role_LEADER {
			n++
		}
	}
	return n
}

func dump(t *testing.T, cl *Cluster) {
	t.Helper()
	t.Logf("states:\n%s", cl.Sim.StatesString())
	if err := cl.Sim.Trace().Lanes(os.Stderr, cl.Sim.Nodes()); err != nil {
		t.Logf("trace dump failed: %v", err)
	}
}
