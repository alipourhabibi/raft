package simulation

import (
	"fmt"
	"os"
	"slices"
	"testing"

	"github.com/alipourhabibi/detsim/sim"
	raftpb "github.com/alipourhabibi/raft/gen/go/raft/v1"
	"github.com/alipourhabibi/raft/internal/config"
	"github.com/alipourhabibi/raft/internal/raft"
)

type Cluster struct {
	Sim      *sim.Sim
	NodesInt map[raft.NodeID]int

	Servers []int // sim ids of servers
	Clients []int // sim ids of clients
}

func New(opts ClusterOpts) *Cluster {
	simCfg := sim.Config{
		Seed:          opts.Seed,
		MaxEvents:     1_000_000,
		TraceLevel:    sim.TraceHashEventsAndState,
		TraceKeep:     sim.KeepAll,
		NetworkConfig: opts.Network,
	}

	cl := Build(simCfg, opts.Nodes, NodeConfigs(opts.Nodes), opts.Clients)
	cl.Sim.AddInvariant(OneLeaderPerTerm)
	cl.Sim.AddInvariant(CommitNotBehindApplied)
	cl.Sim.Start()
	return cl
}

func Build(cfg sim.Config, ids []raft.NodeID, cfgs map[raft.NodeID]*config.Config, clients [][]string) *Cluster {
	nodesMap := map[int]raft.NodeID{}
	nodesInt := map[raft.NodeID]int{}
	servers := make([]int, len(ids))

	for id, nodeID := range ids {
		nodesMap[id] = nodeID
		nodesInt[nodeID] = id
		servers[id] = id
	}

	clientIDs := make([]int, len(clients))
	for i := range clients {
		clientIDs[i] = len(ids) + i
	}
	all := append(append([]int{}, servers...), clientIDs...)

	c := &Cluster{NodesInt: nodesInt, Servers: servers, Clients: clientIDs}

	factory := func(id int) sim.Handler {
		if id >= len(ids) {
			return &clientDriver{
				id:       id,
				servers:  servers,
				ids:      nodesInt,
				commands: clients[id-len(ids)],
				history:  c.Sim.History(),
			}
		}
		return &driver{
			turn:     &sim.Turn{},
			cfg:      cfgs[nodesMap[id]],
			ids:      nodesInt,
			nodesMap: nodesMap,
		}
	}

	c.Sim = sim.New(cfg, all, factory)
	return c
}

type ClusterOpts struct {
	Seed    uint64
	Nodes   []raft.NodeID
	Network sim.NetworkConfig
	Until   sim.Time
	Clients [][]string
}

func DefaultOpts(seed uint64) ClusterOpts {
	return ClusterOpts{
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
func NodeConfigs(ids []raft.NodeID) map[raft.NodeID]*config.Config {
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

// RunUntilAcks steps until n client operations are acked, or deadline passes.
func (c *Cluster) RunUntilAcks(n int, deadline sim.Time) error {
	for c.Sim.Now() < deadline {
		if err := c.Sim.Step(100); err != nil {
			return err
		}
		if ok, _, _ := c.Sim.History().Counts(); ok >= n {
			return nil
		}
	}
	ok, _, _ := c.Sim.History().Counts()
	return fmt.Errorf("want %d acks by %d, got %d", n, deadline, ok)
}

// CheckLogs fails if the server's logs differ, or if a write is missing
// or appears a different number of times than it was sent.
func (c *Cluster) CheckLogs(writes []string) error {
	want := map[string]int{}
	for _, w := range writes {
		want[w]++
	}

	var ref []string
	for _, id := range c.Servers {
		d := c.Sim.Handler(id).(*driver)
		got := []string{}
		count := map[string]int{}
		for _, e := range d.logEntries(c.Sim.ReadCtx(id)) {
			got = append(got, e.Command)
			count[e.Command]++
		}
		for w, n := range want {
			if count[w] != n {
				return fmt.Errorf("node %d log %v has %q %d times, want %d", id, got, w, count[w], n)
			}
		}
		if ref == nil {
			ref = got
			continue
		}
		if !slices.Equal(got, ref) {
			return fmt.Errorf("node %d log %v, want %v", id, got, ref)
		}
	}
	return nil
}

// CheckApplied fails if any server's state machine has a different value for key.
func (c *Cluster) CheckApplied(key, want string) error {
	for _, id := range c.Servers {
		b, ok := c.Sim.Get(id, prefixSM+key)
		if !ok || string(b) != want {
			return fmt.Errorf("node %d: %s = %q (found=%v), want %q", id, key, b, ok, want)
		}
	}
	return nil
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
