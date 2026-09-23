package simulation

import (
	"fmt"
	"slices"

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
	cl.Sim.AddInvariant(CommittedPrefixesAgree)
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

// CheckWritesCommitted checks acked writes are committed.
// everywhere=false: on at least one node (true at any moment).
// everywhere=true:  on every node (only after healing).
func (c *Cluster) CheckWritesCommitted(writes []string, everywhere bool) error {
	logs := committedLogs(c.Sim)
	for _, w := range writes {
		found := 0
		for _, l := range logs {
			got := commands(l.committed)
			if slices.Contains(got, w) {
				found++
			} else if everywhere {
				return fmt.Errorf("write %q not committed on node %d (log %v)", w, l.id, got)
			}
		}
		if found == 0 {
			return fmt.Errorf("acked write %q is not committed on any node", w)
		}
	}
	return nil
}

// CheckStateMachineApplied checks every live server's state machine has value
// we want for key. This is liveness: true only after healing and a catch-up window.
func (c *Cluster) CheckStateMachineApplied(key, want string) error {
	for _, id := range c.Servers {
		if !c.Sim.Up(id) {
			continue // crashed: its state is frozen, not wrong
		}
		b, ok := c.Sim.Get(id, prefixSM+key)
		if !ok {
			return fmt.Errorf("node %d: %s is missing, want %q", id, key, want)
		}
		if string(b) != want {
			return fmt.Errorf("node %d: %s = %q, want %q", id, key, b, want)
		}
	}
	return nil
}

type nodeLog struct {
	id        int
	committed []*raftpb.Entry // entries 1..commitIndex
}

// committedLogs reads the committed part of every live raft node's log.
func committedLogs(s *sim.Sim) []nodeLog {
	var logs []nodeLog
	for _, id := range s.Nodes() {
		if !s.Up(id) {
			continue // crashed: nothing to read
		}
		d, ok := s.Handler(id).(*driver)
		if !ok {
			continue // a client node
		}

		snap := d.snapshot(s.ReadCtx(id))
		entries := d.logEntries(s.ReadCtx(id)) // entries[0] is log index 1

		n := min(int(snap.CommitIndex), len(entries))
		logs = append(logs, nodeLog{id: id, committed: entries[:n]})
	}
	return logs
}

// commands is the command text of entries, for messages and simple checks.
func commands(entries []*raftpb.Entry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = e.Command
	}
	return out
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
