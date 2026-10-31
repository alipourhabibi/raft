package simulation

import (
	"context"
	"encoding/binary"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/alipourhabibi/detsim/sim"
	raftpb "github.com/alipourhabibi/raft/gen/go/raft/v1"
	"github.com/alipourhabibi/raft/internal/config"
	"github.com/alipourhabibi/raft/internal/raft"
	"github.com/alipourhabibi/raft/internal/repository/raft/memory"
	"google.golang.org/protobuf/proto"
)

type Cluster struct {
	Sim      *sim.Sim
	NodesInt map[raft.NodeID]int

	Servers  []int      // sim ids of servers
	Clients  []int      // sim ids of clients
	Commands [][]string // each client's commands

	memo *memo // what the invariants saw over the whole run

	// mem is set when the servers use the real memory repository instead of
	// the sim disk, one per server, kept so the checks can read its log
	mem map[int]*memory.MemoryDB

	// Failover is how long the last CheckFailover took
	Failover sim.Duration

	healed bool // Check is past the fault phase, see store.crashPoint
}

// New builds and starts a cluster with no faults.
func New(opts ClusterOpts) *Cluster {
	cl := Prepare(opts)
	cl.Sim.Start()
	return cl
}

// Prepare builds a cluster with its invariants but does not start it, so a
// fault plan can go in with StartWith, or Sweep can start it.
func Prepare(opts ClusterOpts) *Cluster {
	if opts.Repo != RepoSim && opts.Repo != RepoMemory {
		panic(fmt.Sprintf("harness: unknown repo %q", opts.Repo))
	}
	simCfg := sim.Config{
		Seed:          opts.Seed,
		MaxEvents:     1_000_000,
		TraceLevel:    sim.TraceHashEventsAndState,
		TraceKeep:     sim.KeepAll,
		NetworkConfig: opts.Network,
	}

	cl := Build(simCfg, opts.Nodes, NodeConfigs(opts.Nodes), opts.Clients)
	cl.Commands = opts.Clients
	if opts.Repo == RepoMemory {
		cl.mem = map[int]*memory.MemoryDB{}
	}
	cl.memo = newMemo()
	cl.Sim.AddInvariant(cl.memo.check)
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
				sim:      c.Sim,
			}
		}
		d := &driver{
			healed:   &c.healed,
			turn:     &sim.Turn{},
			cfg:      cfgs[nodesMap[id]],
			ids:      nodesInt,
			nodesMap: nodesMap,
		}
		if c.mem != nil {
			// one memory repo per server, the same one after a restart
			if c.mem[id] == nil {
				c.mem[id] = memory.NewMemoryDB(d.cfg)
			}
			d.mem = c.mem[id]
		}
		return d
	}

	c.Sim = sim.New(cfg, all, factory)
	return c
}

// Repo is which raft repository the servers use.
const (
	RepoSim    = ""       // the sim disk: crashes lose what was not synced
	RepoMemory = "memory" // internal/repository/raft/memory, as main.go uses it
)

type ClusterOpts struct {
	Repo    string
	Seed    uint64
	Nodes   []raft.NodeID
	Network sim.NetworkConfig
	Until   sim.Time
	Clients [][]string
}

func DefaultOpts(seed uint64) ClusterOpts {
	return ClusterOpts{
		Seed:  seed,
		Nodes: []raft.NodeID{"node0", "node1", "node2"},
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
			ElectionTimeoutEnd:   ElectionTimeoutMax,
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

// acked returns the history ops that completed ok, of one kind.
func (c *Cluster) acked(kind string) []sim.Op {
	var out []sim.Op
	for _, op := range c.Sim.History().Ops() {
		if op.Kind == kind && op.Outcome == sim.Ok {
			out = append(out, op)
		}
	}
	return out
}

// CheckAckedWrites checks that every write the client was told succeeded is
// in the durable log of a majority of servers, or of every server.
//
// The durable log, not commitIndex: commitIndex is memory. A leader can ack
// and crash before any follower learns the new commit index, and then no live
// node has it, though the entry is safe on a majority of disks. Matching is by
// serial, two writes of the same command are different writes.
//
// everywhere=false holds at any moment. everywhere=true only after healing.
func (c *Cluster) CheckAckedWrites(everywhere bool) error {
	need := len(c.Servers)/2 + 1
	if everywhere {
		need = len(c.Servers)
	}
	for _, op := range c.acked(opSubmit) {
		info := op.Value.(OpInfo)
		var on []int
		for _, id := range c.Servers {
			for _, e := range c.serverLog(id) {
				if e.SerialNumber == info.Serial {
					on = append(on, id)
					break
				}
			}
		}
		if len(on) < need {
			return fmt.Errorf("acked write %q (%s) is on %d servers %v, want %d",
				info.Cmd, info.Serial, len(on), on, need)
		}
	}
	return nil
}

// CheckReads checks every read returned a value at least as new as the last
// write acked before the read was sent. A read that returns an older value is
// stale: some leader answered without knowing about a newer one.
//
// Writes are ordered by where they committed in the log, which is the order
// raft applied them.
// ponytail: only stale reads, not a full linearizability search; enough for
// one client per key, add a checker like porcupine for concurrent writers.
func (c *Cluster) CheckReads() error {
	index := map[string]uint64{} // serial -> first committed index
	for i, e := range c.memo.committed {
		if e.entry.SerialNumber == "" {
			continue
		}
		if old, ok := index[e.entry.SerialNumber]; !ok || i < old {
			index[e.entry.SerialNumber] = i
		}
	}

	type write struct {
		key, value string
		index      uint64
		returned   sim.Time
	}
	var writes []write
	for _, op := range c.acked(opSubmit) {
		info := op.Value.(OpInfo)
		f := strings.Fields(info.Cmd)
		if len(f) != 3 || !strings.EqualFold(f[0], "SET") {
			continue
		}
		writes = append(writes, write{f[1], f[2], index[info.Serial], op.Returned})
	}

	for _, op := range c.acked(opGet) {
		info := op.Value.(OpInfo)
		f := strings.Fields(info.Cmd)
		if len(f) != 2 {
			continue
		}
		key := f[1]

		// the newest write to key acked before this read was sent
		var last *write
		for i, w := range writes {
			if w.key == key && w.returned < op.Invoked && (last == nil || w.index > last.index) {
				last = &writes[i]
			}
		}
		if last == nil {
			continue // nothing acked yet, any value is fine
		}
		ok := false
		for _, w := range writes {
			if w.key == key && w.value == info.Result && w.index >= last.index {
				ok = true
				break
			}
		}
		if !ok {
			return fmt.Errorf("stale read: %s at t=%d returned %q, but %q was acked at t=%d",
				info.Cmd, op.Returned, info.Result, last.value, last.returned)
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

// durableLog reads a server's log straight from its disk, so it works for a
// crashed node too. log[0] is index 1.
// serverLog is a server's log as its repository has it, also when it is down.
func (c *Cluster) serverLog(id int) []*raftpb.Entry {
	if c.mem != nil {
		entries, _ := c.mem[id].GetEntryFromIndex(context.Background(), 0)
		return entries
	}
	return durableLog(c.Sim, id)
}

func durableLog(s *sim.Sim, id int) []*raftpb.Entry {
	b, ok := s.Get(id, keyLastIndex)
	if !ok {
		return nil
	}
	last := binary.LittleEndian.Uint64(b)
	out := make([]*raftpb.Entry, 0, last)
	for i := uint64(1); i <= last; i++ {
		b, ok := s.Get(id, logKey(i))
		if !ok {
			break
		}
		e := &raftpb.Entry{}
		if err := proto.Unmarshal(b, e); err != nil {
			panic(fmt.Sprintf("harness: node %d log %d: %v", id, i, err))
		}
		out = append(out, e)
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

// Until is how long a seed runs with faults before healing, in fake ms.
const Until = 10_000

// Servers are the sim ids of the servers in DefaultOpts.
var Servers = []int{0, 1, 2}

// FaultPlan makes the fault plan for one seed: crashes, pauses, partitions
// and lossy links on the servers only, never more than one server down, so a
// quorum is always possible. buggifyPercent > 0 also turns on BuggifySkipSync
// for some seeds. crashes=false leaves crashes out, for the memory repo: it
// keeps nothing across a restart, like the real process, so raft can't be
// safe with it once a node crashes.
//
// points are the buggify points the seeds pick from, BuggifySkipSync if none.
func FaultPlan(servers []int, until sim.Time, buggifyPercent int, crashes bool, points ...string) func(uint64, sim.Rand) *sim.Plan {
	cfg := sim.PlanConfig{
		Until:   until,
		MeanGap: 300,
		MaxDown: 1,
		Weights: sim.DefaultWeights(),
		// no disk wipes: a voter that loses its disk can vote twice and
		// forget acked entries, raft is not meant to survive that
		WipeChance: 0,
	}
	if !crashes {
		cfg.Weights.Crash = 0
	}
	if buggifyPercent > 0 {
		cfg.Buggify = points
		if len(points) == 0 {
			cfg.Buggify = []string{BuggifySkipSync}
		}
		cfg.BuggifyPercent = buggifyPercent
	}
	return func(seed uint64, r sim.Rand) *sim.Plan {
		return sim.GeneratePlan(cfg, servers, seed, r)
	}
}

// Check runs a seed to the end and applies every check:
//
//  1. run with faults until `until`; the invariants run after every event
//  2. safety: every acked write is on a majority of disks
//  3. heal everything, then liveness: every command gets acked
//  4. every acked write is on every server, every server applied the last
//     value of each key, and no read was stale
func (c *Cluster) Check(until sim.Time) error {
	if err := c.Sim.RunUntil(until); err != nil {
		return err
	}
	if err := c.CheckAckedWrites(false); err != nil {
		return fmt.Errorf("before heal: %w", err)
	}

	c.healed = true
	if err := c.Sim.RunHealed(3000); err != nil {
		return err
	}
	total := 0
	for _, cmds := range c.Commands {
		total += len(cmds)
	}
	if err := c.RunUntilAcks(total, c.Sim.Now()+20_000); err != nil {
		return fmt.Errorf("liveness: %w", err)
	}
	// a few heartbeats so followers learn the last commit
	if err := c.Sim.RunUntil(c.Sim.Now() + 1000); err != nil {
		return err
	}

	if err := c.CheckAckedWrites(true); err != nil {
		return fmt.Errorf("after heal: %w", err)
	}
	// each key must hold the last write to it in log order. With more than
	// one client that is not the last one in any client's list.
	last := map[string]string{}
	seen := map[string]bool{} // serials: a retried write is applied once
	for _, i := range slices.Sorted(maps.Keys(c.memo.committed)) {
		e := c.memo.committed[i].entry
		f := strings.Fields(e.Command)
		if len(f) != 3 || !strings.EqualFold(f[0], "SET") || seen[e.SerialNumber] {
			continue
		}
		seen[e.SerialNumber] = true
		last[f[1]] = f[2]
	}
	for _, k := range slices.Sorted(maps.Keys(last)) {
		if err := c.CheckStateMachineApplied(k, last[k]); err != nil {
			return fmt.Errorf("after heal: %w", err)
		}
	}
	if err := c.CheckReads(); err != nil {
		return err
	}

	// raft with 3 servers must keep going with any 2, so lose the leader
	d, err := c.CheckFailover(5000)
	c.Failover = d
	return err
}

// ElectionTimeoutMax is the longest election timeout of every server. After
// the leader dies, a follower times out within this and should win the
// election right away, so a failover takes about this long. Much more means
// the election itself is slow.
const ElectionTimeoutMax = 300

// CheckFailover crashes the leader for good and waits for the others to elect
// a new one and commit an entry of its term. It returns how long that took.
func (c *Cluster) CheckFailover(budget sim.Duration) (sim.Duration, error) {
	leader := func(minTerm uint64, committed bool) (int, uint64, bool) {
		for _, id := range c.Servers {
			if !c.Sim.Up(id) {
				continue
			}
			d := c.Sim.Handler(id).(*driver)
			ctx := c.Sim.ReadCtx(id)
			snap := d.snapshot(ctx)
			if snap.Role != raftpb.Role_LEADER || snap.Term < minTerm {
				continue
			}
			if committed {
				// an entry of its own term is committed, so a majority
				// really follows it
				log := d.logEntries(ctx)
				if snap.CommitIndex == 0 || snap.CommitIndex > uint64(len(log)) ||
					log[snap.CommitIndex-1].Term != snap.Term {
					continue
				}
			}
			return id, snap.Term, true
		}
		return 0, 0, false
	}

	start := c.Sim.Now()
	old, term, ok := leader(0, true)
	for ; !ok && c.Sim.Now() < start+sim.Time(budget); old, term, ok = leader(0, true) {
		if err := c.Sim.RunUntil(c.Sim.Now() + 1); err != nil {
			return 0, err
		}
	}
	if !ok {
		return 0, fmt.Errorf("failover: no leader to crash after %dms", budget)
	}

	c.Sim.InjectFault(sim.NewCrashFault(old, false, 0)) // stays down
	start = c.Sim.Now()
	for c.Sim.Now() < start+sim.Time(budget) {
		if err := c.Sim.RunUntil(c.Sim.Now() + 1); err != nil {
			return 0, err
		}
		if _, _, ok := leader(term+1, true); ok {
			return sim.Duration(c.Sim.Now() - start), nil
		}
	}
	return 0, fmt.Errorf("failover: node %d was leader of term %d and crashed, "+
		"no new leader committed anything in %dms", old, term, budget)
}

// Median of the durations, 0 for none.
func Median(ds []sim.Duration) sim.Duration {
	if len(ds) == 0 {
		return 0
	}
	s := slices.Sorted(slices.Values(ds))
	return s[len(s)/2]
}
