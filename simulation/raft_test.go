package simulation

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"testing"

	"github.com/alipourhabibi/detsim/sim"
)

func TestMain(m *testing.M) {
	// raft logs routine events, thousands per seed; they bury test output
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}

func dump(t *testing.T, cl *Cluster) {
	t.Helper()
	t.Logf("states:\n%s", cl.Sim.StatesString())
	if err := cl.Sim.Trace().Lanes(os.Stderr, cl.Sim.Nodes()); err != nil {
		t.Logf("trace dump failed: %v", err)
	}
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

func TestElectsOneLeader(t *testing.T) {
	cl := New(DefaultOpts(1))
	runUntilLeader(t, cl, 5_000)
}

func TestSeedSweep(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	for seed := uint64(1); seed <= 200; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()
			cl := New(DefaultOpts(seed))
			runUntilLeader(t, cl, 5_000)
		})
	}
}

func TestReplicatesOneEntry(t *testing.T) {
	commands := []string{"SET X 1", "SET X 2"}

	opts := DefaultOpts(1)
	opts.Clients = [][]string{commands}
	cl := New(opts)

	// run until the client gets an ack for every command
	wantAcks := len(commands)
	for cl.Sim.Now() < 10_000 {
		if err := cl.Sim.Step(100); err != nil {
			dump(t, cl)
			t.Fatalf("invariant broken: %v", err)
		}
		if ok, _, _ := cl.Sim.History().Counts(); ok >= wantAcks {
			break
		}
	}
	if ok, _, _ := cl.Sim.History().Counts(); ok != wantAcks {
		dump(t, cl)
		t.Fatalf("want %d acks, got %d", wantAcks, ok)
	}

	// give followers a few heartbeats to learn the commit
	if err := cl.Sim.RunUntil(cl.Sim.Now() + 500); err != nil {
		dump(t, cl)
		t.Fatalf("invariant broken: %v", err)
	}

	// every node holds the same log, and each command is there under as
	// many different serials as the client sent it. A retry can append the
	// same serial twice, that is fine: apply skips the second one.
	want := map[string]int{}
	for _, c := range commands {
		want[c]++
	}

	var ref []string // the first node's log
	for _, id := range cl.Servers {
		d := cl.Sim.Handler(id).(*driver)
		entries := d.logEntries(cl.Sim.ReadCtx(id))

		got := make([]string, 0, len(entries))
		serials := map[string]map[string]bool{} // command -> its serials
		for _, e := range entries {
			got = append(got, e.Command)
			if serials[e.Command] == nil {
				serials[e.Command] = map[string]bool{}
			}
			serials[e.Command][e.SerialNumber] = true
		}

		for c, n := range want {
			if len(serials[c]) != n {
				dump(t, cl)
				t.Fatalf("node %d log %v has %q under %d serials, want %d", id, got, c, len(serials[c]), n)
			}
		}

		if ref == nil {
			ref = got
			continue
		}
		if !slices.Equal(got, ref) {
			dump(t, cl)
			t.Fatalf("node %d log %v, want %v", id, got, ref)
		}
	}
	t.Logf("log: %v", ref)
}

func TestReadAfterWrite(t *testing.T) {
	commands := []string{"SET X 1", "SET X 2", "GET X"}

	opts := DefaultOpts(1)
	opts.Clients = [][]string{commands}
	cl := New(opts)

	// run until every command is acked
	wantAcks := len(commands)
	for cl.Sim.Now() < 10_000 {
		if err := cl.Sim.Step(100); err != nil {
			dump(t, cl)
			t.Fatalf("invariant broken: %v", err)
		}
		if ok, _, _ := cl.Sim.History().Counts(); ok >= wantAcks {
			break
		}
	}
	if ok, _, _ := cl.Sim.History().Counts(); ok != wantAcks {
		dump(t, cl)
		t.Fatalf("want %d acks, got %d", wantAcks, ok)
	}

	// the read saw the last write
	var read *sim.Op
	for _, op := range cl.Sim.History().Ops() {
		if op.Kind == opGet && op.Outcome == sim.Ok {
			op := op
			read = &op
		}
	}
	if read == nil {
		dump(t, cl)
		t.Fatal("no completed read")
	}
	if got := read.Value.(OpInfo).Result; got != "2" {
		dump(t, cl)
		t.Fatalf("GET X returned %q, want \"2\"", got)
	}

	// give followers time to apply
	if err := cl.Sim.RunUntil(cl.Sim.Now() + 500); err != nil {
		dump(t, cl)
		t.Fatalf("invariant broken: %v", err)
	}

	// every server applied the writes: X is the last value written
	for _, id := range cl.Servers {
		b, ok := cl.Sim.Get(id, prefixSM+"X")
		if !ok || string(b) != "2" {
			dump(t, cl)
			t.Fatalf("node %d: X = %q (found=%v), want \"2\"", id, b, ok)
		}
	}
}

// faults on: crashes, pauses, partitions and lossy links on the servers,
// every check after every seed. This is what found the two leaders in one
// term and the stale reads.
func TestFaultSweep(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	// two clients writing the same key, so reads can be stale
	clients := [][]string{
		{"SET X a1", "GET X", "SET X a2", "GET X", "SET Y a", "GET X"},
		{"SET X b1", "GET X", "SET X b2", "GET Y", "GET X"},
	}
	all := append(slices.Clone(clients[0]), clients[1]...)
	// ponytail: 200 seeds keeps go test fast; the old code needed 325 for
	// two leaders and 495 for a stale read, run cmd/simulation -n 1000 for those
	f, _ := sim.Sweep(1, 201, FaultPlan([]int{0, 1, 2}, 10_000, 0), func(seed uint64) (*sim.Sim, *Cluster) {
		opts := DefaultOpts(seed)
		opts.Clients = clients
		cl := Prepare(opts)
		return cl.Sim, cl
	}, func(_ *sim.Sim, cl *Cluster) error {
		return cl.Check(all, 10_000)
	})
	if f != nil {
		dump(t, f.Ctx)
		t.Fatalf("seed %d: %v\nplan:\n%s", f.Seed, f.Err, f.Plan)
	}
}
