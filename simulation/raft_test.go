package simulation

import (
	"fmt"
	"slices"
	"testing"

	"github.com/alipourhabibi/detsim/sim"
)

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

	// every node holds the same log, and each command appears exactly
	// as many times as the client sent it
	want := map[string]int{}
	for _, c := range commands {
		want[c]++
	}

	var ref []string // the first node's log
	for _, id := range cl.Servers {
		d := cl.Sim.Handler(id).(*driver)
		entries := d.logEntries(cl.Sim.ReadCtx(id))

		got := make([]string, 0, len(entries))
		count := map[string]int{}
		for _, e := range entries {
			got = append(got, e.Command)
			count[e.Command]++
		}

		for c, n := range want {
			if count[c] != n {
				dump(t, cl)
				t.Fatalf("node %d log %v has %q %d times, want %d", id, got, c, count[c], n)
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
	if read.Value != "2" {
		dump(t, cl)
		t.Fatalf("GET X returned %v, want \"2\"", read.Value)
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
