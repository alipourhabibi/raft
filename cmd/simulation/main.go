// Command simulation runs the raft cluster in detsim with faults and checks
// it. Each seed draws its own faults and its own clients and commands, so a
// plain run needs no flags to find bugs.
//
//	go run ./cmd/simulation                                   sweep seeds 1..1000
//	go run ./cmd/simulation -corpus simulation/testdata/corpus.txt
//	                                                          sweep, save what fails
//	go run ./cmd/simulation -seed 325                         replay one seed
//	go run ./cmd/simulation -seed 325 -corpus simulation/testdata/corpus.txt
//	                                                          replay, save it if it fails
//	go run ./cmd/simulation -rerun -corpus simulation/testdata/corpus.txt
//	                                                          run only the saved seeds
//
// Keep the default flags for the shared corpus: go test replays it with them.
//
// Exits 1 on a failure, 2 when a failure does not reproduce.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/alipourhabibi/detsim/sim"
	"github.com/alipourhabibi/raft/simulation"
)

var (
	seed    = flag.Uint64("seed", 0, "run only this seed (0 = sweep)")
	from    = flag.Uint64("from", 1, "first seed of the sweep")
	count   = flag.Uint64("n", 1000, "how many seeds to sweep")
	loss    = flag.Uint("loss", 0, "message loss, parts per million")
	dup     = flag.Uint("dup", 0, "message duplication, parts per million")
	reorder = flag.Bool("reorder", false, "allow messages to arrive out of order")
	until   = flag.Int64("until", simulation.Until, "fake ms with faults, before healing")
	cmds    = flag.String("cmds", "", "fixed commands instead of the per seed workload: comma separated, ; between clients")
	faults  = flag.Bool("faults", true, "crash, pause and partition servers")
	buggify = flag.Int("buggify", 0, "percent of seeds that skip the end of turn sync")
	trace   = flag.String("trace", "lanes", "trace on failure: lanes, mermaid, none")
	corpus  = flag.String("corpus", "", "file of failing seeds and their shrunk plans; failures are added to it")
	rerun   = flag.Bool("rerun", false, "run only the seeds in -corpus")
)

func main() {
	flag.Parse()
	// raft logs routine events, thousands per seed
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))

	switch {
	case *rerun:
		os.Exit(rerunCorpus())
	case *seed != 0:
		os.Exit(runOne(*seed))
	default:
		os.Exit(sweep())
	}
}

func opts(s uint64) simulation.ClusterOpts {
	o := simulation.DefaultOpts(s)
	o.Network.LossPPM = uint32(*loss)
	o.Network.DuplicationPPM = uint32(*dup)
	o.Network.IsReordering = *reorder
	o.Clients = simulation.Workload(s)
	if *cmds != "" {
		o.Clients = nil
		for _, c := range strings.Split(*cmds, ";") {
			o.Clients = append(o.Clients, strings.Split(c, ","))
		}
	}
	return o
}

func gen(s uint64, r sim.Rand) *sim.Plan {
	if !*faults {
		return &sim.Plan{}
	}
	return simulation.FaultPlan(simulation.Servers, sim.Time(*until), *buggify)(s, r)
}

func planFor(s uint64) *sim.Plan {
	return gen(s, sim.NewStream(s, sim.StreamFault))
}

func run(s uint64, p *sim.Plan) (*simulation.Cluster, error) {
	cl := simulation.Prepare(opts(s))
	cl.Sim.StartWith(p)
	return cl, cl.Check(sim.Time(*until))
}

func sweep() int {
	f, failed := sim.Sweep(*from, *from+*count, gen,
		func(s uint64) (*sim.Sim, *simulation.Cluster) {
			cl := simulation.Prepare(opts(s))
			return cl.Sim, cl
		},
		func(_ *sim.Sim, cl *simulation.Cluster) error {
			return cl.Check(sim.Time(*until))
		})
	if f == nil {
		fmt.Printf("clean: seeds %d..%d\n", *from, *from+*count-1)
		return 0
	}

	fmt.Printf("%d of %d seeds failed:\n", len(failed), *count)
	for _, sf := range failed {
		fmt.Printf("  seed %-6d %s\n", sf.Seed, firstLine(sf.Err))
	}
	fmt.Printf("\nthe lowest is %d, shrinking it\n\n", f.Seed)
	return report(f.Seed, f.Plan, f.Err, failed)
}

func runOne(s uint64) int {
	plan := planFor(s)
	if _, err := run(s, plan); err != nil {
		return report(s, plan, err, []sim.SeedFailure{{Seed: s, Err: err, Plan: plan}})
	}
	fmt.Printf("ok   seed=%d, %d faults\n", s, plan.Len())
	return 0
}

// report shrinks the failing plan, prints it with the trace and saves the
// failures to the corpus.
func report(s uint64, plan *sim.Plan, failErr error, failed []sim.SeedFailure) int {
	// smallest plan that still fails the same way
	want := kind(failErr)
	small := sim.ShrinkToFixpoint(plan, func(p *sim.Plan) bool {
		_, err := run(s, p)
		return err != nil && kind(err) == want
	})
	cl, err := run(s, small)
	if err == nil {
		fmt.Fprintf(os.Stderr, "seed %d failed with %v, then passed when run again.\n"+
			"The run is not deterministic, nothing here can be trusted.\n", s, failErr)
		return 2
	}

	fmt.Printf("FAIL seed=%d: %v\n\nplan (%d of %d faults):\n%s\n%s\n",
		s, err, small.Len(), plan.Len(), small, cl.Sim.StatesString())
	printTrace(cl, *trace)

	// save the shrunk plan, with the error it gives, not the full plan's
	for i := range failed {
		if failed[i].Seed == s {
			failed[i].Plan, failed[i].Err = small, err
		}
	}
	if err := saveCorpus(failed); err != nil {
		fmt.Fprintln(os.Stderr, "corpus:", err)
	}

	fmt.Printf("\nreplay: go run ./cmd/simulation -seed %d%s\n", s, nonDefaultFlags())
	return 1
}

// kind is the part of an error before the first colon, like "liveness" or
// "before heal". Shrinking keeps it, so it doesn't swap one bug for another.
func kind(err error) string {
	var inv *sim.InvariantError
	if errors.As(err, &inv) {
		return "invariant"
	}
	k, _, _ := strings.Cut(err.Error(), ":")
	return k
}

func firstLine(err error) string {
	line, _, _ := strings.Cut(err.Error(), "\n")
	return line
}

func saveCorpus(failed []sim.SeedFailure) error {
	if *corpus == "" || len(failed) == 0 {
		return nil
	}
	var old []sim.SeedEntry
	if f, err := os.Open(*corpus); err == nil {
		old, err = sim.LoadCorpus(f)
		f.Close()
		if err != nil {
			return fmt.Errorf("%s is unreadable, not overwriting it: %w", *corpus, err)
		}
	}
	f, err := os.Create(*corpus)
	if err != nil {
		return err
	}
	defer f.Close()
	header := fmt.Sprintf("raft simulation, workload v%d\nflags:%s",
		simulation.WorkloadVersion, nonDefaultFlags())
	if err := sim.SaveCorpus(f, header, sim.MergeCorpus(old, failed)); err != nil {
		return err
	}
	fmt.Printf("saved %d seeds to %s\n", len(failed), *corpus)
	return nil
}

// rerunCorpus runs every saved seed with its saved plan. It is the
// regression suite: a fixed bug must stay fixed.
func rerunCorpus() int {
	f, err := os.Open(*corpus)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	entries, err := sim.LoadCorpus(f)
	f.Close()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}

	bad := 0
	for _, e := range entries {
		plan := e.Plan
		if plan == nil {
			plan = planFor(e.Seed)
		}
		if _, err := run(e.Seed, plan); err != nil {
			bad++
			fmt.Printf("FAIL seed %-6d %s\n", e.Seed, firstLine(err))
		} else {
			fmt.Printf("ok   seed %-6d (was: %s)\n", e.Seed, e.Reason)
		}
	}
	fmt.Printf("%d of %d seeds still fail\n", bad, len(entries))
	if bad > 0 {
		return 1
	}
	return 0
}

// nonDefaultFlags is the flags a replay needs, the ones not at default.
func nonDefaultFlags() string {
	var b strings.Builder
	flag.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "seed", "from", "n", "corpus", "rerun", "trace":
			return // not part of what a seed means
		}
		fmt.Fprintf(&b, " -%s=%q", f.Name, f.Value.String())
	})
	return b.String()
}

func printTrace(cl *simulation.Cluster, kind string) {
	var err error
	switch kind {
	case "lanes":
		err = cl.Sim.Trace().Lanes(os.Stdout, cl.Sim.Nodes())
	case "mermaid":
		err = cl.Sim.Trace().Mermaid(os.Stdout, cl.Sim.Nodes(), sim.MermaidOpts{Anchor: cl.Sim.Nodes()[0]})
	case "none":
	default:
		err = fmt.Errorf("unknown trace %q", kind)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "trace:", err)
	}
}
