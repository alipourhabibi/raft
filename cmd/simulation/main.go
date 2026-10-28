// Command simulation runs the raft cluster in detsim with faults and checks
// it.
//
//	go run ./cmd/simulation                      sweep seeds 1..200 with faults
//	go run ./cmd/simulation -from 1 -n 2000      sweep further
//	go run ./cmd/simulation -seed 249            replay one seed
//	go run ./cmd/simulation -faults=false        no node faults, network only
//	go run ./cmd/simulation -buggify 30          also skip the sync in some turns
//	go run ./cmd/simulation -corpus bad.txt      save failing seeds and plans
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
	count   = flag.Uint64("n", 200, "how many seeds to sweep")
	loss    = flag.Uint("loss", 0, "message loss, parts per million")
	dup     = flag.Uint("dup", 0, "message duplication, parts per million")
	reorder = flag.Bool("reorder", false, "allow messages to arrive out of order")
	until   = flag.Int64("until", 10_000, "fake ms with faults, before healing")
	cmds    = flag.String("cmds", "SET X 1,SET X 2,GET X,SET Y 1,GET X,GET Y", "commands, comma separated, one client each with ;")
	faults  = flag.Bool("faults", true, "crash, pause and partition servers")
	buggify = flag.Int("buggify", 0, "percent of seeds that skip the end of turn sync")
	trace   = flag.String("trace", "lanes", "trace on failure: lanes, mermaid, none")
	corpus  = flag.String("corpus", "", "append failing seeds and their shrunk plans here")
)

func main() {
	flag.Parse()
	// raft logs routine events, thousands per seed
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))

	clients := [][]string{}
	for _, c := range strings.Split(*cmds, ";") {
		clients = append(clients, strings.Split(c, ","))
	}
	var all []string
	for _, c := range clients {
		all = append(all, c...)
	}

	opts := func(s uint64) simulation.ClusterOpts {
		o := simulation.DefaultOpts(s)
		o.Network.LossPPM = uint32(*loss)
		o.Network.DuplicationPPM = uint32(*dup)
		o.Network.IsReordering = *reorder
		o.Clients = clients
		return o
	}
	servers := make([]int, len(opts(1).Nodes))
	for i := range servers {
		servers[i] = i
	}
	gen := func(s uint64, r sim.Rand) *sim.Plan {
		if !*faults {
			return &sim.Plan{}
		}
		return simulation.FaultPlan(servers, sim.Time(*until), *buggify)(s, r)
	}
	build := func(s uint64) (*sim.Sim, *simulation.Cluster) {
		cl := simulation.Prepare(opts(s))
		return cl.Sim, cl
	}
	check := func(_ *sim.Sim, cl *simulation.Cluster) error {
		return cl.Check(all, sim.Time(*until))
	}
	run := func(s uint64, p *sim.Plan) (*simulation.Cluster, error) {
		cl := simulation.Prepare(opts(s))
		cl.Sim.StartWith(p)
		return cl, check(cl.Sim, cl)
	}

	var failSeed uint64
	var failPlan *sim.Plan
	var failErr error
	var fails []sim.SeedFailure
	if *seed != 0 {
		failSeed, failPlan = *seed, gen(*seed, sim.NewStream(*seed, sim.StreamFault))
		_, failErr = run(failSeed, failPlan)
		if failErr == nil {
			fmt.Printf("ok   seed=%d, %d faults\n", *seed, failPlan.Len())
			return
		}
	} else {
		f, failed := sim.Sweep(*from, *from+*count, gen, build, check)
		if f == nil {
			fmt.Printf("clean: seeds %d..%d\n", *from, *from+*count-1)
			return
		}
		failSeed, failPlan, failErr, fails = f.Seed, f.Plan, f.Err, failed
		fmt.Printf("%d of %d seeds failed, the lowest is %d\n", len(failed), *count, f.Seed)
	}

	// smallest plan that still fails the same way
	want := kind(failErr)
	small := sim.ShrinkToFixpoint(failPlan, func(p *sim.Plan) bool {
		_, err := run(failSeed, p)
		return err != nil && kind(err) == want
	})
	cl, err := run(failSeed, small)
	if err == nil {
		fmt.Fprintf(os.Stderr, "seed %d failed with %v, then passed when run again.\n"+
			"The run is not deterministic, nothing here can be trusted.\n", failSeed, failErr)
		os.Exit(2)
	}

	fmt.Printf("FAIL seed=%d: %v\n\nplan (%d of %d faults):\n%s\n%s\n",
		failSeed, err, small.Len(), failPlan.Len(), small, cl.Sim.StatesString())
	printTrace(cl, *trace)
	saveCorpus(fails, failSeed, small)
	fmt.Printf("\nreplay: go run ./cmd/simulation -seed %d -until %d -cmds %q -loss %d -dup %d -reorder=%v -faults=%v -buggify %d\n",
		failSeed, *until, *cmds, *loss, *dup, *reorder, *faults, *buggify)
	os.Exit(1)
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

func saveCorpus(fails []sim.SeedFailure, s uint64, small *sim.Plan) {
	if *corpus == "" || len(fails) == 0 {
		return
	}
	for i := range fails {
		if fails[i].Seed == s {
			fails[i].Plan = small
		}
	}
	var old []sim.SeedEntry
	if f, err := os.Open(*corpus); err == nil {
		old, _ = sim.LoadCorpus(f) // unreadable: start over
		f.Close()
	}
	f, err := os.Create(*corpus)
	if err != nil {
		fmt.Fprintln(os.Stderr, "corpus:", err)
		return
	}
	defer f.Close()
	header := fmt.Sprintf("raft simulation until=%d cmds=%q loss=%d dup=%d reorder=%v buggify=%d",
		*until, *cmds, *loss, *dup, *reorder, *buggify)
	if err := sim.SaveCorpus(f, header, sim.MergeCorpus(old, fails)); err != nil {
		fmt.Fprintln(os.Stderr, "corpus:", err)
	}
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
