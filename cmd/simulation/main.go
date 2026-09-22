package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/alipourhabibi/detsim/sim"
	"github.com/alipourhabibi/raft/simulation"
)

func main() {
	var (
		seed    = flag.Uint64("seed", 0, "run only this seed (0 = sweep)")
		from    = flag.Uint64("from", 1, "first seed of the sweep")
		count   = flag.Uint64("n", 200, "how many seeds to sweep")
		loss    = flag.Uint("loss", 0, "message loss, parts per million")
		dup     = flag.Uint("dup", 0, "message duplication, parts per million")
		reorder = flag.Bool("reorder", false, "allow messages to arrive out of order")
		until   = flag.Int64("until", 10_000, "fake ms to run per seed")
		cmds    = flag.String("cmds", "SET X 1,SET X 2,GET X", "client commands, comma separated")
		trace   = flag.String("trace", "lanes", "trace on failure: lanes, mermaid, none")
	)
	flag.Parse()

	commands := strings.Split(*cmds, ",")

	seeds := []uint64{*seed}
	if *seed == 0 {
		seeds = seeds[:0]
		for s := *from; s < *from+*count; s++ {
			seeds = append(seeds, s)
		}
	}

	for _, s := range seeds {
		opts := simulation.DefaultOpts(s)
		opts.Network.LossPPM = uint32(*loss)
		opts.Network.DuplicationPPM = uint32(*dup)
		opts.Network.IsReordering = *reorder
		opts.Clients = [][]string{commands}

		cl := simulation.New(opts)
		if err := check(cl, commands, sim.Time(*until)); err != nil {
			fmt.Printf("FAIL seed=%d: %v\n\n", s, err)
			fmt.Println(cl.Sim.StatesString())
			printTrace(cl, *trace)
			fmt.Printf("\nreplay: raftcheck -seed %d -loss %d -dup %d -reorder=%v\n",
				s, *loss, *dup, *reorder)
			os.Exit(1)
		}
		fmt.Printf("ok   seed=%d hash=%016x events=%d\n", s, cl.Sim.Hash(), cl.Sim.EventCount())
	}
}

// check runs one seed and applies every check the tests do.
func check(cl *simulation.Cluster, commands []string, until sim.Time) error {
	if err := cl.RunUntilAcks(len(commands), until); err != nil {
		return err
	}
	// followers catch up
	if err := cl.Sim.RunUntil(cl.Sim.Now() + 500); err != nil {
		return err
	}

	var writes []string
	last := map[string]string{}
	for _, c := range commands {
		f := strings.Fields(c)
		if len(f) == 3 && strings.EqualFold(f[0], "SET") {
			writes = append(writes, c)
			last[f[1]] = f[2]
		}
	}
	if err := cl.CheckLogs(writes); err != nil {
		return err
	}
	for k, v := range last {
		if err := cl.CheckApplied(k, v); err != nil {
			return err
		}
	}
	return nil
}

func printTrace(cl *simulation.Cluster, kind string) {
	var err error
	switch kind {
	case "lanes":
		err = cl.Sim.Trace().Lanes(os.Stdout, cl.Sim.Nodes())
	case "mermaid":
		err = cl.Sim.Trace().Mermaid(os.Stdout, cl.Sim.Nodes())
	case "none":
	default:
		err = fmt.Errorf("unknown trace %q", kind)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "trace:", err)
	}
}
