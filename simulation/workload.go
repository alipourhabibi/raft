package simulation

import (
	"fmt"

	"github.com/alipourhabibi/detsim/sim"
)

// Workload draws the clients and their commands for one seed. Some seeds get
// one client and few reads, some get three clients fighting over one key.
// Nothing here is picked for a known bug: the same seed always gives the same
// workload, and the sweep tries many.
//
// Keys come from a small set so clients collide on the same key, which is
// where stale reads and lost writes show. Every SET writes a value no other
// command writes, so a read's result tells which write it saw.
//
// Changing this changes what every seed in a corpus means. Bump
// WorkloadVersion when you do.
func Workload(seed uint64) [][]string {
	r := sim.NewStream(seed, sim.StreamSwarm)

	keys := []string{"X", "Y", "Z"}[:1+r.IntN(3)]
	readPercent := 20 + r.IntN(60)
	clients := make([][]string, 1+r.IntN(3))
	for c := range clients {
		n := 5 + r.IntN(11)
		for i := range n {
			k := keys[r.IntN(len(keys))]
			if r.IntN(100) < readPercent {
				clients[c] = append(clients[c], "GET "+k)
			} else {
				clients[c] = append(clients[c], fmt.Sprintf("SET %s c%d.%d", k, c, i))
			}
		}
	}
	return clients
}

// WorkloadVersion goes in the corpus header. A seed saved with another
// version runs a different workload.
const WorkloadVersion = 1
