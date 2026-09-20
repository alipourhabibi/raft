package simulation

import (
	"fmt"
	"testing"
)

func TestElectsOneLeader(t *testing.T) {
	cl := buildCluster(t, defaultOpts(1))
	runUntilLeader(t, cl, 5_000)
}

func TestSeedSweep(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	for seed := uint64(1); seed <= 200; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()
			cl := buildCluster(t, defaultOpts(seed))
			runUntilLeader(t, cl, 5_000)
		})
	}
}
