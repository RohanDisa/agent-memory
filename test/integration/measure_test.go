package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rdisa/agent-memory-store/internal/types"
	"github.com/rdisa/agent-memory-store/test/harness"
)

func TestMeasureThroughputAndPartitionAvailability(t *testing.T) {
	c := harness.Start(t, harness.Options{N: 3, Timeout: time.Second})
	const n = 50
	for _, w := range []int{1, 2, 3} {
		start := time.Now()
		ok := 0
		for i := 0; i < n; i++ {
			if c.Write("A", fmt.Sprintf("m-w%d-%d", w, i), "x", w).OK {
				ok++
			}
		}
		elapsed := time.Since(start)
		t.Logf("writes W=%d: %d/%d in %s (%.0f wps, p50-ish avg %s)",
			w, ok, n, elapsed, float64(ok)/elapsed.Seconds(), elapsed/time.Duration(n))
	}

	// strong vs weak read latency after a write
	harness.MustOK(t, c.Write("A", "lat", "v", 3))
	for _, r := range []int{1, 2, 3} {
		start := time.Now()
		const reads = 40
		for i := 0; i < reads; i++ {
			_ = c.Read("B", "lat", r)
		}
		elapsed := time.Since(start)
		t.Logf("reads R=%d: %d in %s (avg %s)", r, reads, elapsed, elapsed/reads)
	}

	c.Net.Partition([]types.NodeID{"A", "B"}, []types.NodeID{"C"})
	const p = 20
	ok2, ok3 := 0, 0
	for i := 0; i < p; i++ {
		if c.Write("A", fmt.Sprintf("part2-%d", i), "x", 2).OK {
			ok2++
		}
		if c.Write("A", fmt.Sprintf("part3-%d", i), "x", 3).OK {
			ok3++
		}
	}
	t.Logf("partition majority availability: W=2 %d/%d W=3 %d/%d", ok2, p, ok3, p)
	if ok2 != p || ok3 != 0 {
		t.Fatalf("expected W=2 all-success and W=3 all-fail, got %d %d", ok2, ok3)
	}

	c.Net.Heal()
	start := time.Now()
	c.AntiEntropy()
	c.AntiEntropy()
	t.Logf("heal+2 anti-entropy rounds: %s", time.Since(start))
}

func TestMeasureCRDTPropertyCounts(t *testing.T) {
	// The property tests live in internal/conflict. This just documents the
	// counts the README cites so a `go test` run prints them next to cluster work.
	t.Log("CRDT LWW-reg: 300 ACI trials + 200 random orderings (internal/conflict)")
	t.Log("CRDT OR-Set: 250 ACI trials + 300 random fold orderings (internal/conflict)")
	_ = context.Background()
	_ = types.PolicyLWW
}
