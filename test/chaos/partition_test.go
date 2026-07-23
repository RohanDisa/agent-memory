package chaos

import (
	"fmt"
	"testing"
	"time"

	"github.com/rdisa/agent-memory-store/internal/types"
	"github.com/rdisa/agent-memory-store/test/harness"
)

func TestPartitionMajorityWritesSucceedMinorityStaleThenHealConverges(t *testing.T) {
	// Seeded so a failure is reproducible. Scenario: {A,B}|{C}.
	c := harness.Start(t, harness.Options{N: 3, Seed: 11, DisableReadRepair: true})
	c.Net.Partition([]types.NodeID{"A", "B"}, []types.NodeID{"C"})

	wr := c.Write("A", "plan", "enterprise", 2)
	if !wr.OK {
		t.Fatalf("majority W=2 must succeed during partition: %+v", wr)
	}
	// Different key: a failed minority write still applies locally (at-least-once),
	// so we must not write "plan" from C or that value can win LWW after heal.
	fail := c.Write("C", "other", "free", 2)
	if fail.OK {
		t.Fatal("minority W=2 must fail (C can only ack itself)")
	}

	if _, ok := c.Local("C", "plan"); ok {
		t.Fatal("C must be stale (no majority write)")
	}

	c.Net.Heal()
	start := time.Now()
	c.AntiEntropy()
	c.AntiEntropy()
	elapsed := time.Since(start)

	got, ok := c.AllAgree("plan")
	if !ok || string(got.Data) != "enterprise" {
		t.Fatalf("after heal+anti-entropy C must catch up: %+v ok=%v", got, ok)
	}
	t.Logf("convergence after heal: %s (2 anti-entropy rounds)", elapsed)
}

func TestIsolateCoordinatorMidWriteCleanFailure(t *testing.T) {
	c := harness.Start(t, harness.Options{N: 3, Seed: 22})
	// First remote send from A is delivered; the rest fail. W=3 cannot be met.
	// Local apply does not go through the net, so this is "coordinator isolated
	// after starting the fanout."
	c.Net.FailFromAfter("A", 1)
	wr := c.Write("A", "plan", "half", 3)
	if wr.OK {
		t.Fatalf("client must see a clean failure, not success: %+v", wr)
	}
	if wr.Error == "" {
		t.Fatal("failed write must carry an error string")
	}
	if wr.AcksReceived >= 3 {
		t.Fatalf("must not report a quorum: %+v", wr)
	}
	// Some replicas may already have the value (at-least-once). We do not
	// claim atomic abort — that would be consensus. The client was not told OK.
}

func TestDrop30PercentThenConverge(t *testing.T) {
	c := harness.Start(t, harness.Options{N: 3, Seed: 33, DisableReadRepair: true})
	c.Net.Drop("A", "B", 0.3)
	c.Net.Drop("A", "C", 0.3)
	c.Net.Drop("B", "C", 0.3)

	const n = 20
	succeeded := 0
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k%d", i)
		// W=2: should usually succeed even with 30% loss.
		if c.Write("A", key, "v", 2).OK {
			succeeded++
		}
	}
	if succeeded == 0 {
		t.Fatal("all writes failed under 30% drop; unlikely with W=2")
	}

	c.Net.ResetFaults()
	// Several anti-entropy rounds so dropped replicates catch up.
	for i := 0; i < 4; i++ {
		c.AntiEntropy()
	}
	// Failed writes during the drop window may still resurface (at-least-once).
	// We do not claim they vanish; we only require keys that exist on one
	// replica after heal to exist on all.
	// Re-write with a healthy cluster so we have a known key to check, then
	// assert full-replica agreement on a post-heal write.
	harness.MustOK(t, c.Write("B", "final", "ok", 3))
	c.AntiEntropy()
	got, ok := c.AllAgree("final")
	if !ok || string(got.Data) != "ok" {
		t.Fatalf("post-drop convergence failed: %+v ok=%v", got, ok)
	}

	// And every successful-during-drop key that at least one replica has should
	// exist on all after enough rounds.
	c.AntiEntropy()
	c.AntiEntropy()
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k%d", i)
		var seen int
		var val types.Value
		for _, id := range c.IDs {
			if v, ok := c.Local(id, key); ok {
				seen++
				val = v
			}
		}
		if seen == 0 {
			continue
		}
		if seen != 3 {
			// one more round
			c.AntiEntropy()
			seen = 0
			for _, id := range c.IDs {
				if v, ok := c.Local(id, key); ok {
					seen++
					val = v
				}
			}
		}
		if seen != 3 {
			t.Fatalf("key %s present on %d/3 replicas after heal: last=%+v", key, seen, val)
		}
	}
}

func TestWriteAvailabilityDuringPartition(t *testing.T) {
	c := harness.Start(t, harness.Options{N: 3, Seed: 44})
	c.Net.Partition([]types.NodeID{"A", "B"}, []types.NodeID{"C"})
	const n = 15
	okW1, okW2, okW3 := 0, 0, 0
	for i := 0; i < n; i++ {
		if c.Write("A", fmt.Sprintf("w1-%d", i), "x", 1).OK {
			okW1++
		}
		if c.Write("A", fmt.Sprintf("w2-%d", i), "x", 2).OK {
			okW2++
		}
		if c.Write("A", fmt.Sprintf("w3-%d", i), "x", 3).OK {
			okW3++
		}
	}
	if okW1 != n || okW2 != n {
		t.Fatalf("majority availability W=1 %d/%d W=2 %d/%d", okW1, n, okW2, n)
	}
	if okW3 != 0 {
		t.Fatalf("W=3 must be 0 during {A,B}|{C}, got %d", okW3)
	}
	t.Logf("majority write availability: W=1 %d/%d W=2 %d/%d W=3 %d/%d", okW1, n, okW2, n, okW3, n)
}
