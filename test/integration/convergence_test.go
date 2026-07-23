package integration

import (
	"context"
	"testing"

	"github.com/rdisa/agent-memory-store/internal/types"
	"github.com/rdisa/agent-memory-store/test/harness"
)

func TestReadRepairConvergesForcedStaleReplica(t *testing.T) {
	c := harness.Start(t, harness.Options{N: 3})
	harness.MustOK(t, c.Write("A", "plan", "enterprise", 3))

	// Force C stale. A subsequent R=3 read must see the disagreement and
	// push the winner to C before (or as) it returns.
	c.Node("C").Replica.Inject("plan", types.Value{
		Data:    []byte("free"),
		Version: types.Version{Lamport: 0, NodeID: "Z"},
		Policy:  types.PolicyLWW,
	})
	before, _ := c.Local("C", "plan")
	if string(before.Data) != "free" {
		t.Fatal("inject failed")
	}

	rd := harness.MustFind(t, c.Read("A", "plan", 3))
	if string(rd.Value.Data) != "enterprise" {
		t.Fatalf("read should return winner, got %s", rd.Value.Data)
	}
	if !rd.Repaired {
		t.Fatal("expected repaired=true")
	}
	after, ok := c.Local("C", "plan")
	if !ok || string(after.Data) != "enterprise" {
		t.Fatalf("C should have been repaired, got %+v ok=%v", after, ok)
	}
}

func TestHintedHandoffDeliversToRecoveredNode(t *testing.T) {
	c := harness.Start(t, harness.Options{N: 3})
	c.Crash("C")
	wr := harness.MustOK(t, c.Write("A", "plan", "enterprise", 2))
	_ = wr
	if c.Node("A").Hints.Count() == 0 && c.Node("B").Hints.Count() == 0 {
		t.Fatal("expected a hint to be stored for down node C")
	}
	if _, ok := c.Local("C", "plan"); ok {
		t.Fatal("C was down and must not have the write yet")
	}

	c.Revive("C")
	c.Handoff()

	got, ok := c.Local("C", "plan")
	if !ok || string(got.Data) != "enterprise" {
		t.Fatalf("handoff should deliver to C, got %+v ok=%v", got, ok)
	}
}

func TestAntiEntropyConvergesWithReadsDisabled(t *testing.T) {
	c := harness.Start(t, harness.Options{N: 3, DisableReadRepair: true})
	for _, id := range c.IDs {
		c.Node(id).Replica.SetReadsEnabled(false)
	}

	// Diverge replicas directly. No client reads will run.
	c.Node("A").Replica.Inject("plan", types.Value{
		Data: []byte("enterprise"), Version: types.Version{5, "A"}, Policy: types.PolicyLWW,
	})
	c.Node("B").Replica.Inject("plan", types.Value{
		Data: []byte("free"), Version: types.Version{2, "B"}, Policy: types.PolicyLWW,
	})
	c.Node("C").Replica.Inject("other", types.Value{
		Data: []byte("only-c"), Version: types.Version{1, "C"}, Policy: types.PolicyLWW,
	})

	c.AntiEntropy()
	c.AntiEntropy()

	plan, ok := c.AllAgree("plan")
	if !ok || string(plan.Data) != "enterprise" {
		t.Fatalf("plan should converge to enterprise: %+v ok=%v", plan, ok)
	}
	other, ok := c.AllAgree("other")
	if !ok || string(other.Data) != "only-c" {
		t.Fatalf("other should propagate from C: %+v ok=%v", other, ok)
	}
}

func TestTombstoneReplicatesAndIsNotUndone(t *testing.T) {
	c := harness.Start(t, harness.Options{N: 3})
	harness.MustOK(t, c.Write("A", "k", "v", 3))
	del := c.Node("A").Delete(context.Background(), "k", 3)
	if !del.OK {
		t.Fatal(del)
	}
	c.AntiEntropy()
	for _, id := range c.IDs {
		v, ok := c.Local(id, "k")
		if !ok || !v.Tombstone {
			t.Fatalf("%s missing tombstone: %+v ok=%v", id, v, ok)
		}
	}
}
