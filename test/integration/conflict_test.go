package integration

import (
	"testing"

	"github.com/rdisa/agent-memory-store/internal/conflict"
	"github.com/rdisa/agent-memory-store/internal/types"
	"github.com/rdisa/agent-memory-store/test/harness"
)

func TestLWWConcurrentWritesEveryReplicaAgrees(t *testing.T) {
	c := harness.Start(t, harness.Options{N: 3, DisableReadRepair: true})
	ra := c.WritePolicy("A", types.WriteReq{Key: "customer-1:plan", Data: []byte("enterprise"), Policy: types.PolicyLWW, W: 2})
	rb := c.WritePolicy("B", types.WriteReq{Key: "customer-1:plan", Data: []byte("free"), Policy: types.PolicyLWW, W: 2})
	if !ra.OK || !rb.OK {
		t.Fatalf("%+v %+v", ra, rb)
	}
	wantVer := ra.Version
	want := "enterprise"
	if types.Compare(rb.Version, ra.Version) > 0 {
		wantVer = rb.Version
		want = "free"
	}
	c.AntiEntropy()
	c.AntiEntropy()
	got, ok := c.AllAgree("customer-1:plan")
	if !ok {
		t.Fatal("replicas disagree after convergence")
	}
	if string(got.Data) != want || !got.Version.Equal(wantVer) {
		t.Fatalf("want %s %v got %s %v", want, wantVer, got.Data, got.Version)
	}
}

func TestCRDTLWWRegOrderIndependenceOnReplicas(t *testing.T) {
	c := harness.Start(t, harness.Options{N: 3, DisableReadRepair: true})
	// Two coordinators write CRDT registers; anti-entropy merge must agree.
	_ = harness.MustOK(t, c.WritePolicy("A", types.WriteReq{
		Key: "customer-1:plan", Data: []byte("enterprise"), Policy: types.PolicyCRDTLWWReg, W: 2,
	}))
	_ = harness.MustOK(t, c.WritePolicy("B", types.WriteReq{
		Key: "customer-1:plan", Data: []byte("free"), Policy: types.PolicyCRDTLWWReg, W: 2,
	}))
	c.AntiEntropy()
	c.AntiEntropy()
	got, ok := c.AllAgree("customer-1:plan")
	if !ok {
		t.Fatal("CRDT LWW-reg replicas diverged")
	}
	if string(got.Data) != "enterprise" && string(got.Data) != "free" {
		t.Fatalf("unexpected %s", got.Data)
	}
}

func TestORSetConcurrentAddRemoveAddWinsOnEveryReplica(t *testing.T) {
	c := harness.Start(t, harness.Options{N: 3, DisableReadRepair: true})
	// Seed the set on all nodes so a subsequent remove observes the original tag.
	harness.MustOK(t, c.WritePolicy("A", types.WriteReq{
		Key: "user-1:tags", Policy: types.PolicyCRDTORSet, W: 3, ORSetOp: "add", ORSetElem: "vip",
	}))

	// Concurrent: A re-adds vip (new tag); C removes vip (tombstones old tags only).
	c.Net.Partition([]types.NodeID{"A", "B"}, []types.NodeID{"C"})
	add := c.WritePolicy("A", types.WriteReq{
		Key: "user-1:tags", Policy: types.PolicyCRDTORSet, W: 2, ORSetOp: "add", ORSetElem: "vip",
	})
	// C is minority: W=1 so the remove is accepted on C only.
	rem := c.WritePolicy("C", types.WriteReq{
		Key: "user-1:tags", Policy: types.PolicyCRDTORSet, W: 1, ORSetOp: "remove", ORSetElem: "vip",
	})
	if !add.OK || !rem.OK {
		t.Fatalf("add=%+v rem=%+v", add, rem)
	}

	c.Net.Heal()
	c.AntiEntropy()
	c.AntiEntropy()

	for _, id := range c.IDs {
		v, ok := c.Local(id, "user-1:tags")
		if !ok || !conflict.ORSetContains(v, "vip") {
			t.Fatalf("%s missing vip after add-wins merge: %v ok=%v", id, conflict.ORSetElements(v), ok)
		}
	}
}

func TestAgentAPIPutGetTags(t *testing.T) {
	c := harness.Start(t, harness.Options{N: 3})
	// Exercise the same write path the agent API uses.
	harness.MustOK(t, c.WritePolicy("A", types.WriteReq{
		Key: "acct-9:plan", Data: []byte("enterprise"), Policy: types.PolicyLWW, W: 2,
	}))
	rd := harness.MustFind(t, c.Read("B", "acct-9:plan", 2))
	if string(rd.Value.Data) != "enterprise" {
		t.Fatal(string(rd.Value.Data))
	}
	harness.MustOK(t, c.WritePolicy("A", types.WriteReq{
		Key: "acct-9:tags", Policy: types.PolicyCRDTORSet, W: 2, ORSetOp: "add", ORSetElem: "beta",
	}))
	got := harness.MustFind(t, c.Read("C", "acct-9:tags", 2))
	if !conflict.ORSetContains(got.Value, "beta") {
		t.Fatalf("tags: %v", conflict.ORSetElements(got.Value))
	}
}
