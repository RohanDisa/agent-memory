package netx

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rdisa/agent-memory-store/internal/types"
)

type stub struct {
	hits atomic.Int32
}

func (s *stub) ApplyReplicate(from NodeID, key string, val types.Value) error {
	s.hits.Add(1)
	return nil
}
func (s *stub) ServeRead(from NodeID, key string) (types.Value, bool, error) {
	s.hits.Add(1)
	return types.Value{Data: []byte("ok")}, true, nil
}
func (s *stub) ServeDigest(from NodeID) (map[string]types.Value, error) {
	return map[string]types.Value{}, nil
}

func TestPartitionIsSymmetric(t *testing.T) {
	n := NewFaultNet(1)
	a, b, c := &stub{}, &stub{}, &stub{}
	n.Register("A", a)
	n.Register("B", b)
	n.Register("C", c)
	n.Partition([]NodeID{"A", "B"}, []NodeID{"C"})

	ea, ec := n.Endpoint("A"), n.Endpoint("C")
	ctx := context.Background()
	if err := ea.ReplicateWrite(ctx, "C", "k", types.Value{}); err == nil {
		t.Fatal("A→C should be blocked")
	}
	if err := ec.ReplicateWrite(ctx, "A", "k", types.Value{}); err == nil {
		t.Fatal("C→A should be blocked (symmetry)")
	}
	if err := ea.ReplicateWrite(ctx, "B", "k", types.Value{}); err != nil {
		t.Fatalf("A→B should work: %v", err)
	}
	n.Heal()
	if err := ea.ReplicateWrite(ctx, "C", "k", types.Value{}); err != nil {
		t.Fatalf("after heal: %v", err)
	}
}

func TestIsolateAndRecover(t *testing.T) {
	n := NewFaultNet(2)
	a, b := &stub{}, &stub{}
	n.Register("A", a)
	n.Register("B", b)
	n.Isolate("A")
	if err := n.Endpoint("A").ReplicateWrite(context.Background(), "B", "k", types.Value{}); err == nil {
		t.Fatal("isolated A cannot send")
	}
	if err := n.Endpoint("B").ReplicateWrite(context.Background(), "A", "k", types.Value{}); err == nil {
		t.Fatal("nobody can reach isolated A")
	}
	n.Recover("A")
	if err := n.Endpoint("A").ReplicateWrite(context.Background(), "B", "k", types.Value{}); err != nil {
		t.Fatal(err)
	}
}

func TestDropDeterministicUnderSeed(t *testing.T) {
	run := func(seed int64) int {
		n := NewFaultNet(seed)
		s := &stub{}
		n.Register("B", s)
		n.Drop("A", "B", 0.5)
		e := n.Endpoint("A")
		ctx := context.Background()
		ok := 0
		for i := 0; i < 40; i++ {
			if err := e.ReplicateWrite(ctx, "B", "k", types.Value{}); err == nil {
				ok++
			}
		}
		return ok
	}
	if run(99) != run(99) {
		t.Fatal("same seed must replay the same drop sequence")
	}
	// different seed is allowed to differ; we only require reproducibility
}

func TestDelayHonored(t *testing.T) {
	n := NewFaultNet(3)
	n.Register("B", &stub{})
	n.Delay("A", "B", 25*time.Millisecond)
	start := time.Now()
	if err := n.Endpoint("A").ReplicateWrite(context.Background(), "B", "k", types.Value{}); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 20*time.Millisecond {
		t.Fatal("expected injected delay")
	}
}

func TestUnregisteredIsUnreachable(t *testing.T) {
	n := NewFaultNet(4)
	if err := n.Endpoint("A").ReplicateWrite(context.Background(), "Z", "k", types.Value{}); err == nil {
		t.Fatal("missing handler")
	}
}
