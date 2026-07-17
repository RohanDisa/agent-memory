package node

import (
	"context"
	"testing"
	"time"

	"github.com/rdisa/agent-memory-store/internal/cluster"
	netx "github.com/rdisa/agent-memory-store/internal/net"
	"github.com/rdisa/agent-memory-store/internal/store"
	"github.com/rdisa/agent-memory-store/internal/types"
)

func single(t *testing.T) *Node {
	t.Helper()
	wal, err := store.OpenWAL("")
	if err != nil {
		t.Fatal(err)
	}
	id := types.NodeID("A")
	netw := netx.NewFaultNet(1)
	n := New(Config{
		ID:        id,
		Peers:     cluster.Membership{Self: id, Peers: []cluster.Peer{{ID: id}}},
		Transport: netw.Endpoint(id),
		WAL:       NewReplica(id, wal),
		Timeout:   200 * time.Millisecond,
	})
	netw.Register(id, n)
	t.Cleanup(func() { _ = n.Close() })
	return n
}

func TestSingleNodeWriteRead(t *testing.T) {
	n := single(t)
	ctx := context.Background()
	wr := n.Write(ctx, types.WriteReq{Key: "customer-1:plan", Data: []byte("enterprise"), W: 1})
	if !wr.OK || wr.AcksReceived != 1 {
		t.Fatalf("%+v", wr)
	}
	rd := n.Read(ctx, types.ReadReq{Key: "customer-1:plan", R: 1})
	if !rd.Found || string(rd.Value.Data) != "enterprise" {
		t.Fatalf("%+v", rd)
	}
	if wr.Version.Lamport == 0 || wr.Version.NodeID != "A" {
		t.Fatalf("version: %+v", wr.Version)
	}
}

func TestSingleNodeDeleteTombstone(t *testing.T) {
	n := single(t)
	ctx := context.Background()
	_ = n.Write(ctx, types.WriteReq{Key: "k", Data: []byte("v"), W: 1})
	del := n.Delete(ctx, "k", 1)
	if !del.OK {
		t.Fatal(del)
	}
	rd := n.Read(ctx, types.ReadReq{Key: "k", R: 1})
	if rd.Found {
		t.Fatal("tombstone should hide the value")
	}
	v, ok := n.Replica.Get("k")
	if !ok || !v.Tombstone {
		t.Fatal("tombstone must remain stored so it can replicate")
	}
}

func TestSingleNodeHistoryFromWAL(t *testing.T) {
	n := single(t)
	ctx := context.Background()
	_ = n.Write(ctx, types.WriteReq{Key: "k", Data: []byte("v1"), W: 1})
	_ = n.Write(ctx, types.WriteReq{Key: "k", Data: []byte("v2"), W: 1})
	h := n.Replica.History("k")
	if len(h) != 2 || string(h[0].Value.Data) != "v1" || string(h[1].Value.Data) != "v2" {
		t.Fatalf("%+v", h)
	}
}

func TestWriteRejectsBadW(t *testing.T) {
	n := single(t)
	resp := n.Write(context.Background(), types.WriteReq{Key: "k", Data: []byte("v"), W: 2})
	if resp.OK || resp.Error == "" {
		t.Fatalf("W=2 on N=1 must fail: %+v", resp)
	}
}

func TestAntiEntropyLoopStartsAndStops(t *testing.T) {
	n := single(t)
	n.antiEvery = 20 * time.Millisecond
	n.StartAntiEntropy()
	n.StartAntiEntropy() // idempotent
	time.Sleep(50 * time.Millisecond)
	n.StopAntiEntropy()
	n.StopAntiEntropy()
}

func TestClockBumpsOnEveryWrite(t *testing.T) {
	n := single(t)
	ctx := context.Background()
	a := n.Write(ctx, types.WriteReq{Key: "k", Data: []byte("1"), W: 1})
	b := n.Write(ctx, types.WriteReq{Key: "k", Data: []byte("2"), W: 1})
	if b.Version.Lamport <= a.Version.Lamport {
		t.Fatalf("clock must tick: %d then %d", a.Version.Lamport, b.Version.Lamport)
	}
}
