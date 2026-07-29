package agentapi

import (
	"context"
	"testing"
	"time"

	"github.com/rdisa/agent-memory-store/internal/cluster"
	netx "github.com/rdisa/agent-memory-store/internal/net"
	"github.com/rdisa/agent-memory-store/internal/node"
	"github.com/rdisa/agent-memory-store/internal/store"
	"github.com/rdisa/agent-memory-store/internal/types"
)

func TestPutGetFactAndTags(t *testing.T) {
	id := types.NodeID("A")
	wal, _ := store.OpenWAL("")
	netw := netx.NewFaultNet(1)
	n := node.New(node.Config{
		ID:        id,
		Peers:     cluster.Membership{Self: id, Peers: []cluster.Peer{{ID: id}}},
		Transport: netw.Endpoint(id),
		WAL:       node.NewReplica(id, wal),
		Timeout:   time.Second,
	})
	netw.Register(id, n)
	t.Cleanup(func() { _ = n.Close() })

	m := New(n)
	ctx := context.Background()
	wr := m.PutFact(ctx, "customer-1", "plan", "enterprise", types.PolicyLWW, 1)
	if !wr.OK {
		t.Fatal(wr)
	}
	rd := m.GetFact(ctx, "customer-1", "plan", 1)
	if !rd.Found || DecodeString(rd.Value) != "enterprise" {
		t.Fatal(rd)
	}
	if !m.AddTag(ctx, "customer-1", "vip", 1).OK {
		t.Fatal("add tag")
	}
	if !m.AddTag(ctx, "customer-1", "beta", 1).OK {
		t.Fatal("add beta")
	}
	if !m.RemoveTag(ctx, "customer-1", "beta", 1).OK {
		t.Fatal("remove beta")
	}
	tags, resp := m.GetTags(ctx, "customer-1", 1)
	if resp.Error != "" || len(tags) != 1 || tags[0] != "vip" {
		t.Fatalf("tags %v resp %+v", tags, resp)
	}
	if DecodeString(resp.Value) != `["vip"]` && DecodeString(resp.Value) != "[\"vip\"]" {
		// JSON array of remaining tags
		if got := DecodeString(resp.Value); got != `["vip"]` {
			t.Logf("DecodeString tags: %s", got)
		}
	}
	facts := m.GetFacts(ctx, "customer-1", 1)
	if len(facts) < 2 {
		t.Fatalf("facts: %v", facts)
	}
	if len(m.GetHistory("customer-1", "plan")) == 0 {
		t.Fatal("history")
	}
}
