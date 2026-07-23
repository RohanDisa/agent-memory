package harness

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/rdisa/agent-memory-store/internal/cluster"
	netx "github.com/rdisa/agent-memory-store/internal/net"
	"github.com/rdisa/agent-memory-store/internal/node"
	"github.com/rdisa/agent-memory-store/internal/store"
	"github.com/rdisa/agent-memory-store/internal/types"
)

var DefaultIDs = []types.NodeID{"A", "B", "C"}

type Cluster struct {
	T       *testing.T
	Net     *netx.Network
	Nodes   map[types.NodeID]*node.Node
	IDs     []types.NodeID
	Timeout time.Duration
}

type Options struct {
	N                 int
	Seed              int64
	Timeout           time.Duration
	AntiEntropyEvery  time.Duration
	DisableReadRepair bool
	WALDir            string
}

func Start(t *testing.T, opt Options) *Cluster {
	t.Helper()
	if opt.N == 0 {
		opt.N = 3
	}
	if opt.Timeout == 0 {
		opt.Timeout = 400 * time.Millisecond
	}
	if opt.Seed == 0 {
		opt.Seed = 1
	}
	ids := make([]types.NodeID, opt.N)
	copy(ids, DefaultIDs)
	extra := []types.NodeID{"D", "E"}
	for i := 3; i < opt.N; i++ {
		ids[i] = extra[i-3]
	}
	if opt.N > 5 {
		t.Fatalf("N=%d not supported in harness (max 5)", opt.N)
	}

	netw := netx.NewFaultNet(opt.Seed)
	c := &Cluster{
		T:       t,
		Net:     netw,
		Nodes:   map[types.NodeID]*node.Node{},
		IDs:     ids[:opt.N],
		Timeout: opt.Timeout,
	}

	var peers []cluster.Peer
	for _, id := range c.IDs {
		peers = append(peers, cluster.Peer{ID: id})
	}

	for _, id := range c.IDs {
		var wal *store.WAL
		if opt.WALDir != "" {
			w, err := store.OpenWAL(filepath.Join(opt.WALDir, string(id)+".wal"))
			if err != nil {
				t.Fatal(err)
			}
			wal = w
		} else {
			w, err := store.OpenWAL("")
			if err != nil {
				t.Fatal(err)
			}
			wal = w
		}
		rep := node.NewReplica(id, wal)
		n := node.New(node.Config{
			ID:                id,
			Peers:             cluster.Membership{Self: id, Peers: peers},
			Transport:         netw.Endpoint(id),
			WAL:               rep,
			Timeout:           opt.Timeout,
			AntiEntropyEvery:  opt.AntiEntropyEvery,
			DisableReadRepair: opt.DisableReadRepair,
		})
		netw.Register(id, n)
		c.Nodes[id] = n
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func (c *Cluster) Close() {
	for _, n := range c.Nodes {
		_ = n.Close()
	}
}

func (c *Cluster) Node(id types.NodeID) *node.Node {
	return c.Nodes[id]
}

func (c *Cluster) Ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), c.Timeout)
}

func (c *Cluster) Write(id types.NodeID, key, data string, w int) types.WriteResp {
	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer cancel()
	return c.Nodes[id].Write(ctx, types.WriteReq{
		Key: key, Data: []byte(data), Policy: types.PolicyLWW, W: w,
	})
}

func (c *Cluster) WritePolicy(id types.NodeID, req types.WriteReq) types.WriteResp {
	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer cancel()
	return c.Nodes[id].Write(ctx, req)
}

func (c *Cluster) Read(id types.NodeID, key string, r int) types.ReadResp {
	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer cancel()
	return c.Nodes[id].Read(ctx, types.ReadReq{Key: key, R: r})
}

func (c *Cluster) Local(id types.NodeID, key string) (types.Value, bool) {
	return c.Nodes[id].Replica.Get(key)
}

func (c *Cluster) Crash(id types.NodeID) {
	c.Net.Unregister(id)
}

func (c *Cluster) Revive(id types.NodeID) {
	c.Net.Register(id, c.Nodes[id])
}

func (c *Cluster) AntiEntropy() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*c.Timeout)
	defer cancel()
	for _, n := range c.Nodes {
		n.AntiEntropyRound(ctx)
	}
}

func (c *Cluster) Handoff() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*c.Timeout)
	defer cancel()
	for _, n := range c.Nodes {
		n.ReplayHints(ctx)
	}
}

func (c *Cluster) AllAgree(key string) (types.Value, bool) {
	var first types.Value
	var ok bool
	for i, id := range c.IDs {
		v, found := c.Local(id, key)
		if i == 0 {
			first, ok = v, found
			continue
		}
		if found != ok || (found && !v.Equal(first)) {
			return first, false
		}
	}
	return first, true
}

func MustOK(t *testing.T, resp types.WriteResp) types.WriteResp {
	t.Helper()
	if !resp.OK {
		t.Fatalf("write failed: %+v", resp)
	}
	return resp
}

func MustFind(t *testing.T, resp types.ReadResp) types.ReadResp {
	t.Helper()
	if resp.Error != "" || !resp.Found {
		t.Fatalf("read failed: %+v", resp)
	}
	return resp
}
