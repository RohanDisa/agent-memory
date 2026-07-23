package integration

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/rdisa/agent-memory-store/internal/cluster"
	netx "github.com/rdisa/agent-memory-store/internal/net"
	"github.com/rdisa/agent-memory-store/internal/node"
	"github.com/rdisa/agent-memory-store/internal/store"
	"github.com/rdisa/agent-memory-store/internal/types"
)

func TestHTTPGatewayWriteRead(t *testing.T) {
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
	srv := node.NewHTTPServer(n, "127.0.0.1:0")
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close(); _ = n.Close() })

	base := "http://" + srv.Addr
	resp, err := http.Get(base + "/health")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatal(resp.Status)
	}

	body, _ := json.Marshal(map[string]any{"key": "k", "data": "v", "w": 1, "policy": "LWW"})
	resp, err = http.Post(base+"/write", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	slurp, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("write %s %s", resp.Status, slurp)
	}

	resp, err = http.Get(base + "/read?key=k&r=1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var raw map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		t.Fatal(err)
	}
	if found, _ := raw["found"].(bool); !found {
		t.Fatalf("read: %v", raw)
	}
}

func TestHTTPInternalReplicate(t *testing.T) {
	id := types.NodeID("A")
	wal, _ := store.OpenWAL("")
	netw := netx.NewFaultNet(1)
	n := node.New(node.Config{
		ID: id, Peers: cluster.Membership{Self: id, Peers: []cluster.Peer{{ID: id}}},
		Transport: netw.Endpoint(id), WAL: node.NewReplica(id, wal), Timeout: time.Second,
	})
	netw.Register(id, n)
	srv := node.NewHTTPServer(n, "127.0.0.1:0")
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close(); _ = n.Close() })

	val := types.Value{Data: []byte("x"), Version: types.Version{1, "B"}, Policy: types.PolicyLWW}
	b, _ := json.Marshal(map[string]any{"key": "rk", "value": val, "from": "B"})
	resp, err := http.Post("http://"+srv.Addr+"/internal/replicate", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatal(resp.Status)
	}
	got, ok := n.Replica.Get("rk")
	if !ok || string(got.Data) != "x" {
		t.Fatalf("%+v ok=%v", got, ok)
	}
}
