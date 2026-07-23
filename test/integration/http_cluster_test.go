package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/rdisa/agent-memory-store/internal/cluster"
	netx "github.com/rdisa/agent-memory-store/internal/net"
	"github.com/rdisa/agent-memory-store/internal/node"
	"github.com/rdisa/agent-memory-store/internal/repair"
	"github.com/rdisa/agent-memory-store/internal/store"
	"github.com/rdisa/agent-memory-store/internal/types"
)

func startHTTPNode(t *testing.T, id types.NodeID, peers []cluster.Peer, addrs map[types.NodeID]string) (*node.Node, *node.HTTPServer) {
	t.Helper()
	wal, err := store.OpenWAL("")
	if err != nil {
		t.Fatal(err)
	}
	tr := netx.NewHTTPTransport(id, addrs)
	n := node.New(node.Config{
		ID:        id,
		Peers:     cluster.Membership{Self: id, Peers: peers},
		Transport: tr,
		WAL:       node.NewReplica(id, wal),
		Timeout:   time.Second,
	})
	srv := node.NewHTTPServer(n, "127.0.0.1:0")
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close(); _ = n.Close() })
	return n, srv
}

func TestHTTPTwoNodeReplicateAndGateway(t *testing.T) {
	peers := []cluster.Peer{{ID: "A"}, {ID: "B"}}
	addrs := map[types.NodeID]string{}
	_, srvA := startHTTPNode(t, "A", peers, addrs)
	nb, srvB := startHTTPNode(t, "B", peers, addrs)
	addrs["A"] = srvA.Addr
	addrs["B"] = srvB.Addr

	baseA := "http://" + srvA.Addr
	post := func(url string, body any) *http.Response {
		t.Helper()
		b, _ := json.Marshal(body)
		resp, err := http.Post(url, "application/json", bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	resp := post(baseA+"/write", map[string]any{"key": "k", "data": "v", "w": 2, "policy": "LWW"})
	slurp, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("write: %s %s", resp.Status, slurp)
	}
	got, ok := nb.Replica.Get("k")
	if !ok || string(got.Data) != "v" {
		t.Fatalf("B should have replicated via HTTP: %+v ok=%v", got, ok)
	}

	resp = post(baseA+"/fact", map[string]any{
		"entity": "e", "attribute": "plan", "value": "enterprise", "w": 2, "policy": "LWW",
	})
	resp.Body.Close()

	resp, err := http.Get(baseA + "/fact?entity=e&attribute=plan&r=2")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	resp, err = http.Get(baseA + "/facts?entity=e")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	resp = post(baseA+"/tag", map[string]any{"entity": "e", "tag": "vip", "w": 2})
	resp.Body.Close()
	req, _ := http.NewRequest(http.MethodDelete, baseA+"/tag", bytes.NewReader([]byte(`{"entity":"e","tag":"vip","w":2}`)))
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	resp, err = http.Get(baseA + "/tag?entity=e")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	resp, err = http.Get(baseA + "/history?entity=e&attribute=plan")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	resp = post(baseA+"/delete", map[string]any{"key": "k", "w": 2})
	resp.Body.Close()

	resp, err = http.Get(baseA + "/debug/kv?key=e:plan")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	resp, err = http.Get(baseA + "/debug/kv")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	resp, err = http.Get(baseA + "/debug/hints")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	resp = post(baseA+"/debug/anti-entropy", map[string]any{})
	resp.Body.Close()
	resp = post(baseA+"/debug/handoff", map[string]any{})
	resp.Body.Close()

	resp = post(baseA+"/debug/fault", map[string]any{"action": "block", "peers": []string{"B"}})
	slurp, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("fault block: %s %s", resp.Status, slurp)
	}
	resp = post(baseA+"/debug/fault", map[string]any{"action": "heal"})
	resp.Body.Close()

	// internal read + digest
	resp = post(baseA+"/internal/read", map[string]any{"key": "e:plan", "from": "B"})
	resp.Body.Close()
	resp, err = http.Get(baseA + "/internal/digest")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	resp = post(baseA+"/read", map[string]any{"key": "e:plan", "r": 2})
	resp.Body.Close()
}

func TestPushWinnerAndWALPath(t *testing.T) {
	wal, _ := store.OpenWAL("")
	if wal.Path() != "" {
		t.Fatal("memory wal has empty path")
	}
	n := &fakePush{}
	if err := repair.PushWinner(context.Background(), n, "B", "k", types.Value{Data: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if n.n != 1 {
		t.Fatal(n.n)
	}
}

type fakePush struct{ n int }

func (f *fakePush) ReplicateWrite(ctx context.Context, to types.NodeID, key string, val types.Value) error {
	f.n++
	return nil
}
