package repair

import (
	"testing"

	"github.com/rdisa/agent-memory-store/internal/types"
)

func TestHintsStoreTake(t *testing.T) {
	h := NewHints()
	h.Store("C", "k", types.Value{Data: []byte("v"), Version: types.Version{1, "A"}})
	if h.Count() != 1 {
		t.Fatal(h.Count())
	}
	got := h.Take("C")
	if len(got) != 1 || got[0].Key != "k" {
		t.Fatalf("%+v", got)
	}
	if h.Count() != 0 {
		t.Fatal("take should drain")
	}
}

func TestScanPushPull(t *testing.T) {
	local := map[string]types.Value{
		"only-local": {Data: []byte("a"), Version: types.Version{1, "A"}, Policy: types.PolicyLWW},
		"shared":     {Data: []byte("new"), Version: types.Version{3, "A"}, Policy: types.PolicyLWW},
	}
	remote := map[string]types.Value{
		"only-remote": {Data: []byte("b"), Version: types.Version{1, "B"}, Policy: types.PolicyLWW},
		"shared":      {Data: []byte("old"), Version: types.Version{1, "B"}, Policy: types.PolicyLWW},
	}
	p := Scan(local, remote)
	if _, ok := p.Push["only-local"]; !ok {
		t.Fatal("should push only-local")
	}
	if _, ok := p.Pull["only-remote"]; !ok {
		t.Fatal("should pull only-remote")
	}
	if string(p.Push["shared"].Data) != "new" {
		t.Fatal("should push winner to remote")
	}
	if _, ok := p.Pull["shared"]; ok {
		t.Fatal("local already has winner")
	}
}

func TestStaleAmong(t *testing.T) {
	winner := types.Value{Data: []byte("new"), Version: types.Version{2, "A"}}
	views := []types.ReplicaView{
		{NodeID: "A", Found: true, Value: winner},
		{NodeID: "B", Found: true, Value: types.Value{Data: []byte("old"), Version: types.Version{1, "B"}}},
		{NodeID: "C", Found: false},
	}
	stale := StaleAmong(views, winner)
	if len(stale) != 2 {
		t.Fatalf("stale: %v", stale)
	}
	if !NeedsRepair(views, winner) {
		t.Fatal("needs repair")
	}
}
