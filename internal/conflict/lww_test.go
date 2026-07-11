package conflict

import (
	"testing"

	"github.com/rdisa/agent-memory-store/internal/types"
)

func TestLWWDeterministicWinner(t *testing.T) {
	low := types.Value{Data: []byte("free"), Version: types.Version{1, "B"}}
	high := types.Value{Data: []byte("enterprise"), Version: types.Version{2, "A"}}
	if string(LWW(low, high).Data) != "enterprise" {
		t.Fatal("higher lamport must win")
	}
	if string(LWW(high, low).Data) != "enterprise" {
		t.Fatal("merge must be commutative")
	}
}

func TestLWWTieBreakByNodeID(t *testing.T) {
	a := types.Value{Data: []byte("from-A"), Version: types.Version{4, "A"}}
	b := types.Value{Data: []byte("from-B"), Version: types.Version{4, "B"}}
	if string(LWW(a, b).Data) != "from-B" {
		t.Fatal("B > A at equal lamport")
	}
	if string(LWW(b, a).Data) != "from-B" {
		t.Fatal("tie-break must be commutative")
	}
}

func TestLWWIdempotent(t *testing.T) {
	v := types.Value{Data: []byte("x"), Version: types.Version{1, "A"}}
	if !LWW(v, v).Equal(v) {
		t.Fatal("LWW(v,v) must equal v")
	}
}

func TestWinnerIgnoresMissing(t *testing.T) {
	views := []types.ReplicaView{
		{NodeID: "A", Found: false},
		{NodeID: "B", Found: true, Value: types.Value{Data: []byte("x"), Version: types.Version{1, "B"}}},
		{NodeID: "C", Found: true, Value: types.Value{Data: []byte("y"), Version: types.Version{3, "C"}}},
	}
	w, ok := Winner(views)
	if !ok || string(w.Data) != "y" {
		t.Fatalf("winner: %+v ok=%v", w, ok)
	}
}
