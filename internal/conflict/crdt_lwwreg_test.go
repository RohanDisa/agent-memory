package conflict

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/rdisa/agent-memory-store/internal/types"
)

func reg(data string, lamport uint64, node string) types.Value {
	return ApplyLWWRegWrite(types.Value{}, false, []byte(data), types.Version{Lamport: lamport, NodeID: node}, false)
}

func TestLWWRegCommutativeAssociativeIdempotent(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	nodes := []string{"A", "B", "C", "D"}
	const trials = 300
	for i := 0; i < trials; i++ {
		mk := func() types.Value {
			return reg(fmt.Sprintf("v%d", rng.Intn(50)), uint64(rng.Intn(15)), nodes[rng.Intn(len(nodes))])
		}
		a, b, c := mk(), mk(), mk()

		if !MergeLWWReg(a, b).Equal(MergeLWWReg(b, a)) {
			t.Fatalf("not commutative at trial %d", i)
		}
		if !MergeLWWReg(a, a).Equal(encodeLWWReg(decodeLWWReg(a))) && !MergeLWWReg(a, a).Equal(a) {
			// Merge re-encodes; compare via timestamp+data
			got := MergeLWWReg(a, a)
			if string(got.Data) != string(a.Data) || !got.Version.Equal(a.Version) {
				t.Fatalf("not idempotent at trial %d: %+v vs %+v", i, got, a)
			}
		}
		left := MergeLWWReg(MergeLWWReg(a, b), c)
		right := MergeLWWReg(a, MergeLWWReg(b, c))
		if string(left.Data) != string(right.Data) || !left.Version.Equal(right.Version) {
			t.Fatalf("not associative at trial %d: %s %s vs %s %s", i, left.Data, left.Version, right.Data, right.Version)
		}
	}
}

func TestLWWRegConvergesRegardlessOfDeliveryOrder(t *testing.T) {
	updates := []types.Value{
		reg("free", 1, "A"),
		reg("pro", 2, "B"),
		reg("enterprise", 2, "C"), // C > B at lamport 2
	}
	perms := [][]int{{0, 1, 2}, {2, 1, 0}, {1, 0, 2}, {2, 0, 1}, {0, 2, 1}, {1, 2, 0}}
	var states []types.Value
	for _, p := range perms {
		acc := types.Value{Policy: types.PolicyCRDTLWWReg}
		found := false
		for _, i := range p {
			if !found {
				acc = updates[i]
				found = true
				continue
			}
			acc = MergeLWWReg(acc, updates[i])
		}
		states = append(states, acc)
	}
	for i := 1; i < len(states); i++ {
		if string(states[i].Data) != string(states[0].Data) || !states[i].Version.Equal(states[0].Version) {
			t.Fatalf("delivery order %d diverged: %s vs %s", i, states[i].Data, states[0].Data)
		}
	}
	if string(states[0].Data) != "enterprise" {
		t.Fatalf("expected enterprise, got %s", states[0].Data)
	}
}

func TestLWWRegPropertyRandomOrderings(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	const n = 200
	for trial := 0; trial < n; trial++ {
		k := 3 + rng.Intn(4)
		ops := make([]types.Value, k)
		for i := 0; i < k; i++ {
			ops[i] = reg(fmt.Sprintf("x%d", i), uint64(rng.Intn(8)+1), string(rune('A'+rng.Intn(4))))
		}
		fold := func(order []int) types.Value {
			acc := ops[order[0]]
			for _, i := range order[1:] {
				acc = MergeLWWReg(acc, ops[i])
			}
			return acc
		}
		order := rng.Perm(k)
		rev := append([]int(nil), order...)
		for i, j := 0, len(rev)-1; i < j; i, j = i+1, j-1 {
			rev[i], rev[j] = rev[j], rev[i]
		}
		s1, s2 := fold(order), fold(rev)
		if string(s1.Data) != string(s2.Data) || !s1.Version.Equal(s2.Version) {
			t.Fatalf("trial %d diverged", trial)
		}
	}
}
