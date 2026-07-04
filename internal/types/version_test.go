package types

import (
	"math/rand"
	"testing"
)

func TestCompareEqual(t *testing.T) {
	a := Version{Lamport: 3, NodeID: "A"}
	if Compare(a, a) != 0 {
		t.Fatalf("equal versions must compare 0")
	}
}

func TestCompareLamportDominates(t *testing.T) {
	a := Version{Lamport: 2, NodeID: "Z"}
	b := Version{Lamport: 3, NodeID: "A"}
	if Compare(a, b) >= 0 {
		t.Fatalf("higher lamport must win, got %d", Compare(a, b))
	}
	if Compare(b, a) <= 0 {
		t.Fatalf("compare is not antisymmetric on lamport")
	}
}

func TestCompareNodeIDTiebreak(t *testing.T) {
	a := Version{Lamport: 7, NodeID: "A"}
	b := Version{Lamport: 7, NodeID: "B"}
	if Compare(a, b) >= 0 {
		t.Fatalf("equal lamport must break ties by node_id: A < B")
	}
	if Compare(b, a) <= 0 {
		t.Fatalf("tie-break is not antisymmetric")
	}
}

func TestCompareAntisymmetric(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	nodes := []string{"A", "B", "C", "n0", "n1"}
	for i := 0; i < 200; i++ {
		a := Version{Lamport: uint64(rng.Intn(20)), NodeID: nodes[rng.Intn(len(nodes))]}
		b := Version{Lamport: uint64(rng.Intn(20)), NodeID: nodes[rng.Intn(len(nodes))]}
		ab, ba := Compare(a, b), Compare(b, a)
		if a.Equal(b) {
			if ab != 0 || ba != 0 {
				t.Fatalf("equal pair must be 0 both ways: %v %v", a, b)
			}
			continue
		}
		if ab == 0 || ba == 0 || ab != -ba {
			t.Fatalf("antisymmetry failed for %v vs %v: %d %d", a, b, ab, ba)
		}
	}
}

func TestCompareTransitive(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	nodes := []string{"A", "B", "C", "D"}
	ver := func() Version {
		return Version{Lamport: uint64(rng.Intn(12)), NodeID: nodes[rng.Intn(len(nodes))]}
	}
	for i := 0; i < 400; i++ {
		a, b, c := ver(), ver(), ver()
		if Compare(a, b) <= 0 && Compare(b, c) <= 0 && Compare(a, c) > 0 {
			t.Fatalf("transitivity violated: %v <= %v <= %v but a > c", a, b, c)
		}
		if Compare(a, b) >= 0 && Compare(b, c) >= 0 && Compare(a, c) < 0 {
			t.Fatalf("transitivity violated: %v >= %v >= %v but a < c", a, b, c)
		}
	}
}

func TestCompareTotalOrder(t *testing.T) {
	// Every pair is comparable: Compare never "refuses". Zero is the least
	// non-zero-node value only when lamport is 0 and node_id is empty.
	a := Version{}
	b := Version{Lamport: 0, NodeID: "A"}
	if Compare(a, b) >= 0 {
		t.Fatalf("zero version should lose to 0@A")
	}
}

func TestParsePolicy(t *testing.T) {
	cases := []struct {
		in   string
		want Policy
	}{
		{"", PolicyLWW},
		{"LWW", PolicyLWW},
		{"CRDT_LWWREG", PolicyCRDTLWWReg},
		{"orset", PolicyCRDTORSet},
	}
	for _, tc := range cases {
		got, err := ParsePolicy(tc.in)
		if err != nil || got != tc.want {
			t.Fatalf("ParsePolicy(%q) = %v, %v", tc.in, got, err)
		}
	}
	if _, err := ParsePolicy("raft"); err == nil {
		t.Fatal("expected error for unknown policy")
	}
}

func TestStringers(t *testing.T) {
	if PolicyLWW.String() != "LWW" || PolicyCRDTLWWReg.String() != "CRDT_LWWREG" || PolicyCRDTORSet.String() != "CRDT_ORSET" {
		t.Fatal("policy strings")
	}
	if Policy(9).String() != "Policy(9)" {
		t.Fatal("unknown policy")
	}
	v := Version{Lamport: 3, NodeID: "A"}
	if v.String() != "3@A" {
		t.Fatal(v.String())
	}
	_ = MustJSON(v)
}

func TestLamportWitness(t *testing.T) {
	var c Lamport
	if c.Tick() != 1 {
		t.Fatal("first tick")
	}
	c.Observe(10)
	if c.Time() != 10 {
		t.Fatalf("observe: got %d", c.Time())
	}
	if c.Witness(5) != 11 {
		t.Fatalf("witness should tick past max")
	}
}
