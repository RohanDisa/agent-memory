package cluster

import (
	"testing"

	"github.com/rdisa/agent-memory-store/internal/types"
)

func TestParsePeers(t *testing.T) {
	m, err := ParsePeers("B", "A=127.0.0.1:8081,B=127.0.0.1:8082,C=127.0.0.1:8083")
	if err != nil {
		t.Fatal(err)
	}
	if m.N() != 3 || m.Self != "B" || m.Addr("C") != "127.0.0.1:8083" {
		t.Fatalf("%+v", m)
	}
	if len(m.Others()) != 2 {
		t.Fatal(m.Others())
	}
}

func TestQuorumMath(t *testing.T) {
	if !QuorumOK(2, 2) || QuorumOK(1, 2) || QuorumOK(2, 0) {
		t.Fatal("QuorumOK")
	}
	// N=3, W=2, one down: still succeeds
	if WriteFails(3, 2, 1) {
		t.Fatal("W=2 should survive 1 down")
	}
	// N=3, W=3, one down: fails
	if !WriteFails(3, 3, 1) {
		t.Fatal("W=3 must fail with 1 down")
	}
	if WriteFails(3, 1, 2) {
		t.Fatal("W=1 survives 2 down")
	}
}

func TestParsePeersAddsSelf(t *testing.T) {
	m, err := ParsePeers("A", "")
	if err != nil || m.N() != 1 || m.IDs()[0] != types.NodeID("A") {
		t.Fatalf("%+v %v", m, err)
	}
}
