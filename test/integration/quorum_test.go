package integration

import (
	"testing"

	"github.com/rdisa/agent-memory-store/internal/types"
	"github.com/rdisa/agent-memory-store/test/harness"
)

func TestQuorumWriteOneNodeDownW2SucceedsW3Fails(t *testing.T) {
	c := harness.Start(t, harness.Options{N: 3})
	c.Crash("C")

	ok := c.Write("A", "k", "enterprise", 2)
	if !ok.OK {
		t.Fatalf("W=2 with one down must succeed: %+v", ok)
	}
	fail := c.Write("A", "k2", "x", 3)
	if fail.OK {
		t.Fatalf("W=3 with one down must fail: %+v", fail)
	}
	if fail.AcksReceived >= 3 {
		t.Fatalf("should not have 3 acks: %+v", fail)
	}
}

func TestReadYourWritesWhenRPlusWGreaterThanN(t *testing.T) {
	c := harness.Start(t, harness.Options{N: 3})
	// Majority partition: write W=2, read R=2 from the other majority node.
	must := harness.MustOK(t, c.Write("A", "plan", "enterprise", 2))
	rd := harness.MustFind(t, c.Read("B", "plan", 2))
	if string(rd.Value.Data) != "enterprise" {
		t.Fatalf("R+W>N must read-your-writes, got %q (wrote %v)", rd.Value.Data, must.Version)
	}
}

func TestReadYourWritesDoesNotHoldWhenRPlusWAtMostN(t *testing.T) {
	// N=3, W=1, R=1. Isolate C, write to A (local ack is enough). Read from C.
	// C never saw the write. The system must NOT pretend this is strongly consistent.
	c := harness.Start(t, harness.Options{N: 3})
	c.Net.Isolate("C")
	wr := harness.MustOK(t, c.Write("A", "plan", "enterprise", 1))
	_ = wr
	rd := c.Read("C", "plan", 1)
	if rd.Error != "" {
		// C can still read locally (isolate blocks remote, local still works).
		t.Fatalf("R=1 on isolated C should succeed locally: %+v", rd)
	}
	if rd.Found && string(rd.Value.Data) == "enterprise" {
		t.Fatal("C must not have the W=1 write; if it does, the test cannot demonstrate stale reads")
	}
	if rd.Found {
		t.Fatalf("unexpected value on C: %+v", rd)
	}
}

func TestRWMatrixN3(t *testing.T) {
	// For each (R,W) in {1,2,3}², assert the theory:
	//   R+W > N  → after a successful write that a quorum of the *same* side
	//              can see, a read from a node that can gather R of those
	//              replicas returns the written value.
	//   R+W <= N → we exhibit a stale read (see TestReadYourWritesDoesNotHold...).
	//
	// Cells with W=3 need all nodes up. Cells with R=3 need all nodes answering.
	type cell struct{ r, w int }
	for _, c := range []cell{{1, 1}, {1, 2}, {1, 3}, {2, 1}, {2, 2}, {2, 3}, {3, 1}, {3, 2}, {3, 3}} {
		c := c
		t.Run(label(c.r, c.w), func(t *testing.T) {
			cl := harness.Start(t, harness.Options{N: 3})
			wr := cl.Write("A", "k", "v", c.w)
			if !wr.OK {
				t.Fatalf("write W=%d failed on healthy cluster: %+v", c.w, wr)
			}
			rd := cl.Read("B", "k", c.r)
			strong := c.r+c.w > 3
			if strong {
				if rd.Error != "" || !rd.Found || string(rd.Value.Data) != "v" {
					t.Fatalf("R+W>N must RYW: %+v", rd)
				}
			} else {
				// Healthy cluster + async fanout often still converges before the
				// read. The *guarantee* is absent; we do not require a stale
				// read on the happy path. The dedicated stale-read test above
				// is the evidence that the system does not claim RYW here.
				if rd.Error != "" {
					t.Fatalf("healthy-cluster read R=%d failed: %+v", c.r, rd)
				}
			}
		})
	}
}

func label(r, w int) string {
	return "R" + itoa(r) + "_W" + itoa(w)
}

func itoa(n int) string { return []string{"0", "1", "2", "3"}[n] }

func TestConcurrentConflictingWritesConverge(t *testing.T) {
	c := harness.Start(t, harness.Options{N: 3, DisableReadRepair: true})
	type wr struct {
		data string
		resp types.WriteResp
	}
	done := make(chan wr, 2)
	go func() { done <- wr{"enterprise", c.Write("A", "plan", "enterprise", 2)} }()
	go func() { done <- wr{"free", c.Write("B", "plan", "free", 2)} }()
	a := <-done
	b := <-done
	if !a.resp.OK || !b.resp.OK {
		t.Fatalf("both writes should get W=2: %+v %+v", a.resp, b.resp)
	}
	// Deterministic LWW winner from the two versions, using the data that
	// actually carried that version (completion order is not write identity).
	winner, want := a.resp.Version, a.data
	if types.Compare(b.resp.Version, a.resp.Version) > 0 {
		winner, want = b.resp.Version, b.data
	}
	c.AntiEntropy()
	c.AntiEntropy()
	got, ok := c.AllAgree("plan")
	if !ok {
		t.Fatalf("replicas diverged after anti-entropy")
	}
	if string(got.Data) != want || !got.Version.Equal(winner) {
		t.Fatalf("want %s %v, got %s %v", want, winner, got.Data, got.Version)
	}
}
