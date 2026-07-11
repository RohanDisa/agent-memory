package conflict

import (
	"bytes"
	"encoding/json"

	"github.com/rdisa/agent-memory-store/internal/types"
)

// lwwReg is a state-based LWW-register CRDT. The timestamp is Lamport+node_id,
// not wall-clock: clock skew cannot invert the order. Merge is max(ts).
//
// This is the difference from PolicyLWW: PolicyLWW is "the coordinator picked a
// version and we keep the max." PolicyCRDTLWWReg is a payload whose Merge is
// associative, commutative, and idempotent as a mathematical operation on the
// register itself. Tests prove those three properties over random states, not
// one hand-picked example.
type lwwReg struct {
	Data      []byte        `json:"data"`
	TS        types.Version `json:"ts"`
	Tombstone bool          `json:"tombstone"`
}

func newLWWReg(data []byte, ts types.Version, tombstone bool) lwwReg {
	return lwwReg{Data: append([]byte(nil), data...), TS: ts, Tombstone: tombstone}
}

func decodeLWWReg(v types.Value) lwwReg {
	if len(v.CRDT) > 0 {
		var r lwwReg
		if err := json.Unmarshal(v.CRDT, &r); err == nil && !r.TS.IsZero() {
			return r
		}
	}
	return lwwReg{Data: append([]byte(nil), v.Data...), TS: v.Version, Tombstone: v.Tombstone}
}

func encodeLWWReg(r lwwReg) types.Value {
	b, _ := json.Marshal(r)
	return types.Value{
		Data:      append([]byte(nil), r.Data...),
		Version:   r.TS,
		Policy:    types.PolicyCRDTLWWReg,
		Tombstone: r.Tombstone,
		CRDT:      b,
	}
}

func cmpLWWReg(a, b lwwReg) int {
	if c := types.Compare(a.TS, b.TS); c != 0 {
		return c
	}
	if a.Tombstone != b.Tombstone {
		if a.Tombstone {
			return 1
		}
		return -1
	}
	return bytes.Compare(a.Data, b.Data)
}

// MergeLWWReg is the CvRDT merge: the register with the greater timestamp wins.
// Equal timestamps then compare tombstone and payload so Merge is commutative
// even if two distinct writes were (incorrectly) given the same tag.
func MergeLWWReg(a, b types.Value) types.Value {
	ra, rb := decodeLWWReg(a), decodeLWWReg(b)
	if cmpLWWReg(ra, rb) >= 0 {
		return encodeLWWReg(ra)
	}
	return encodeLWWReg(rb)
}

// ApplyLWWRegWrite builds a register from a coordinator-assigned timestamp.
func ApplyLWWRegWrite(current types.Value, found bool, data []byte, ts types.Version, tombstone bool) types.Value {
	incoming := encodeLWWReg(newLWWReg(data, ts, tombstone))
	if !found {
		return incoming
	}
	return MergeLWWReg(current, incoming)
}
