package conflict

import (
	"bytes"

	"github.com/rdisa/agent-memory-store/internal/types"
)

// LWW merges two replica values with last-write-wins under the Lamport+node_id
// total order. The older write is discarded by design. This is the coordinator
// / read-repair path for PolicyLWW, not a CRDT: merge is still deterministic
// because the order is total, but it is "highest version wins" rather than a
// payload-level merge that is independently proven as a CvRDT.
func LWW(a, b types.Value) types.Value {
	if cmpValue(a, b) >= 0 {
		return a.Clone()
	}
	return b.Clone()
}

// cmpValue is a total order: version first, then tombstone, then payload bytes.
// The extra keys make merge commutative when two values share a version
// (should not happen in the protocol; property tests may generate it).
func cmpValue(a, b types.Value) int {
	if c := types.Compare(a.Version, b.Version); c != 0 {
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

// Winner returns the LWW winner among views that have a value.
func Winner(views []types.ReplicaView) (types.Value, bool) {
	var best types.Value
	found := false
	for _, v := range views {
		if !v.Found {
			continue
		}
		if !found || types.Compare(v.Value.Version, best.Version) > 0 {
			best = v.Value.Clone()
			found = true
		}
	}
	return best, found
}
