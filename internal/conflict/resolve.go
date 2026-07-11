package conflict

import "github.com/rdisa/agent-memory-store/internal/types"

// Resolve merges two values under the key's namespace policy.
// If policies disagree, the incoming policy of a is preferred only when both
// are LWW-comparable; mixed-policy keys are not a supported production path.
func Resolve(a, b types.Value) types.Value {
	policy := a.Policy
	if a.Version.IsZero() {
		policy = b.Policy
	}
	if b.Policy != a.Policy && !b.Version.IsZero() && types.Compare(b.Version, a.Version) > 0 {
		policy = b.Policy
	}
	switch policy {
	case types.PolicyCRDTLWWReg:
		return MergeLWWReg(a, b)
	case types.PolicyCRDTORSet:
		return MergeORSet(a, b)
	default:
		return LWW(a, b)
	}
}

// ResolveMany folds Resolve over views. Used at read time.
func ResolveMany(views []types.ReplicaView) (types.Value, bool) {
	var acc types.Value
	found := false
	for _, v := range views {
		if !v.Found {
			continue
		}
		if !found {
			acc = v.Value.Clone()
			found = true
			continue
		}
		acc = Resolve(acc, v.Value)
	}
	return acc, found
}

// Agreed reports whether every found view equals the resolved winner
// (byte-level, including CRDT payload).
func Agreed(views []types.ReplicaView, winner types.Value) bool {
	seen := 0
	for _, v := range views {
		if !v.Found {
			continue
		}
		seen++
		if !v.Value.Equal(winner) {
			return false
		}
	}
	return seen > 0
}
