package conflict

import (
	"encoding/json"
	"sort"

	"github.com/rdisa/agent-memory-store/internal/types"
)

// orSet is a state-based Observed-Remove Set (OR-Set).
//
// Each add(e) is tagged with a unique id. A remove(e) tombstones every tag
// currently observed for e. An element is in the set iff at least one add-tag
// is not in the remove set.
//
// Concurrent add(x) and remove(x): the remove can only tombstone tags it has
// observed. A concurrent add has a new tag the remove never saw, so x survives.
// That is add-wins, and it is why a naive set (or a 2P-set that forbids re-add)
// is the wrong structure for agent tags.
type orSet struct {
	Adds map[string]map[string]bool `json:"adds"`
	Rems map[string]map[string]bool `json:"rems"`
}

func emptyORSet() orSet {
	return orSet{
		Adds: map[string]map[string]bool{},
		Rems: map[string]map[string]bool{},
	}
}

func decodeORSet(v types.Value) orSet {
	s := emptyORSet()
	if len(v.CRDT) == 0 {
		return s
	}
	_ = json.Unmarshal(v.CRDT, &s)
	if s.Adds == nil {
		s.Adds = map[string]map[string]bool{}
	}
	if s.Rems == nil {
		s.Rems = map[string]map[string]bool{}
	}
	return s
}

func encodeORSet(s orSet, ver types.Version) types.Value {
	b, _ := json.Marshal(s)
	elems := s.Elements()
	data, _ := json.Marshal(elems)
	return types.Value{
		Data:    data,
		Version: ver,
		Policy:  types.PolicyCRDTORSet,
		CRDT:    b,
	}
}

func ensure(m map[string]map[string]bool, k string) map[string]bool {
	if m[k] == nil {
		m[k] = map[string]bool{}
	}
	return m[k]
}

func (s orSet) Add(elem, tag string) orSet {
	ensure(s.Adds, elem)[tag] = true
	return s
}

func (s orSet) Remove(elem string) orSet {
	rems := ensure(s.Rems, elem)
	for tag := range s.Adds[elem] {
		rems[tag] = true
	}
	return s
}

func (s orSet) Contains(elem string) bool {
	for tag := range s.Adds[elem] {
		if !s.Rems[elem][tag] {
			return true
		}
	}
	return false
}

func (s orSet) Elements() []string {
	var out []string
	seen := map[string]bool{}
	for elem := range s.Adds {
		if s.Contains(elem) && !seen[elem] {
			seen[elem] = true
			out = append(out, elem)
		}
	}
	sort.Strings(out)
	return out
}

func mergeORSetStates(a, b orSet) orSet {
	out := emptyORSet()
	union := func(dst map[string]map[string]bool, src map[string]map[string]bool) {
		for elem, tags := range src {
			d := ensure(dst, elem)
			for tag, on := range tags {
				if on {
					d[tag] = true
				}
			}
		}
	}
	union(out.Adds, a.Adds)
	union(out.Adds, b.Adds)
	union(out.Rems, a.Rems)
	union(out.Rems, b.Rems)
	return out
}

func maxVer(a, b types.Version) types.Version {
	if types.Compare(a, b) >= 0 {
		return a
	}
	return b
}

// MergeORSet unions add-tags and remove-tags. Associative, commutative, idempotent.
func MergeORSet(a, b types.Value) types.Value {
	sa, sb := decodeORSet(a), decodeORSet(b)
	return encodeORSet(mergeORSetStates(sa, sb), maxVer(a.Version, b.Version))
}

// ApplyORSetOp applies an add or remove on top of current state, then returns
// the new state-based payload ready to replicate.
func ApplyORSetOp(current types.Value, found bool, op, elem, tag string, ver types.Version) types.Value {
	var s orSet
	if found {
		s = decodeORSet(current)
	} else {
		s = emptyORSet()
	}
	switch op {
	case "remove":
		s = s.Remove(elem)
	default:
		s = s.Add(elem, tag)
	}
	return encodeORSet(s, ver)
}

func ORSetElements(v types.Value) []string {
	return decodeORSet(v).Elements()
}

func ORSetContains(v types.Value, elem string) bool {
	return decodeORSet(v).Contains(elem)
}
