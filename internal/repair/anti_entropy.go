package repair

import (
	"github.com/rdisa/agent-memory-store/internal/conflict"
	"github.com/rdisa/agent-memory-store/internal/types"
)

// Plan is the result of one pairwise version-scan. Push is what we send to
// the peer; Pull is what we should apply locally. CRDT keys are merged and
// appear on both sides if the merge differs from either replica.
type Plan struct {
	Push map[string]types.Value
	Pull map[string]types.Value
}

// Scan compares two full-replica maps (this system does not shard). This is
// the simple version-scan anti-entropy; a Merkle tree is the named stretch.
func Scan(local, remote map[string]types.Value) Plan {
	p := Plan{
		Push: map[string]types.Value{},
		Pull: map[string]types.Value{},
	}
	seen := map[string]bool{}
	for k, lv := range local {
		seen[k] = true
		rv, ok := remote[k]
		if !ok {
			p.Push[k] = lv
			continue
		}
		merged := conflict.Resolve(lv, rv)
		if !merged.Equal(lv) {
			p.Pull[k] = merged
		}
		if !merged.Equal(rv) {
			p.Push[k] = merged
		}
	}
	for k, rv := range remote {
		if seen[k] {
			continue
		}
		p.Pull[k] = rv
	}
	return p
}
