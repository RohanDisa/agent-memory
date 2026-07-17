package repair

import (
	"sync"

	"github.com/rdisa/agent-memory-store/internal/types"
)

type Hint struct {
	Target types.NodeID
	Key    string
	Value  types.Value
}

// Hints stores writes that could not reach a replica. Another node (the
// coordinator that saw the failure) keeps them and replays when the target
// is reachable again. This is hinted handoff, not a durability guarantee
// if the hint-holder itself dies before replay — the README says so.
type Hints struct {
	mu sync.Mutex
	m  map[types.NodeID][]Hint
}

func NewHints() *Hints {
	return &Hints{m: map[types.NodeID][]Hint{}}
}

func (h *Hints) Store(target types.NodeID, key string, val types.Value) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.m[target] = append(h.m[target], Hint{Target: target, Key: key, Value: val.Clone()})
}

func (h *Hints) Take(target types.NodeID) []Hint {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := h.m[target]
	delete(h.m, target)
	return out
}

func (h *Hints) Peek(target types.NodeID) []Hint {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]Hint(nil), h.m[target]...)
}

func (h *Hints) Count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, hs := range h.m {
		n += len(hs)
	}
	return n
}

func (h *Hints) Targets() []types.NodeID {
	h.mu.Lock()
	defer h.mu.Unlock()
	var ids []types.NodeID
	for id := range h.m {
		ids = append(ids, id)
	}
	return ids
}
