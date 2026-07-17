package node

import (
	"fmt"
	"sync"

	"github.com/rdisa/agent-memory-store/internal/conflict"
	"github.com/rdisa/agent-memory-store/internal/store"
	"github.com/rdisa/agent-memory-store/internal/types"
)

// Replica is the local copy of the full keyspace plus its WAL.
type Replica struct {
	mu     sync.Mutex
	ID     types.NodeID
	KV     *store.KV
	WAL    *store.WAL
	Clock  *types.Lamport
	Reads  bool // when false, ServeRead refuses — used to prove anti-entropy, not read repair, did the work
}

func NewReplica(id types.NodeID, wal *store.WAL) *Replica {
	r := &Replica{
		ID:    id,
		KV:    store.NewKV(),
		WAL:   wal,
		Clock: &types.Lamport{},
		Reads: true,
	}
	if wal != nil {
		for _, e := range wal.Replay() {
			merged := r.mergeIncoming(e.Key, e.Value)
			r.KV.Apply(e.Key, merged)
			r.Clock.Observe(e.Value.Version.Lamport)
		}
	}
	return r
}

func (r *Replica) mergeIncoming(key string, incoming types.Value) types.Value {
	cur, ok := r.KV.Get(key)
	if !ok {
		return incoming.Clone()
	}
	return conflict.Resolve(cur, incoming)
}

// Apply is the local replicate path. Idempotent: a duplicate version is a
// no-op (at-least-once delivery). Merges under the key's policy.
func (r *Replica) Apply(key string, incoming types.Value) (types.Value, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Clock.Observe(incoming.Version.Lamport)
	// Always merge. Re-applying a version is a no-op when the current state
	// already equals the merge (at-least-once). Do not skip on Seen alone:
	// a test inject (or a later CRDT merge) can leave current behind a
	// version we have already logged.
	merged := r.mergeIncoming(key, incoming)
	if cur, ok := r.KV.Get(key); ok && cur.Equal(merged) {
		return cur, nil
	}
	if r.WAL != nil {
		if _, err := r.WAL.Append(key, incoming); err != nil {
			return merged, fmt.Errorf("wal: %w", err)
		}
	}
	return r.KV.Apply(key, merged), nil
}

func (r *Replica) Get(key string) (types.Value, bool) {
	return r.KV.Get(key)
}

func (r *Replica) Snapshot() map[string]types.Value {
	return r.KV.Snapshot()
}

func (r *Replica) History(key string) []types.HistoryEntry {
	if r.WAL == nil {
		return nil
	}
	return r.WAL.History(key)
}

func (r *Replica) SetReadsEnabled(on bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Reads = on
}

func (r *Replica) ReadsEnabled() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.Reads
}

func (r *Replica) Inject(key string, v types.Value) {
	r.KV.Inject(key, v)
}
