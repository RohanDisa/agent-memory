package store

import (
	"strings"
	"sync"

	"github.com/rdisa/agent-memory-store/internal/types"
)

// Record is one key's current replica state.
type Record struct {
	Value types.Value
}

// KV is an in-memory versioned key-value map. Apply is idempotent by version:
// re-applying the same (key, version) is a no-op. Merging is left to the
// caller (conflict.Resolve); Apply stores whatever Value it is given.
type KV struct {
	mu      sync.RWMutex
	m       map[string]types.Value
	applied map[string]map[types.Version]struct{} // key -> versions already applied
}

func NewKV() *KV {
	return &KV{
		m:       make(map[string]types.Value),
		applied: make(map[string]map[types.Version]struct{}),
	}
}

func (kv *KV) Get(key string) (types.Value, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	v, ok := kv.m[key]
	if !ok {
		return types.Value{}, false
	}
	return v.Clone(), true
}

// Apply stores v as the current value of key. It records the incoming version
// so a duplicate replicate is idempotent at the apply-log level. The caller
// must pass the already-merged value.
func (kv *KV) Apply(key string, v types.Value) types.Value {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.applied[key] == nil {
		kv.applied[key] = make(map[types.Version]struct{})
	}
	kv.applied[key][v.Version] = struct{}{}
	cloned := v.Clone()
	kv.m[key] = cloned
	return cloned
}

// Seen reports whether this exact version was already applied to key.
func (kv *KV) Seen(key string, ver types.Version) bool {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	_, ok := kv.applied[key][ver]
	return ok
}

func (kv *KV) Snapshot() map[string]types.Value {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	out := make(map[string]types.Value, len(kv.m))
	for k, v := range kv.m {
		out[k] = v.Clone()
	}
	return out
}

func (kv *KV) Prefix(prefix string) map[string]types.Value {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	out := make(map[string]types.Value)
	for k, v := range kv.m {
		if strings.HasPrefix(k, prefix) && !v.Tombstone {
			out[k] = v.Clone()
		}
	}
	return out
}

func (kv *KV) Len() int {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return len(kv.m)
}

// Inject overwrites a key without going through replication. Tests use this
// to force a stale replica.
func (kv *KV) Inject(key string, v types.Value) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.m[key] = v.Clone()
	if kv.applied[key] == nil {
		kv.applied[key] = make(map[types.Version]struct{})
	}
	kv.applied[key][v.Version] = struct{}{}
}

func FactKey(entity, attribute string) string {
	return entity + ":" + attribute
}

func SplitFactKey(key string) (entity, attribute string) {
	i := strings.LastIndex(key, ":")
	if i < 0 {
		return key, ""
	}
	return key[:i], key[i+1:]
}
