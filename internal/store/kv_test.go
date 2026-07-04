package store

import (
	"testing"

	"github.com/rdisa/agent-memory-store/internal/types"
)

func TestKVApplyGet(t *testing.T) {
	kv := NewKV()
	v := types.Value{Data: []byte("enterprise"), Version: types.Version{Lamport: 1, NodeID: "A"}}
	kv.Apply("customer-1:plan", v)
	got, ok := kv.Get("customer-1:plan")
	if !ok || string(got.Data) != "enterprise" {
		t.Fatalf("get: %+v ok=%v", got, ok)
	}
}

func TestKVSeenIdempotent(t *testing.T) {
	kv := NewKV()
	v := types.Value{Data: []byte("x"), Version: types.Version{Lamport: 1, NodeID: "A"}}
	kv.Apply("k", v)
	if !kv.Seen("k", v.Version) {
		t.Fatal("expected seen")
	}
	kv.Apply("k", v)
	if kv.Len() != 1 {
		t.Fatal("duplicate apply must not create a second key")
	}
}

func TestKVPrefixAndSplit(t *testing.T) {
	kv := NewKV()
	kv.Apply("cust-1:plan", types.Value{Data: []byte("e"), Version: types.Version{1, "A"}})
	kv.Apply("cust-1:email", types.Value{Data: []byte("a@b"), Version: types.Version{2, "A"}})
	kv.Apply("cust-2:plan", types.Value{Data: []byte("f"), Version: types.Version{1, "B"}})
	got := kv.Prefix("cust-1:")
	if len(got) != 2 {
		t.Fatalf("prefix: %d", len(got))
	}
	e, a := SplitFactKey("customer:123:plan")
	if e != "customer:123" || a != "plan" {
		t.Fatalf("split: %q %q", e, a)
	}
	if FactKey("c", "p") != "c:p" {
		t.Fatal("fact key")
	}
}

func TestKVInject(t *testing.T) {
	kv := NewKV()
	kv.Apply("k", types.Value{Data: []byte("new"), Version: types.Version{2, "A"}})
	kv.Inject("k", types.Value{Data: []byte("old"), Version: types.Version{1, "B"}})
	got, _ := kv.Get("k")
	if string(got.Data) != "old" {
		t.Fatalf("inject should force stale state, got %s", got.Data)
	}
}

func TestKVTombstoneHiddenFromPrefix(t *testing.T) {
	kv := NewKV()
	kv.Apply("e:a", types.Value{Data: []byte("x"), Version: types.Version{1, "A"}, Tombstone: true})
	if len(kv.Prefix("e:")) != 0 {
		t.Fatal("tombstones must not appear in prefix scans")
	}
}
