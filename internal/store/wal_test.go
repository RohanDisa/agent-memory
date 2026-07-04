package store

import (
	"path/filepath"
	"testing"

	"github.com/rdisa/agent-memory-store/internal/types"
)

func TestWALAppendAndHistory(t *testing.T) {
	w, err := OpenWAL("")
	if err != nil {
		t.Fatal(err)
	}
	e1, err := w.Append("k", types.Value{Data: []byte("v1"), Version: types.Version{1, "A"}})
	if err != nil || e1.Seq != 1 {
		t.Fatalf("append1: %+v %v", e1, err)
	}
	_, _ = w.Append("other", types.Value{Data: []byte("z"), Version: types.Version{1, "B"}})
	_, _ = w.Append("k", types.Value{Data: []byte("v2"), Version: types.Version{2, "A"}})
	h := w.History("k")
	if len(h) != 2 || string(h[1].Value.Data) != "v2" {
		t.Fatalf("history: %+v", h)
	}
}

func TestWALReplayFromDisk(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "node.wal")
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = w.Append("plan", types.Value{Data: []byte("enterprise"), Version: types.Version{1, "A"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	all := w2.Replay()
	if len(all) != 1 || string(all[0].Value.Data) != "enterprise" {
		t.Fatalf("replay: %+v", all)
	}
	// next append continues the sequence
	e, _ := w2.Append("plan", types.Value{Data: []byte("free"), Version: types.Version{2, "A"}})
	if e.Seq != 2 {
		t.Fatalf("seq after replay: %d", e.Seq)
	}
}

func TestWALEmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.wal")
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if len(w.All()) != 0 {
		t.Fatal("empty wal")
	}
}
