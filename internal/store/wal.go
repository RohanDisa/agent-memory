package store

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"

	"github.com/rdisa/agent-memory-store/internal/types"
)

// WAL is an append-only JSON-lines log of applied writes. Replay rebuilds
// in-memory state after a process restart. A full-cluster restart recovering
// every node from WAL is a stretch goal; this WAL is enough to demonstrate
// per-node durability and get_history.
type WAL struct {
	mu   sync.Mutex
	path string
	f    *os.File
	seq  uint64
	// in-memory copy so History does not have to re-read the file
	log []types.HistoryEntry
}

func OpenWAL(path string) (*WAL, error) {
	if path == "" {
		return &WAL{path: "", log: nil}, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil && !os.IsExist(err) {
		// Dir may be "." — ignore
		if filepath.Dir(path) != "." {
			return nil, err
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	w := &WAL{path: path, f: f}
	if err := w.load(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return w, nil
}

func (w *WAL) load() error {
	if w.f == nil {
		return nil
	}
	rf, err := os.Open(w.path)
	if err != nil {
		return err
	}
	defer rf.Close()
	sc := bufio.NewScanner(rf)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(bytesTrimSpace(line)) == 0 {
			continue
		}
		var e types.HistoryEntry
		if err := json.Unmarshal(line, &e); err != nil {
			return err
		}
		w.log = append(w.log, e)
		if e.Seq > w.seq {
			w.seq = e.Seq
		}
	}
	return sc.Err()
}

func bytesTrimSpace(b []byte) []byte {
	i, j := 0, len(b)
	for i < j && (b[i] == ' ' || b[i] == '\t' || b[i] == '\n' || b[i] == '\r') {
		i++
	}
	for j > i && (b[j-1] == ' ' || b[j-1] == '\t' || b[j-1] == '\n' || b[j-1] == '\r') {
		j--
	}
	return b[i:j]
}

func (w *WAL) Append(key string, v types.Value) (types.HistoryEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.seq++
	e := types.HistoryEntry{Seq: w.seq, Key: key, Value: v.Clone()}
	w.log = append(w.log, e)
	if w.f == nil {
		return e, nil
	}
	b, err := json.Marshal(e)
	if err != nil {
		return e, err
	}
	if _, err := w.f.Write(append(b, '\n')); err != nil {
		return e, err
	}
	return e, w.f.Sync()
}

func (w *WAL) History(key string) []types.HistoryEntry {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []types.HistoryEntry
	for _, e := range w.log {
		if e.Key == key {
			out = append(out, e)
		}
	}
	return out
}

func (w *WAL) All() []types.HistoryEntry {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]types.HistoryEntry, len(w.log))
	copy(out, w.log)
	return out
}

func (w *WAL) Replay() []types.HistoryEntry {
	return w.All()
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f != nil {
		return w.f.Close()
	}
	return nil
}

func (w *WAL) Path() string { return w.path }
