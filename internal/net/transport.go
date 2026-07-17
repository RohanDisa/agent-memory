package netx

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/rdisa/agent-memory-store/internal/types"
)

// Transport is the inter-node RPC surface. FaultNet endpoints and the HTTP
// client both implement it so the coordinator does not know which it is using.
type Transport interface {
	LocalID() NodeID
	ReplicateWrite(ctx context.Context, to NodeID, key string, val types.Value) error
	ReplicateRead(ctx context.Context, to NodeID, key string) (types.Value, bool, error)
	GetDigest(ctx context.Context, to NodeID) (map[string]types.Value, error)
	ReplayHint(ctx context.Context, to NodeID, key string, val types.Value) error
}

type replicateBody struct {
	Key   string      `json:"key"`
	Value types.Value `json:"value"`
	From  NodeID      `json:"from"`
}

type readBody struct {
	Key  string `json:"key"`
	From NodeID `json:"from"`
}

type readReply struct {
	Value types.Value `json:"value"`
	Found bool        `json:"found"`
}

type digestReply struct {
	Values map[string]types.Value `json:"values"`
}

// HTTPTransport talks to peers' /internal/* endpoints. Addr of a peer is
// host:port (no scheme). Used by Docker / memctl-era processes.
type HTTPTransport struct {
	ID     NodeID
	Addrs  map[NodeID]string
	Client *http.Client
	// Deny is a local, process-level fault toggle for Docker e2e partitions.
	Deny map[NodeID]bool
}

func NewHTTPTransport(id NodeID, addrs map[NodeID]string) *HTTPTransport {
	return &HTTPTransport{
		ID:     id,
		Addrs:  addrs,
		Client: &http.Client{Timeout: 2 * time.Second},
		Deny:   map[NodeID]bool{},
	}
}

func (t *HTTPTransport) LocalID() NodeID { return t.ID }

func (t *HTTPTransport) setDeny(ids []NodeID, deny bool) {
	if t.Deny == nil {
		t.Deny = map[NodeID]bool{}
	}
	for _, id := range ids {
		if deny {
			t.Deny[id] = true
		} else {
			delete(t.Deny, id)
		}
	}
}

func (t *HTTPTransport) Block(ids ...NodeID) { t.setDeny(ids, true) }
func (t *HTTPTransport) Unblock(ids ...NodeID) {
	if len(ids) == 0 {
		t.Deny = map[NodeID]bool{}
		return
	}
	t.setDeny(ids, false)
}

func (t *HTTPTransport) blocked(to NodeID) bool {
	return t.Deny[to]
}

func (t *HTTPTransport) url(to NodeID, path string) (string, error) {
	if t.blocked(to) {
		return "", ErrUnreachable
	}
	addr := t.Addrs[to]
	if addr == "" {
		return "", fmt.Errorf("%w: no addr for %s", ErrUnreachable, to)
	}
	if len(addr) >= 4 && addr[:4] == "http" {
		return addr + path, nil
	}
	return "http://" + addr + path, nil
}

func (t *HTTPTransport) ReplicateWrite(ctx context.Context, to NodeID, key string, val types.Value) error {
	u, err := t.url(to, "/internal/replicate")
	if err != nil {
		return err
	}
	b, _ := json.Marshal(replicateBody{Key: key, Value: val, From: t.ID})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.Client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUnreachable, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		slurp, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%w: status %d %s", ErrUnreachable, resp.StatusCode, slurp)
	}
	return nil
}

func (t *HTTPTransport) ReplicateRead(ctx context.Context, to NodeID, key string) (types.Value, bool, error) {
	u, err := t.url(to, "/internal/read")
	if err != nil {
		return types.Value{}, false, err
	}
	b, _ := json.Marshal(readBody{Key: key, From: t.ID})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(b))
	if err != nil {
		return types.Value{}, false, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.Client.Do(req)
	if err != nil {
		return types.Value{}, false, fmt.Errorf("%w: %v", ErrUnreachable, err)
	}
	defer resp.Body.Close()
	var out readReply
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return types.Value{}, false, err
	}
	return out.Value, out.Found, nil
}

func (t *HTTPTransport) GetDigest(ctx context.Context, to NodeID) (map[string]types.Value, error) {
	u, err := t.url(to, "/internal/digest")
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := t.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnreachable, err)
	}
	defer resp.Body.Close()
	var out digestReply
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	if out.Values == nil {
		out.Values = map[string]types.Value{}
	}
	return out.Values, nil
}

func (t *HTTPTransport) ReplayHint(ctx context.Context, to NodeID, key string, val types.Value) error {
	u, err := t.url(to, "/internal/hint")
	if err != nil {
		return err
	}
	b, _ := json.Marshal(replicateBody{Key: key, Value: val, From: t.ID})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.Client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUnreachable, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%w: hint status %d", ErrUnreachable, resp.StatusCode)
	}
	return nil
}
