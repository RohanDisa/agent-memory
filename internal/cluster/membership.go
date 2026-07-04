package cluster

import (
	"fmt"
	"sort"
	"strings"

	"github.com/rdisa/agent-memory-store/internal/types"
)

// Peer is a statically configured cluster member. The default build has no
// gossip membership; the node list is the config. That is intentional: this
// project isolates consistency, not membership churn.
type Peer struct {
	ID   types.NodeID
	Addr string // host:port for HTTP inter-node / client
}

type Membership struct {
	Self  types.NodeID
	Peers []Peer
}

func ParsePeers(self types.NodeID, spec string) (Membership, error) {
	m := Membership{Self: self}
	if strings.TrimSpace(spec) == "" {
		m.Peers = []Peer{{ID: self}}
		return m, nil
	}
	seen := map[types.NodeID]bool{}
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, addr, ok := strings.Cut(part, "=")
		p := Peer{ID: types.NodeID(strings.TrimSpace(id))}
		if ok {
			p.Addr = strings.TrimSpace(addr)
		} else {
			p.ID = types.NodeID(part)
		}
		if p.ID == "" {
			return m, fmt.Errorf("empty peer id in %q", spec)
		}
		if seen[p.ID] {
			continue
		}
		seen[p.ID] = true
		m.Peers = append(m.Peers, p)
	}
	if !seen[self] {
		m.Peers = append(m.Peers, Peer{ID: self})
	}
	sort.Slice(m.Peers, func(i, j int) bool { return m.Peers[i].ID < m.Peers[j].ID })
	return m, nil
}

func (m Membership) N() int { return len(m.Peers) }

func (m Membership) IDs() []types.NodeID {
	ids := make([]types.NodeID, len(m.Peers))
	for i, p := range m.Peers {
		ids[i] = p.ID
	}
	return ids
}

func (m Membership) Addr(id types.NodeID) string {
	for _, p := range m.Peers {
		if p.ID == id {
			return p.Addr
		}
	}
	return ""
}

func (m Membership) Others() []Peer {
	var out []Peer
	for _, p := range m.Peers {
		if p.ID != m.Self {
			out = append(out, p)
		}
	}
	return out
}

// QuorumOK reports whether got acknowledgements satisfy required.
func QuorumOK(got, required int) bool {
	return required > 0 && got >= required
}

// WriteFails reports whether a write with W cannot succeed given unreachable
// nodes: reachable = N - unreachable, fail when reachable < W.
func WriteFails(n, w, unreachable int) bool {
	return n-unreachable < w
}
