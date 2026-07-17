package node

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rdisa/agent-memory-store/internal/cluster"
	"github.com/rdisa/agent-memory-store/internal/conflict"
	netx "github.com/rdisa/agent-memory-store/internal/net"
	"github.com/rdisa/agent-memory-store/internal/repair"
	"github.com/rdisa/agent-memory-store/internal/types"
)

// Node is one replica plus the coordinator role. Every node is a peer;
// whichever process receives the client request coordinates that request.
type Node struct {
	ID      types.NodeID
	Members cluster.Membership
	Replica *Replica
	Net     netx.Transport
	Hints   *repair.Hints
	Timeout time.Duration

	mu          sync.Mutex
	writeMu     sync.Mutex
	antiOn      bool
	antiEvery   time.Duration
	stopAnti    chan struct{}
	tagCounter  uint64
	ReadRepair  bool // default true; tests can disable so anti-entropy is the only repair path
}

type Config struct {
	ID                types.NodeID
	Peers             cluster.Membership
	Transport         netx.Transport
	WAL               *Replica
	Timeout           time.Duration
	AntiEntropyEvery  time.Duration
	DisableReadRepair bool
}

func New(cfg Config) *Node {
	to := cfg.Timeout
	if to == 0 {
		to = 2 * time.Second
	}
	n := &Node{
		ID:         cfg.ID,
		Members:    cfg.Peers,
		Replica:    cfg.WAL,
		Net:        cfg.Transport,
		Hints:      repair.NewHints(),
		Timeout:    to,
		ReadRepair: !cfg.DisableReadRepair,
		antiEvery:  cfg.AntiEntropyEvery,
	}
	if n.Replica == nil {
		n.Replica = NewReplica(cfg.ID, nil)
	}
	return n
}

func (n *Node) N() int { return n.Members.N() }

func (n *Node) validateQuorum(q int, what string) error {
	if q < 1 || q > n.N() {
		return fmt.Errorf("%s=%d out of range for N=%d", what, q, n.N())
	}
	return nil
}

func (n *Node) nextORSetTag() string {
	n.mu.Lock()
	n.tagCounter++
	c := n.tagCounter
	n.mu.Unlock()
	return fmt.Sprintf("%s:%d", n.ID, c)
}

// Write is the coordinator write path: assign a Lamport version, apply locally,
// fan out to every replica, return success at W acks. Unreachable peers get a
// hint. Replication is at-least-once; Apply is idempotent by version.
func (n *Node) Write(ctx context.Context, req types.WriteReq) types.WriteResp {
	if err := n.validateQuorum(req.W, "W"); err != nil {
		return types.WriteResp{Error: err.Error()}
	}
	n.writeMu.Lock()
	lamport := n.Replica.Clock.Tick()
	ver := types.Version{Lamport: lamport, NodeID: string(n.ID)}

	payload, err := n.localPrepare(req, ver)
	n.writeMu.Unlock()
	if err != nil {
		return types.WriteResp{Error: err.Error()}
	}

	ctx, cancel := context.WithTimeout(ctx, n.Timeout)
	defer cancel()

	type result struct {
		id  types.NodeID
		err error
	}
	ch := make(chan result, n.N())

	for _, peer := range n.Members.Peers {
		peer := peer
		go func() {
			if peer.ID == n.ID {
				_, err := n.Replica.Apply(req.Key, payload)
				ch <- result{id: n.ID, err: err}
				return
			}
			err := n.Net.ReplicateWrite(ctx, peer.ID, req.Key, payload)
			if err != nil {
				n.Hints.Store(peer.ID, req.Key, payload)
			}
			ch <- result{id: peer.ID, err: err}
		}()
	}

	acks := 0
	remaining := n.N()
	for remaining > 0 {
		select {
		case <-ctx.Done():
			return types.WriteResp{OK: false, Version: ver, AcksReceived: acks, Error: "quorum timeout"}
		case r := <-ch:
			remaining--
			if r.err == nil {
				acks++
				if acks >= req.W {
					return types.WriteResp{OK: true, Version: ver, AcksReceived: acks}
				}
			}
		}
	}
	if acks >= req.W {
		return types.WriteResp{OK: true, Version: ver, AcksReceived: acks}
	}
	return types.WriteResp{OK: false, Version: ver, AcksReceived: acks, Error: fmt.Sprintf("need W=%d acks, got %d", req.W, acks)}
}

func (n *Node) localPrepare(req types.WriteReq, ver types.Version) (types.Value, error) {
	cur, found := n.Replica.Get(req.Key)
	switch req.Policy {
	case types.PolicyCRDTORSet:
		tag := n.nextORSetTag()
		return conflict.ApplyORSetOp(cur, found, req.ORSetOp, req.ORSetElem, tag, ver), nil
	case types.PolicyCRDTLWWReg:
		return conflict.ApplyLWWRegWrite(cur, found, req.Data, ver, req.Tombstone), nil
	default:
		return types.Value{
			Data:      append([]byte(nil), req.Data...),
			Version:   ver,
			Policy:    types.PolicyLWW,
			Tombstone: req.Tombstone,
		}, nil
	}
}

// Read waits for R replica responses, resolves them under the key policy, and
// optionally pushes the winner to stale respondents (read repair).
func (n *Node) Read(ctx context.Context, req types.ReadReq) types.ReadResp {
	if err := n.validateQuorum(req.R, "R"); err != nil {
		return types.ReadResp{Error: err.Error()}
	}
	ctx, cancel := context.WithTimeout(ctx, n.Timeout)
	defer cancel()

	type one struct {
		view types.ReplicaView
		err  error
	}
	ch := make(chan one, n.N())
	for _, peer := range n.Members.Peers {
		peer := peer
		go func() {
			if peer.ID == n.ID {
				if !n.Replica.ReadsEnabled() {
					ch <- one{err: errors.New("reads disabled")}
					return
				}
				v, ok := n.Replica.Get(req.Key)
				ch <- one{view: types.ReplicaView{NodeID: n.ID, Value: v, Found: ok}}
				return
			}
			v, ok, err := n.Net.ReplicateRead(ctx, peer.ID, req.Key)
			if err != nil {
				ch <- one{err: err}
				return
			}
			ch <- one{view: types.ReplicaView{NodeID: peer.ID, Value: v, Found: ok}}
		}()
	}

	var views []types.ReplicaView
	remaining := n.N()
	for remaining > 0 && len(views) < req.R {
		select {
		case <-ctx.Done():
			if len(views) < req.R {
				return types.ReadResp{ReplicasResponded: len(views), Error: fmt.Sprintf("need R=%d responses, got %d", req.R, len(views))}
			}
		case r := <-ch:
			remaining--
			if r.err == nil {
				views = append(views, r.view)
			}
		}
	}
	if len(views) < req.R {
		// drain is unnecessary; return failure
		return types.ReadResp{ReplicasResponded: len(views), Error: fmt.Sprintf("need R=%d responses, got %d", req.R, len(views))}
	}

	winner, found := conflict.ResolveMany(views)
	agreed := found && conflict.Agreed(views, winner)
	repaired := false
	if n.ReadRepair && found && !agreed {
		for _, dest := range repair.StaleAmong(views, winner) {
			if dest == n.ID {
				_, _ = n.Replica.Apply(req.Key, winner)
				repaired = true
				continue
			}
			if err := n.Net.ReplicateWrite(ctx, dest, req.Key, winner); err == nil {
				repaired = true
			}
		}
	}
	if found && winner.Tombstone {
		return types.ReadResp{
			Value:             winner,
			Found:             false,
			ReplicasResponded: len(views),
			ReplicasAgreed:    agreed,
			Repaired:          repaired,
		}
	}
	return types.ReadResp{
		Value:             winner,
		Found:             found,
		ReplicasResponded: len(views),
		ReplicasAgreed:    agreed,
		Repaired:          repaired,
	}
}

func (n *Node) Delete(ctx context.Context, key string, w int) types.WriteResp {
	return n.Write(ctx, types.WriteReq{Key: key, W: w, Tombstone: true, Policy: types.PolicyLWW})
}

// ApplyReplicate implements netx.ReplicaHandler.
func (n *Node) ApplyReplicate(from types.NodeID, key string, val types.Value) error {
	_, err := n.Replica.Apply(key, val)
	return err
}

func (n *Node) ServeRead(from types.NodeID, key string) (types.Value, bool, error) {
	if !n.Replica.ReadsEnabled() {
		return types.Value{}, false, errors.New("reads disabled")
	}
	v, ok := n.Replica.Get(key)
	return v, ok, nil
}

func (n *Node) ServeDigest(from types.NodeID) (map[string]types.Value, error) {
	return n.Replica.Snapshot(), nil
}

// AntiEntropyRound runs one pairwise version-scan against every peer.
func (n *Node) AntiEntropyRound(ctx context.Context) (pushed, pulled int) {
	for _, peer := range n.Members.Others() {
		remote, err := n.Net.GetDigest(ctx, peer.ID)
		if err != nil {
			continue
		}
		plan := repair.Scan(n.Replica.Snapshot(), remote)
		for k, v := range plan.Pull {
			_, _ = n.Replica.Apply(k, v)
			pulled++
		}
		for k, v := range plan.Push {
			if err := n.Net.ReplicateWrite(ctx, peer.ID, k, v); err == nil {
				pushed++
			}
		}
	}
	return pushed, pulled
}

// ReplayHints tries to deliver every stored hint. Successful targets are drained.
func (n *Node) ReplayHints(ctx context.Context) int {
	delivered := 0
	for _, target := range n.Hints.Targets() {
		hints := n.Hints.Peek(target)
		ok := true
		for _, h := range hints {
			if err := n.Net.ReplayHint(ctx, target, h.Key, h.Value); err != nil {
				ok = false
				break
			}
			delivered++
		}
		if ok && len(hints) > 0 {
			_ = n.Hints.Take(target)
		}
	}
	return delivered
}

func (n *Node) StartAntiEntropy() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.antiOn || n.antiEvery <= 0 {
		return
	}
	n.antiOn = true
	n.stopAnti = make(chan struct{})
	every := n.antiEvery
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-n.stopAnti:
				return
			case <-t.C:
				ctx, cancel := context.WithTimeout(context.Background(), n.Timeout)
				n.AntiEntropyRound(ctx)
				n.ReplayHints(ctx)
				cancel()
			}
		}
	}()
}

func (n *Node) StopAntiEntropy() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.antiOn {
		return
	}
	close(n.stopAnti)
	n.antiOn = false
}

func (n *Node) Close() error {
	n.StopAntiEntropy()
	if n.Replica != nil && n.Replica.WAL != nil {
		return n.Replica.WAL.Close()
	}
	return nil
}
