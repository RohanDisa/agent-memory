package netx

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/rdisa/agent-memory-store/internal/types"
)

var ErrUnreachable = errors.New("faultnet: unreachable")

type NodeID = types.NodeID

// ReplicaHandler is the inbound side of a node. The in-process cluster
// delivers messages by calling these methods after the fault injector allows
// the send. Production HTTP transport calls the same methods on the receiver.
type ReplicaHandler interface {
	ApplyReplicate(from NodeID, key string, val types.Value) error
	ServeRead(from NodeID, key string) (types.Value, bool, error)
	ServeDigest(from NodeID) (map[string]types.Value, error)
}

// SendHook is invoked on every remote send attempt (after fault checks, or
// before — see Network). Used to isolate a coordinator mid-write.
type SendHook func(from, to NodeID, seq int)

type link struct{ from, to NodeID }

// Network is the fault injector. It is deterministic under Seed. Partitions
// are symmetric: A can reach B iff they share a partition group and neither
// is isolated. Drop/Delay apply to both directions when set via Drop/Delay.
//
// Every "survives X" claim in the README is induced here and asserted by a
// test in test/chaos.
type Network struct {
	mu       sync.Mutex
	rng      *rand.Rand
	seed     int64
	group    map[NodeID]int
	isolated map[NodeID]bool
	drop     map[link]float64
	delay    map[link]time.Duration
	handlers  map[NodeID]ReplicaHandler
	seq       int
	hook      SendHook
	failAfter map[NodeID]int
	failCount map[NodeID]int
}

func NewFaultNet(seed int64) *Network {
	if seed == 0 {
		seed = 1
	}
	return &Network{
		rng:      rand.New(rand.NewSource(seed)),
		seed:     seed,
		group:    map[NodeID]int{},
		isolated: map[NodeID]bool{},
		drop:     map[link]float64{},
		delay:    map[link]time.Duration{},
		handlers:  map[NodeID]ReplicaHandler{},
		failAfter: map[NodeID]int{},
		failCount: map[NodeID]int{},
	}
}

func (n *Network) Seed() int64 { return n.seed }

func (n *Network) Register(id NodeID, h ReplicaHandler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
}

func (n *Network) Unregister(id NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.handlers, id)
}

func (n *Network) Handler(id NodeID) ReplicaHandler {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.handlers[id]
}

func (n *Network) SetHook(h SendHook) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.hook = h
}

// FailFromAfter drops every remote send from id after `after` successful
// allow-checks. after=1 means the first outbound message is delivered and
// the rest fail. Used to isolate a coordinator mid-write without a racey hook.
func (n *Network) FailFromAfter(id NodeID, after int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.failAfter[id] = after
	n.failCount[id] = 0
}

// Partition splits the cluster into two groups. Links across groups fail
// in both directions. Nodes not named stay in group 0 (reachable to neither
// side unless they were already grouped).
func (n *Network) Partition(left, right []NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, id := range left {
		n.group[id] = 1
	}
	for _, id := range right {
		n.group[id] = 2
	}
}

func (n *Network) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group = map[NodeID]int{}
}

func (n *Network) Isolate(id NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.isolated[id] = true
}

func (n *Network) Recover(id NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.isolated, id)
}

func (n *Network) Drop(from, to NodeID, p float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.drop[link{from, to}] = p
	n.drop[link{to, from}] = p
}

func (n *Network) Delay(from, to NodeID, d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.delay[link{from, to}] = d
	n.delay[link{to, from}] = d
}

func (n *Network) ResetFaults() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group = map[NodeID]int{}
	n.isolated = map[NodeID]bool{}
	n.drop = map[link]float64{}
	n.delay = map[link]time.Duration{}
	n.hook = nil
	n.failAfter = map[NodeID]int{}
	n.failCount = map[NodeID]int{}
}

// Allow reports whether a message from→to is delivered, and any injected delay.
// Local (from==to) is always allowed; those calls should not go through the net.
func (n *Network) Allow(from, to NodeID) (bool, time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.allowLocked(from, to)
}

func (n *Network) allowLocked(from, to NodeID) (bool, time.Duration) {
	if from == to {
		return true, 0
	}
	if n.isolated[from] || n.isolated[to] {
		return false, 0
	}
	if n.group[from] != n.group[to] {
		return false, 0
	}
	if p := n.drop[link{from, to}]; p > 0 && n.rng.Float64() < p {
		return false, 0
	}
	if after, ok := n.failAfter[from]; ok {
		n.failCount[from]++
		if n.failCount[from] > after {
			return false, 0
		}
	}
	return true, n.delay[link{from, to}]
}

func (n *Network) dispatch(from, to NodeID) (ReplicaHandler, time.Duration, error) {
	n.mu.Lock()
	n.seq++
	seq := n.seq
	hook := n.hook
	ok, delay := n.allowLocked(from, to)
	h := n.handlers[to]
	n.mu.Unlock()

	if hook != nil {
		hook(from, to, seq)
		// Re-check after hook so Isolate-mid-write can take effect on
		// subsequent sends; this send already passed allow.
	}
	if !ok {
		return nil, 0, ErrUnreachable
	}
	if h == nil {
		return nil, 0, ErrUnreachable
	}
	return h, delay, nil
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Endpoint is a Transport bound to one node on this Network.
type Endpoint struct {
	net *Network
	id  NodeID
}

func (n *Network) Endpoint(id NodeID) *Endpoint {
	return &Endpoint{net: n, id: id}
}

func (e *Endpoint) LocalID() NodeID { return e.id }

func (e *Endpoint) ReplicateWrite(ctx context.Context, to NodeID, key string, val types.Value) error {
	h, delay, err := e.net.dispatch(e.id, to)
	if err != nil {
		return err
	}
	if err := sleep(ctx, delay); err != nil {
		return err
	}
	return h.ApplyReplicate(e.id, key, val)
}

func (e *Endpoint) ReplicateRead(ctx context.Context, to NodeID, key string) (types.Value, bool, error) {
	h, delay, err := e.net.dispatch(e.id, to)
	if err != nil {
		return types.Value{}, false, err
	}
	if err := sleep(ctx, delay); err != nil {
		return types.Value{}, false, err
	}
	return h.ServeRead(e.id, key)
}

func (e *Endpoint) GetDigest(ctx context.Context, to NodeID) (map[string]types.Value, error) {
	h, delay, err := e.net.dispatch(e.id, to)
	if err != nil {
		return nil, err
	}
	if err := sleep(ctx, delay); err != nil {
		return nil, err
	}
	return h.ServeDigest(e.id)
}

func (e *Endpoint) ReplayHint(ctx context.Context, to NodeID, key string, val types.Value) error {
	return e.ReplicateWrite(ctx, to, key, val)
}
