package types

import "sync"

// Lamport is a process-local Lamport clock.
// Tick on every local event (client write). Witness on every received version.
type Lamport struct {
	mu sync.Mutex
	t  uint64
}

func (c *Lamport) Time() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

// Tick increments the clock and returns the new time. Used on local writes.
func (c *Lamport) Tick() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t++
	return c.t
}

// Witness updates the clock for a received timestamp, then ticks,
// so the next local event is strictly later than both.
func (c *Lamport) Witness(remote uint64) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if remote > c.t {
		c.t = remote
	}
	c.t++
	return c.t
}

// Observe raises the clock to at least remote without ticking.
// Used when applying a replica write that already carries a version.
func (c *Lamport) Observe(remote uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if remote > c.t {
		c.t = remote
	}
}
