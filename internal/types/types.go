package types

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Policy selects how concurrent writes to a key are merged.
// It is stored with the value (per-key namespace policy).
type Policy int

const (
	PolicyLWW Policy = iota
	PolicyCRDTLWWReg
	PolicyCRDTORSet
)

func (p Policy) String() string {
	switch p {
	case PolicyLWW:
		return "LWW"
	case PolicyCRDTLWWReg:
		return "CRDT_LWWREG"
	case PolicyCRDTORSet:
		return "CRDT_ORSET"
	default:
		return fmt.Sprintf("Policy(%d)", int(p))
	}
}

func ParsePolicy(s string) (Policy, error) {
	switch s {
	case "", "LWW", "lww":
		return PolicyLWW, nil
	case "CRDT_LWWREG", "crdt_lwwreg", "lwwreg":
		return PolicyCRDTLWWReg, nil
	case "CRDT_ORSET", "crdt_orset", "orset":
		return PolicyCRDTORSet, nil
	default:
		return 0, fmt.Errorf("unknown namespace policy %q", s)
	}
}

// Version is a Lamport timestamp plus the writing node id.
// Comparison is a total order: lamport first, then node_id.
// Wall-clock time is never used for ordering (client_ts is advisory only).
type Version struct {
	Lamport uint64 `json:"lamport"`
	NodeID  string `json:"node_id"`
}

func (v Version) String() string {
	return fmt.Sprintf("%d@%s", v.Lamport, v.NodeID)
}

func (v Version) Equal(o Version) bool {
	return v.Lamport == o.Lamport && v.NodeID == o.NodeID
}

func (v Version) IsZero() bool {
	return v.Lamport == 0 && v.NodeID == ""
}

// Compare returns -1 if v < o, 0 if v == o, 1 if v > o.
// Total order: higher lamport wins; equal lamport breaks ties by node_id.
func Compare(a, b Version) int {
	if a.Lamport != b.Lamport {
		if a.Lamport < b.Lamport {
			return -1
		}
		return 1
	}
	if a.NodeID < b.NodeID {
		return -1
	}
	if a.NodeID > b.NodeID {
		return 1
	}
	return 0
}

// Value is one versioned replica of a key.
type Value struct {
	Data      []byte  `json:"data"`
	Version   Version `json:"version"`
	Policy    Policy  `json:"policy"`
	Tombstone bool    `json:"tombstone"`
	// CRDT is the serialized CRDT payload for CRDT_* policies.
	// LWW keys leave this empty and use Data + Version.
	CRDT []byte `json:"crdt,omitempty"`
}

func (v Value) Equal(o Value) bool {
	return v.Version.Equal(o.Version) &&
		v.Policy == o.Policy &&
		v.Tombstone == o.Tombstone &&
		bytes.Equal(v.Data, o.Data) &&
		bytes.Equal(v.CRDT, o.CRDT)
}

func (v Value) Clone() Value {
	out := v
	if v.Data != nil {
		out.Data = append([]byte(nil), v.Data...)
	}
	if v.CRDT != nil {
		out.CRDT = append([]byte(nil), v.CRDT...)
	}
	return out
}

// ReplicaView is one replica's response to a coordinated read.
type ReplicaView struct {
	NodeID NodeID
	Value  Value
	Found  bool
}

type NodeID string

// WriteReq is the coordinator-facing write.
type WriteReq struct {
	Key      string
	Data     []byte
	Policy   Policy
	W        int
	ClientTS int64 // advisory only
	// OR-Set operation, applied by the coordinator then replicated as state.
	ORSetOp   string // "add" | "remove"
	ORSetElem string
	Tombstone bool
}

// WriteResp is returned to the client after a quorum write attempt.
type WriteResp struct {
	OK           bool    `json:"ok"`
	Version      Version `json:"version"`
	AcksReceived int     `json:"acks_received"`
	Error        string  `json:"error,omitempty"`
}

// ReadReq is the coordinator-facing read.
type ReadReq struct {
	Key string
	R   int
}

// ReadResp is returned to the client after a quorum read.
type ReadResp struct {
	Value             Value  `json:"value"`
	Found             bool   `json:"found"`
	ReplicasResponded int    `json:"replicas_responded"`
	ReplicasAgreed    bool   `json:"replicas_agreed"`
	Repaired          bool   `json:"repaired"`
	Error             string `json:"error,omitempty"`
}

// HistoryEntry is one WAL record for a key.
type HistoryEntry struct {
	Seq   uint64 `json:"seq"`
	Key   string `json:"key"`
	Value Value  `json:"value"`
}

func MustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
