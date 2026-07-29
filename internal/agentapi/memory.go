package agentapi

import (
	"context"
	"encoding/json"

	"github.com/rdisa/agent-memory-store/internal/conflict"
	"github.com/rdisa/agent-memory-store/internal/node"
	"github.com/rdisa/agent-memory-store/internal/store"
	"github.com/rdisa/agent-memory-store/internal/types"
)

// Memory is a thin, legible layer over the versioned KV so tests and the demo
// read as agent facts rather than a generic store. The distributed system is
// the project; this package does not add consistency of its own.
type Memory struct {
	Node *node.Node
}

func New(n *node.Node) *Memory { return &Memory{Node: n} }

func (m *Memory) PutFact(ctx context.Context, entity, attribute, value string, policy types.Policy, w int) types.WriteResp {
	return m.Node.Write(ctx, types.WriteReq{
		Key:    store.FactKey(entity, attribute),
		Data:   []byte(value),
		Policy: policy,
		W:      w,
	})
}

func (m *Memory) GetFact(ctx context.Context, entity, attribute string, r int) types.ReadResp {
	return m.Node.Read(ctx, types.ReadReq{Key: store.FactKey(entity, attribute), R: r})
}

func (m *Memory) GetFacts(ctx context.Context, entity string, r int) map[string]types.Value {
	// Prefix scan is local (this node). A serious production system would
	// coordinate a range read; we do not claim that. Demo / inspect only.
	return m.Node.Replica.KV.Prefix(entity + ":")
}

func (m *Memory) AddTag(ctx context.Context, entity, tag string, w int) types.WriteResp {
	return m.Node.Write(ctx, types.WriteReq{
		Key:       store.FactKey(entity, "tags"),
		Policy:    types.PolicyCRDTORSet,
		W:         w,
		ORSetOp:   "add",
		ORSetElem: tag,
	})
}

func (m *Memory) RemoveTag(ctx context.Context, entity, tag string, w int) types.WriteResp {
	return m.Node.Write(ctx, types.WriteReq{
		Key:       store.FactKey(entity, "tags"),
		Policy:    types.PolicyCRDTORSet,
		W:         w,
		ORSetOp:   "remove",
		ORSetElem: tag,
	})
}

func (m *Memory) GetTags(ctx context.Context, entity string, r int) ([]string, types.ReadResp) {
	resp := m.GetFact(ctx, entity, "tags", r)
	if !resp.Found {
		return nil, resp
	}
	return conflict.ORSetElements(resp.Value), resp
}

func (m *Memory) GetHistory(entity, attribute string) []types.HistoryEntry {
	return m.Node.Replica.History(store.FactKey(entity, attribute))
}

func DecodeString(v types.Value) string {
	if v.Policy == types.PolicyCRDTORSet {
		b, _ := json.Marshal(conflict.ORSetElements(v))
		return string(b)
	}
	return string(v.Data)
}
