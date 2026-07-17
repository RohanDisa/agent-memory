package repair

import (
	"context"

	"github.com/rdisa/agent-memory-store/internal/conflict"
	"github.com/rdisa/agent-memory-store/internal/types"
)

// StaleAmong returns the node IDs whose found value is not the resolved winner.
func StaleAmong(views []types.ReplicaView, winner types.Value) []types.NodeID {
	var stale []types.NodeID
	for _, v := range views {
		if !v.Found {
			stale = append(stale, v.NodeID)
			continue
		}
		if !v.Value.Equal(winner) {
			stale = append(stale, v.NodeID)
		}
	}
	return stale
}

// PushWinner sends the resolved value to dest. Used by read repair.
type Pusher interface {
	ReplicateWrite(ctx context.Context, to types.NodeID, key string, val types.Value) error
}

func PushWinner(ctx context.Context, t Pusher, dest types.NodeID, key string, winner types.Value) error {
	return t.ReplicateWrite(ctx, dest, key, winner)
}

func NeedsRepair(views []types.ReplicaView, winner types.Value) bool {
	return !conflict.Agreed(views, winner)
}
