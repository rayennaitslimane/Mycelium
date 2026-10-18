// Package transitions holds the only map rules: pure (map, event) -> map'.
// Every successful transition bumps the epoch.
package transitions

import (
	"errors"
	"fmt"
	"slices"

	"mycelium/core/mapmodel"
)

type Kind string

const (
	KindJoin         Kind = "join"
	KindOut          Kind = "out"
	KindBoot         Kind = "boot"
	KindBackfillDone Kind = "backfill_done"
)

// Event is flat so it can be JSON-encoded as a Raft command.
type Event struct {
	Kind  Kind            `json:"kind"`
	Node  mapmodel.NodeID `json:"node"`
	Addr  string          `json:"addr,omitempty"`  // join
	PG    mapmodel.PGID   `json:"pg,omitempty"`    // backfill_done
	Slot  int             `json:"slot,omitempty"`  // backfill_done
	Epoch mapmodel.Epoch  `json:"epoch,omitempty"` // backfill_done
}

var (
	ErrAlreadyUp   = errors.New("node already up")
	ErrUnknownNode = errors.New("unknown node")
	ErrNotUp       = errors.New("node not up")
	ErrStale       = errors.New("stale backfill_done")
)

// Initial returns the empty map at epoch 0.
func Initial(p mapmodel.Params) *mapmodel.Map {
	return &mapmodel.Map{Params: p, Nodes: map[mapmodel.NodeID]mapmodel.Node{}}
}

// Apply returns a new map with e applied, out or vacant slots reassigned, and the
// epoch bumped. m is not modified. It is deterministic, so Raft replicas agree.
func Apply(m *mapmodel.Map, e Event) (*mapmodel.Map, error) {
	n := m.Clone()
	if n.Nodes == nil {
		n.Nodes = map[mapmodel.NodeID]mapmodel.Node{}
	}
	var err error
	switch e.Kind {
	case KindJoin:
		err = join(n, e.Node, e.Addr)
	case KindOut:
		err = out(n, e.Node)
	case KindBoot:
		err = newBoot(n, e.Node)
	case KindBackfillDone:
		err = backfillDone(n, e.PG, e.Slot, e.Epoch, e.Node)
	default:
		err = fmt.Errorf("unknown event kind %q", e.Kind)
	}
	if err != nil {
		return nil, err
	}
	reassign(n)
	n.Epoch++
	return n, nil
}

// join marks n up (new, or out after purging) and creates the PG table once
// bootstrap_nodes are up.
func join(m *mapmodel.Map, n mapmodel.NodeID, addr string) error {
	if m.Nodes[n].State == mapmodel.Up {
		return ErrAlreadyUp
	}
	m.Nodes[n] = mapmodel.Node{Addr: addr, State: mapmodel.Up}
	if m.PGs == nil && len(upNodes(m)) >= m.Params.BootstrapNodes {
		createPGs(m)
	}
	return nil
}

// out marks n out and flags all its slots backfilling.
func out(m *mapmodel.Map, n mapmodel.NodeID) error {
	if err := mustBeUp(m, n); err != nil {
		return err
	}
	m.Nodes[n] = mapmodel.Node{Addr: m.Nodes[n].Addr, State: mapmodel.Out}
	for _, slots := range m.PGs {
		for i := range slots {
			if slots[i].Node == n {
				slots[i].Backfilling = true
			}
		}
	}
	return nil
}

// newBoot records a new boot_id of an up node; only the epoch bump matters.
func newBoot(m *mapmodel.Map, n mapmodel.NodeID) error {
	return mustBeUp(m, n)
}

// backfillDone clears the slot's flag if epoch is current and the slot's node is n.
func backfillDone(m *mapmodel.Map, pg mapmodel.PGID, slot int, epoch mapmodel.Epoch, n mapmodel.NodeID) error {
	if epoch != m.Epoch || int(pg) >= len(m.PGs) || slot < 0 || slot >= len(m.PGs[pg]) {
		return ErrStale
	}
	s := &m.PGs[pg][slot]
	if s.Node != n || !s.Backfilling {
		return ErrStale
	}
	s.Backfilling = false
	return nil
}

// createPGs builds the PG table once: slot i of pg p = sorted up node ids[(p+i) mod N].
func createPGs(m *mapmodel.Map) {
	ids := upNodes(m)
	m.PGs = make([][]mapmodel.Slot, m.Params.PGNum)
	for p := range m.PGs {
		m.PGs[p] = make([]mapmodel.Slot, m.Params.N())
		for i := range m.PGs[p] {
			m.PGs[p][i] = mapmodel.Slot{Node: ids[(p+i)%len(ids)]}
		}
	}
}

// reassign gives each slot whose node is out (or vacant) to the up node not
// already in that PG with the fewest slots (ties: lowest id), still backfilling.
// With no candidate the slot is left vacant.
func reassign(m *mapmodel.Map) {
	if m.PGs == nil {
		return
	}
	ids := upNodes(m)
	count := map[mapmodel.NodeID]int{}
	for _, slots := range m.PGs {
		for _, s := range slots {
			count[s.Node]++
		}
	}
	for _, slots := range m.PGs {
		for i, s := range slots {
			if s.Node != "" && m.Nodes[s.Node].State == mapmodel.Up {
				continue
			}
			var best mapmodel.NodeID
			for _, id := range ids {
				if inPG(slots, id) {
					continue
				}
				if best == "" || count[id] < count[best] {
					best = id
				}
			}
			slots[i] = mapmodel.Slot{Node: best, Backfilling: true}
			if best != "" {
				count[best]++
			}
		}
	}
}

func mustBeUp(m *mapmodel.Map, n mapmodel.NodeID) error {
	node, ok := m.Nodes[n]
	if !ok {
		return ErrUnknownNode
	}
	if node.State != mapmodel.Up {
		return ErrNotUp
	}
	return nil
}

// upNodes returns the sorted ids of the up nodes.
func upNodes(m *mapmodel.Map) []mapmodel.NodeID {
	var ids []mapmodel.NodeID
	for id, n := range m.Nodes {
		if n.State == mapmodel.Up {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

func inPG(slots []mapmodel.Slot, n mapmodel.NodeID) bool {
	return slices.ContainsFunc(slots, func(s mapmodel.Slot) bool { return s.Node == n })
}
