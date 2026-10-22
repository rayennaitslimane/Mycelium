package primary

import (
	"context"
	"errors"
	"slices"

	"mycelium/core/mapmodel"
	"mycelium/core/prof"
	"mycelium/node/peers"
)

// backfill runs backfillSlot for each backfilling slot in U.
func (s *pgState) backfill(ctx context.Context, m *mapmodel.Map) error {
	for _, slot := range m.U(s.id) {
		if m.PGs[s.id][slot].Backfilling {
			if err := s.backfillSlot(ctx, m, slot); err != nil {
				return err
			}
		}
	}
	return nil
}

// backfillSlot copies every local object onto the slot, then sends backfill_done.
// Concurrent writes already reach the slot, because it is in U.
func (s *pgState) backfillSlot(ctx context.Context, m *mapmodel.Map, slot int) error {
	t := prof.Start("BACKFILL_SLOT")
	defer t.End()
	s.mu.Lock()
	current := s.active == m
	s.mu.Unlock()
	if !current {
		return errInactive
	}
	items, err := s.mg.store.List(s.id)
	if err != nil {
		return err
	}
	t.Stage("list")
	for _, it := range items {
		if err := s.backfillObject(ctx, m, slot, it.ID); err != nil {
			return err
		}
	}
	t.Stage("objects")
	err = s.sendBackfillDone(ctx, m, slot)
	t.Stage("done")
	return err
}

// backfillObject: under the object lock, skip id if the slot holds the local latest
// committed version (lookup), else re-encode that version onto the slot.
func (s *pgState) backfillObject(ctx context.Context, m *mapmodel.Map, slot int, id string) error {
	t := prof.Start("BACKFILL")
	defer t.End()
	am, release, err := s.begin(false)
	if err != nil {
		return err
	}
	defer release()
	if am != m {
		return errInactive
	}
	defer s.locks.lock(id)()
	t.Stage("lock")

	self, _ := m.Primary(s.id)
	local, err := s.mg.peers.Get(ctx, m, s.id, self, id, nil)
	if errors.Is(err, peers.ErrNotFound) {
		return nil // deleted meanwhile
	}
	if err != nil {
		return err
	}
	t.Stage("get")
	held, err := s.mg.peers.Lookup(ctx, m, s.id, slot, []string{id})
	if err != nil {
		return err
	}
	t.Stage("lookup")
	for _, h := range held {
		if h.ID == id && slices.Contains(h.Versions, local.Version) {
			return nil
		}
	}
	return s.reencode(ctx, m, id, local.Version, []int{slot}, t)
}

// sendBackfillDone reports the slot to the monitor. If rejected as stale, run
// refetches the map, re-peers and backfills again, which resends it.
func (s *pgState) sendBackfillDone(ctx context.Context, m *mapmodel.Map, slot int) error {
	stale, err := s.mg.agent.BackfillDone(ctx, s.id, slot, m.Epoch, m.PGs[s.id][slot].Node)
	if err != nil {
		return err
	}
	if stale {
		return errStale
	}
	s.mg.agent.RefreshMap(ctx) // the new epoch moves the slot into B; re-peer there
	return nil
}
