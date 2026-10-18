package primary

import (
	"context"
	"slices"
	"sync"

	"mycelium/core/mapmodel"
	"mycelium/core/wire"
)

// peer resolves every dirty object at m: scanAll, lookupAll, then resolve each id.
// The caller holds ioMu exclusively.
func (s *pgState) peer(ctx context.Context, m *mapmodel.Map) error {
	ids, seen, err := s.scanAll(ctx, m)
	if err != nil || len(ids) == 0 {
		return err
	}
	held, err := s.lookupAll(ctx, m, ids)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := s.resolve(ctx, m, id, held[id], seen[id]); err != nil {
			return err
		}
	}
	return nil
}

// scanAll scans every slot in U, which fences them at m.Epoch. All must answer.
// It returns the union of dirty ids and, per id, the versions the scans reported.
func (s *pgState) scanAll(ctx context.Context, m *mapmodel.Map) ([]string, map[string][]mapmodel.Version, error) {
	var mu sync.Mutex
	seen := map[string][]mapmodel.Version{}
	err := s.forSlots(m.U(s.id), func(slot int) error {
		items, err := s.mg.peers.Scan(ctx, m, s.id, slot)
		mu.Lock()
		defer mu.Unlock()
		for _, it := range items {
			seen[it.ID] = append(seen[it.ID], it.Versions...)
		}
		return err
	})
	if err != nil {
		return nil, nil, err
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids, seen, nil
}

// lookupAll returns, per id, the versions each B slot holds. All must answer.
func (s *pgState) lookupAll(ctx context.Context, m *mapmodel.Map, ids []string) (map[string]map[int][]mapmodel.Version, error) {
	var mu sync.Mutex
	held := map[string]map[int][]mapmodel.Version{}
	err := s.forSlots(m.B(s.id), func(slot int) error {
		items, err := s.mg.peers.Lookup(ctx, m, s.id, slot, ids)
		mu.Lock()
		defer mu.Unlock()
		for _, it := range items {
			if held[it.ID] == nil {
				held[it.ID] = map[int][]mapmodel.Version{}
			}
			held[it.ID][slot] = append(held[it.ID][slot], it.Versions...)
		}
		return err
	})
	return held, err
}

// winner is the highest version held by at least k slots, if any.
func winner(held map[int][]mapmodel.Version, k int) (mapmodel.Version, bool) {
	count := map[mapmodel.Version]int{}
	for _, vs := range held {
		for _, v := range slices.Compact(slices.SortedFunc(slices.Values(vs), cmpVersion)) {
			count[v]++
		}
	}
	var w mapmodel.Version
	found := false
	for v, n := range count {
		if n >= k && (!found || w.Less(v)) {
			w, found = v, true
		}
	}
	return w, found
}

// resolve: with a winner w, commit w where held in B, re-encode it onto the B slots
// lacking it, and discard every version above w. Without one, discard every version
// seen. Discards go to all of U, so backfilling slots are cleaned too.
func (s *pgState) resolve(ctx context.Context, m *mapmodel.Map, id string, held map[int][]mapmodel.Version, seen []mapmodel.Version) error {
	for _, vs := range held {
		seen = append(seen, vs...)
	}
	w, ok := winner(held, m.Params.K)
	var drop []wire.IDVersion
	for _, v := range slices.Compact(slices.SortedFunc(slices.Values(seen), cmpVersion)) {
		if !ok || w.Less(v) {
			drop = append(drop, wire.IDVersion{ID: id, Version: v})
		}
	}

	if ok {
		var have, lack []int
		for _, slot := range m.B(s.id) {
			if slices.Contains(held[slot], w) {
				have = append(have, slot)
			} else {
				lack = append(lack, slot)
			}
		}
		items := []wire.IDVersion{{ID: id, Version: w}}
		if err := s.forSlots(have, func(slot int) error {
			return s.mg.peers.Commit(ctx, m, s.id, slot, items)
		}); err != nil {
			return err
		}
		if len(lack) > 0 {
			if err := s.reencode(ctx, m, id, w, lack); err != nil {
				return err
			}
		}
	}
	if len(drop) == 0 {
		return nil
	}
	return s.forSlots(m.U(s.id), func(slot int) error {
		return s.mg.peers.Discard(ctx, m, s.id, slot, drop)
	})
}

func cmpVersion(a, b mapmodel.Version) int {
	switch {
	case a.Less(b):
		return -1
	case b.Less(a):
		return 1
	}
	return 0
}
