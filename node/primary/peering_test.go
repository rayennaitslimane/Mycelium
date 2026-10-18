package primary

import (
	"testing"

	"mycelium/core/mapmodel"
)

func v(e, s uint64) mapmodel.Version { return mapmodel.Version{Epoch: mapmodel.Epoch(e), Seq: s} }

func TestWinner(t *testing.T) {
	cases := []struct {
		name string
		held map[int][]mapmodel.Version
		k    int
		want mapmodel.Version
		ok   bool
	}{
		{"highest held by k wins", map[int][]mapmodel.Version{
			0: {v(1, 1), v(2, 1)}, 1: {v(1, 1), v(2, 1)}, 2: {v(1, 1)},
		}, 2, v(2, 1), true},
		{"higher version below k is ignored", map[int][]mapmodel.Version{
			0: {v(1, 1), v(3, 1)}, 1: {v(1, 1)}, 2: {v(1, 1)},
		}, 2, v(1, 1), true},
		{"no version reaches k", map[int][]mapmodel.Version{
			0: {v(1, 1)}, 1: {v(1, 2)}, 2: {},
		}, 2, mapmodel.Version{}, false},
		{"a slot reporting a version twice counts once", map[int][]mapmodel.Version{
			0: {v(4, 2), v(4, 2)}, 1: {v(1, 1)},
		}, 2, mapmodel.Version{}, false},
		{"k=1 takes the highest anywhere", map[int][]mapmodel.Version{
			0: {v(1, 1)}, 1: {v(5, 3)},
		}, 1, v(5, 3), true},
		{"nothing held", map[int][]mapmodel.Version{}, 1, mapmodel.Version{}, false},
	}
	for _, c := range cases {
		got, ok := winner(c.held, c.k)
		if ok != c.ok || (ok && got != c.want) {
			t.Errorf("%s: winner = %v,%v, want %v,%v", c.name, got, ok, c.want, c.ok)
		}
	}
}
