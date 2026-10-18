package transitions

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"mycelium/core/mapmodel"
)

var params = mapmodel.Params{
	K: 2, M: 1, PGNum: 4, MinSize: 3, BootstrapNodes: 3,
	Lease: time.Second, Grace: 2 * time.Second, MaxObject: 1 << 20,
}

func apply(t *testing.T, m *mapmodel.Map, e Event) *mapmodel.Map {
	t.Helper()
	next, err := Apply(m, e)
	if err != nil {
		t.Fatalf("%+v: %v", e, err)
	}
	if next.Epoch != m.Epoch+1 {
		t.Fatalf("%+v: epoch %d -> %d, want +1", e, m.Epoch, next.Epoch)
	}
	checkInvariants(t, next)
	return next
}

func joinAll(t *testing.T, m *mapmodel.Map, ids ...mapmodel.NodeID) *mapmodel.Map {
	for _, id := range ids {
		m = apply(t, m, Event{Kind: KindJoin, Node: id, Addr: "http://" + string(id)})
	}
	return m
}

// checkInvariants: every slot names a known node or is vacant, and no node holds
// two slots of the same PG.
func checkInvariants(t *testing.T, m *mapmodel.Map) {
	t.Helper()
	for pg, slots := range m.PGs {
		if len(slots) != m.Params.N() {
			t.Fatalf("pg %d has %d slots, want %d", pg, len(slots), m.Params.N())
		}
		seen := map[mapmodel.NodeID]bool{}
		for _, s := range slots {
			if s.Node == "" {
				continue
			}
			if _, ok := m.Nodes[s.Node]; !ok {
				t.Fatalf("pg %d names unknown node %q", pg, s.Node)
			}
			if seen[s.Node] {
				t.Fatalf("pg %d holds %q twice", pg, s.Node)
			}
			seen[s.Node] = true
		}
	}
}

func nodes(m *mapmodel.Map, pg int) []mapmodel.NodeID {
	var ids []mapmodel.NodeID
	for _, s := range m.PGs[pg] {
		ids = append(ids, s.Node)
	}
	return ids
}

func TestBootstrapCreatesTableOnceAndSparesStayIdle(t *testing.T) {
	m := joinAll(t, Initial(params), "c", "a")
	if m.PGs != nil {
		t.Fatal("PG table created before bootstrap_nodes joined")
	}
	m = joinAll(t, m, "b")
	// slot i of pg p = sorted ids [a b c] at (p+i) mod 3
	want := [][]mapmodel.NodeID{{"a", "b", "c"}, {"b", "c", "a"}, {"c", "a", "b"}, {"a", "b", "c"}}
	for pg := range want {
		if got := nodes(m, pg); !reflect.DeepEqual(got, want[pg]) {
			t.Errorf("pg %d = %v, want %v", pg, got, want[pg])
		}
		for _, s := range m.PGs[pg] {
			if s.Backfilling {
				t.Errorf("pg %d: bootstrap slot is backfilling", pg)
			}
		}
	}
	table := m.PGs
	m = joinAll(t, m, "d", "e")
	if !reflect.DeepEqual(m.PGs, table) {
		t.Fatal("joining spares changed the PG table (no rebalancing expected)")
	}
}

func TestOutReassignsToFewestSlotsThenLowestID(t *testing.T) {
	m := joinAll(t, Initial(params), "a", "b", "c", "d", "e")
	before := m
	m = apply(t, m, Event{Kind: KindOut, Node: "b"})
	if m.Nodes["b"].State != mapmodel.Out {
		t.Fatal("b not out")
	}
	// b held pg0 slot1, pg1 slot0, pg2 slot2, pg3 slot1. d and e start with 0 slots:
	// d (tie, lowest id), e (fewer), d, e.
	want := map[[2]int]mapmodel.NodeID{{0, 1}: "d", {1, 0}: "e", {2, 2}: "d", {3, 1}: "e"}
	for pos, id := range want {
		s := m.PGs[pos[0]][pos[1]]
		if s.Node != id || !s.Backfilling {
			t.Errorf("pg %d slot %d = %+v, want %s backfilling", pos[0], pos[1], s, id)
		}
	}
	for pg := range m.PGs {
		for i, s := range m.PGs[pg] {
			if _, moved := want[[2]int{pg, i}]; !moved && s != before.PGs[pg][i] {
				t.Errorf("pg %d slot %d changed without a failure: %+v", pg, i, s)
			}
		}
	}
}

func TestSlotStaysVacantWithoutCandidate(t *testing.T) {
	m := joinAll(t, Initial(params), "a", "b", "c")
	m = apply(t, m, Event{Kind: KindOut, Node: "a"})
	for pg, slots := range m.PGs {
		for _, s := range slots {
			if s.Node == "a" {
				t.Fatalf("pg %d still names out node a", pg)
			}
		}
	}
	if s := m.PGs[0][0]; s.Node != "" || !s.Backfilling {
		t.Fatalf("pg 0 slot 0 = %+v, want vacant", s)
	}
	m = joinAll(t, m, "a") // rejoins (after purging): fills the vacancies, backfilling
	if s := m.PGs[0][0]; s.Node != "a" || !s.Backfilling {
		t.Fatalf("pg 0 slot 0 = %+v, want a backfilling", s)
	}
}

func TestGuards(t *testing.T) {
	m := joinAll(t, Initial(params), "a", "b", "c", "d")
	m = apply(t, m, Event{Kind: KindOut, Node: "a"}) // d replaces a, backfilling

	slot := -1
	for i, s := range m.PGs[0] {
		if s.Node == "d" {
			slot = i
		}
	}
	reject := map[string]struct {
		e    Event
		want error
	}{
		"duplicate join":        {Event{Kind: KindJoin, Node: "b", Addr: "x"}, ErrAlreadyUp},
		"boot of unknown node":  {Event{Kind: KindBoot, Node: "zz"}, ErrUnknownNode},
		"boot of out node":      {Event{Kind: KindBoot, Node: "a"}, ErrNotUp},
		"out of out node":       {Event{Kind: KindOut, Node: "a"}, ErrNotUp},
		"backfill_done stale":   {Event{Kind: KindBackfillDone, Node: "d", PG: 0, Slot: slot, Epoch: m.Epoch - 1}, ErrStale},
		"backfill_done wrong":   {Event{Kind: KindBackfillDone, Node: "b", PG: 0, Slot: slot, Epoch: m.Epoch}, ErrStale},
		"backfill_done no slot": {Event{Kind: KindBackfillDone, Node: "d", PG: 0, Slot: 7, Epoch: m.Epoch}, ErrStale},
		"backfill_done no pg":   {Event{Kind: KindBackfillDone, Node: "d", PG: 99, Slot: 0, Epoch: m.Epoch}, ErrStale},
		"unknown event kind":    {Event{Kind: "bogus"}, nil},
	}
	for name, c := range reject {
		got, err := Apply(m, c.e)
		if err == nil || (c.want != nil && !errors.Is(err, c.want)) || got != nil {
			t.Errorf("%s: got map=%v err=%v, want %v", name, got != nil, err, c.want)
		}
	}

	m = apply(t, m, Event{Kind: KindBackfillDone, Node: "d", PG: 0, Slot: slot, Epoch: m.Epoch})
	if m.PGs[0][slot].Backfilling {
		t.Fatal("backfill_done did not clear the flag")
	}
	if _, err := Apply(m, Event{Kind: KindBackfillDone, Node: "d", PG: 0, Slot: slot, Epoch: m.Epoch}); !errors.Is(err, ErrStale) {
		t.Fatalf("repeated backfill_done: %v, want ErrStale", err)
	}
	apply(t, m, Event{Kind: KindBoot, Node: "b"}) // accepted: only bumps the epoch
}

// Raft replicas apply the same events and must reach the same map (ADR 0009).
func TestApplyIsDeterministicAndPure(t *testing.T) {
	events := []Event{
		{Kind: KindJoin, Node: "n3", Addr: "3"}, {Kind: KindJoin, Node: "n1", Addr: "1"},
		{Kind: KindJoin, Node: "n2", Addr: "2"}, {Kind: KindJoin, Node: "n5", Addr: "5"},
		{Kind: KindJoin, Node: "n4", Addr: "4"}, {Kind: KindOut, Node: "n2"},
		{Kind: KindBoot, Node: "n1"}, {Kind: KindOut, Node: "n1"},
		{Kind: KindJoin, Node: "n2", Addr: "2b"},
	}
	run := func() *mapmodel.Map {
		m := Initial(params)
		for _, e := range events {
			before, _ := json.Marshal(m)
			next, err := Apply(m, e)
			if err != nil {
				t.Fatalf("%+v: %v", e, err)
			}
			if after, _ := json.Marshal(m); string(after) != string(before) {
				t.Fatalf("%+v: Apply modified its input map", e)
			}
			checkInvariants(t, next)
			m = next
		}
		return m
	}
	a, b := run(), run()
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("same events gave different maps:\n%s\n%s", dump(a), dump(b))
	}
}

func dump(m *mapmodel.Map) string {
	b, _ := json.Marshal(m)
	return fmt.Sprint(string(b))
}
