package mapmodel

import (
	"slices"
	"testing"
	"time"
)

// Pinned values, computed independently (Python's hashlib). Changing pg(id)
// makes every stored object unreachable (ADR 0002), so this must fail loudly.
func TestPGIsPinned(t *testing.T) {
	cases := []struct {
		id    string
		pgNum int
		want  PGID
	}{
		{"a", 16, 10}, {"a", 1000, 250},
		{"greeting", 16, 12}, {"greeting", 64, 44},
		{"obj/1", 16, 13}, {"obj/1", 1000, 997},
		{"", 64, 20},
		{"ünïcode", 1000, 954},
	}
	for _, c := range cases {
		if got := PG(c.id, c.pgNum); got != c.want {
			t.Errorf("PG(%q, %d) = %d, want %d", c.id, c.pgNum, got, c.want)
		}
	}
}

func TestDerivedSets(t *testing.T) {
	m := &Map{
		Params: Params{K: 2, M: 2, MinSize: 3},
		Nodes: map[NodeID]Node{
			"a": {State: Up}, "b": {State: Up}, "c": {State: Out}, "d": {State: Up},
		},
		PGs: [][]Slot{
			{{Node: "a"}, {Node: "b"}, {Node: "c"}, {Node: "d"}},                    // c out
			{{Node: "c"}, {Node: "a", Backfilling: true}, {Node: "b"}, {Node: "d"}}, // primary skips out + backfilling
			{{Node: ""}, {Node: "d", Backfilling: true}, {Node: "a", Backfilling: true}, {Node: "c"}},
		},
	}
	cases := []struct {
		pg          PGID
		u, b        []int
		primary     int
		ok, service bool
	}{
		{0, []int{0, 1, 3}, []int{0, 1, 3}, 0, true, true},
		{1, []int{1, 2, 3}, []int{2, 3}, 2, true, false},
		{2, []int{1, 2}, nil, 0, false, false},
		{9, nil, nil, 0, false, false}, // out of range, e.g. before the PG table exists
	}
	for _, c := range cases {
		if u := m.U(c.pg); !slices.Equal(u, c.u) {
			t.Errorf("pg %d: U = %v, want %v", c.pg, u, c.u)
		}
		if b := m.B(c.pg); !slices.Equal(b, c.b) {
			t.Errorf("pg %d: B = %v, want %v", c.pg, b, c.b)
		}
		if p, ok := m.Primary(c.pg); ok != c.ok || (ok && p != c.primary) {
			t.Errorf("pg %d: Primary = %d,%v, want %d,%v", c.pg, p, ok, c.primary, c.ok)
		}
		if s := m.Serviceable(c.pg); s != c.service {
			t.Errorf("pg %d: Serviceable = %v, want %v", c.pg, s, c.service)
		}
	}
	if got := m.PrimaryOf("b"); !slices.Equal(got, []PGID{1}) {
		t.Errorf("PrimaryOf(b) = %v, want [1]", got)
	}
	if got := m.PrimaryOf("d"); len(got) != 0 {
		t.Errorf("PrimaryOf(d) = %v, want none", got)
	}
	if got := m.PrimaryOf("a"); !slices.Equal(got, []PGID{0}) {
		t.Errorf("PrimaryOf(a) = %v, want [0]", got)
	}
}

func TestParamsValidate(t *testing.T) {
	ok := Params{K: 2, M: 1, PGNum: 8, MinSize: 3, BootstrapNodes: 3, Lease: time.Second, Grace: 2 * time.Second, MaxObject: 1}
	if err := ok.Validate(); err != nil {
		t.Fatalf("valid params rejected: %v", err)
	}
	bad := map[string]func(*Params){
		"min_size < k":        func(p *Params) { p.MinSize = 1 },
		"min_size > n":        func(p *Params) { p.MinSize = 4 },
		"bootstrap_nodes < n": func(p *Params) { p.BootstrapNodes = 2 },
		"grace < 2*lease":     func(p *Params) { p.Grace = p.Lease },
	}
	for name, mutate := range bad {
		p := ok
		mutate(&p)
		if p.Validate() == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestVersion(t *testing.T) {
	ordered := []Version{{1, 0}, {1, 9}, {1, 10}, {2, 0}, {10, 1}}
	for i := range ordered {
		for j := range ordered {
			if got := ordered[i].Less(ordered[j]); got != (i < j) {
				t.Errorf("%v.Less(%v) = %v", ordered[i], ordered[j], got)
			}
		}
		s := ordered[i].String()
		if v, err := ParseVersion(s); err != nil || v != ordered[i] {
			t.Errorf("ParseVersion(%q) = %v, %v", s, v, err)
		}
	}
	for _, s := range []string{"", "1", "1.", ".1", "a.1", "1.2.3", "-1.2"} {
		if _, err := ParseVersion(s); err == nil {
			t.Errorf("ParseVersion(%q) accepted", s)
		}
	}
}

func TestCloneIsDeep(t *testing.T) {
	m := &Map{Nodes: map[NodeID]Node{"a": {State: Up}}, PGs: [][]Slot{{{Node: "a"}}}}
	c := m.Clone()
	c.Nodes["a"] = Node{State: Out}
	c.PGs[0][0].Backfilling = true
	if m.Nodes["a"].State != Up || m.PGs[0][0].Backfilling {
		t.Fatal("Clone shares state with the original")
	}
}
