// Package mapmodel defines the cluster map and the rules everyone derives from it.
// It performs no I/O.
package mapmodel

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"
)

type (
	NodeID string
	Epoch  uint64
	PGID   uint32
)

// State is a node's state in the map.
type State string

const (
	Up  State = "up"
	Out State = "out"
)

// Params is the configuration fixed at bootstrap.
type Params struct {
	K              int           `json:"k"`
	M              int           `json:"m"`
	PGNum          int           `json:"pg_num"`
	MinSize        int           `json:"min_size"`
	BootstrapNodes int           `json:"bootstrap_nodes"`
	Lease          time.Duration `json:"lease"`
	Grace          time.Duration `json:"grace"`
	MaxObject      int64         `json:"max_object"`
}

// N is the number of shards (and slots) per PG: k+m.
func (p Params) N() int {
	return p.K + p.M
}

// Validate checks k <= min_size <= n, bootstrap_nodes >= n and grace >= 2*lease.
func (p Params) Validate() error {
	switch {
	case p.K < 1 || p.M < 0:
		return fmt.Errorf("need k >= 1 and m >= 0, got k=%d m=%d", p.K, p.M)
	case p.PGNum < 1:
		return fmt.Errorf("need pg_num >= 1, got %d", p.PGNum)
	case p.MinSize < p.K || p.MinSize > p.N():
		return fmt.Errorf("need k <= min_size <= n, got k=%d min_size=%d n=%d", p.K, p.MinSize, p.N())
	case p.BootstrapNodes < p.N():
		return fmt.Errorf("need bootstrap_nodes >= n, got %d < %d", p.BootstrapNodes, p.N())
	case p.Lease <= 0:
		return fmt.Errorf("need lease > 0, got %v", p.Lease)
	case p.Grace < 2*p.Lease:
		return fmt.Errorf("need grace >= 2*lease, got grace=%v lease=%v", p.Grace, p.Lease)
	case p.MaxObject < 1:
		return fmt.Errorf("need max_object >= 1, got %d", p.MaxObject)
	}
	return nil
}

type Node struct {
	Addr  string `json:"addr"`
	State State  `json:"state"`
}

// Slot is one position of a PG. An empty Node means the slot is vacant.
type Slot struct {
	Node        NodeID `json:"node"`
	Backfilling bool   `json:"backfilling"`
}

// Map is the cluster map. PGs[pg][slot]; the slot index is also the shard index.
type Map struct {
	Epoch  Epoch           `json:"epoch"`
	Params Params          `json:"params"`
	Nodes  map[NodeID]Node `json:"nodes"`
	PGs    [][]Slot        `json:"pgs"`
}

// PG returns the first 8 bytes of sha256(id), big-endian, mod pgNum.
func PG(id string, pgNum int) PGID {
	sum := sha256.Sum256([]byte(id))
	return PGID(binary.BigEndian.Uint64(sum[:8]) % uint64(pgNum))
}

// slots returns pg's slots, or nil if pg is out of range (e.g. before the PG table exists).
func (m *Map) slots(pg PGID) []Slot {
	if int(pg) >= len(m.PGs) {
		return nil
	}
	return m.PGs[pg]
}

func (m *Map) PGOf(id string) PGID {
	return PG(id, m.Params.PGNum)
}

// U returns the slots of pg whose node is up.
func (m *Map) U(pg PGID) []int {
	var u []int
	for i, s := range m.slots(pg) {
		if s.Node != "" && m.Nodes[s.Node].State == Up {
			u = append(u, i)
		}
	}
	return u
}

// B returns U minus the backfilling slots.
func (m *Map) B(pg PGID) []int {
	slots := m.slots(pg)
	var b []int
	for _, i := range m.U(pg) {
		if !slots[i].Backfilling {
			b = append(b, i)
		}
	}
	return b
}

// Primary returns the lowest slot in B; ok is false if B is empty.
func (m *Map) Primary(pg PGID) (slot int, ok bool) {
	b := m.B(pg)
	if len(b) == 0 {
		return 0, false
	}
	return b[0], true
}

// Serviceable reports whether |B| >= min_size.
func (m *Map) Serviceable(pg PGID) bool {
	return len(m.B(pg)) >= m.Params.MinSize
}

// PrimaryOf returns the PGs whose primary is node n, serviceable or not.
func (m *Map) PrimaryOf(n NodeID) []PGID {
	var pgs []PGID
	for pg := range m.PGs {
		if slot, ok := m.Primary(PGID(pg)); ok && m.PGs[pg][slot].Node == n {
			pgs = append(pgs, PGID(pg))
		}
	}
	return pgs
}

func (m *Map) Clone() *Map {
	c := *m
	c.Nodes = maps.Clone(m.Nodes)
	c.PGs = slices.Clone(m.PGs)
	for i := range c.PGs {
		c.PGs[i] = slices.Clone(c.PGs[i])
	}
	return &c
}

// Version is (epoch, seq), compared numerically and written "epoch.seq".
type Version struct {
	Epoch Epoch  `json:"epoch"`
	Seq   uint64 `json:"seq"`
}

func (v Version) Less(o Version) bool {
	if v.Epoch != o.Epoch {
		return v.Epoch < o.Epoch
	}
	return v.Seq < o.Seq
}

func (v Version) String() string {
	return fmt.Sprintf("%d.%d", v.Epoch, v.Seq)
}

func ParseVersion(s string) (Version, error) {
	e, q, ok := strings.Cut(s, ".")
	if !ok {
		return Version{}, errBadVersion(s)
	}
	epoch, err := strconv.ParseUint(e, 10, 64)
	if err != nil {
		return Version{}, errBadVersion(s)
	}
	seq, err := strconv.ParseUint(q, 10, 64)
	if err != nil {
		return Version{}, errBadVersion(s)
	}
	return Version{Epoch: Epoch(epoch), Seq: seq}, nil
}

var ErrBadVersion = errors.New("bad version")

func errBadVersion(s string) error {
	return fmt.Errorf("%w: %q", ErrBadVersion, s)
}
