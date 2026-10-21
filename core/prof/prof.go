// Package prof is a tiny stage timer: Start(op), then Stage(name) after each step.
package prof

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"
)

type stat struct {
	n          int
	total, max time.Duration
}

var (
	mu    sync.Mutex
	stats = map[string]*stat{}
)

type T struct {
	op          string
	start, last time.Time
}

func Start(op string) *T {
	now := time.Now()
	return &T{op, now, now}
}

// Stage records the time since the previous mark under op.name. Nil-safe.
func (t *T) Stage(name string) {
	if t == nil {
		return
	}
	now := time.Now()
	record(t.op+"."+name, now.Sub(t.last))
	t.last = now
}

// End records the whole operation as op.total. Nil-safe.
func (t *T) End() {
	if t != nil {
		record(t.op+".total", time.Since(t.start))
	}
}

func record(k string, d time.Duration) {
	mu.Lock()
	defer mu.Unlock()
	s := stats[k]
	if s == nil {
		s = &stat{}
		stats[k] = s
	}
	s.n++
	s.total += d
	s.max = max(s.max, d)
}

// Handler prints one line per stage: count, mean, max.
func Handler(w http.ResponseWriter, _ *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	keys := make([]string, 0, len(stats))
	for k := range stats {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		s := stats[k]
		fmt.Fprintf(w, "%-20s n=%-6d mean=%-10v max=%v\n", k, s.n, s.total/time.Duration(s.n), s.max)
	}
}
