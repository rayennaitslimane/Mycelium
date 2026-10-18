// Package liveness tracks in-memory heartbeat times and boot_ids, and emits "out"
// for nodes silent for longer than grace.
package liveness

import (
	"context"
	"sync"
	"time"

	"mycelium/core/mapmodel"
)

type Liveness struct {
	grace time.Duration
	now   func() time.Time

	mu    sync.Mutex
	seen  map[mapmodel.NodeID]time.Time
	boots map[mapmodel.NodeID]string
}

func New(grace time.Duration, now func() time.Time) *Liveness {
	return &Liveness{
		grace: grace,
		now:   now,
		seen:  map[mapmodel.NodeID]time.Time{},
		boots: map[mapmodel.NodeID]string{},
	}
}

// Seed marks nodes as just heard from, so a monitor start (or a new Raft leader)
// does not mark them out. Recorded boot_ids are forgotten.
func (l *Liveness) Seed(nodes []mapmodel.NodeID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	t := l.now()
	for _, n := range nodes {
		l.seen[n] = t
	}
	clear(l.boots)
}

// Heard records a heartbeat and reports whether bootID differs from the recorded one.
// An unknown boot_id (e.g. after a monitor restart) counts as new.
// The boot_id itself is recorded only by RecordBoot, once the transition is persisted.
func (l *Liveness) Heard(n mapmodel.NodeID, bootID string) (newBoot bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seen[n] = l.now()
	old, ok := l.boots[n]
	return !ok || old != bootID
}

func (l *Liveness) RecordBoot(n mapmodel.NodeID, bootID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.boots[n] = bootID
}

// Run calls emit for each node silent for longer than grace, every grace/4, until ctx is done.
// Nodes keep being emitted while silent; the caller ignores those already out.
func (l *Liveness) Run(ctx context.Context, emit func(mapmodel.NodeID)) {
	t := time.NewTicker(l.grace / 4)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		l.mu.Lock()
		now := l.now()
		var silent []mapmodel.NodeID
		for n, s := range l.seen {
			if now.Sub(s) > l.grace {
				silent = append(silent, n)
			}
		}
		l.mu.Unlock()
		for _, n := range silent {
			emit(n)
		}
	}
}
