package agent

import (
	"testing"
	"time"

	"mycelium/core/mapmodel"
)

// The serve gate (ADR 0008): last successful heartbeat sent less than lease ago,
// said up, and the local map epoch equals the reply epoch.
func TestServeGate(t *testing.T) {
	const lease = time.Second
	m := &mapmodel.Map{Epoch: 7, Params: mapmodel.Params{Lease: lease}}
	cases := []struct {
		name  string
		m     *mapmodel.Map
		state mapmodel.State
		reply mapmodel.Epoch
		sent  time.Duration // ago
		want  bool
	}{
		{"fresh, up, same epoch", m, mapmodel.Up, 7, lease / 2, true},
		{"heartbeat older than lease", m, mapmodel.Up, 7, lease + time.Millisecond, false},
		{"monitor said out", m, mapmodel.Out, 7, lease / 2, false},
		{"map behind the monitor", m, mapmodel.Up, 8, lease / 2, false},
		{"no map yet", nil, mapmodel.Up, 7, lease / 2, false},
		{"never heard from the monitor", m, "", 0, 0, false},
	}
	for _, c := range cases {
		a := &Agent{m: c.m, lastState: c.state, replyEpoch: c.reply, lastOKSent: time.Now().Add(-c.sent)}
		if got := a.CanServe(); got != c.want {
			t.Errorf("%s: CanServe = %v, want %v", c.name, got, c.want)
		}
		release, ok := a.Acquire()
		if ok != c.want {
			t.Errorf("%s: Acquire = %v, want %v", c.name, ok, c.want)
		}
		if ok {
			release()
		}
	}
}
