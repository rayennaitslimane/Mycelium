// Package agent boots the node, runs the heartbeat loop, joins/purges and refreshes
// the map. It is the only owner of the serve gate.
package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"mycelium/core/mapmodel"
	"mycelium/core/wire"
	"mycelium/node/store"
)

const bootRetry = time.Second

type Config struct {
	NodeID   mapmodel.NodeID
	Addr     string
	Monitors []string
}

type Agent struct {
	cfg    Config
	client *wire.Client
	store  *store.Store
	bootID string

	// serveMu is held shared by every served operation and exclusively by a purge,
	// so no shard operation runs across a purge.
	serveMu sync.RWMutex

	mu         sync.Mutex
	m          *mapmodel.Map
	lastOKSent time.Time // send time of the last successful heartbeat
	lastState  mapmodel.State
	replyEpoch mapmodel.Epoch
	subs       []func(*mapmodel.Map)
	mon        int // monitor to try first: the last one that answered
}

// New generates a fresh boot_id.
func New(cfg Config, st *store.Store, c *wire.Client) *Agent {
	b := make([]byte, 8)
	rand.Read(b)
	return &Agent{cfg: cfg, client: c, store: st, bootID: hex.EncodeToString(b)}
}

// Boot heartbeats until it succeeds and the map is fetched. On 404 or out it
// purges all local PG data and joins.
func (a *Agent) Boot(ctx context.Context) error {
	for {
		err := a.beat(ctx)
		if err == nil {
			return nil
		}
		log.Printf("agent: boot: %v", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(bootRetry):
		}
	}
}

// Run heartbeats every lease/4 until ctx is done.
func (a *Agent) Run(ctx context.Context) {
	for {
		d := a.lease() / 4
		if d <= 0 {
			d = bootRetry
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(d):
		}
		if err := a.beat(ctx); err != nil {
			log.Printf("agent: heartbeat: %v", err)
		}
	}
}

// CanServe: last successful heartbeat sent less than lease ago, said up,
// and the local map epoch equals the reply epoch.
func (a *Agent) CanServe() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.m != nil && a.lastState == mapmodel.Up && a.m.Epoch == a.replyEpoch &&
		time.Since(a.lastOKSent) < a.m.Params.Lease
}

// Acquire admits one served operation if CanServe; a purge waits for it to call release.
func (a *Agent) Acquire() (release func(), ok bool) {
	a.serveMu.RLock()
	if !a.CanServe() {
		a.serveMu.RUnlock()
		return nil, false
	}
	return a.serveMu.RUnlock, true
}

func (a *Agent) Map() *mapmodel.Map {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.m
}

// Subscribe registers fn to be called with each newer map. Call before Boot.
func (a *Agent) Subscribe(fn func(*mapmodel.Map)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.subs = append(a.subs, fn)
}

// RefreshMap fetches the map and, if newer, installs it and notifies subscribers.
func (a *Agent) RefreshMap(ctx context.Context) error {
	m, err := a.fetchMap(ctx)
	if err != nil {
		return err
	}
	a.mu.Lock()
	newer := a.m == nil || m.Epoch > a.m.Epoch
	if newer {
		a.m = m
	}
	subs := a.subs
	a.mu.Unlock()
	if newer {
		for _, fn := range subs {
			fn(m)
		}
	}
	return nil
}

// BackfillDone reports that slot of pg, held by target, is backfilled at epoch.
// stale is true if the monitor rejected it.
func (a *Agent) BackfillDone(ctx context.Context, pg mapmodel.PGID, slot int, epoch mapmodel.Epoch, target mapmodel.NodeID) (stale bool, err error) {
	path := fmt.Sprintf("/pg/%d/backfill_done?slot=%d", pg, slot)
	status, err := a.call(ctx, http.MethodPost, path, wire.BackfillDoneReq{Epoch: epoch, NodeID: target}, nil)
	switch {
	case err != nil:
		return false, err
	case status == http.StatusConflict:
		return true, nil
	case status/100 != 2:
		return false, fmt.Errorf("backfill_done: status %d", status)
	}
	return false, nil
}

// beat runs one heartbeat round: heartbeat, purge + join if 404 or out, map fetch
// if the reply epoch differs from the local map.
func (a *Agent) beat(ctx context.Context) error {
	timeout := 5 * time.Second
	if l := a.lease(); l > 0 {
		timeout = l / 4
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	sent := time.Now()
	rep, status, err := a.heartbeat(ctx)
	if err != nil {
		return err
	}
	if status == http.StatusNotFound || (status == http.StatusOK && rep.State == mapmodel.Out) {
		a.mu.Lock()
		a.lastState = mapmodel.Out // closes the gate before purging
		a.mu.Unlock()
		if err := a.purge(); err != nil {
			return fmt.Errorf("purge: %w", err)
		}
		sent = time.Now()
		if rep, status, err = a.join(ctx); err != nil {
			return err
		}
	}
	if status != http.StatusOK {
		return fmt.Errorf("monitor: status %d", status)
	}
	a.mu.Lock()
	a.lastOKSent, a.lastState, a.replyEpoch = sent, rep.State, rep.Epoch
	stale := a.m == nil || a.m.Epoch != rep.Epoch
	a.mu.Unlock()
	if stale {
		return a.RefreshMap(ctx)
	}
	return nil
}

func (a *Agent) purge() error {
	a.serveMu.Lock()
	defer a.serveMu.Unlock()
	return a.store.PurgeAll()
}

func (a *Agent) heartbeat(ctx context.Context) (wire.NodeReply, int, error) {
	var rep wire.NodeReply
	status, err := a.call(ctx, http.MethodPost, "/node/heartbeat", wire.HeartbeatReq{NodeID: a.cfg.NodeID, BootID: a.bootID}, &rep)
	return rep, status, err
}

func (a *Agent) join(ctx context.Context) (wire.NodeReply, int, error) {
	var rep wire.NodeReply
	status, err := a.call(ctx, http.MethodPost, "/node/join", wire.JoinReq{NodeID: a.cfg.NodeID, BootID: a.bootID, Addr: a.cfg.Addr}, &rep)
	return rep, status, err
}

func (a *Agent) fetchMap(ctx context.Context) (*mapmodel.Map, error) {
	var m mapmodel.Map
	status, err := a.call(ctx, http.MethodGet, "/map", nil, &m)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("get map: status %d", status)
	}
	return &m, nil
}

// call tries the monitors in turn, starting with the last one that answered,
// skipping unreachable ones and non-leaders (421).
func (a *Agent) call(ctx context.Context, method, path string, in, out any) (int, error) {
	a.mu.Lock()
	start := a.mon
	a.mu.Unlock()
	var errs []error
	for i := range a.cfg.Monitors {
		idx := (start + i) % len(a.cfg.Monitors)
		status, err := a.client.DoJSON(ctx, method, a.cfg.Monitors[idx]+path, in, out)
		if err == nil && status == wire.StatusNotPrimary {
			err = errors.New("not leader")
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", a.cfg.Monitors[idx], err))
			continue
		}
		a.mu.Lock()
		a.mon = idx
		a.mu.Unlock()
		return status, nil
	}
	return 0, errors.Join(errs...)
}

func (a *Agent) lease() time.Duration {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.m == nil {
		return 0
	}
	return a.m.Params.Lease
}
