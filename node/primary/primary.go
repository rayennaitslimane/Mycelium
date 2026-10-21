// Package primary runs the primary role for the PGs the map assigns to this node:
// per-PG state, object locks, seq, active/inactive. See io.go, peering.go, backfill.go.
//
// A PG peers and backfills while |B| >= k (enough for peering to find every
// acknowledged write), and serves client I/O only while |B| >= min_size.
package primary

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"mycelium/core/mapmodel"
	"mycelium/core/seal"
	"mycelium/core/wire"
	"mycelium/node/agent"
	"mycelium/node/codec"
	"mycelium/node/peers"
	"mycelium/node/store"
)

const retryDelay = time.Second

var (
	errInactive = errors.New("pg inactive")
	errNotReady = errors.New("fewer than k slots in B")
	errNotFound = errors.New("object not found")
	errStale    = errors.New("backfill_done rejected as stale")
)

type Manager struct {
	self  mapmodel.NodeID
	agent *agent.Agent
	peers *peers.Peers
	store *store.Store
	codec *codec.Codec

	wg sync.WaitGroup // PG goroutines

	mu     sync.Mutex
	m      *mapmodel.Map // latest map seen by OnMap
	pgs    map[mapmodel.PGID]*pgState
	closed bool
}

func New(self mapmodel.NodeID, a *agent.Agent, p *peers.Peers, st *store.Store, c *codec.Codec) *Manager {
	return &Manager{self: self, agent: a, peers: p, store: st, codec: c, pgs: map[mapmodel.PGID]*pgState{}}
}

// OnMap is subscribed to the agent: it starts and stops PG roles and makes every
// kept PG re-peer at the new epoch. Older maps are ignored.
func (mg *Manager) OnMap(m *mapmodel.Map) {
	mg.mu.Lock()
	defer mg.mu.Unlock()
	if mg.closed || m == nil || (mg.m != nil && m.Epoch <= mg.m.Epoch) {
		return
	}
	mg.m = m
	for i := range m.PGs {
		pg := mapmodel.PGID(i)
		slot, ok := m.Primary(pg)
		mine := ok && m.PGs[pg][slot].Node == mg.self
		s := mg.pgs[pg]
		switch {
		case mine && s == nil:
			mg.pgs[pg] = newPGState(mg, pg, m)
		case mine:
			s.setMap(m)
		case s != nil:
			s.stop()
			delete(mg.pgs, pg)
		}
	}
}

// Close stops every PG role and waits for their goroutines to exit; later maps are
// ignored. Call it after the HTTP server has stopped, before closing the store.
func (mg *Manager) Close() {
	mg.mu.Lock()
	mg.closed = true
	for pg, s := range mg.pgs {
		s.stop()
		delete(mg.pgs, pg)
	}
	mg.mu.Unlock()
	mg.wg.Wait()
}

// Register adds GET, PUT and DELETE /objects/{id}.
func (mg *Manager) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /objects/{id}", mg.handleObject)
	mux.HandleFunc("PUT /objects/{id}", mg.handleObject)
	mux.HandleFunc("DELETE /objects/{id}", mg.handleObject)
}

// handleObject answers 421 if not primary for pg(id), 503 if the PG is inactive or
// the serve gate is closed, and replies with the version on success.
func (mg *Manager) handleObject(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	m := mg.agent.Map()
	if m == nil || !mg.agent.CanServe() {
		http.Error(w, "not serving", wire.StatusInactive)
		return
	}
	mg.mu.Lock()
	s := mg.pgs[m.PGOf(id)]
	mg.mu.Unlock()
	if s == nil {
		http.Error(w, "not primary", wire.StatusNotPrimary)
		return
	}

	var data []byte
	var v mapmodel.Version
	var err error
	switch r.Method {
	case http.MethodGet:
		data, v, err = s.read(r.Context(), id)
	case http.MethodPut:
		// A sealed object is up to seal.Overhead bytes larger than the plaintext limit.
		limit := m.Params.MaxObject + int64(seal.Overhead)
		body, rerr := io.ReadAll(io.LimitReader(r.Body, limit+1))
		if rerr != nil {
			http.Error(w, rerr.Error(), http.StatusBadRequest)
			return
		}
		if int64(len(body)) > limit {
			http.Error(w, "object too large", http.StatusRequestEntityTooLarge)
			return
		}
		v, err = s.write(r.Context(), id, body)
	case http.MethodDelete:
		v, err = s.delete(r.Context(), id)
	}
	switch {
	case errors.Is(err, errNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	case err != nil:
		http.Error(w, err.Error(), wire.StatusInactive)
		return
	}
	w.Header().Set(wire.HeaderVersion, v.String())
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

type pgState struct {
	id     mapmodel.PGID
	mg     *Manager
	locks  objLocks
	wake   chan struct{} // new map, re-peer request or step-down
	cancel context.CancelFunc

	// ioMu is held shared by each operation (client I/O, one backfilled object)
	// and exclusively by peering, which therefore never overlaps an operation.
	ioMu sync.RWMutex

	mu       sync.Mutex
	m        *mapmodel.Map // latest map
	active   *mapmodel.Map // map this PG peered at; nil while inactive
	seq      uint64
	seqEpoch mapmodel.Epoch // seq restarts at 0 when this changes
	fencedAt mapmodel.Epoch // a holder fenced us at this epoch: wait for a newer map
}

func newPGState(mg *Manager, pg mapmodel.PGID, m *mapmodel.Map) *pgState {
	ctx, cancel := context.WithCancel(context.Background())
	s := &pgState{id: pg, mg: mg, m: m, wake: make(chan struct{}, 1), cancel: cancel}
	s.signal()
	mg.wg.Add(1)
	go func() {
		defer mg.wg.Done()
		s.run(ctx)
	}()
	return s
}

// run loops: peer, backfill, then wait for a re-peer request or a new map.
// Failed peering is retried until it succeeds or the map changes.
func (s *pgState) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.wake:
		}
		for ctx.Err() == nil {
			s.mu.Lock()
			m, fenced := s.m, s.m.Epoch <= s.fencedAt
			s.mu.Unlock()
			if fenced {
				s.mg.agent.RefreshMap(ctx)
				break
			}
			err := s.activate(ctx, m)
			if err == nil {
				err = s.backfill(ctx, m)
			}
			if err == nil || errors.Is(err, errInactive) || errors.Is(err, errNotReady) {
				break
			}
			if errors.Is(err, peers.ErrStepDown) {
				s.stepDown(m)
				continue
			}
			log.Printf("primary: pg %d at epoch %d: %v", s.id, m.Epoch, err)
			if errors.Is(err, errStale) {
				s.mg.agent.RefreshMap(ctx)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(retryDelay):
			}
		}
	}
}

// activate peers at m and marks the PG active.
func (s *pgState) activate(ctx context.Context, m *mapmodel.Map) error {
	s.ioMu.Lock()
	defer s.ioMu.Unlock()
	s.mu.Lock()
	s.active = nil
	current := s.m == m
	s.mu.Unlock()
	if !current {
		return errInactive
	}
	if len(m.B(s.id)) < m.Params.K {
		return errNotReady
	}
	if err := s.peer(ctx, m); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m != m {
		return errInactive
	}
	if s.seqEpoch != m.Epoch {
		s.seq, s.seqEpoch = 0, m.Epoch
	}
	s.active = m
	return nil
}

// begin starts an operation on the active PG; release ends it. With serving, the
// PG must also be serviceable.
func (s *pgState) begin(serving bool) (am *mapmodel.Map, release func(), err error) {
	s.ioMu.RLock()
	s.mu.Lock()
	am = s.active
	s.mu.Unlock()
	if am == nil || (serving && !am.Serviceable(s.id)) {
		s.ioMu.RUnlock()
		return nil, nil, errInactive
	}
	return am, s.ioMu.RUnlock, nil
}

func (s *pgState) setMap(m *mapmodel.Map) {
	s.mu.Lock()
	s.m, s.active = m, nil
	s.mu.Unlock()
	s.signal()
}

func (s *pgState) stop() {
	s.cancel()
	s.mu.Lock()
	s.active = nil
	s.mu.Unlock()
}

func (s *pgState) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// requestRepeer marks the PG inactive (if still active at am) and wakes run.
func (s *pgState) requestRepeer(am *mapmodel.Map) {
	s.mu.Lock()
	if s.active == am {
		s.active = nil
	}
	s.mu.Unlock()
	s.signal()
}

// stepDown drops the primary role until a map newer than am arrives; run refetches it.
func (s *pgState) stepDown(am *mapmodel.Map) {
	s.mu.Lock()
	s.fencedAt = max(s.fencedAt, am.Epoch)
	if s.active == am {
		s.active = nil
	}
	s.mu.Unlock()
	s.signal()
}

// fail handles a failed operation at am: step down if fenced, else re-peer.
func (s *pgState) fail(am *mapmodel.Map, err error) error {
	if errors.Is(err, peers.ErrStepDown) {
		s.stepDown(am)
	} else {
		s.requestRepeer(am)
	}
	log.Printf("primary: pg %d: %v", s.id, err)
	return errInactive
}

// nextVersion returns (epoch, ++seq).
func (s *pgState) nextVersion(am *mapmodel.Map) mapmodel.Version {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	return mapmodel.Version{Epoch: am.Epoch, Seq: s.seq}
}

// objLocks serializes operations per object; entries are dropped when unused.
type objLocks struct {
	mu sync.Mutex
	m  map[string]*objLock
}

type objLock struct {
	sync.Mutex
	users int
}

func (l *objLocks) lock(id string) (unlock func()) {
	l.mu.Lock()
	if l.m == nil {
		l.m = map[string]*objLock{}
	}
	e := l.m[id]
	if e == nil {
		e = &objLock{}
		l.m[id] = e
	}
	e.users++
	l.mu.Unlock()
	e.Lock()
	return func() {
		e.Unlock()
		l.mu.Lock()
		if e.users--; e.users == 0 {
			delete(l.m, id)
		}
		l.mu.Unlock()
	}
}
