// Package api serves the monitor routes: liveness -> transitions -> store.
// The map is persisted before any reply that depends on it.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"mycelium/core/mapmodel"
	"mycelium/core/wire"
	"mycelium/monitor/liveness"
	"mycelium/monitor/raft"
	"mycelium/monitor/store"
	"mycelium/monitor/transitions"
)

const submitTimeout = 5 * time.Second

type API struct {
	store *store.Store
	live  *liveness.Liveness
	raft  *raft.Raft // nil in single-instance mode

	mu      sync.Mutex
	m       *mapmodel.Map // persisted; never mutated, only replaced
	applied uint64
}

// New seeds liveness with every known node, so a restart does not mark them out.
func New(m *mapmodel.Map, applied uint64, st *store.Store, l *liveness.Liveness) *API {
	a := &API{store: st, live: l, m: m, applied: applied}
	l.Seed(nodeIDs(m))
	return a
}

// SetRaft switches to replicated mode. Call before serving.
func (a *API) SetRaft(r *raft.Raft) {
	a.raft = r
}

// Handler serves GET /map, POST /node/heartbeat, POST /node/join,
// POST /pg/{pg_id}/backfill_done and, with Raft, /raft/. A non-leader answers 421.
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /map", a.getMap)
	mux.HandleFunc("POST /node/heartbeat", a.heartbeat)
	mux.HandleFunc("POST /node/join", a.join)
	mux.HandleFunc("POST /pg/{pg_id}/backfill_done", a.backfillDone)
	if a.raft != nil {
		mux.Handle("/raft/", a.raft.Handler())
	}
	return mux
}

func (a *API) getMap(w http.ResponseWriter, r *http.Request) {
	if !a.leader(w) {
		return
	}
	wire.WriteJSON(w, http.StatusOK, a.current())
}

// heartbeat: 404 if unknown; a new boot_id from an up node bumps the epoch first.
func (a *API) heartbeat(w http.ResponseWriter, r *http.Request) {
	if !a.leader(w) {
		return
	}
	var q wire.HeartbeatReq
	if err := wire.ReadJSON(r, &q); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	m := a.current()
	node, ok := m.Nodes[q.NodeID]
	if !ok {
		http.Error(w, "unknown node", http.StatusNotFound)
		return
	}
	if node.State == mapmodel.Up && a.live.Heard(q.NodeID, q.BootID) {
		ctx, cancel := context.WithTimeout(r.Context(), submitTimeout)
		defer cancel()
		_, err := a.submit(ctx, transitions.Event{Kind: transitions.KindBoot, Node: q.NodeID})
		switch {
		case err == nil:
			a.live.RecordBoot(q.NodeID, q.BootID)
		case !errors.Is(err, transitions.ErrNotUp): // went out meanwhile: reply below
			writeErr(w, err)
			return
		}
		m = a.current()
	}
	wire.WriteJSON(w, http.StatusOK, wire.NodeReply{Epoch: m.Epoch, State: m.Nodes[q.NodeID].State})
}

// join: 409 if already up.
func (a *API) join(w http.ResponseWriter, r *http.Request) {
	if !a.leader(w) {
		return
	}
	var q wire.JoinReq
	if err := wire.ReadJSON(r, &q); err != nil || q.NodeID == "" || q.BootID == "" || q.Addr == "" {
		http.Error(w, "need node_id, boot_id and addr", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), submitTimeout)
	defer cancel()
	a.live.Heard(q.NodeID, q.BootID)
	m, err := a.submit(ctx, transitions.Event{Kind: transitions.KindJoin, Node: q.NodeID, Addr: q.Addr})
	if err != nil {
		writeErr(w, err)
		return
	}
	a.live.RecordBoot(q.NodeID, q.BootID)
	wire.WriteJSON(w, http.StatusOK, wire.NodeReply{Epoch: m.Epoch, State: mapmodel.Up})
}

// backfillDone: 409 if stale (epoch not current or slot not held by node_id).
func (a *API) backfillDone(w http.ResponseWriter, r *http.Request) {
	if !a.leader(w) {
		return
	}
	pg, err1 := strconv.ParseUint(r.PathValue("pg_id"), 10, 32)
	slot, err2 := strconv.Atoi(r.URL.Query().Get("slot"))
	var q wire.BackfillDoneReq
	if err := errors.Join(err1, err2, wire.ReadJSON(r, &q)); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), submitTimeout)
	defer cancel()
	_, err := a.submit(ctx, transitions.Event{
		Kind: transitions.KindBackfillDone, Node: q.NodeID, PG: mapmodel.PGID(pg), Slot: slot, Epoch: q.Epoch,
	})
	if err != nil {
		writeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// OnOut is the liveness callback for a node silent past grace.
func (a *API) OnOut(n mapmodel.NodeID) {
	if a.raft != nil && !a.raft.IsLeader() {
		return
	}
	if a.current().Nodes[n].State != mapmodel.Up {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), submitTimeout)
	defer cancel()
	if _, err := a.submit(ctx, transitions.Event{Kind: transitions.KindOut, Node: n}); err != nil &&
		!errors.Is(err, transitions.ErrNotUp) {
		log.Printf("monitor: mark %s out: %v", n, err)
	}
}

// OnLeader seeds liveness with all nodes once this monitor leads with an up-to-date map.
func (a *API) OnLeader() {
	a.live.Seed(nodeIDs(a.current()))
}

// submit applies e: directly (Apply + Save) in single mode, through Raft otherwise.
// It returns a map that includes e.
func (a *API) submit(ctx context.Context, e transitions.Event) (*mapmodel.Map, error) {
	if a.raft != nil {
		cmd, err := json.Marshal(e)
		if err != nil {
			return nil, err
		}
		if err := a.raft.Propose(ctx, cmd); err != nil {
			return nil, err
		}
		return a.current(), nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	next, err := transitions.Apply(a.m, e)
	if err != nil {
		return nil, err
	}
	if err := a.store.Save(next, a.applied); err != nil {
		return nil, err
	}
	a.m = next
	return next, nil
}

// ApplyEntry is the Raft apply callback: decode, Apply, Save. A rejected event
// leaves the map unchanged on every replica; a failed Save is fatal.
func (a *API) ApplyEntry(index uint64, cmd []byte) error {
	var e transitions.Event
	if err := json.Unmarshal(cmd, &e); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	next, err := transitions.Apply(a.m, e)
	if err != nil {
		return err
	}
	if err := a.store.Save(next, index); err != nil {
		log.Fatalf("monitor: persist map at raft index %d: %v", index, err)
	}
	a.m, a.applied = next, index
	return nil
}

func (a *API) current() *mapmodel.Map {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.m
}

// leader writes 421 and returns false if another monitor leads.
func (a *API) leader(w http.ResponseWriter) bool {
	if a.raft == nil || a.raft.IsLeader() {
		return true
	}
	http.Error(w, "not leader", wire.StatusNotPrimary)
	return false
}

func writeErr(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, transitions.ErrAlreadyUp), errors.Is(err, transitions.ErrNotUp),
		errors.Is(err, transitions.ErrStale):
		status = http.StatusConflict
	case errors.Is(err, transitions.ErrUnknownNode):
		status = http.StatusNotFound
	case errors.Is(err, raft.ErrNotLeader):
		status = wire.StatusNotPrimary
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		status = http.StatusServiceUnavailable
	}
	http.Error(w, err.Error(), status)
}

func nodeIDs(m *mapmodel.Map) []mapmodel.NodeID {
	ids := make([]mapmodel.NodeID, 0, len(m.Nodes))
	for id := range m.Nodes {
		ids = append(ids, id)
	}
	return ids
}
