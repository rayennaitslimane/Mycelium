// Package holder serves the shard routes: serve gate, then store (which checks the fence).
package holder

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"mycelium/core/mapmodel"
	"mycelium/core/wire"
	"mycelium/node/agent"
	"mycelium/node/store"
)

var ErrNotServing = errors.New("not serving")

type Holder struct {
	agent *agent.Agent
	store *store.Store
}

func New(a *agent.Agent, st *store.Store) *Holder {
	return &Holder{agent: a, store: st}
}

// Register adds PUT /pg/{pg}/shards/{id}/{version}, GET /pg/{pg}/shards/{id}[/{version}],
// and POST /pg/{pg}/{commit,discard,scan,lookup}.
func (h *Holder) Register(mux *http.ServeMux) {
	mux.HandleFunc("PUT /pg/{pg}/shards/{id}/{version}", h.putShard)
	mux.HandleFunc("GET /pg/{pg}/shards/{id}", h.getShard)
	mux.HandleFunc("GET /pg/{pg}/shards/{id}/{version}", h.getShard)
	mux.HandleFunc("POST /pg/{pg}/commit", h.commit)
	mux.HandleFunc("POST /pg/{pg}/discard", h.discard)
	mux.HandleFunc("POST /pg/{pg}/scan", h.scan)
	mux.HandleFunc("POST /pg/{pg}/lookup", h.lookup)
}

// Direct API, also used by peers for the node's own slot.

func (h *Holder) Put(pg mapmodel.PGID, epoch mapmodel.Epoch, sh *wire.Shard) error {
	return h.serve(func() error { return h.store.Put(pg, epoch, sh) })
}

func (h *Holder) Get(pg mapmodel.PGID, epoch mapmodel.Epoch, id string, v *mapmodel.Version) (sh *wire.Shard, err error) {
	err = h.serve(func() (err error) {
		sh, err = h.store.Get(pg, epoch, id, v)
		return err
	})
	return sh, err
}

func (h *Holder) Commit(pg mapmodel.PGID, epoch mapmodel.Epoch, items []wire.IDVersion) error {
	return h.serve(func() error { return h.store.Commit(pg, epoch, items) })
}

func (h *Holder) Discard(pg mapmodel.PGID, epoch mapmodel.Epoch, items []wire.IDVersion) error {
	return h.serve(func() error { return h.store.Discard(pg, epoch, items) })
}

func (h *Holder) Scan(pg mapmodel.PGID, epoch mapmodel.Epoch) (r []wire.IDVersions, err error) {
	err = h.serve(func() (err error) {
		r, err = h.store.Scan(pg, epoch)
		return err
	})
	return r, err
}

func (h *Holder) Lookup(pg mapmodel.PGID, epoch mapmodel.Epoch, ids []string) (r []wire.IDVersions, err error) {
	err = h.serve(func() (err error) {
		r, err = h.store.Lookup(pg, epoch, ids)
		return err
	})
	return r, err
}

func (h *Holder) serve(fn func() error) error {
	release, ok := h.agent.Acquire()
	if !ok {
		return ErrNotServing
	}
	defer release()
	return fn()
}

// HTTP wrappers.

func (h *Holder) putShard(w http.ResponseWriter, r *http.Request) {
	pg, err1 := parsePG(r)
	epoch, err2 := parseEpoch(r.Header.Get(wire.HeaderEpoch))
	v, err3 := mapmodel.ParseVersion(r.PathValue("version"))
	data, err4 := io.ReadAll(r.Body)
	if err := errors.Join(err1, err2, err3, err4); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	sh, err := ParseShard(r.PathValue("id"), v, r.Header, data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := h.Put(pg, epoch, sh); err != nil {
		writeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Holder) getShard(w http.ResponseWriter, r *http.Request) {
	pg, err1 := parsePG(r)
	epoch, err2 := parseEpoch(r.Header.Get(wire.HeaderEpoch))
	var v *mapmodel.Version
	var err3 error
	if s := r.PathValue("version"); s != "" {
		var pv mapmodel.Version
		pv, err3 = mapmodel.ParseVersion(s)
		v = &pv
	}
	if err := errors.Join(err1, err2, err3); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	sh, err := h.Get(pg, epoch, r.PathValue("id"), v)
	if err != nil {
		writeErr(w, err)
		return
	}
	for k, vs := range ShardHeader(sh) {
		w.Header()[k] = vs
	}
	w.WriteHeader(http.StatusOK)
	w.Write(sh.Data)
}

func (h *Holder) commit(w http.ResponseWriter, r *http.Request) {
	h.batch(w, r, h.Commit)
}

func (h *Holder) discard(w http.ResponseWriter, r *http.Request) {
	h.batch(w, r, h.Discard)
}

func (h *Holder) batch(w http.ResponseWriter, r *http.Request, fn func(mapmodel.PGID, mapmodel.Epoch, []wire.IDVersion) error) {
	pg, err := parsePG(r)
	var q wire.BatchReq
	if err := errors.Join(err, wire.ReadJSON(r, &q)); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := fn(pg, q.Epoch, q.Items); err != nil {
		writeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Holder) scan(w http.ResponseWriter, r *http.Request) {
	pg, err := parsePG(r)
	var q wire.ScanReq
	if err := errors.Join(err, wire.ReadJSON(r, &q)); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	items, err := h.Scan(pg, q.Epoch)
	if err != nil {
		writeErr(w, err)
		return
	}
	wire.WriteJSON(w, http.StatusOK, wire.IDVersionsReply{Items: items})
}

func (h *Holder) lookup(w http.ResponseWriter, r *http.Request) {
	pg, err := parsePG(r)
	var q wire.LookupReq
	if err := errors.Join(err, wire.ReadJSON(r, &q)); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	items, err := h.Lookup(pg, q.Epoch, q.IDs)
	if err != nil {
		writeErr(w, err)
		return
	}
	wire.WriteJSON(w, http.StatusOK, wire.IDVersionsReply{Items: items})
}

// ShardHeader carries a shard's metadata; the data travels as the body.
func ShardHeader(sh *wire.Shard) http.Header {
	h := http.Header{}
	h.Set(wire.HeaderVersion, sh.Version.String())
	h.Set(wire.HeaderSize, strconv.FormatInt(sh.Size, 10))
	h.Set(wire.HeaderSha256, hex.EncodeToString(sh.Sha256[:]))
	if sh.Deleted {
		h.Set(wire.HeaderDeleted, "1")
	}
	return h
}

// ParseShard rebuilds a shard from ShardHeader metadata; the version comes from the caller.
func ParseShard(id string, v mapmodel.Version, h http.Header, data []byte) (*wire.Shard, error) {
	size, err := strconv.ParseInt(h.Get(wire.HeaderSize), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("bad %s: %w", wire.HeaderSize, err)
	}
	sum, err := hex.DecodeString(h.Get(wire.HeaderSha256))
	if err != nil || len(sum) != 32 {
		return nil, fmt.Errorf("bad %s", wire.HeaderSha256)
	}
	sh := &wire.Shard{ID: id, Version: v, Size: size, Deleted: h.Get(wire.HeaderDeleted) == "1", Data: data}
	copy(sh.Sha256[:], sum)
	return sh, nil
}

func parsePG(r *http.Request) (mapmodel.PGID, error) {
	pg, err := strconv.ParseUint(r.PathValue("pg"), 10, 32)
	return mapmodel.PGID(pg), err
}

func parseEpoch(s string) (mapmodel.Epoch, error) {
	e, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("bad %s: %w", wire.HeaderEpoch, err)
	}
	return mapmodel.Epoch(e), nil
}

// writeErr maps: fenced -> 409 {fence}, not serving -> 503, not found -> 404.
func writeErr(w http.ResponseWriter, err error) {
	var fe *store.FencedError
	switch {
	case errors.As(err, &fe):
		wire.WriteJSON(w, wire.StatusFenced, wire.FenceReply{Fence: fe.Fence})
	case errors.Is(err, ErrNotServing):
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	case errors.Is(err, store.ErrNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
