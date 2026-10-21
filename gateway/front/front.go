// Package front serves /objects/{id} and forwards each request to the PG's primary.
package front

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"mycelium/core/prof"
	"mycelium/core/seal"
	"mycelium/core/wire"
	"mycelium/gateway/mapcache"
)

const (
	backoffMin = 50 * time.Millisecond
	backoffMax = time.Second
)

var errNoPrimary = errors.New("no primary")

type Front struct {
	maps       *mapcache.Cache
	client     *wire.Client
	maxRetries int
	ring       *seal.Ring
}

// New returns the front. With a non-nil ring, bodies are sealed before they leave the
// gateway and opened on the way back, so nodes never see plaintext (ADR 0011).
func New(maps *mapcache.Cache, c *wire.Client, maxRetries int, ring *seal.Ring) *Front {
	return &Front{maps: maps, client: c, maxRetries: maxRetries, ring: ring}
}

// ServeHTTP handles GET, PUT and DELETE /objects/{id}. Bodies over max_object get 413.
// The primary's reply (status, version header, body) is passed through; a 200 GET body is
// opened first when sealing is on, and failure to open it gives 502.
func (f *Front) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	id, ok := strings.CutPrefix(r.URL.Path, "/objects/")
	if !ok || id == "" {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodGet, http.MethodPut, http.MethodDelete:
	default:
		w.Header().Set("Allow", "GET, PUT, DELETE")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	t := prof.Start("GW." + r.Method)
	defer t.End()
	m := f.maps.Get()
	if m == nil {
		var err error
		if m, err = f.maps.Refresh(r.Context()); m == nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
	}

	var body []byte
	if r.Method == http.MethodPut {
		var err error
		body, err = io.ReadAll(io.LimitReader(r.Body, m.Params.MaxObject+1))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if int64(len(body)) > m.Params.MaxObject {
			http.Error(w, "object too large", http.StatusRequestEntityTooLarge)
			return
		}
		t.Stage("read")
		if f.ring != nil {
			var err error
			if body, err = f.ring.Seal(id, body); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			t.Stage("seal")
		}
	}

	status, h, rb, err := f.forward(r.Context(), r.Method, id, body)
	t.Stage("forward")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	if f.ring != nil && r.Method == http.MethodGet && status == http.StatusOK {
		if rb, err = f.ring.Open(id, rb); err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		t.Stage("open")
	}
	for _, k := range []string{wire.HeaderVersion, "Content-Type"} {
		if v := h.Get(k); v != "" {
			w.Header().Set(k, v)
		}
	}
	w.WriteHeader(status)
	w.Write(rb)
}

// forward sends the request to the primary of pg(id). On 421/503 or a transport
// error it backs off, refreshes the map and retries, at most maxRetries times.
// When retries run out, 421 becomes 503 and a transport error is returned as err.
func (f *Front) forward(ctx context.Context, method, id string, body []byte) (int, http.Header, []byte, error) {
	backoff := backoffMin
	for attempt := 0; ; attempt++ {
		status, h, rb, err := f.try(ctx, method, id, body)
		retry := err != nil || status == wire.StatusNotPrimary || status == wire.StatusInactive
		if !retry {
			return status, h, rb, nil
		}
		if attempt >= f.maxRetries || ctx.Err() != nil {
			switch {
			case errors.Is(err, errNoPrimary):
				return http.StatusServiceUnavailable, http.Header{}, []byte("no primary\n"), nil
			case err != nil:
				return 0, nil, nil, err
			case status == wire.StatusNotPrimary:
				return http.StatusServiceUnavailable, http.Header{}, []byte("primary moved\n"), nil
			}
			return status, h, rb, nil
		}
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, backoffMax)
		f.maps.Refresh(ctx) // on failure, keep using the cached map
	}
}

// try sends one request to the primary named by the cached map.
func (f *Front) try(ctx context.Context, method, id string, body []byte) (int, http.Header, []byte, error) {
	m := f.maps.Get()
	pg := m.PGOf(id)
	slot, ok := m.Primary(pg)
	if !ok {
		return 0, nil, nil, errNoPrimary
	}
	addr := m.Nodes[m.PGs[pg][slot].Node].Addr
	return f.client.DoRaw(ctx, method, addr+"/objects/"+url.PathEscape(id), nil, body)
}
