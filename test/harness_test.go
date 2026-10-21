// Package test runs whole-cluster scenarios in one process: a monitor, N nodes and
// a gateway, wired exactly like cmd/*, with short lease and grace. Tests reach into
// each node's store to prepare or check state. Skipped with -short.
package test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"mycelium/core/mapmodel"
	"mycelium/core/seal"
	"mycelium/core/wire"
	"mycelium/gateway/front"
	"mycelium/gateway/mapcache"
	"mycelium/monitor/api"
	"mycelium/monitor/liveness"
	mstore "mycelium/monitor/store"
	"mycelium/monitor/transitions"
	"mycelium/node/agent"
	"mycelium/node/codec"
	"mycelium/node/holder"
	"mycelium/node/peers"
	"mycelium/node/primary"
	"mycelium/node/store"
)

const (
	lease     = time.Second
	grace     = 2 * time.Second
	maxObject = 64 << 10
	waitLong  = 60 * time.Second // upper bound for recovery; tests poll, never sleep blindly
)

type Cluster struct {
	t      *testing.T
	Params mapmodel.Params
	monURL string
	gwURL  string
	Nodes  []*Node
	http   *http.Client
}

type Node struct {
	ID    mapmodel.NodeID
	Store *store.Store
	Addr  string // host:port, kept across restarts

	dir    string
	codec  *codec.Codec
	mg     *primary.Manager
	srv    *http.Server
	gate   *gate
	stopHB context.CancelFunc
	hbDone chan struct{}
	up     bool // HTTP server and primary running
}

// NewCluster starts a monitor, n nodes (the first k+m bootstrap the PG table, the
// rest are spares) and a gateway. Everything is stopped when the test ends.
func NewCluster(t *testing.T, n, k, m int) *Cluster {
	t.Helper()
	return newCluster(t, n, k, m, nil)
}

// NewSealedCluster is NewCluster with a gateway that seals every object under ring.
func NewSealedCluster(t *testing.T, n, k, m int, ring *seal.Ring) *Cluster {
	t.Helper()
	return newCluster(t, n, k, m, ring)
}

func newCluster(t *testing.T, n, k, m int, ring *seal.Ring) *Cluster {
	t.Helper()
	if testing.Short() {
		t.Skip("cluster scenario: skipped with -short")
	}
	c := &Cluster{
		t: t,
		Params: mapmodel.Params{
			K: k, M: m, PGNum: 8, MinSize: min(k+1, k+m), BootstrapNodes: k + m,
			Lease: lease, Grace: grace, MaxObject: maxObject,
		},
		http: &http.Client{Timeout: 30 * time.Second},
	}

	st := mstore.Open(filepath.Join(t.TempDir(), "map.json"))
	live := liveness.New(grace, time.Now)
	mon := api.New(transitions.Initial(c.Params), 0, st, live)
	ctx, cancel := context.WithCancel(context.Background())
	go live.Run(ctx, mon.OnOut)
	monSrv := httptest.NewServer(mon.Handler())
	c.monURL = monSrv.URL

	client := wire.NewClient(10 * time.Second)
	gwMux := http.NewServeMux()
	gwMux.Handle("/objects/", front.New(mapcache.New([]string{c.monURL}, client), client, 5, ring))
	gwSrv := httptest.NewServer(gwMux)
	c.gwURL = gwSrv.URL

	t.Cleanup(func() {
		for _, n := range c.Nodes {
			c.Kill(n)
		}
		gwSrv.Close()
		cancel()
		monSrv.Close()
	})

	for i := 1; i <= n; i++ {
		node := &Node{ID: mapmodel.NodeID(fmt.Sprintf("n%d", i)), dir: t.TempDir()}
		c.Nodes = append(c.Nodes, node)
		c.start(node)
	}
	c.WaitHealthy()
	return c
}

// start boots a node like cmd/node: serve holder routes, boot (purge+join if
// needed), then start the primary and the heartbeat loop.
func (c *Cluster) start(n *Node) {
	t := c.t
	t.Helper()
	st, err := store.Open(n.dir)
	if err != nil {
		t.Fatal(err)
	}
	addr := n.Addr
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	n.Addr, n.Store = ln.Addr().String(), st

	client := wire.NewClient(5 * time.Second)
	a := agent.New(agent.Config{NodeID: n.ID, Addr: "http://" + n.Addr, Monitors: []string{c.monURL}}, st, client)
	h := holder.New(a, st)
	mux := http.NewServeMux()
	h.Register(mux)
	n.gate = &gate{h: mux}
	n.srv = &http.Server{Handler: n.gate}
	go n.srv.Serve(ln)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := a.Boot(ctx); err != nil {
		t.Fatalf("%s: boot: %v", n.ID, err)
	}
	if n.codec, err = codec.New(c.Params.K, c.Params.M); err != nil {
		t.Fatal(err)
	}
	n.mg = primary.New(n.ID, a, peers.New(n.ID, h, client), st, n.codec)
	n.mg.Register(mux)
	a.Subscribe(n.mg.OnMap)
	n.mg.OnMap(a.Map())

	hbCtx, stop := context.WithCancel(context.Background())
	n.stopHB, n.hbDone = stop, make(chan struct{})
	go func() {
		a.Run(hbCtx)
		close(n.hbDone)
	}()
	n.up = true
}

// CutOff stops a node's heartbeats but leaves its HTTP server and primary running,
// like a partition between the node and the monitor.
func (c *Cluster) CutOff(n *Node) {
	if n.stopHB != nil {
		n.stopHB()
		<-n.hbDone
		n.stopHB = nil
	}
}

// Kill stops a node abruptly: no more heartbeats, HTTP refused, in-flight work
// aborted. Its data directory is kept for Restart.
func (c *Cluster) Kill(n *Node) {
	c.CutOff(n)
	if !n.up {
		return
	}
	n.srv.Close()  // closes connections, which cancels in-flight request contexts
	n.gate.close() // waits for in-flight handlers
	n.mg.Close()   // stops PG goroutines
	n.codec.Close()
	n.Store.Close()
	n.up = false
}

// Restart starts a killed node again on the same address and data directory.
func (c *Cluster) Restart(n *Node) { c.start(n) }

// gate tracks in-flight handlers so Kill can wait for them before closing the store.
type gate struct {
	mu   sync.RWMutex
	dead bool
	h    http.Handler
}

func (g *gate) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.dead {
		http.Error(w, "killed", http.StatusServiceUnavailable)
		return
	}
	g.h.ServeHTTP(w, r)
}

func (g *gate) close() {
	g.mu.Lock()
	g.dead = true
	g.mu.Unlock()
}

// Map and waiting.

func (c *Cluster) Map() *mapmodel.Map {
	c.t.Helper()
	var m mapmodel.Map
	if st, err := wire.NewClient(5*time.Second).DoJSON(context.Background(), http.MethodGet, c.monURL+"/map", nil, &m); err != nil || st != 200 {
		c.t.Fatalf("GET /map: %d %v", st, err)
	}
	return &m
}

func (c *Cluster) Node(id mapmodel.NodeID) *Node {
	for _, n := range c.Nodes {
		if n.ID == id {
			return n
		}
	}
	c.t.Fatalf("no node %s", id)
	return nil
}

// Healthy: the PG table exists and every slot is held by an up node, not backfilling.
func Healthy(m *mapmodel.Map) bool {
	if m.PGs == nil {
		return false
	}
	for _, slots := range m.PGs {
		for _, s := range slots {
			if s.Node == "" || s.Backfilling || m.Nodes[s.Node].State != mapmodel.Up {
				return false
			}
		}
	}
	return true
}

func (c *Cluster) WaitFor(what string, cond func() bool) {
	c.t.Helper()
	for deadline := time.Now().Add(waitLong); !cond(); time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			c.t.Fatalf("timed out after %v waiting for %s", waitLong, what)
		}
	}
}

func (c *Cluster) WaitHealthy() {
	c.t.Helper()
	c.WaitFor("a healthy map", func() bool { return Healthy(c.Map()) })
}

// Client side, through the gateway.

type Reply struct {
	Status  int
	Body    string
	Version string
}

// Do sends one request through the gateway (which itself retries a few times).
func (c *Cluster) Do(method, id string, body []byte) (Reply, error) {
	var r io.Reader
	if body != nil {
		r = strings.NewReader(string(body))
	}
	req, err := http.NewRequest(method, c.gwURL+"/objects/"+url.PathEscape(id), r)
	if err != nil {
		return Reply{}, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return Reply{}, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return Reply{resp.StatusCode, string(b), resp.Header.Get(wire.HeaderVersion)}, err
}

// Must retries while the PG is unavailable (502/503) and returns the first
// definitive reply. Use it where availability is not what the test is about.
func (c *Cluster) Must(method, id string, body []byte) Reply {
	c.t.Helper()
	var last Reply
	for deadline := time.Now().Add(waitLong); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		r, err := c.Do(method, id, body)
		if err == nil && r.Status != http.StatusBadGateway && r.Status != http.StatusServiceUnavailable {
			return r
		}
		last = r
	}
	c.t.Fatalf("%s %s: still unavailable after %v (last: %d %q)", method, id, waitLong, last.Status, last.Body)
	return last
}

func (c *Cluster) Put(id, value string) string {
	c.t.Helper()
	r := c.Must(http.MethodPut, id, []byte(value))
	if r.Status != 200 || r.Version == "" {
		c.t.Fatalf("PUT %s: %d %q (version %q)", id, r.Status, r.Body, r.Version)
	}
	return r.Version
}

// Expect reads id and fails unless it holds value (or is absent, if value is nil).
func (c *Cluster) Expect(id string, value *string) {
	c.t.Helper()
	r := c.Must(http.MethodGet, id, nil)
	switch {
	case value == nil && r.Status != 404:
		c.t.Fatalf("GET %s: %d %q, want 404", id, r.Status, trim(r.Body))
	case value != nil && (r.Status != 200 || r.Body != *value):
		c.t.Fatalf("GET %s: %d %q, want 200 %q", id, r.Status, trim(r.Body), trim(*value))
	}
}

func ptr(s string) *string { return &s }

func trim(s string) string {
	if len(s) > 60 {
		return s[:60] + "..."
	}
	return s
}
