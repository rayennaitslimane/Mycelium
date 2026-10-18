package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"mycelium/core/mapmodel"
	"mycelium/core/wire"
	"mycelium/monitor/api"
	"mycelium/monitor/liveness"
	"mycelium/monitor/store"
	"mycelium/monitor/transitions"
)

var params = mapmodel.Params{
	K: 2, M: 1, PGNum: 4, MinSize: 3, BootstrapNodes: 3,
	Lease: 100 * time.Millisecond, Grace: 400 * time.Millisecond, MaxObject: 1 << 20,
}

// start runs a single-instance monitor on dir, like cmd/monitor does.
func start(t *testing.T, dir string) (url string, stop func()) {
	t.Helper()
	st := store.Open(filepath.Join(dir, "map.json"))
	m, applied, err := st.Load()
	if err != nil {
		t.Fatal(err)
	}
	if m == nil {
		m = transitions.Initial(params)
	}
	live := liveness.New(m.Params.Grace, time.Now)
	a := api.New(m, applied, st, live)
	ctx, cancel := context.WithCancel(context.Background())
	go live.Run(ctx, a.OnOut)
	srv := httptest.NewServer(a.Handler())
	return srv.URL, func() { cancel(); srv.Close() }
}

func post(t *testing.T, url string, body any) (int, wire.NodeReply) {
	t.Helper()
	b, _ := json.Marshal(body)
	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var rep wire.NodeReply
	json.NewDecoder(resp.Body).Decode(&rep)
	return resp.StatusCode, rep
}

func getMap(t *testing.T, url string) *mapmodel.Map {
	t.Helper()
	var m mapmodel.Map
	if st, err := wire.NewClient(time.Second).DoJSON(context.Background(), http.MethodGet, url+"/map", nil, &m); err != nil || st != 200 {
		t.Fatalf("GET /map: %d %v", st, err)
	}
	return &m
}

func want(t *testing.T, what string, gotStatus int, got wire.NodeReply, status int, epoch mapmodel.Epoch, state mapmodel.State) {
	t.Helper()
	if gotStatus != status || (status == 200 && (got.Epoch != epoch || got.State != state)) {
		t.Fatalf("%s: %d %+v, want %d {epoch %d, %s}", what, gotStatus, got, status, epoch, state)
	}
}

func hb(id, boot string) wire.HeartbeatReq {
	return wire.HeartbeatReq{NodeID: mapmodel.NodeID(id), BootID: boot}
}

func TestMonitorAPI(t *testing.T) {
	dir := t.TempDir()
	url, stop := start(t, dir)

	st, rep := post(t, url+"/node/heartbeat", hb("a", "1"))
	want(t, "heartbeat from unknown node", st, rep, 404, 0, "")
	for i, id := range []string{"a", "b", "c"} {
		st, rep = post(t, url+"/node/join", wire.JoinReq{NodeID: mapmodel.NodeID(id), BootID: "1", Addr: "http://" + id})
		want(t, "join "+id, st, rep, 200, mapmodel.Epoch(i+1), mapmodel.Up)
	}
	st, rep = post(t, url+"/node/join", wire.JoinReq{NodeID: "a", BootID: "1", Addr: "http://a"})
	want(t, "duplicate join", st, rep, 409, 0, "")

	st, rep = post(t, url+"/node/heartbeat", hb("a", "1"))
	want(t, "heartbeat, same boot", st, rep, 200, 3, mapmodel.Up)
	st, rep = post(t, url+"/node/heartbeat", hb("a", "2"))
	want(t, "heartbeat, new boot", st, rep, 200, 4, mapmodel.Up)
	st, rep = post(t, url+"/node/heartbeat", hb("a", "2"))
	want(t, "heartbeat, recorded boot", st, rep, 200, 4, mapmodel.Up)

	st, _ = post(t, url+"/pg/0/backfill_done?slot=0", wire.BackfillDoneReq{Epoch: 1, NodeID: "a"})
	if st != 409 {
		t.Fatalf("stale backfill_done: %d, want 409", st)
	}
	if m := getMap(t, url); m.Epoch != 4 || len(m.PGs) != params.PGNum {
		t.Fatalf("map: epoch %d with %d PGs, want 4 with %d", m.Epoch, len(m.PGs), params.PGNum)
	}

	// Restart from the same directory: the epoch survives, and nobody is marked
	// out before a full grace period without heartbeats (ADRs 0001, 0008).
	stop()
	url, stop = start(t, dir)
	defer stop()
	if m := getMap(t, url); m.Epoch != 4 {
		t.Fatalf("after restart: epoch %d, want 4", m.Epoch)
	}
	time.Sleep(params.Grace / 2)
	for id, n := range getMap(t, url).Nodes {
		if n.State != mapmodel.Up {
			t.Fatalf("after restart: %s is %s before grace elapsed", id, n.State)
		}
	}
	st, rep = post(t, url+"/node/heartbeat", hb("a", "2"))
	want(t, "boot unknown after restart counts as new", st, rep, 200, 5, mapmodel.Up)

	deadline := time.Now().Add(5 * time.Second)
	for {
		m := getMap(t, url)
		if m.Nodes["a"].State == mapmodel.Out && m.Nodes["b"].State == mapmodel.Out && m.Nodes["c"].State == mapmodel.Out {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("silent nodes not marked out: %+v", m.Nodes)
		}
		time.Sleep(50 * time.Millisecond)
	}
	st, rep = post(t, url+"/node/heartbeat", hb("b", "1"))
	if st != 200 || rep.State != mapmodel.Out {
		t.Fatalf("heartbeat from out node: %d %+v, want 200 out", st, rep)
	}
}
