package raft_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"mycelium/core/mapmodel"
	"mycelium/core/wire"
	"mycelium/monitor/api"
	"mycelium/monitor/liveness"
	"mycelium/monitor/raft"
	"mycelium/monitor/store"
	"mycelium/monitor/transitions"
)

var params = mapmodel.Params{
	K: 1, M: 2, PGNum: 4, MinSize: 2, BootstrapNodes: 3,
	Lease: time.Second, Grace: 2 * time.Second, MaxObject: 1 << 20,
}

// swap lets the HTTP server exist (and have a URL) before its handler does.
type swap struct{ h atomic.Value }

func (s *swap) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h, ok := s.h.Load().(http.Handler); ok {
		h.ServeHTTP(w, r)
		return
	}
	http.Error(w, "starting", http.StatusServiceUnavailable)
}

type replica struct {
	id     string
	srv    *httptest.Server
	st     *store.Store
	cancel context.CancelFunc
}

func status(url string) int {
	resp, err := http.Get(url + "/map")
	if err != nil {
		return 0
	}
	resp.Body.Close()
	return resp.StatusCode
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); !cond(); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

func join(t *testing.T, url, id string) wire.NodeReply {
	t.Helper()
	var rep wire.NodeReply
	st, err := wire.NewClient(5*time.Second).DoJSON(context.Background(), http.MethodPost, url+"/node/join",
		wire.JoinReq{NodeID: mapmodel.NodeID(id), BootID: "1", Addr: "http://" + id}, &rep)
	if err != nil || st != 200 {
		t.Fatalf("join %s: %d %v", id, st, err)
	}
	return rep
}

// ADR 0009: one leader, identical replicas, committed events survive a leader crash.
func TestRaftMonitors(t *testing.T) {
	var reps []*replica
	peers := map[string]string{}
	handlers := map[string]*swap{}
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("m%d", i)
		h := &swap{}
		r := &replica{id: id, srv: httptest.NewServer(h)}
		handlers[id], peers[id] = h, r.srv.URL
		reps = append(reps, r)
	}
	for _, r := range reps {
		dir := t.TempDir()
		r.st = store.Open(filepath.Join(dir, "map.json"))
		a := api.New(transitions.Initial(params), 0, r.st, liveness.New(params.Grace, time.Now))
		rf, err := raft.New(raft.Config{
			ID: r.id, Peers: peers, Dir: filepath.Join(dir, "raft"),
			ElectionTimeout: 300 * time.Millisecond, HeartbeatInterval: 50 * time.Millisecond,
		}, wire.NewClient(time.Second), a.ApplyEntry, a.OnLeader)
		if err != nil {
			t.Fatal(err)
		}
		a.SetRaft(rf)
		handlers[r.id].h.Store(a.Handler())
		ctx, cancel := context.WithCancel(context.Background())
		r.cancel = cancel
		go rf.Run(ctx)
		t.Cleanup(func() { cancel(); r.srv.Close() })
	}

	leader := func(live []*replica) (l *replica, others []*replica) {
		for _, r := range live {
			switch status(r.srv.URL) {
			case 200:
				if l != nil {
					t.Fatalf("two leaders at once: %s and %s", l.id, r.id)
				}
				l = r
			case wire.StatusNotPrimary:
				others = append(others, r)
			}
		}
		return l, others
	}
	var l *replica
	waitFor(t, "a leader", func() bool {
		var others []*replica
		l, others = leader(reps)
		return l != nil && len(others) == 2
	})

	for i, id := range []string{"n1", "n2", "n3"} {
		if rep := join(t, l.srv.URL, id); rep.Epoch != mapmodel.Epoch(i+1) {
			t.Fatalf("join %s: epoch %d", id, rep.Epoch)
		}
	}
	sameMaps := func(rs []*replica, epoch mapmodel.Epoch) func() bool {
		return func() bool {
			var first *mapmodel.Map
			for _, r := range rs {
				m, _, err := r.st.Load()
				if err != nil || m == nil || m.Epoch != epoch {
					return false
				}
				if first != nil && !reflect.DeepEqual(first, m) {
					t.Fatalf("replicas %s and %s diverged at epoch %d", rs[0].id, r.id, epoch)
				}
				first = m
			}
			return true
		}
	}
	waitFor(t, "all replicas to persist epoch 3", sameMaps(reps, 3))

	// Crash the leader.
	l.cancel()
	l.srv.Close()
	var survivors []*replica
	for _, r := range reps {
		if r != l {
			survivors = append(survivors, r)
		}
	}
	var nl *replica
	var follower []*replica
	waitFor(t, "a new leader", func() bool {
		nl, follower = leader(survivors)
		return nl != nil && len(follower) == 1
	})

	var m mapmodel.Map
	if st, err := wire.NewClient(time.Second).DoJSON(context.Background(), http.MethodGet, nl.srv.URL+"/map", nil, &m); err != nil || st != 200 {
		t.Fatalf("GET /map from new leader: %d %v", st, err)
	}
	for _, id := range []mapmodel.NodeID{"n1", "n2", "n3"} {
		if m.Nodes[id].State != mapmodel.Up {
			t.Fatalf("new leader lost committed join of %s: %+v", id, m.Nodes)
		}
	}
	if rep := join(t, nl.srv.URL, "n4"); rep.Epoch != m.Epoch+1 {
		t.Fatalf("join n4 on new leader: epoch %d, want %d", rep.Epoch, m.Epoch+1)
	}
	if st := status(follower[0].srv.URL); st != wire.StatusNotPrimary {
		t.Fatalf("follower answered %d, want 421", st)
	}
	waitFor(t, "survivors to persist the new epoch", sameMaps(survivors, m.Epoch+1))
}
