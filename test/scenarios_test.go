package test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mycelium/core/mapmodel"
	"mycelium/core/seal"
	"mycelium/core/wire"
	"mycelium/node/codec"
	"mycelium/node/peers"
)

// ADR 0003: the client contract, through the gateway.
func TestClientContract(t *testing.T) {
	c := NewCluster(t, 3, 2, 1)

	v1, _ := mapmodel.ParseVersion(c.Put("greeting", "hello"))
	c.Expect("greeting", ptr("hello"))
	v2, err := mapmodel.ParseVersion(c.Put("greeting", "hello again"))
	if err != nil || !v1.Less(v2) {
		t.Fatalf("second write got version %v, want > %v (%v)", v2, v1, err)
	}
	if r := c.Must(http.MethodGet, "greeting", nil); r.Version != v2.String() {
		t.Fatalf("GET version %q, want %q", r.Version, v2)
	}

	for id, value := range map[string]string{
		"empty":             "",
		"dir/sub file ü%":   "odd id",
		"exactly-max":       strings.Repeat("x", maxObject),
		"not-multiple-of-k": "abc",
	} {
		c.Put(id, value)
		c.Expect(id, ptr(value))
	}
	if r := c.Must(http.MethodPut, "too-big", []byte(strings.Repeat("x", maxObject+1))); r.Status != http.StatusRequestEntityTooLarge {
		t.Fatalf("PUT over max_object: %d, want 413", r.Status)
	}

	for i := range 2 { // deleting is idempotent
		if r := c.Must(http.MethodDelete, "greeting", nil); r.Status != 200 || r.Version == "" {
			t.Fatalf("DELETE #%d: %d %q", i+1, r.Status, r.Body)
		}
	}
	c.Expect("greeting", nil)
	if r := c.Must(http.MethodDelete, "never-existed", nil); r.Status != 200 {
		t.Fatalf("DELETE of a missing object: %d", r.Status)
	}
	c.Expect("never-existed", nil)
}

// ADRs 0002, 0007, 0010: a failed node's slots move to the spare, which is
// backfilled (rebuilding shards from parity); the failed node comes back empty.
func TestNodeFailureAndBackfill(t *testing.T) {
	c := NewCluster(t, 4, 2, 1)
	want := map[string]string{}
	for i := range 30 {
		id := fmt.Sprintf("obj/%d", i)
		want[id] = strings.Repeat(id+";", 1+i*37)
		c.Put(id, want[id])
	}

	victim := c.Node(c.Map().PGs[0][0].Node)
	c.Kill(victim)
	c.WaitFor(string(victim.ID)+" out and the spare backfilled", func() bool {
		m := c.Map()
		return m.Nodes[victim.ID].State == mapmodel.Out && Healthy(m)
	})
	for id, v := range want {
		c.Expect(id, ptr(v))
	}

	c.Restart(victim)
	m := c.Map()
	if m.Nodes[victim.ID].State != mapmodel.Up {
		t.Fatalf("restarted %s is %s, want up", victim.ID, m.Nodes[victim.ID].State)
	}
	for pg, slots := range m.PGs {
		for _, s := range slots {
			if s.Node == victim.ID {
				t.Fatalf("rejoined %s holds a slot of pg %d; it should be a spare", victim.ID, pg)
			}
		}
	}
	if left, _ := filepath.Glob(filepath.Join(victim.dir, "pg-*")); len(left) != 0 {
		t.Fatalf("rejoined %s did not purge its data: %v", victim.ID, left)
	}
	for id, v := range want {
		c.Expect(id, ptr(v))
	}
}

// ADRs 0004, 0006: kill the primary while clients write. Afterwards every object
// holds its last acknowledged value, or a later attempt whose outcome was unknown.
// Each writer owns its ids, so per-object history is sequential.
func TestPrimaryFailoverUnderLoad(t *testing.T) {
	c := NewCluster(t, 4, 2, 1)

	type history struct {
		acked    *string   // nil: deleted, or never written
		attempts []*string // unacknowledged since the last ack
	}
	const writers, idsPerWriter = 4, 5
	hist := make([]map[string]*history, writers)
	var acks, acksAfterKill atomic.Int64
	var killed atomic.Bool
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := range writers {
		hist[w] = map[string]*history{}
		for i := range idsPerWriter {
			hist[w][fmt.Sprintf("w%d/%d", w, i)] = &history{}
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			rnd := rand.New(rand.NewPCG(uint64(w), 42))
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				id := fmt.Sprintf("w%d/%d", w, rnd.IntN(idsPerWriter))
				method, value := http.MethodDelete, (*string)(nil)
				if rnd.IntN(100) >= 15 {
					method, value = http.MethodPut, ptr(fmt.Sprintf("%s#%d#%s", id, n, strings.Repeat("v", rnd.IntN(3000))))
				}
				var body []byte
				if value != nil {
					body = []byte(*value)
				}
				r, err := c.Do(method, id, body)
				h := hist[w][id]
				if err == nil && r.Status == 200 {
					h.acked, h.attempts = value, nil
					acks.Add(1)
					if killed.Load() {
						acksAfterKill.Add(1)
					}
				} else {
					h.attempts = append(h.attempts, value)
				}
			}
		}()
	}

	time.Sleep(1500 * time.Millisecond)
	victim := c.Node(c.Map().PGs[0][0].Node)
	c.Kill(victim)
	killed.Store(true)
	c.WaitFor("failover and backfill", func() bool {
		m := c.Map()
		return m.Nodes[victim.ID].State == mapmodel.Out && Healthy(m)
	})
	time.Sleep(time.Second) // keep writing on the recovered cluster
	close(stop)
	wg.Wait()

	if acks.Load() < 50 || acksAfterKill.Load() == 0 {
		t.Fatalf("too little load to mean anything: %d acks, %d after the kill", acks.Load(), acksAfterKill.Load())
	}
	for w := range writers {
		for id, h := range hist[w] {
			r := c.Must(http.MethodGet, id, nil)
			var got *string
			switch r.Status {
			case 200:
				got = &r.Body
			case 404:
			default:
				t.Fatalf("GET %s: %d %q", id, r.Status, r.Body)
			}
			allowed := append([]*string{h.acked}, h.attempts...)
			if !slices.ContainsFunc(allowed, func(a *string) bool {
				return (a == nil && got == nil) || (a != nil && got != nil && *a == *got)
			}) {
				t.Errorf("%s holds %q; last acked %q, %d unacknowledged attempts since",
					id, show(got), show(h.acked), len(h.attempts))
			}
		}
	}
	t.Logf("%d acknowledged writes, %d after the kill", acks.Load(), acksAfterKill.Load())
}

func show(s *string) string {
	if s == nil {
		return "<absent>"
	}
	return trim(*s)
}

// ADR 0006: prepare holders directly, force a re-peer, check the outcome.
//   - "fwd": version A staged on 2 slots (= k), version B > A staged on the third.
//     A wins: committed everywhere, re-encoded onto the slot lacking it; B discarded.
//   - "back": version C staged on 1 slot (< k): discarded, the old value stays.
func TestPeeringRollsForwardAndBack(t *testing.T) {
	c := NewCluster(t, 3, 2, 1) // every PG holds all three nodes: B is complete
	c.Put("fwd", "old-fwd")
	c.Put("back", "old-back")

	m := c.Map()
	A := mapmodel.Version{Epoch: m.Epoch, Seq: 1_000_000}
	B := mapmodel.Version{Epoch: m.Epoch, Seq: 2_000_000}
	C := mapmodel.Version{Epoch: m.Epoch, Seq: 3_000_000}
	stage := func(id string, v mapmodel.Version, value string, slots ...int) {
		cd, _ := codec.New(c.Params.K, c.Params.M)
		defer cd.Close()
		shards, _ := cd.Encode([]byte(value))
		pg := m.PGOf(id)
		for _, slot := range slots {
			sh := &wire.Shard{ID: id, Version: v, Size: int64(len(value)), Data: shards[slot]}
			sh.Sha256 = codec.Sha256(sh.Data)
			if err := c.Node(m.PGs[pg][slot].Node).Store.Put(pg, m.Epoch, sh); err != nil {
				t.Fatalf("stage %s %v on slot %d: %v", id, v, slot, err)
			}
		}
	}
	stage("fwd", A, "new-fwd", 0, 1)
	stage("fwd", B, "lost-fwd", 2)
	stage("back", C, "lost-back", 1)

	// A new boot_id bumps the epoch, which makes every primary re-peer.
	var rep wire.NodeReply
	if st, err := wire.NewClient(5*time.Second).DoJSON(context.Background(), http.MethodPost, c.monURL+"/node/heartbeat",
		wire.HeartbeatReq{NodeID: c.Nodes[0].ID, BootID: "forced-repeer"}, &rep); err != nil || st != 200 || rep.Epoch != m.Epoch+1 {
		t.Fatalf("forcing an epoch bump: %d %+v %v", st, rep, err)
	}

	held := func(id string) [][]mapmodel.Version {
		pg := m.PGOf(id)
		var out [][]mapmodel.Version
		for _, s := range m.PGs[pg] {
			items, err := c.Node(s.Node).Store.Lookup(pg, ^mapmodel.Epoch(0), []string{id})
			if err != nil {
				t.Fatal(err)
			}
			var vs []mapmodel.Version
			for _, it := range items {
				vs = append(vs, it.Versions...)
			}
			out = append(out, vs)
		}
		return out
	}
	only := func(id string, v mapmodel.Version) func() bool {
		return func() bool {
			for _, vs := range held(id) {
				if !slices.Equal(vs, []mapmodel.Version{v}) {
					return false
				}
			}
			return true
		}
	}
	c.WaitFor("fwd settled on A on every slot", only("fwd", A))
	c.Expect("fwd", ptr("new-fwd"))
	pg := m.PGOf("fwd")
	for slot, s := range m.PGs[pg] {
		sh, err := c.Node(s.Node).Store.Get(pg, ^mapmodel.Epoch(0), "fwd", nil)
		if err != nil || sh.Version != A {
			t.Fatalf("fwd slot %d: latest committed %v, %v; want %v", slot, sh, err, A)
		}
	}

	var old mapmodel.Version
	c.WaitFor("back's staged version discarded", func() bool {
		vs := held("back")
		for _, h := range vs {
			if len(h) != 1 || h[0] == C {
				return false
			}
		}
		old = vs[0][0]
		return true
	})
	if !only("back", old)() {
		t.Fatalf("back: slots disagree after peering: %v", held("back"))
	}
	c.Expect("back", ptr("old-back"))
}

// ADRs 0005, 0008: a primary cut off from the monitor stops serving before the
// monitor replaces it, and once the holders are fenced it cannot complete a write.
func TestCutOffPrimary(t *testing.T) {
	c := NewCluster(t, 4, 2, 1)
	for i := range 10 {
		c.Put(fmt.Sprintf("co/%d", i), fmt.Sprintf("value %d", i))
	}
	stale := c.Map()
	pg := stale.PGOf("co/0")
	slot, _ := stale.Primary(pg)
	victim := c.Node(stale.PGs[pg][slot].Node)

	c.CutOff(victim)
	direct := func() (int, string) {
		resp, err := http.Get("http://" + victim.Addr + "/objects/co%2F0")
		if err != nil {
			return 0, err.Error()
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	// The serve gate must close (answer "not serving") while the monitor still
	// lists the node as up. A later 503 from a fenced read would not prove that.
	refused := false
	c.WaitFor(string(victim.ID)+" to refuse, then be marked out", func() bool {
		before := c.Map().Nodes[victim.ID].State
		status, body := direct()
		after := c.Map().Nodes[victim.ID].State
		if after == mapmodel.Out {
			if !refused {
				t.Fatalf("%s was still serving when the monitor marked it out", victim.ID)
			}
			return true
		}
		if before == mapmodel.Up && status == http.StatusServiceUnavailable && strings.Contains(body, "not serving") {
			refused = true
		}
		return false
	})

	c.WaitHealthy()
	for i := range 10 {
		c.Expect(fmt.Sprintf("co/%d", i), ptr(fmt.Sprintf("value %d", i)))
	}
	c.Put("co/0", "written by the new primary")
	c.Expect("co/0", ptr("written by the new primary"))

	// The deposed primary, still holding the old map, tries to stage a write on a
	// healthy holder of that PG: the fence must reject it.
	ghost := peers.New("deposed", nil, wire.NewClient(5*time.Second))
	for s, sl := range stale.PGs[pg] {
		if sl.Node == victim.ID {
			continue
		}
		sh := &wire.Shard{ID: "co/0", Version: mapmodel.Version{Epoch: stale.Epoch, Seq: 1 << 40}, Data: []byte("x")}
		sh.Sha256 = codec.Sha256(sh.Data)
		if err := ghost.Put(context.Background(), stale, pg, s, sh); !errors.Is(err, peers.ErrStepDown) {
			t.Fatalf("stale write to slot %d (%s): %v, want ErrStepDown", s, sl.Node, err)
		}
	}
	c.Expect("co/0", ptr("written by the new primary"))
}

// ADR 0011: with a keyring the gateway seals objects, so nodes only ever hold ciphertext.
func TestSealedObjects(t *testing.T) {
	rec, err := seal.Generate(1)
	if err != nil {
		t.Fatal(err)
	}
	ring, err := seal.LoadRing(rec)
	if err != nil {
		t.Fatal(err)
	}
	defer ring.Close()

	c := NewSealedCluster(t, 3, 1, 2, ring)
	secret := strings.Repeat("plaintext-marker;", 8)
	for id, value := range map[string]string{"s": secret, "empty": "", "exactly-max": strings.Repeat("x", maxObject)} {
		c.Put(id, value)
		c.Expect(id, ptr(value))
	}
	if r := c.Must(http.MethodPut, "too-big", []byte(strings.Repeat("x", maxObject+1))); r.Status != http.StatusRequestEntityTooLarge {
		t.Fatalf("PUT over max_object: %d, want 413", r.Status)
	}

	m := c.Map()
	pg := m.PGOf("s")
	for slot, s := range m.PGs[pg] {
		sh, err := c.Node(s.Node).Store.Get(pg, ^mapmodel.Epoch(0), "s", nil)
		if err != nil {
			t.Fatalf("slot %d: %v", slot, err)
		}
		if bytes.Contains(sh.Data, []byte("plaintext-marker")) {
			t.Fatalf("slot %d holds plaintext", slot)
		}
		if int(sh.Size) != len(secret)+seal.Overhead {
			t.Fatalf("slot %d: size %d, want %d", slot, sh.Size, len(secret)+seal.Overhead)
		}
	}
}
