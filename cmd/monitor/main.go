// Command monitor owns the map and node states.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"mycelium/core/mapmodel"
	"mycelium/core/wire"
	"mycelium/monitor/api"
	"mycelium/monitor/liveness"
	"mycelium/monitor/raft"
	"mycelium/monitor/store"
	"mycelium/monitor/transitions"
)

const (
	raftHeartbeat = 100 * time.Millisecond
	raftElection  = time.Second
)

var (
	listen  = flag.String("listen", ":7000", "HTTP listen address")
	dataDir = flag.String("data", "./monitor-data", "directory for the durable map (and Raft state)")

	// Bootstrap config: used only when no map is persisted yet. With Raft, every
	// monitor must be started with identical values.
	k              = flag.Int("k", 1, "data shards")
	m              = flag.Int("m", 2, "coding shards")
	pgNum          = flag.Int("pg-num", 64, "number of placement groups")
	minSize        = flag.Int("min-size", 0, "min |B| for a PG to be serviceable (0 = k+1)")
	bootstrapNodes = flag.Int("bootstrap-nodes", 3, "nodes required before the PG table is created (>= k+m)")
	lease          = flag.Duration("lease", 4*time.Second, "node serve lease")
	grace          = flag.Duration("grace", 10*time.Second, "silence before a node is marked out (>= 2*lease)")
	maxObject      = flag.Int64("max-object", 4<<20, "maximum object size in bytes")

	raftID    = flag.String("id", "", "Raft id of this monitor (empty = single instance)")
	raftPeers = flag.String("peers", "", "comma-separated id=url Raft peers, including self")
)

func main() {
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		return err
	}
	st := store.Open(filepath.Join(*dataDir, "map.json"))
	mp, applied, err := st.Load()
	if err != nil {
		return err
	}
	if mp == nil {
		p := mapmodel.Params{
			K: *k, M: *m, PGNum: *pgNum, MinSize: *minSize, BootstrapNodes: *bootstrapNodes,
			Lease: *lease, Grace: *grace, MaxObject: *maxObject,
		}
		if p.MinSize == 0 {
			p.MinSize = p.K + 1
		}
		if err := p.Validate(); err != nil {
			return err
		}
		mp = transitions.Initial(p)
	} else {
		log.Printf("monitor: loaded map at epoch %d; bootstrap flags ignored", mp.Epoch)
	}

	live := liveness.New(mp.Params.Grace, time.Now)
	a := api.New(mp, applied, st, live)
	if *raftID != "" {
		peers, err := parsePeers(*raftPeers)
		if err != nil {
			return err
		}
		r, err := raft.New(raft.Config{
			ID:                *raftID,
			Peers:             peers,
			Dir:               filepath.Join(*dataDir, "raft"),
			ElectionTimeout:   raftElection,
			HeartbeatInterval: raftHeartbeat,
			Applied:           applied,
		}, wire.NewClient(raftElection), a.ApplyEntry, a.OnLeader)
		if err != nil {
			return err
		}
		a.SetRaft(r)
		go r.Run(ctx)
	}
	go live.Run(ctx, a.OnOut)

	srv := &http.Server{Addr: *listen, Handler: a.Handler()}
	go func() {
		<-ctx.Done()
		srv.Close()
	}()
	log.Printf("monitor: listening on %s", *listen)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func parsePeers(s string) (map[string]string, error) {
	peers := map[string]string{}
	for _, kv := range strings.Split(s, ",") {
		id, url, ok := strings.Cut(strings.TrimSpace(kv), "=")
		if !ok || id == "" || url == "" {
			return nil, fmt.Errorf("bad peer %q, want id=url", kv)
		}
		peers[id] = strings.TrimSuffix(url, "/")
	}
	return peers, nil
}
