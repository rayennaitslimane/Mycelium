// Command node runs a holder and, for the PGs the map assigns it, a primary.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"mycelium/core/mapmodel"
	"mycelium/core/wire"
	"mycelium/node/agent"
	"mycelium/node/codec"
	"mycelium/node/holder"
	"mycelium/node/peers"
	"mycelium/node/primary"
	"mycelium/node/store"
)

const requestTimeout = 10 * time.Second

var (
	nodeID   = flag.String("id", "", "node id")
	addr     = flag.String("addr", "", "base URL other components use to reach this node")
	listen   = flag.String("listen", ":9000", "HTTP listen address")
	dataDir  = flag.String("data", "./node-data", "shard store root directory")
	monitors = flag.String("monitors", "", "comma-separated monitor base URLs")
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
	var mons []string
	for _, s := range strings.Split(*monitors, ",") {
		if s = strings.TrimSuffix(strings.TrimSpace(s), "/"); s != "" {
			mons = append(mons, s)
		}
	}
	if *nodeID == "" || *addr == "" || len(mons) == 0 {
		return errors.New("need -id, -addr and -monitors")
	}
	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		return err
	}
	st, err := store.Open(*dataDir)
	if err != nil {
		return err
	}
	defer st.Close()

	self := mapmodel.NodeID(*nodeID)
	client := wire.NewClient(requestTimeout)
	a := agent.New(agent.Config{NodeID: self, Addr: strings.TrimSuffix(*addr, "/"), Monitors: mons}, st, client)
	h := holder.New(a, st)
	mux := http.NewServeMux()
	h.Register(mux)

	// Serve before booting: once joined, peers reach this holder right away.
	srv := &http.Server{Addr: *listen, Handler: mux}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	go func() {
		<-ctx.Done()
		srv.Close()
	}()
	log.Printf("node %s: listening on %s", self, *listen)

	if err := a.Boot(ctx); err != nil {
		return err
	}
	p := a.Map().Params
	c, err := codec.New(p.K, p.M)
	if err != nil {
		return err
	}
	defer c.Close()
	mg := primary.New(self, a, peers.New(self, h, client), st, c)
	defer mg.Close()
	mg.Register(mux)
	a.Subscribe(mg.OnMap)
	mg.OnMap(a.Map())
	go a.Run(ctx)

	if err := <-errc; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
