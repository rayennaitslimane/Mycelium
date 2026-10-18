// Command gateway serves /objects/{id} and forwards each request to the PG's primary.
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

	"mycelium/core/wire"
	"mycelium/gateway/front"
	"mycelium/gateway/mapcache"
)

const (
	requestTimeout = 30 * time.Second
	maxRetries     = 5
)

var (
	listen   = flag.String("listen", ":8080", "HTTP listen address")
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
	if len(mons) == 0 {
		return errors.New("need -monitors")
	}
	client := wire.NewClient(requestTimeout)
	maps := mapcache.New(mons, client)
	if _, err := maps.Refresh(ctx); err != nil {
		log.Printf("gateway: %v (will retry on first request)", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/objects/", front.New(maps, client, maxRetries))
	srv := &http.Server{Addr: *listen, Handler: mux}
	go func() {
		<-ctx.Done()
		srv.Close()
	}()
	log.Printf("gateway: listening on %s", *listen)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
