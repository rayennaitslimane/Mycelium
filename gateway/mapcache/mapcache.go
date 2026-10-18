// Package mapcache fetches GET /map and keeps the latest epoch.
package mapcache

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"mycelium/core/mapmodel"
	"mycelium/core/wire"
)

type Cache struct {
	monitors []string
	client   *wire.Client

	mu   sync.RWMutex
	m    *mapmodel.Map
	next int // monitor to try first: the last one that answered
}

func New(monitors []string, c *wire.Client) *Cache {
	return &Cache{monitors: monitors, client: c}
}

// Get returns the cached map, or nil before the first successful Refresh.
func (c *Cache) Get() *mapmodel.Map {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.m
}

// Refresh fetches the map from the monitors in turn (a non-leader answers 421) and
// keeps it only if its epoch is newer. It returns the cached map, which may be
// nil, and an error if no monitor answered.
func (c *Cache) Refresh(ctx context.Context) (*mapmodel.Map, error) {
	c.mu.RLock()
	start := c.next
	c.mu.RUnlock()

	var errs []error
	for i := range c.monitors {
		idx := (start + i) % len(c.monitors)
		var m mapmodel.Map
		status, err := c.client.DoJSON(ctx, http.MethodGet, c.monitors[idx]+"/map", nil, &m)
		if err == nil && status != http.StatusOK {
			err = fmt.Errorf("status %d", status)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", c.monitors[idx], err))
			continue
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		c.next = idx
		if c.m == nil || m.Epoch > c.m.Epoch {
			c.m = &m
		}
		return c.m, nil
	}
	return c.Get(), fmt.Errorf("fetch map: %w", errors.Join(errs...))
}
