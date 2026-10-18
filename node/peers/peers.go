// Package peers is the holder client used by the primary. It stamps the map epoch,
// maps 409 to ErrStepDown, and calls the local holder directly for the node's own slot.
package peers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"mycelium/core/mapmodel"
	"mycelium/core/wire"
	"mycelium/node/holder"
	"mycelium/node/store"
)

// ErrStepDown means a holder fenced this primary: drop the role and refetch the map.
var ErrStepDown = errors.New("fenced: step down")

// ErrNotFound means the holder has no such shard (or no committed version).
var ErrNotFound = errors.New("shard not found")

type Peers struct {
	self   mapmodel.NodeID
	holder *holder.Holder
	client *wire.Client
}

func New(self mapmodel.NodeID, h *holder.Holder, c *wire.Client) *Peers {
	return &Peers{self: self, holder: h, client: c}
}

func (p *Peers) Put(ctx context.Context, m *mapmodel.Map, pg mapmodel.PGID, slot int, sh *wire.Shard) error {
	addr, local, err := p.target(m, pg, slot)
	if err != nil {
		return err
	}
	if local {
		return localErr(p.holder.Put(pg, m.Epoch, sh))
	}
	u := fmt.Sprintf("%s/pg/%d/shards/%s/%s", addr, pg, url.PathEscape(sh.ID), sh.Version)
	h := holder.ShardHeader(sh)
	h.Set(wire.HeaderEpoch, strconv.FormatUint(uint64(m.Epoch), 10))
	status, _, _, err := p.client.DoRaw(ctx, http.MethodPut, u, h, sh.Data)
	return remoteErr(status, err)
}

// Get returns the latest committed shard if v is nil, else the named version.
func (p *Peers) Get(ctx context.Context, m *mapmodel.Map, pg mapmodel.PGID, slot int, id string, v *mapmodel.Version) (*wire.Shard, error) {
	addr, local, err := p.target(m, pg, slot)
	if err != nil {
		return nil, err
	}
	if local {
		sh, err := p.holder.Get(pg, m.Epoch, id, v)
		return sh, localErr(err)
	}
	u := fmt.Sprintf("%s/pg/%d/shards/%s", addr, pg, url.PathEscape(id))
	if v != nil {
		u += "/" + v.String()
	}
	h := http.Header{}
	h.Set(wire.HeaderEpoch, strconv.FormatUint(uint64(m.Epoch), 10))
	status, rh, body, err := p.client.DoRaw(ctx, http.MethodGet, u, h, nil)
	if err := remoteErr(status, err); err != nil {
		return nil, err
	}
	got, err := mapmodel.ParseVersion(rh.Get(wire.HeaderVersion))
	if err != nil {
		return nil, err
	}
	return holder.ParseShard(id, got, rh, body)
}

func (p *Peers) Commit(ctx context.Context, m *mapmodel.Map, pg mapmodel.PGID, slot int, items []wire.IDVersion) error {
	addr, local, err := p.target(m, pg, slot)
	if err != nil {
		return err
	}
	if local {
		return localErr(p.holder.Commit(pg, m.Epoch, items))
	}
	return p.post(ctx, addr, pg, "commit", wire.BatchReq{Epoch: m.Epoch, Items: items}, nil)
}

func (p *Peers) Discard(ctx context.Context, m *mapmodel.Map, pg mapmodel.PGID, slot int, items []wire.IDVersion) error {
	addr, local, err := p.target(m, pg, slot)
	if err != nil {
		return err
	}
	if local {
		return localErr(p.holder.Discard(pg, m.Epoch, items))
	}
	return p.post(ctx, addr, pg, "discard", wire.BatchReq{Epoch: m.Epoch, Items: items}, nil)
}

func (p *Peers) Scan(ctx context.Context, m *mapmodel.Map, pg mapmodel.PGID, slot int) ([]wire.IDVersions, error) {
	addr, local, err := p.target(m, pg, slot)
	if err != nil {
		return nil, err
	}
	if local {
		r, err := p.holder.Scan(pg, m.Epoch)
		return r, localErr(err)
	}
	var rep wire.IDVersionsReply
	err = p.post(ctx, addr, pg, "scan", wire.ScanReq{Epoch: m.Epoch}, &rep)
	return rep.Items, err
}

func (p *Peers) Lookup(ctx context.Context, m *mapmodel.Map, pg mapmodel.PGID, slot int, ids []string) ([]wire.IDVersions, error) {
	addr, local, err := p.target(m, pg, slot)
	if err != nil {
		return nil, err
	}
	if local {
		r, err := p.holder.Lookup(pg, m.Epoch, ids)
		return r, localErr(err)
	}
	var rep wire.IDVersionsReply
	err = p.post(ctx, addr, pg, "lookup", wire.LookupReq{Epoch: m.Epoch, IDs: ids}, &rep)
	return rep.Items, err
}

func (p *Peers) post(ctx context.Context, addr string, pg mapmodel.PGID, op string, in, out any) error {
	status, err := p.client.DoJSON(ctx, http.MethodPost, fmt.Sprintf("%s/pg/%d/%s", addr, pg, op), in, out)
	return remoteErr(status, err)
}

// target resolves slot -> node address; local is true for the node's own slot.
func (p *Peers) target(m *mapmodel.Map, pg mapmodel.PGID, slot int) (addr string, local bool, err error) {
	if int(pg) >= len(m.PGs) || slot < 0 || slot >= len(m.PGs[pg]) {
		return "", false, fmt.Errorf("no slot %d in pg %d", slot, pg)
	}
	n := m.PGs[pg][slot].Node
	if n == "" {
		return "", false, fmt.Errorf("slot %d of pg %d is vacant", slot, pg)
	}
	if n == p.self {
		return "", true, nil
	}
	return m.Nodes[n].Addr, false, nil
}

func localErr(err error) error {
	var fe *store.FencedError
	switch {
	case errors.As(err, &fe):
		return ErrStepDown
	case errors.Is(err, store.ErrNotFound):
		return ErrNotFound
	}
	return err
}

func remoteErr(status int, err error) error {
	switch {
	case err != nil:
		return err
	case status/100 == 2:
		return nil
	case status == wire.StatusFenced:
		return ErrStepDown
	case status == http.StatusNotFound:
		return ErrNotFound
	}
	return fmt.Errorf("holder: status %d", status)
}
