package primary

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"mycelium/core/mapmodel"
	"mycelium/core/wire"
	"mycelium/node/codec"
	"mycelium/node/peers"
)

var (
	errDisagree = errors.New("holders disagree")
	errChecksum = errors.New("shard checksum mismatch")
	errTooFew   = errors.New("fewer than k shards")
)

// read: under the object lock, GET the latest committed shard from B (self first,
// as the primary is B's lowest slot) until k agree on a version, absence counting
// as one (k absent -> not found). Disagreement, checksum failure or fewer than k
// answers -> inactive + re-peer.
func (s *pgState) read(ctx context.Context, id string) ([]byte, mapmodel.Version, error) {
	am, release, err := s.begin(true)
	if err != nil {
		return nil, mapmodel.Version{}, err
	}
	defer release()
	defer s.locks.lock(id)()

	shards := make([][]byte, am.Params.N())
	var agreed *wire.Shard
	count, absent := 0, 0
	for _, slot := range am.B(s.id) {
		sh, err := s.mg.peers.Get(ctx, am, s.id, slot, id, nil)
		switch {
		case errors.Is(err, peers.ErrNotFound) || (err == nil && sh.Deleted):
			if agreed != nil {
				return nil, mapmodel.Version{}, s.fail(am, errDisagree)
			}
			if absent++; absent == am.Params.K {
				return nil, mapmodel.Version{}, errNotFound
			}
			continue
		case errors.Is(err, peers.ErrStepDown):
			return nil, mapmodel.Version{}, s.fail(am, err)
		case err != nil:
			continue // no answer
		case codec.Sha256(sh.Data) != sh.Sha256:
			return nil, mapmodel.Version{}, s.fail(am, errChecksum)
		case absent > 0 || (agreed != nil && agreed.Version != sh.Version):
			return nil, mapmodel.Version{}, s.fail(am, errDisagree)
		}
		agreed = sh
		shards[slot] = sh.Data
		if count++; count == am.Params.K {
			obj, err := s.mg.codec.Decode(shards, sh.Size)
			if err != nil {
				return nil, mapmodel.Version{}, s.fail(am, err)
			}
			return obj, sh.Version, nil
		}
	}
	return nil, mapmodel.Version{}, s.fail(am, errTooFew)
}

func (s *pgState) write(ctx context.Context, id string, data []byte) (mapmodel.Version, error) {
	return s.put(ctx, id, data, false)
}

// delete writes a tombstone; committing it removes the object.
func (s *pgState) delete(ctx context.Context, id string) (mapmodel.Version, error) {
	return s.put(ctx, id, nil, true)
}

// put: under the object lock, v = (epoch, ++seq); encode, PUT to every slot in U,
// commit on every slot in U. Any failure: inactive + re-peer.
func (s *pgState) put(ctx context.Context, id string, data []byte, deleted bool) (mapmodel.Version, error) {
	am, release, err := s.begin(true)
	if err != nil {
		return mapmodel.Version{}, err
	}
	defer release()
	defer s.locks.lock(id)()

	v := s.nextVersion(am)
	shards := make([][]byte, am.Params.N())
	if !deleted {
		if shards, err = s.mg.codec.Encode(data); err != nil {
			return mapmodel.Version{}, s.fail(am, err)
		}
	}
	if err := s.putAndCommit(ctx, am, id, v, int64(len(data)), deleted, shards, am.U(s.id)); err != nil {
		return mapmodel.Version{}, s.fail(am, err)
	}
	return v, nil
}

// putAndCommit PUTs shards[slot] of version v to each of slots (all must ack), then
// commits v on them (all must ack). If a PUT fails, nothing was committed yet and
// v is discarded best-effort. After a commit failure v is left for peering: some
// holders may have committed it, and a discard would delete it there with nothing older left.
func (s *pgState) putAndCommit(ctx context.Context, am *mapmodel.Map, id string, v mapmodel.Version, size int64, deleted bool, shards [][]byte, slots []int) error {
	err := s.forSlots(slots, func(slot int) error {
		sh := &wire.Shard{ID: id, Version: v, Size: size, Deleted: deleted, Data: shards[slot]}
		sh.Sha256 = codec.Sha256(sh.Data)
		return s.mg.peers.Put(ctx, am, s.id, slot, sh)
	})
	items := []wire.IDVersion{{ID: id, Version: v}}
	if err != nil {
		s.discardBestEffort(ctx, am, items, slots)
		return err
	}
	return s.forSlots(slots, func(slot int) error {
		return s.mg.peers.Commit(ctx, am, s.id, slot, items)
	})
}

// discardBestEffort ignores errors and the caller's cancellation (the client may be gone).
func (s *pgState) discardBestEffort(ctx context.Context, am *mapmodel.Map, items []wire.IDVersion, slots []int) {
	ctx = context.WithoutCancel(ctx)
	s.forSlots(slots, func(slot int) error {
		return s.mg.peers.Discard(ctx, am, s.id, slot, items)
	})
}

// fetchShards reads version v of id from B (self first) until k shards with a valid
// checksum are found, or one tombstone. It returns n slots (nil = missing) and the
// metadata of one shard.
func (s *pgState) fetchShards(ctx context.Context, am *mapmodel.Map, id string, v mapmodel.Version) ([][]byte, *wire.Shard, error) {
	shards := make([][]byte, am.Params.N())
	count := 0
	for _, slot := range am.B(s.id) {
		sh, err := s.mg.peers.Get(ctx, am, s.id, slot, id, &v)
		if errors.Is(err, peers.ErrStepDown) {
			return nil, nil, err
		}
		if err != nil || codec.Sha256(sh.Data) != sh.Sha256 {
			continue
		}
		if sh.Deleted {
			return nil, sh, nil
		}
		shards[slot] = sh.Data
		if count++; count == am.Params.K {
			return shards, sh, nil
		}
	}
	return nil, nil, fmt.Errorf("%w of %s at %s", errTooFew, id, v)
}

// reencode rebuilds version v of id from B and PUTs + commits its shard on each of
// slots. A tombstone needs no decoding.
func (s *pgState) reencode(ctx context.Context, am *mapmodel.Map, id string, v mapmodel.Version, slots []int) error {
	shards, meta, err := s.fetchShards(ctx, am, id, v)
	if err != nil {
		return err
	}
	out := make([][]byte, am.Params.N())
	if !meta.Deleted {
		obj, err := s.mg.codec.Decode(shards, meta.Size)
		if err != nil {
			return err
		}
		if out, err = s.mg.codec.Encode(obj); err != nil {
			return err
		}
	}
	return s.putAndCommit(ctx, am, id, v, meta.Size, meta.Deleted, out, slots)
}

// forSlots runs fn on every slot in parallel. It returns ErrStepDown if any call
// was fenced, else the first error.
func (s *pgState) forSlots(slots []int, fn func(slot int) error) error {
	errs := make([]error, len(slots))
	var wg sync.WaitGroup
	for i, slot := range slots {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = fn(slot)
		}()
	}
	wg.Wait()
	var first error
	for _, err := range errs {
		if errors.Is(err, peers.ErrStepDown) {
			return err
		}
		if first == nil {
			first = err
		}
	}
	return first
}
