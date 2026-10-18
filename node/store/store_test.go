package store

import (
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"mycelium/core/mapmodel"
	"mycelium/core/wire"
)

func v(e, s uint64) mapmodel.Version { return mapmodel.Version{Epoch: mapmodel.Epoch(e), Seq: s} }

func shard(id string, ver mapmodel.Version, data string) *wire.Shard {
	return &wire.Shard{ID: id, Version: ver, Size: int64(len(data)), Sha256: sha256.Sum256([]byte(data)), Data: []byte(data)}
}

func tombstone(id string, ver mapmodel.Version) *wire.Shard {
	return &wire.Shard{ID: id, Version: ver, Deleted: true, Sha256: sha256.Sum256(nil)}
}

func open(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func wantData(t *testing.T, s *Store, pg mapmodel.PGID, id string, ver *mapmodel.Version, want string) {
	t.Helper()
	sh, err := s.Get(pg, 1, id, ver)
	if err != nil || string(sh.Data) != want {
		t.Fatalf("Get(%q, %v): %v, %v; want %q", id, ver, sh, err, want)
	}
}

func wantNotFound(t *testing.T, s *Store, pg mapmodel.PGID, id string, ver *mapmodel.Version) {
	t.Helper()
	if _, err := s.Get(pg, 1, id, ver); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(%q, %v): %v, want ErrNotFound", id, ver, err)
	}
}

func versions(t *testing.T, s *Store, pg mapmodel.PGID, id string) []mapmodel.Version {
	t.Helper()
	items, err := s.Lookup(pg, 1, []string{id})
	must(t, err)
	if len(items) == 0 {
		return nil
	}
	return items[0].Versions
}

func dirty(t *testing.T, s *Store, pg mapmodel.PGID) []string {
	t.Helper()
	items, err := s.Scan(pg, 1)
	must(t, err)
	var ids []string
	for _, it := range items {
		ids = append(ids, it.ID)
	}
	return ids
}

// The holder-side contract peering and backfill rely on (ADRs 0004, 0006, 0007).
func TestContract(t *testing.T) {
	s := open(t, t.TempDir())
	const pg = 3
	for _, id := range []string{"plain", "a/b/../c", "nul\x00byte", "ünïcode"} {
		v1, v2, v3, v4 := v(1, 1), v(1, 2), v(1, 3), v(1, 4)

		must(t, s.Put(pg, 1, shard(id, v1, "one")))
		wantNotFound(t, s, pg, id, nil) // staged: invisible as "latest"
		wantData(t, s, pg, id, &v1, "one")
		if !slices.Contains(dirty(t, s, pg), id) {
			t.Fatalf("%q: staged object not reported dirty", id)
		}

		must(t, s.Commit(pg, 1, []wire.IDVersion{{ID: id, Version: v1}}))
		wantData(t, s, pg, id, nil, "one")
		if slices.Contains(dirty(t, s, pg), id) {
			t.Fatalf("%q: committed object still dirty", id)
		}

		must(t, s.Put(pg, 1, shard(id, v2, "two")))
		wantData(t, s, pg, id, nil, "one")
		must(t, s.Commit(pg, 1, []wire.IDVersion{{ID: id, Version: v2}}))
		wantData(t, s, pg, id, nil, "two")
		if got := versions(t, s, pg, id); !slices.Equal(got, []mapmodel.Version{v2}) {
			t.Fatalf("%q: after commit, versions = %v, want only %v", id, got, v2)
		}

		missing := []wire.IDVersion{{ID: id, Version: v(9, 9)}, {ID: "absent", Version: v1}}
		must(t, s.Commit(pg, 1, missing))
		must(t, s.Discard(pg, 1, missing))
		must(t, s.Put(pg, 1, shard(id, v3, "three")))
		must(t, s.Discard(pg, 1, []wire.IDVersion{{ID: id, Version: v3}}))
		if got := versions(t, s, pg, id); !slices.Equal(got, []mapmodel.Version{v2}) {
			t.Fatalf("%q: after no-op commit and discard, versions = %v", id, got)
		}

		list, err := s.List(pg)
		must(t, err)
		if !slices.Contains(list, wire.IDVersion{ID: id, Version: v2}) {
			t.Fatalf("%q: List = %v, want %v", id, list, v2)
		}

		must(t, s.Put(pg, 1, tombstone(id, v4)))
		wantData(t, s, pg, id, nil, "two") // staged tombstone hides nothing yet
		must(t, s.Commit(pg, 1, []wire.IDVersion{{ID: id, Version: v4}}))
		wantNotFound(t, s, pg, id, nil)
		wantNotFound(t, s, pg, id, &v4)
		if got := versions(t, s, pg, id); got != nil {
			t.Fatalf("%q: committed tombstone left versions %v", id, got)
		}
		list, _ = s.List(pg)
		if slices.ContainsFunc(list, func(it wire.IDVersion) bool { return it.ID == id }) {
			t.Fatalf("%q: deleted object still listed", id)
		}
	}
}

func TestPurgeAll(t *testing.T) {
	s := open(t, t.TempDir())
	for _, pg := range []mapmodel.PGID{0, 5} {
		must(t, s.Put(pg, 1, shard("x", v(1, 1), "x")))
		must(t, s.Commit(pg, 1, []wire.IDVersion{{ID: "x", Version: v(1, 1)}}))
	}
	_, err := s.Scan(0, 9) // fence pg 0 at 9
	must(t, err)
	must(t, s.PurgeAll())
	for _, pg := range []mapmodel.PGID{0, 5} {
		wantNotFound(t, s, pg, "x", nil)
		if list, _ := s.List(pg); len(list) != 0 {
			t.Fatalf("pg %d: List after purge = %v", pg, list)
		}
	}
	must(t, s.Put(0, 1, shard("y", v(1, 1), "y"))) // the fence went with the purge
}

// ADR 0005: epoch < fence is rejected by every fenced operation, durably.
func TestFence(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	const pg = 1
	must(t, s.Put(pg, 1, shard("x", v(1, 1), "x")))
	_, err := s.Scan(pg, 5)
	must(t, err)

	ops := map[string]func(e mapmodel.Epoch) error{
		"put": func(e mapmodel.Epoch) error { return s.Put(pg, e, shard("y", v(uint64(e), 1), "y")) },
		"get": func(e mapmodel.Epoch) error {
			_, err := s.Get(pg, e, "x", &mapmodel.Version{Epoch: 1, Seq: 1})
			return err
		},
		"commit":  func(e mapmodel.Epoch) error { return s.Commit(pg, e, nil) },
		"discard": func(e mapmodel.Epoch) error { return s.Discard(pg, e, nil) },
		"scan":    func(e mapmodel.Epoch) error { _, err := s.Scan(pg, e); return err },
		"lookup":  func(e mapmodel.Epoch) error { _, err := s.Lookup(pg, e, []string{"x"}); return err },
	}
	for name, op := range ops {
		var fe *FencedError
		if err := op(4); !errors.As(err, &fe) || fe.Fence != 5 {
			t.Errorf("%s at epoch 4: %v, want FencedError{5}", name, err)
		}
		if err := op(5); err != nil {
			t.Errorf("%s at epoch 5 (= fence): %v", name, err)
		}
	}
	if err := s.Put(2, 0, shard("z", v(0, 1), "z")); err != nil {
		t.Fatalf("fence leaked to another PG: %v", err)
	}

	s.Close()
	s = open(t, dir)
	var fe *FencedError
	if _, err := s.Lookup(pg, 4, nil); !errors.As(err, &fe) || fe.Fence != 5 {
		t.Fatalf("after reopen: %v, want FencedError{5}", err)
	}
	if _, err := s.List(pg); err != nil {
		t.Fatalf("List is not fenced: %v", err)
	}
}

// ADR 0010: checksums on write and read; data survives a reopen.
func TestIntegrity(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	bad := shard("x", v(1, 1), "payload")
	bad.Sha256[0] ^= 0xff
	if err := s.Put(0, 1, bad); !errors.Is(err, ErrChecksum) {
		t.Fatalf("Put with a wrong sha256: %v, want ErrChecksum", err)
	}
	if got := versions(t, s, 0, "x"); got != nil {
		t.Fatalf("rejected shard was stored: %v", got)
	}

	must(t, s.Put(0, 1, shard("x", v(1, 1), "payload")))
	must(t, s.Commit(0, 1, []wire.IDVersion{{ID: "x", Version: v(1, 1)}}))
	s.Close()
	s = open(t, dir)
	wantData(t, s, 0, "x", nil, "payload")

	files, _ := filepath.Glob(filepath.Join(dir, "pg-0", "*", "1-1.c"))
	if len(files) != 1 {
		t.Fatalf("expected one committed shard file, found %v", files)
	}
	b, err := os.ReadFile(files[0])
	must(t, err)
	b[len(b)-1] ^= 1
	must(t, os.WriteFile(files[0], b, 0o644))
	if _, err := s.Get(0, 1, "x", nil); !errors.Is(err, ErrChecksum) {
		t.Fatalf("Get of a corrupted shard: %v, want ErrChecksum", err)
	}
}
