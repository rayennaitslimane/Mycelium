// Package store is the cgo binding to the C++ shard store (store.h).
// It is the only owner of the per-PG fence.
package store

// botan-2.pc emits link flags cgo rejects by default; build with
// CGO_LDFLAGS_ALLOW='-fstack-protector|-m64' (or set it once with go env -w).

/*
#cgo CXXFLAGS: -std=c++20
#cgo pkg-config: botan-2
#include <stdlib.h>
#include "store.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"unsafe"

	"mycelium/core/mapmodel"
	"mycelium/core/wire"
)

var (
	ErrNotFound = errors.New("not found")
	ErrChecksum = errors.New("checksum mismatch")
	ErrInvalid  = errors.New("store: invalid argument")
	ErrIO       = errors.New("store: I/O error (details on stderr)")
)

// FencedError is returned when epoch < the PG's fence.
type FencedError struct {
	Fence mapmodel.Epoch
}

func (e *FencedError) Error() string {
	return fmt.Sprintf("fenced at epoch %d", e.Fence)
}

// Store is safe for concurrent use.
type Store struct {
	h *C.mc_store
}

func Open(root string) (*Store, error) {
	croot := C.CString(root)
	defer C.free(unsafe.Pointer(croot))
	var h *C.mc_store
	if err := statusErr(C.mc_store_open(croot, &h), 0); err != nil {
		return nil, err
	}
	return &Store{h: h}, nil
}

func (s *Store) Close() {
	if s.h != nil {
		C.mc_store_close(s.h)
		s.h = nil
	}
}

func (s *Store) Put(pg mapmodel.PGID, epoch mapmodel.Epoch, sh *wire.Shard) error {
	meta := C.mc_shard_meta{version: cver(sh.Version), size: C.uint64_t(sh.Size)}
	if sh.Deleted {
		meta.deleted = 1
	}
	for i, b := range sh.Sha256 {
		meta.sha256[i] = C.uint8_t(b)
	}
	var fence C.uint64_t
	st := C.mc_store_put(s.h, C.uint32_t(pg), C.uint64_t(epoch), strPtr(sh.ID), C.size_t(len(sh.ID)),
		&meta, bytesPtr(sh.Data), C.size_t(len(sh.Data)), &fence)
	return statusErr(st, fence)
}

// Get returns the latest committed version if v is nil, else the named version.
func (s *Store) Get(pg mapmodel.PGID, epoch mapmodel.Epoch, id string, v *mapmodel.Version) (*wire.Shard, error) {
	var cv *C.mc_version
	if v != nil {
		x := cver(*v)
		cv = &x
	}
	var meta C.mc_shard_meta
	var data *C.uint8_t
	var n C.size_t
	var fence C.uint64_t
	st := C.mc_store_get(s.h, C.uint32_t(pg), C.uint64_t(epoch), strPtr(id), C.size_t(len(id)),
		cv, &meta, &data, &n, &fence)
	if err := statusErr(st, fence); err != nil {
		return nil, err
	}
	defer C.mc_store_free(unsafe.Pointer(data))
	sh := &wire.Shard{
		ID:      id,
		Version: goVersion(meta.version),
		Size:    int64(meta.size),
		Deleted: meta.deleted != 0,
		Data:    C.GoBytes(unsafe.Pointer(data), C.int(n)),
	}
	for i := range sh.Sha256 {
		sh.Sha256[i] = byte(meta.sha256[i])
	}
	return sh, nil
}

func (s *Store) Commit(pg mapmodel.PGID, epoch mapmodel.Epoch, items []wire.IDVersion) error {
	arr, free := cItems(items)
	defer free()
	var fence C.uint64_t
	return statusErr(C.mc_store_commit(s.h, C.uint32_t(pg), C.uint64_t(epoch), arr, C.size_t(len(items)), &fence), fence)
}

func (s *Store) Discard(pg mapmodel.PGID, epoch mapmodel.Epoch, items []wire.IDVersion) error {
	arr, free := cItems(items)
	defer free()
	var fence C.uint64_t
	return statusErr(C.mc_store_discard(s.h, C.uint32_t(pg), C.uint64_t(epoch), arr, C.size_t(len(items)), &fence), fence)
}

func (s *Store) Scan(pg mapmodel.PGID, epoch mapmodel.Epoch) ([]wire.IDVersions, error) {
	var out *C.mc_id_versions
	var n C.size_t
	var fence C.uint64_t
	if err := statusErr(C.mc_store_scan(s.h, C.uint32_t(pg), C.uint64_t(epoch), &out, &n, &fence), fence); err != nil {
		return nil, err
	}
	return goIDVersions(out, n), nil
}

func (s *Store) Lookup(pg mapmodel.PGID, epoch mapmodel.Epoch, ids []string) ([]wire.IDVersions, error) {
	arr, free := cIDs(ids)
	defer free()
	var out *C.mc_id_versions
	var n C.size_t
	var fence C.uint64_t
	st := C.mc_store_lookup(s.h, C.uint32_t(pg), C.uint64_t(epoch), arr, C.size_t(len(ids)), &out, &n, &fence)
	if err := statusErr(st, fence); err != nil {
		return nil, err
	}
	return goIDVersions(out, n), nil
}

// List returns the latest committed version of each local object in pg.
func (s *Store) List(pg mapmodel.PGID) ([]wire.IDVersion, error) {
	var out *C.mc_id_versions
	var n C.size_t
	if err := statusErr(C.mc_store_list(s.h, C.uint32_t(pg), &out, &n), 0); err != nil {
		return nil, err
	}
	var items []wire.IDVersion
	for _, iv := range goIDVersions(out, n) {
		items = append(items, wire.IDVersion{ID: iv.ID, Version: iv.Versions[0]})
	}
	return items, nil
}

func (s *Store) PurgeAll() error {
	return statusErr(C.mc_store_purge_all(s.h), 0)
}

func statusErr(st C.mc_store_status, fence C.uint64_t) error {
	switch st {
	case C.MC_STORE_OK:
		return nil
	case C.MC_STORE_NOT_FOUND:
		return ErrNotFound
	case C.MC_STORE_FENCED:
		return &FencedError{Fence: mapmodel.Epoch(fence)}
	case C.MC_STORE_CHECKSUM:
		return ErrChecksum
	case C.MC_STORE_INVALID:
		return ErrInvalid
	}
	return ErrIO
}

func cver(v mapmodel.Version) C.mc_version {
	return C.mc_version{epoch: C.uint64_t(v.Epoch), seq: C.uint64_t(v.Seq)}
}

func goVersion(v C.mc_version) mapmodel.Version {
	return mapmodel.Version{Epoch: mapmodel.Epoch(v.epoch), Seq: uint64(v.seq)}
}

// strPtr and bytesPtr pass Go bytes (which hold no Go pointers) to C for one call.
func strPtr(s string) *C.char {
	if len(s) == 0 {
		return nil
	}
	return (*C.char)(unsafe.Pointer(unsafe.StringData(s)))
}

func bytesPtr(b []byte) *C.uint8_t {
	if len(b) == 0 {
		return nil
	}
	return (*C.uint8_t)(unsafe.Pointer(&b[0]))
}

// cStrings copies ids into one C buffer and returns each copy; C arrays must not
// hold pointers to Go memory.
func cStrings(ids []string) ([]*C.char, func()) {
	total := 1
	for _, id := range ids {
		total += len(id)
	}
	buf := C.malloc(C.size_t(total))
	dst := unsafe.Slice((*byte)(buf), total)
	ptrs := make([]*C.char, len(ids))
	off := 0
	for i, id := range ids {
		copy(dst[off:], id)
		ptrs[i] = (*C.char)(unsafe.Add(buf, off))
		off += len(id)
	}
	return ptrs, func() { C.free(buf) }
}

func cItems(items []wire.IDVersion) (*C.mc_id_version, func()) {
	ids := make([]string, len(items))
	for i, it := range items {
		ids[i] = it.ID
	}
	ptrs, freeIDs := cStrings(ids)
	arr := (*C.mc_id_version)(C.malloc(C.size_t(max(1, len(items))) * C.size_t(unsafe.Sizeof(C.mc_id_version{}))))
	elems := unsafe.Slice(arr, len(items))
	for i := range elems {
		elems[i] = C.mc_id_version{id: ptrs[i], id_len: C.size_t(len(ids[i])), version: cver(items[i].Version)}
	}
	return arr, func() {
		C.free(unsafe.Pointer(arr))
		freeIDs()
	}
}

func cIDs(ids []string) (*C.mc_id, func()) {
	ptrs, freeIDs := cStrings(ids)
	arr := (*C.mc_id)(C.malloc(C.size_t(max(1, len(ids))) * C.size_t(unsafe.Sizeof(C.mc_id{}))))
	elems := unsafe.Slice(arr, len(ids))
	for i := range elems {
		elems[i] = C.mc_id{id: ptrs[i], id_len: C.size_t(len(ids[i]))}
	}
	return arr, func() {
		C.free(unsafe.Pointer(arr))
		freeIDs()
	}
}

func goIDVersions(p *C.mc_id_versions, n C.size_t) []wire.IDVersions {
	defer C.mc_store_free_id_versions(p, n)
	var out []wire.IDVersions
	for _, iv := range unsafe.Slice(p, int(n)) {
		w := wire.IDVersions{ID: C.GoStringN(iv.id, C.int(iv.id_len))}
		for _, v := range unsafe.Slice(iv.versions, int(iv.n)) {
			w.Versions = append(w.Versions, goVersion(v))
		}
		out = append(out, w)
	}
	return out
}
