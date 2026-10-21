// Package seal is the cgo binding to the C++ envelope encryption (seal.h): ML-KEM-768 wraps a
// per-object AES-256-GCM key. Keys never leave C++. Pure.
package seal

// botan-3.pc emits link flags cgo rejects by default; build with
// CGO_LDFLAGS_ALLOW='-fstack-protector|-m64' (or set it once with go env -w).
// Botan 3.12 is built by scripts/build-botan.sh; see the README.

/*
#cgo CXXFLAGS: -std=c++20
#cgo pkg-config: botan-3
#include "seal.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"unsafe"
)

var (
	ErrInvalid     = errors.New("seal: invalid input")
	ErrUnknownKEK  = errors.New("seal: unknown kek id")
	ErrNoSecretKey = errors.New("seal: no secret key for this kek")
	ErrAuth        = errors.New("seal: authentication failed")
)

// Overhead is the number of bytes a sealed blob adds to the plaintext.
var Overhead = int(C.mc_seal_overhead())

// Ring holds the KEKs. It is safe for concurrent use.
type Ring struct{ h *C.mc_ring }

// LoadRing parses a ring file (see seal.hpp). It does not retain file, so the caller can
// wipe it afterwards.
func LoadRing(file []byte) (*Ring, error) {
	var h *C.mc_ring
	if err := statusErr(C.mc_ring_load(ptr(file), C.size_t(len(file)), &h)); err != nil {
		return nil, err
	}
	return &Ring{h: h}, nil
}

func (r *Ring) Close() {
	if r.h != nil {
		C.mc_ring_free(r.h)
		r.h = nil
	}
}

// Seal encrypts pt under a fresh key wrapped to the ring's first KEK. id is authenticated
// but not stored, so Open needs the same id.
func (r *Ring) Seal(id string, pt []byte) ([]byte, error) {
	out := make([]byte, len(pt)+Overhead)
	b := []byte(id)
	err := statusErr(C.mc_seal(r.h, ptr(b), C.size_t(len(b)), ptr(pt), C.size_t(len(pt)), ptr(out)))
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Open reverses Seal. It returns ErrAuth for tampering, a wrong id or a wrong key, and
// ErrNoSecretKey if the ring has only the public half of the blob's KEK.
func (r *Ring) Open(id string, blob []byte) ([]byte, error) {
	if len(blob) < Overhead {
		return nil, ErrInvalid
	}
	out := make([]byte, len(blob)-Overhead)
	b := []byte(id)
	err := statusErr(C.mc_open(r.h, ptr(b), C.size_t(len(b)), ptr(blob), C.size_t(len(blob)), ptr(out)))
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Generate returns a ring file with one new KEK, public and secret halves. Join ring
// files by concatenation; the first KEK seals.
func Generate(id uint32) ([]byte, error) {
	out := make([]byte, C.mc_ring_secret_record_size())
	if err := statusErr(C.mc_ring_generate(C.uint32_t(id), ptr(out))); err != nil {
		return nil, err
	}
	return out, nil
}

// PublicOnly returns the ring file with every secret key dropped.
func PublicOnly(file []byte) ([]byte, error) {
	out := make([]byte, len(file))
	var n C.size_t
	if err := statusErr(C.mc_ring_public(ptr(file), C.size_t(len(file)), ptr(out), &n)); err != nil {
		return nil, err
	}
	return out[:n], nil
}

func statusErr(s C.mc_seal_status) error {
	switch s {
	case C.MC_SEAL_OK:
		return nil
	case C.MC_SEAL_INVALID:
		return ErrInvalid
	case C.MC_SEAL_UNKNOWN_KEK:
		return ErrUnknownKEK
	case C.MC_SEAL_NO_SECRET_KEY:
		return ErrNoSecretKey
	case C.MC_SEAL_AUTH_FAILED:
		return ErrAuth
	}
	return fmt.Errorf("seal: internal error (%d)", int(s))
}

// ptr passes Go bytes (which hold no Go pointers) to C for the duration of a call.
func ptr(b []byte) *C.uint8_t {
	if len(b) == 0 {
		return nil
	}
	return (*C.uint8_t)(unsafe.Pointer(&b[0]))
}
