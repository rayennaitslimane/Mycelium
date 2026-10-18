// Package codec is the cgo binding to the C++ erasure codec and sha256 (codec.h). Pure.
package codec

// botan-2.pc emits link flags cgo rejects by default; build with
// CGO_LDFLAGS_ALLOW='-fstack-protector|-m64' (or set it once with go env -w).

/*
#cgo CXXFLAGS: -std=c++20
#cgo pkg-config: libisal botan-2
#include "codec.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"unsafe"
)

var ErrTooFewShards = errors.New("codec: fewer than k shards")

// Codec is safe for concurrent use.
type Codec struct {
	h    *C.mc_codec
	k, m int
}

func New(k, m int) (*Codec, error) {
	var h *C.mc_codec
	if C.mc_codec_new(C.int(k), C.int(m), &h) != C.MC_CODEC_OK {
		return nil, fmt.Errorf("codec: invalid k=%d m=%d", k, m)
	}
	return &Codec{h: h, k: k, m: m}, nil
}

func (c *Codec) Close() {
	if c.h != nil {
		C.mc_codec_free(c.h)
		c.h = nil
	}
}

// Encode returns n = k+m shards; shard i belongs to slot i.
func (c *Codec) Encode(obj []byte) ([][]byte, error) {
	n := c.k + c.m
	ss := int(C.mc_codec_shard_size(c.h, C.size_t(len(obj))))
	buf := make([]byte, n*ss)
	if C.mc_codec_encode(c.h, ptr(obj), C.size_t(len(obj)), ptr(buf)) != C.MC_CODEC_OK {
		return nil, errors.New("codec: encode failed")
	}
	shards := make([][]byte, n)
	for i := range shards {
		shards[i] = buf[i*ss : (i+1)*ss : (i+1)*ss]
	}
	return shards, nil
}

// Decode rebuilds an object of size bytes from shards (len n, nil = missing, at least k present).
func (c *Codec) Decode(shards [][]byte, size int64) ([]byte, error) {
	n := c.k + c.m
	if len(shards) != n || size < 0 {
		return nil, fmt.Errorf("codec: need %d shards, got %d", n, len(shards))
	}
	ss := int(C.mc_codec_shard_size(c.h, C.size_t(size)))
	buf := make([]byte, n*ss)
	present := make([]byte, n)
	for i, s := range shards {
		if s == nil {
			continue
		}
		if len(s) != ss {
			return nil, fmt.Errorf("codec: shard %d has %d bytes, want %d", i, len(s), ss)
		}
		copy(buf[i*ss:], s)
		present[i] = 1
	}
	out := make([]byte, size)
	switch C.mc_codec_decode(c.h, ptr(buf), ptr(present), C.size_t(ss), ptr(out), C.size_t(size)) {
	case C.MC_CODEC_OK:
		return out, nil
	case C.MC_CODEC_TOO_FEW_SHARDS:
		return nil, ErrTooFewShards
	}
	return nil, errors.New("codec: decode failed")
}

func Sha256(b []byte) (sum [32]byte) {
	C.mc_sha256(ptr(b), C.size_t(len(b)), (*C.uint8_t)(unsafe.Pointer(&sum[0])))
	return sum
}

// ptr passes Go bytes (which hold no Go pointers) to C for the duration of a call.
func ptr(b []byte) *C.uint8_t {
	if len(b) == 0 {
		return nil
	}
	return (*C.uint8_t)(unsafe.Pointer(&b[0]))
}
