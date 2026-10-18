package codec

import (
	"bytes"
	"encoding/hex"
	"errors"
	"math/rand/v2"
	"testing"
)

// Any k of the n = k+m shards must rebuild the object (ADR 0010).
func TestRoundTripWithAnyMShardsMissing(t *testing.T) {
	rnd := rand.New(rand.NewPCG(1, 2))
	for _, km := range [][2]int{{1, 2}, {2, 1}, {3, 2}, {4, 2}} {
		k, m := km[0], km[1]
		c, err := New(k, m)
		if err != nil {
			t.Fatal(err)
		}
		n := k + m
		for _, size := range []int{0, 1, 7, 1000, 4097} {
			obj := make([]byte, size)
			for i := range obj {
				obj[i] = byte(rnd.Uint32())
			}
			shards, err := c.Encode(obj)
			if err != nil || len(shards) != n {
				t.Fatalf("k=%d m=%d size=%d: Encode = %d shards, %v", k, m, size, len(shards), err)
			}
			for mask := 0; mask < 1<<n; mask++ {
				if popcount(mask) > m {
					continue
				}
				in := make([][]byte, n)
				for i := range shards {
					if mask&(1<<i) == 0 {
						in[i] = bytes.Clone(shards[i])
					}
				}
				got, err := c.Decode(in, int64(size))
				if err != nil || !bytes.Equal(got, obj) {
					t.Fatalf("k=%d m=%d size=%d missing=%b: decode mismatch (err %v)", k, m, size, mask, err)
				}
			}
		}
		c.Close()
	}
}

func TestTooFewShards(t *testing.T) {
	c, err := New(3, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	shards, _ := c.Encode([]byte("hello, erasure coding"))
	shards[0], shards[2], shards[4] = nil, nil, nil
	if _, err := c.Decode(shards, 21); !errors.Is(err, ErrTooFewShards) {
		t.Fatalf("Decode with k-1 shards: %v, want ErrTooFewShards", err)
	}
	if _, err := New(0, 1); err == nil {
		t.Fatal("New(0, 1) accepted")
	}
}

func TestSha256KnownVectors(t *testing.T) {
	for in, want := range map[string]string{
		"":    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		"abc": "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
	} {
		if got := Sha256([]byte(in)); hex.EncodeToString(got[:]) != want {
			t.Errorf("Sha256(%q) = %x", in, got)
		}
	}
}

func popcount(x int) int {
	n := 0
	for ; x > 0; x &= x - 1 {
		n++
	}
	return n
}
