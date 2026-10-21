package seal

import (
	"bytes"
	"errors"
	"sync"
	"testing"
)

func mustRing(t *testing.T, file []byte) *Ring {
	t.Helper()
	r, err := LoadRing(file)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	return r
}

func mustGenerate(t *testing.T, id uint32) []byte {
	t.Helper()
	rec, err := Generate(id)
	if err != nil {
		t.Fatal(err)
	}
	return rec
}

func TestRoundTrip(t *testing.T) {
	r := mustRing(t, mustGenerate(t, 1))
	for _, n := range []int{0, 1, 100, 1 << 16} {
		pt := bytes.Repeat([]byte{0xab}, n)
		blob, err := r.Seal("obj", pt)
		if err != nil {
			t.Fatal(err)
		}
		if len(blob) != n+Overhead {
			t.Fatalf("n=%d: blob is %d bytes, want %d", n, len(blob), n+Overhead)
		}
		got, err := r.Open("obj", blob)
		if err != nil || !bytes.Equal(got, pt) {
			t.Fatalf("n=%d: open = %d bytes, %v", n, len(got), err)
		}
	}
}

func TestSealIsRandomized(t *testing.T) {
	r := mustRing(t, mustGenerate(t, 1))
	a, _ := r.Seal("obj", []byte("same"))
	b, _ := r.Seal("obj", []byte("same"))
	if bytes.Equal(a, b) {
		t.Fatal("two seals of the same object are identical")
	}
}

func TestTamper(t *testing.T) {
	r := mustRing(t, mustGenerate(t, 1))
	blob, _ := r.Seal("obj", []byte("secret payload"))

	if _, err := r.Open("other", blob); !errors.Is(err, ErrAuth) {
		t.Fatalf("wrong id: %v", err)
	}
	// Every byte after the kek id: kem_ct, wrapped dek, nonce, ciphertext, tag.
	for i := 6; i < len(blob); i++ {
		bad := bytes.Clone(blob)
		bad[i] ^= 1
		if _, err := r.Open("obj", bad); !errors.Is(err, ErrAuth) {
			t.Fatalf("flip at %d: %v", i, err)
		}
	}
	for _, i := range []int{0, 1} { // version and suite
		bad := bytes.Clone(blob)
		bad[i] ^= 1
		if _, err := r.Open("obj", bad); !errors.Is(err, ErrInvalid) {
			t.Fatalf("flip at %d: %v", i, err)
		}
	}
	if _, err := r.Open("obj", blob[:Overhead-1]); !errors.Is(err, ErrInvalid) {
		t.Fatalf("truncated: %v", err)
	}
	if _, err := r.Open("obj", blob[:len(blob)-1]); !errors.Is(err, ErrAuth) {
		t.Fatalf("short tag: %v", err)
	}
}

func TestWrongKey(t *testing.T) {
	a := mustRing(t, mustGenerate(t, 1))
	b := mustRing(t, mustGenerate(t, 1)) // same id, different key
	blob, _ := a.Seal("obj", []byte("x"))
	if _, err := b.Open("obj", blob); !errors.Is(err, ErrAuth) {
		t.Fatalf("wrong key: %v", err)
	}
	c := mustRing(t, mustGenerate(t, 2))
	if _, err := c.Open("obj", blob); !errors.Is(err, ErrUnknownKEK) {
		t.Fatalf("unknown kek: %v", err)
	}
}

func TestPublicOnly(t *testing.T) {
	full := mustGenerate(t, 7)
	pub, err := PublicOnly(full)
	if err != nil {
		t.Fatal(err)
	}
	if len(pub) >= len(full) {
		t.Fatalf("public ring has %d bytes, full has %d", len(pub), len(full))
	}
	w, rd := mustRing(t, pub), mustRing(t, full)
	blob, err := w.Seal("obj", []byte("write-only"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Open("obj", blob); !errors.Is(err, ErrNoSecretKey) {
		t.Fatalf("open with public-only ring: %v", err)
	}
	if got, err := rd.Open("obj", blob); err != nil || string(got) != "write-only" {
		t.Fatalf("open with full ring: %q, %v", got, err)
	}
}

// Blobs sealed under an older KEK still open once a new one is first in the ring.
func TestRingOrder(t *testing.T) {
	oldRec, newRec := mustGenerate(t, 1), mustGenerate(t, 2)
	oldOnly := mustRing(t, oldRec)
	both := mustRing(t, append(bytes.Clone(newRec), oldRec...))

	oldBlob, _ := oldOnly.Seal("obj", []byte("old"))
	if got, err := both.Open("obj", oldBlob); err != nil || string(got) != "old" {
		t.Fatalf("old blob: %q, %v", got, err)
	}
	newBlob, _ := both.Seal("obj", []byte("new"))
	if _, err := oldOnly.Open("obj", newBlob); !errors.Is(err, ErrUnknownKEK) {
		t.Fatalf("new blob under old ring: %v", err)
	}
	if got, err := both.Open("obj", newBlob); err != nil || string(got) != "new" {
		t.Fatalf("new blob: %q, %v", got, err)
	}
}

func TestLoadRingInvalid(t *testing.T) {
	rec := mustGenerate(t, 1)
	other := mustGenerate(t, 2)
	mismatch := bytes.Clone(rec)
	copy(mismatch[len(mismatch)-64:], other[len(other)-64:]) // seed of another key

	cases := map[string][]byte{
		"empty":     nil,
		"truncated": rec[:len(rec)-1],
		"bad flags": append(bytes.Clone(rec[:4]), append([]byte{2}, rec[5:]...)...),
		"duplicate": append(bytes.Clone(rec), rec...),
		"mismatch":  mismatch,
	}
	for name, file := range cases {
		if r, err := LoadRing(file); !errors.Is(err, ErrInvalid) {
			if r != nil {
				r.Close()
			}
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestConcurrent(t *testing.T) {
	r := mustRing(t, mustGenerate(t, 1))
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				pt := []byte{byte(g), byte(i)}
				blob, err := r.Seal("obj", pt)
				if err != nil {
					t.Error(err)
					return
				}
				if got, err := r.Open("obj", blob); err != nil || !bytes.Equal(got, pt) {
					t.Errorf("open: %v %v", got, err)
					return
				}
			}
		}()
	}
	wg.Wait()
}
