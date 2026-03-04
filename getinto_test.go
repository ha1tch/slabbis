package slabbis_test

// getinto_test.go — tests for Cache.GetInto.
//
// GetInto is the zero-allocation sibling of GetCopy: it copies the value
// into a caller-supplied buffer, growing it only when cap(dst) < len(value).
// The server uses it with a pooled per-connection buffer to eliminate
// per-GET heap allocations in steady state.

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestGetIntoMiss verifies GetInto returns (dst[:0], false) for a missing key.
func TestGetIntoMiss(t *testing.T) {
	c := cache(t)
	dst := make([]byte, 8)
	out, ok := c.GetInto("ghost", dst)
	if ok {
		t.Fatal("GetInto on missing key: want ok=false")
	}
	if len(out) != 0 {
		t.Fatalf("GetInto on missing key: want empty slice, got len=%d", len(out))
	}
}

// TestGetIntoHit verifies GetInto returns the correct value.
func TestGetIntoHit(t *testing.T) {
	c := cache(t)
	c.Set("k", []byte("hello"), 0)

	out, ok := c.GetInto("k", make([]byte, 0, 16))
	if !ok {
		t.Fatal("GetInto: want ok=true")
	}
	if string(out) != "hello" {
		t.Fatalf("GetInto: got %q, want %q", out, "hello")
	}
}

// TestGetIntoNilDst verifies GetInto handles a nil dst: it allocates as
// needed and returns correctly.
func TestGetIntoNilDst(t *testing.T) {
	c := cache(t)
	c.Set("k", []byte("hello"), 0)

	out, ok := c.GetInto("k", nil)
	if !ok || string(out) != "hello" {
		t.Fatalf("GetInto(nil dst): got ok=%v, val=%q", ok, out)
	}
}

// TestGetIntoReusesBufferWhenLargeEnough verifies the core contract: when
// cap(dst) >= len(value), no new allocation occurs. We test this by
// pre-allocating a buffer of sufficient capacity, recording its data pointer,
// and checking that GetInto returns a slice backed by the same array.
func TestGetIntoReusesBufferWhenLargeEnough(t *testing.T) {
	c := cache(t)
	val := []byte("reused")
	c.Set("k", val, 0)

	dst := make([]byte, 0, 64)
	ptrBefore := &dst[:cap(dst)][0]

	out, ok := c.GetInto("k", dst)
	if !ok {
		t.Fatal("GetInto: want ok=true")
	}
	if string(out) != "reused" {
		t.Fatalf("GetInto: got %q", out)
	}

	ptrAfter := &out[:cap(out)][0]
	if ptrBefore != ptrAfter {
		t.Fatal("GetInto allocated a new buffer despite cap(dst) being sufficient")
	}
}

// TestGetIntoGrowsBufferWhenTooSmall verifies that GetInto allocates a new
// backing array when cap(dst) < len(value), and returns the correct value.
func TestGetIntoGrowsBufferWhenTooSmall(t *testing.T) {
	c := cache(t)
	val := bytes.Repeat([]byte("x"), 128)
	c.Set("k", val, 0)

	// dst has capacity 4 — too small for a 128-byte value.
	out, ok := c.GetInto("k", make([]byte, 0, 4))
	if !ok {
		t.Fatal("GetInto: want ok=true")
	}
	if !bytes.Equal(out, val) {
		t.Fatalf("GetInto: got %d bytes, want %d", len(out), len(val))
	}
	if cap(out) < 128 {
		t.Fatalf("GetInto: grown buffer cap=%d, want >= 128", cap(out))
	}
}

// TestGetIntoExpiredKey verifies GetInto applies lazy expiry before the
// reaper has run.
func TestGetIntoExpiredKey(t *testing.T) {
	c := ttlCache(t)
	c.Set("k", []byte("v"), 20*time.Millisecond)
	time.Sleep(50 * time.Millisecond)

	out, ok := c.GetInto("k", make([]byte, 0, 16))
	if ok || len(out) != 0 {
		t.Fatalf("GetInto on expired key: want (empty, false), got (%q, %v)", out, ok)
	}
}

// TestGetIntoMatchesGetCopy verifies GetInto and GetCopy return identical
// content across the full range of active size classes.
func TestGetIntoMatchesGetCopy(t *testing.T) {
	c := cache(t)
	for _, size := range []int{1, 8, 64, 512, 4096} {
		size := size
		t.Run(fmt.Sprintf("%dB", size), func(t *testing.T) {
			val := make([]byte, size)
			for i := range val {
				val[i] = byte(i % 251)
			}
			key := fmt.Sprintf("k-%d", size)
			c.Set(key, val, 0)

			cpy, ok := c.GetCopy(key)
			if !ok {
				t.Fatal("GetCopy miss")
			}
			into, ok := c.GetInto(key, make([]byte, 0, size))
			if !ok {
				t.Fatal("GetInto miss")
			}
			if !bytes.Equal(cpy, into) {
				t.Fatal("GetCopy and GetInto returned different content")
			}
		})
	}
}

// TestGetIntoBufferStaysIndependent verifies that mutating the returned slice
// does not affect a subsequent GetInto call — the returned slice aliases dst,
// but overwriting it does not corrupt the cache value.
func TestGetIntoBufferStaysIndependent(t *testing.T) {
	c := cache(t)
	c.Set("k", []byte("original"), 0)

	dst := make([]byte, 0, 32)
	out, _ := c.GetInto("k", dst)
	// Mutate the returned slice.
	for i := range out {
		out[i] = 'X'
	}
	// Reuse the same underlying buffer for the next call.
	out2, ok := c.GetInto("k", out[:0])
	if !ok {
		t.Fatal("second GetInto: want ok=true")
	}
	if string(out2) != "original" {
		t.Fatalf("mutation of prior result corrupted cache: got %q", out2)
	}
}

// TestGetIntoPoolPattern exercises the exact usage pattern of the server:
// one buffer borrowed from a pool, reused across many GetInto calls.
// Verifies correctness and that the pool pattern doesn't alias or corrupt
// values across calls.
func TestGetIntoPoolPattern(t *testing.T) {
	c := cache(t)
	const keys = 50
	vals := make([][]byte, keys)
	for i := 0; i < keys; i++ {
		vals[i] = []byte(fmt.Sprintf("value-%04d", i))
		c.Set(fmt.Sprintf("k%d", i), vals[i], 0)
	}

	pool := sync.Pool{New: func() any {
		b := make([]byte, 0, 64)
		return &b
	}}

	for round := 0; round < 3; round++ {
		bp := pool.Get().(*[]byte)
		for i := 0; i < keys; i++ {
			key := fmt.Sprintf("k%d", i)
			dst, ok := c.GetInto(key, *bp)
			if !ok {
				t.Fatalf("round %d key %d: GetInto miss", round, i)
			}
			*bp = dst
			// Verify immediately — do not retain across next GetInto.
			if !bytes.Equal(dst, vals[i]) {
				t.Fatalf("round %d key %d: got %q, want %q", round, i, dst, vals[i])
			}
		}
		pool.Put(bp)
	}
}

// TestGetIntoSafeUnderConcurrentSet is the race safety test, mirroring
// TestGetCopySafeUnderConcurrentSet. Concurrent writers overwrite the same
// key with single-fill values; readers verify each result is uniform.
// A torn read would manifest as a mixed-byte slice.
func TestGetIntoSafeUnderConcurrentSet(t *testing.T) {
	c := manyCache(t)

	const (
		goroutines = 32
		ops        = 300
		valSize    = 64
	)

	c.Set("hot", bytes.Repeat([]byte("A"), valSize), 0)

	var wg sync.WaitGroup
	var torn int64

	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			dst := make([]byte, 0, valSize)
			for i := 0; i < ops; i++ {
				out, ok := c.GetInto("hot", dst)
				dst = out
				if !ok {
					continue
				}
				first := out[0]
				for _, b := range out[1:] {
					if b != first {
						atomic.AddInt64(&torn, 1)
						return
					}
				}
				// Overwrite to surface aliasing bugs.
				for j := range out {
					out[j] = 0xFF
				}
			}
		}()
	}

	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			fill := byte('A' + g%26)
			val := bytes.Repeat([]byte{fill}, valSize)
			for i := 0; i < ops; i++ {
				c.Set("hot", val, 0)
			}
		}(g)
	}

	wg.Wait()

	if torn > 0 {
		t.Fatalf("GetInto returned %d torn slices under concurrent writes", torn)
	}
}

// TestGetIntoServerPoolSteadyState verifies the steady-state server pattern:
// a single pooled buffer, reused across GET and MGET-style access patterns,
// produces correct results and does not leak stale data between calls.
func TestGetIntoServerPoolSteadyState(t *testing.T) {
	c := cache(t)
	keys := []string{"alpha", "beta", "gamma", "delta"}
	vals := map[string]string{
		"alpha": "AAA",
		"beta":  "BBBBBBBB",
		"gamma": "C",
		"delta": "DDDDDDDDDDDDDDDD",
	}
	for k, v := range vals {
		c.Set(k, []byte(v), 0)
	}

	// Simulate a server connection: one buffer, many requests.
	buf := make([]byte, 0, 4)
	for round := 0; round < 20; round++ {
		for _, k := range keys {
			var ok bool
			buf, ok = c.GetInto(k, buf[:0])
			if !ok {
				t.Fatalf("round %d key %q: miss", round, k)
			}
			if string(buf) != vals[k] {
				t.Fatalf("round %d key %q: got %q, want %q", round, k, buf, vals[k])
			}
			// Simulate write to wire: buf content is consumed, buf reused.
		}
	}
	// After 20 rounds across 4 keys of varying sizes, buf should have grown
	// to accommodate the largest value and stabilised there.
	if cap(buf) < len(vals["delta"]) {
		t.Fatalf("buffer should have grown to at least %d bytes, cap=%d", len(vals["delta"]), cap(buf))
	}
}

// TestGetIntoTTLPreservation verifies that GetInto respects TTL correctly:
// a key with a future expiry is returned; the same key after expiry is not.
func TestGetIntoTTLPreservation(t *testing.T) {
	c := ttlCache(t)
	c.Set("k", []byte("alive"), 200*time.Millisecond)

	dst := make([]byte, 0, 8)
	out, ok := c.GetInto("k", dst)
	if !ok || string(out) != "alive" {
		t.Fatalf("before expiry: got ok=%v val=%q", ok, out)
	}

	time.Sleep(250 * time.Millisecond)
	out, ok = c.GetInto("k", dst[:0])
	if ok || len(out) != 0 {
		t.Fatalf("after expiry: want (empty, false), got (%q, %v)", out, ok)
	}
}
