package slabbis_test

// gaps_test.go — targeted tests for five coverage gaps identified after v0.1.1
//
//  1. GetCopy — direct correctness + isolation from concurrent mutation
//  2. Oversized-value silent drop in Set
//  3. BucketsPerShard config field
//  4. Stats field accuracy (SlotSize, UsedSlots, TotalSlots, FreeSlots, MemoryMB)
//  5. Rename non-atomicity: concurrent Set on destination between GetDel and Set

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ha1tch/slabber"
	"github.com/ha1tch/slabbis"
)

// ---------------------------------------------------------------------------
// 1. GetCopy
// ---------------------------------------------------------------------------

// TestGetCopyMiss verifies GetCopy returns (nil, false) for a missing key.
func TestGetCopyMiss(t *testing.T) {
	c := cache(t)
	val, ok := c.GetCopy("ghost")
	if ok {
		t.Fatal("GetCopy on missing key: want ok=false")
	}
	if val != nil {
		t.Fatalf("GetCopy on missing key: want nil slice, got len=%d", len(val))
	}
}

// TestGetCopyHit verifies GetCopy returns the stored value.
func TestGetCopyHit(t *testing.T) {
	c := cache(t)
	c.Set("k", []byte("hello"), 0)

	val, ok := c.GetCopy("k")
	if !ok {
		t.Fatal("GetCopy: want ok=true")
	}
	if string(val) != "hello" {
		t.Fatalf("GetCopy: got %q, want %q", val, "hello")
	}
}

// TestGetCopyMatchesGet verifies GetCopy and Get return identical content
// across the full range of active size classes.
func TestGetCopyMatchesGet(t *testing.T) {
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

			got, ok := c.Get(key)
			if !ok {
				t.Fatalf("Get miss")
			}
			cpy, ok := c.GetCopy(key)
			if !ok {
				t.Fatalf("GetCopy miss")
			}
			if !bytes.Equal(got, cpy) {
				t.Fatalf("Get and GetCopy returned different content for %d-byte value", size)
			}
		})
	}
}

// TestGetCopyReturnsIndependentSlice verifies that mutating the slice returned
// by GetCopy does not corrupt the cached value. The copy must be independent
// of the underlying slabber slot.
func TestGetCopyReturnsIndependentSlice(t *testing.T) {
	c := cache(t)
	c.Set("k", []byte("original"), 0)

	got, ok := c.GetCopy("k")
	if !ok {
		t.Fatal("GetCopy: want ok=true")
	}

	// Overwrite every byte of the returned copy.
	for i := range got {
		got[i] = 'X'
	}

	// The cached value must be unaffected.
	got2, ok := c.GetCopy("k")
	if !ok {
		t.Fatal("GetCopy (second call): want ok=true")
	}
	if string(got2) != "original" {
		t.Fatalf("mutating GetCopy result corrupted cache: got %q", got2)
	}
}

// TestGetCopyExpiredKey verifies GetCopy applies lazy expiry: a key whose TTL
// has elapsed returns (nil, false) even before the reaper runs.
func TestGetCopyExpiredKey(t *testing.T) {
	c := ttlCache(t)
	c.Set("k", []byte("v"), 20*time.Millisecond)
	time.Sleep(50 * time.Millisecond)

	val, ok := c.GetCopy("k")
	if ok || val != nil {
		t.Fatalf("GetCopy on expired key: want (nil, false), got (%q, %v)", val, ok)
	}
}

// TestGetCopySafeUnderConcurrentSet is the core race scenario GetCopy was
// introduced to prevent. Concurrent goroutines write single-fill values
// (every byte the same character); readers verify each returned slice is
// uniform. A torn read — one byte from a new write, the rest from the old —
// would produce a mixed-byte slice.
//
// Without GetCopy (bare Get), this is a data race: the slot is live memory
// freed and overwritten by Set while the server holds a reference to it.
// With GetCopy the copy happens under RLock, guaranteeing the returned slice
// is a consistent snapshot.
func TestGetCopySafeUnderConcurrentSet(t *testing.T) {
	c := manyCache(t)

	const (
		goroutines = 32
		ops        = 300
		valSize    = 64
	)

	c.Set("hot", bytes.Repeat([]byte("A"), valSize), 0)

	var wg sync.WaitGroup
	var torn int64

	// Readers: verify every returned slice is uniform.
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				val, ok := c.GetCopy("hot")
				if !ok {
					continue
				}
				first := val[0]
				for _, b := range val[1:] {
					if b != first {
						atomic.AddInt64(&torn, 1)
						return
					}
				}
				// Mutate to surface aliasing bugs: if val shares memory
				// with the slot, future readers would see 0xFF mixed in.
				for j := range val {
					val[j] = 0xFF
				}
			}
		}()
	}

	// Writers: hammer with single-fill values of varying fill bytes.
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
		t.Fatalf("GetCopy returned %d torn slices (partial write visible during copy)", torn)
	}
}

// ---------------------------------------------------------------------------
// 2. Oversized-value silent drop
// ---------------------------------------------------------------------------

// tinyCache returns a cache with a single 64-byte size class, making it
// easy to trigger the oversized-value path with a 65-byte value.
func tinyCache(t *testing.T) slabbis.Cache {
	t.Helper()
	c := slabbis.New(slabbis.Config{
		Shards:         1,
		ReaperInterval: time.Hour,
		Classes:        []slabber.SizeClass{{MaxSize: 64}},
	})
	t.Cleanup(func() { c.Close() })
	return c
}

// TestSetOversizedResultsInMiss verifies that setting a value larger than the
// largest size class produces a Get miss. The value is silently dropped.
func TestSetOversizedResultsInMiss(t *testing.T) {
	c := tinyCache(t)
	c.Set("k", make([]byte, 65), 0)

	if _, ok := c.Get("k"); ok {
		t.Fatal("Set of oversized value: want Get miss, got hit")
	}
}

// TestSetOversizedEvictsExistingEntry verifies the documented eviction
// behaviour: an oversized Set on an already-stored key removes the old entry.
// The key must be absent after the call — not holding the old value.
func TestSetOversizedEvictsExistingEntry(t *testing.T) {
	c := tinyCache(t)
	c.Set("k", []byte("small"), 0)
	if _, ok := c.Get("k"); !ok {
		t.Fatal("setup: small value not stored")
	}

	c.Set("k", make([]byte, 65), 0) // oversized

	if _, ok := c.Get("k"); ok {
		t.Fatal("after oversized Set: old value must not be retrievable")
	}
	if c.Exists("k") {
		t.Fatal("after oversized Set: Exists must return false")
	}
}

// TestSetOversizedOnMissingKey verifies that an oversized Set on a key that
// does not yet exist leaves the key absent and does not affect DBSize.
func TestSetOversizedOnMissingKey(t *testing.T) {
	c := tinyCache(t)
	c.Set("ghost", make([]byte, 65), 0)

	if c.Exists("ghost") {
		t.Fatal("oversized Set on missing key must not create the key")
	}
	if c.DBSize() != 0 {
		t.Fatalf("DBSize: want 0, got %d", c.DBSize())
	}
}

// TestSetOversizedKeysCountDecreases verifies Stats().Keys and DBSize() reflect
// the eviction caused by an oversized Set on an existing key.
func TestSetOversizedKeysCountDecreases(t *testing.T) {
	c := tinyCache(t)
	for i := 0; i < 5; i++ {
		c.Set(fmt.Sprintf("k%d", i), []byte("v"), 0)
	}
	if got := c.DBSize(); got != 5 {
		t.Fatalf("setup: want 5 keys, got %d", got)
	}

	c.Set("k2", make([]byte, 65), 0) // evicts k2, stores nothing

	if got := c.DBSize(); got != 4 {
		t.Fatalf("DBSize after oversized Set: want 4, got %d", got)
	}
	if got := c.Stats().Keys; got != 4 {
		t.Fatalf("Stats().Keys after oversized Set: want 4, got %d", got)
	}
}

// ---------------------------------------------------------------------------
// 3. BucketsPerShard
// ---------------------------------------------------------------------------

// TestBucketsPerShardExplicit verifies that a cache with an explicit
// BucketsPerShard stores and retrieves values correctly.
//
// Note: in v0.1.1, BucketsPerShard is accepted by Config but not yet
// propagated to slabber.NewArena (see the TODO in newShard). The test
// therefore exercises correctness, not that the bucket count is honoured.
// When the TODO is resolved, a Stats-based assertion should be added here.
func TestBucketsPerShardExplicit(t *testing.T) {
	for _, bps := range []int{1, 2, 4, 8} {
		bps := bps
		t.Run(fmt.Sprintf("bps=%d", bps), func(t *testing.T) {
			c := slabbis.New(slabbis.Config{
				Shards:          2,
				ReaperInterval:  time.Hour,
				Classes:         testClasses,
				BucketsPerShard: bps,
			})
			defer c.Close()

			for i := 0; i < 20; i++ {
				key := fmt.Sprintf("k%d", i)
				val := []byte(fmt.Sprintf("val%d", i))
				c.Set(key, val, 0)
				got, ok := c.Get(key)
				if !ok {
					t.Fatalf("Get miss for key %q", key)
				}
				if !bytes.Equal(got, val) {
					t.Fatalf("key %q: got %q, want %q", key, got, val)
				}
			}
			if got := c.DBSize(); got != 20 {
				t.Fatalf("DBSize: want 20, got %d", got)
			}
		})
	}
}

// TestBucketsPerShardZeroDefaults verifies that BucketsPerShard=0 falls back
// gracefully and the cache operates correctly.
func TestBucketsPerShardZeroDefaults(t *testing.T) {
	c := slabbis.New(slabbis.Config{
		Shards:          1,
		ReaperInterval:  time.Hour,
		Classes:         testClasses,
		BucketsPerShard: 0,
	})
	defer c.Close()

	c.Set("k", []byte("v"), 0)
	val, ok := c.Get("k")
	if !ok || string(val) != "v" {
		t.Fatalf("BucketsPerShard=0: want Get hit with \"v\", got ok=%v val=%q", ok, val)
	}
}

// ---------------------------------------------------------------------------
// 4. Stats field accuracy
// ---------------------------------------------------------------------------

// TestStatsSlabStatsLengthMatchesClasses verifies that len(SlabStats) equals
// the number of configured size classes.
func TestStatsSlabStatsLengthMatchesClasses(t *testing.T) {
	classes := []slabber.SizeClass{
		{MaxSize: 64},
		{MaxSize: 512},
		{MaxSize: 4096},
	}
	c := slabbis.New(slabbis.Config{
		Shards:         1,
		ReaperInterval: time.Hour,
		Classes:        classes,
	})
	defer c.Close()

	stats := c.Stats()
	if got := len(stats.SlabStats); got != len(classes) {
		t.Fatalf("len(SlabStats): want %d, got %d", len(classes), got)
	}
}

// TestStatsSlotSizeMatchesClass verifies that each SlabStats entry reports
// a SlotSize >= the corresponding class MaxSize, and that SlotSizes are
// in ascending order (matching the class order).
func TestStatsSlotSizeMatchesClass(t *testing.T) {
	classes := []slabber.SizeClass{
		{MaxSize: 64},
		{MaxSize: 512},
		{MaxSize: 4096},
	}
	c := slabbis.New(slabbis.Config{
		Shards:         1,
		ReaperInterval: time.Hour,
		Classes:        classes,
	})
	defer c.Close()

	ss := c.Stats().SlabStats
	for i, sc := range classes {
		if ss[i].SlotSize < sc.MaxSize {
			t.Errorf("class %d: SlotSize %d < MaxSize %d", i, ss[i].SlotSize, sc.MaxSize)
		}
		if ss[i].SlotSize <= 0 {
			t.Errorf("class %d: SlotSize must be positive, got %d", i, ss[i].SlotSize)
		}
		if i > 0 && ss[i].SlotSize <= ss[i-1].SlotSize {
			t.Errorf("class %d: SlotSize %d not greater than class %d SlotSize %d",
				i, ss[i].SlotSize, i-1, ss[i-1].SlotSize)
		}
	}
}

// TestStatsUsedSlotsAccounting verifies that UsedSlots increases by 1 after
// a Set and returns to its prior value after a Del. The test uses a
// single-shard, single-class cache to keep accounting deterministic.
func TestStatsUsedSlotsAccounting(t *testing.T) {
	c := slabbis.New(slabbis.Config{
		Shards:         1,
		ReaperInterval: time.Hour,
		Classes:        []slabber.SizeClass{{MaxSize: 64}},
	})
	defer c.Close()

	before := c.Stats().SlabStats[0].UsedSlots

	c.Set("k", []byte("hello"), 0)
	afterSet := c.Stats().SlabStats[0].UsedSlots
	if afterSet != before+1 {
		t.Fatalf("UsedSlots after Set: want %d, got %d", before+1, afterSet)
	}

	c.Del("k")
	afterDel := c.Stats().SlabStats[0].UsedSlots
	if afterDel != before {
		t.Fatalf("UsedSlots after Del: want %d, got %d", before, afterDel)
	}
}

// TestStatsSlotInvariants verifies the invariants that must hold for every
// size class regardless of cache activity:
//   - TotalSlots >= UsedSlots >= 0
//   - FreeSlots == TotalSlots - UsedSlots
//   - MemoryMB > 0 (at least one bucket exists)
func TestStatsSlotInvariants(t *testing.T) {
	c := slabbis.New(slabbis.Config{
		Shards:         1,
		ReaperInterval: time.Hour,
		Classes:        testClasses,
	})
	defer c.Close()

	// Seed a few values to ensure the arena has allocated something.
	for i := 0; i < 10; i++ {
		c.Set(fmt.Sprintf("k%d", i), make([]byte, 8), 0)
	}

	for i, ss := range c.Stats().SlabStats {
		if ss.TotalSlots < ss.UsedSlots {
			t.Errorf("class %d: TotalSlots %d < UsedSlots %d", i, ss.TotalSlots, ss.UsedSlots)
		}
		if ss.UsedSlots < 0 {
			t.Errorf("class %d: UsedSlots is negative: %d", i, ss.UsedSlots)
		}
		if ss.FreeSlots != ss.TotalSlots-ss.UsedSlots {
			t.Errorf("class %d: FreeSlots %d != TotalSlots %d - UsedSlots %d",
				i, ss.FreeSlots, ss.TotalSlots, ss.UsedSlots)
		}
		if ss.MemoryMB <= 0 {
			t.Errorf("class %d: MemoryMB must be > 0, got %f", i, ss.MemoryMB)
		}
		if ss.Buckets <= 0 {
			t.Errorf("class %d: Buckets must be > 0, got %d", i, ss.Buckets)
		}
	}
}

// TestStatsKeysExcludesExpired verifies that Stats().Keys counts only
// live (non-expired) keys.
func TestStatsKeysExcludesExpired(t *testing.T) {
	c := slabbis.New(slabbis.Config{
		Shards:         1,
		ReaperInterval: time.Hour, // reaper won't run during this test
		Classes:        testClasses,
	})
	defer c.Close()

	c.Set("live", []byte("v"), 0)
	c.Set("dying", []byte("v"), 20*time.Millisecond)

	time.Sleep(50 * time.Millisecond)

	// "dying" is expired via lazy check; Stats must not count it.
	if got := c.Stats().Keys; got != 1 {
		t.Fatalf("Stats().Keys: want 1 (expired key excluded), got %d", got)
	}
}

// ---------------------------------------------------------------------------
// 5. Rename non-atomicity
// ---------------------------------------------------------------------------

// TestRenameSameShardSafety verifies that Rename between two keys in the same
// shard does not deadlock. Rename uses GetDel + Set rather than acquiring
// both shard locks simultaneously, so same-shard renames must complete cleanly.
func TestRenameSameShardSafety(t *testing.T) {
	// Single-shard cache forces every key into the same shard.
	c := slabbis.New(slabbis.Config{
		Shards:         1,
		ReaperInterval: time.Hour,
		Classes:        testClasses,
	})
	defer c.Close()

	c.Set("from", []byte("value"), 0)
	if !c.Rename("from", "to") {
		t.Fatal("Rename returned false for existing key")
	}
	if c.Exists("from") {
		t.Fatal("source key still exists after same-shard Rename")
	}
	val, ok := c.Get("to")
	if !ok || string(val) != "value" {
		t.Fatalf("destination after same-shard Rename: ok=%v val=%q", ok, val)
	}
}

// TestRenameSameShardConcurrent stress-tests same-shard Rename under
// concurrent load to surface deadlocks or panics.
func TestRenameSameShardConcurrent(t *testing.T) {
	c := slabbis.New(slabbis.Config{
		Shards:         1,
		ReaperInterval: time.Hour,
		Classes:        testClasses,
	})
	defer c.Close()

	const goroutines = 16
	const ops = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				src := fmt.Sprintf("g%d-src-%d", g, i)
				dst := fmt.Sprintf("g%d-dst-%d", g, i)
				c.Set(src, []byte("v"), 0)
				c.Rename(src, dst)
				c.Del(dst)
			}
		}(g)
	}
	wg.Wait()
	// No assertion beyond "no deadlock and no panic".
}

// TestRenameConcurrentSetOnDest documents and tests the non-atomic window in
// Rename. Rename is implemented as GetDel(from) + Set(to): a concurrent Set
// on the destination between those two steps will be overwritten by the
// Rename's own Set.
//
// Assertions:
//   - source key is absent after Rename
//   - destination key exists (either the renamed or the concurrent value)
//   - no panic or deadlock under load
//
// The specific value held by the destination is intentionally not asserted:
// it is non-deterministic depending on goroutine scheduling.
func TestRenameConcurrentSetOnDest(t *testing.T) {
	c := manyCache(t)
	const iterations = 200

	for iter := 0; iter < iterations; iter++ {
		src := fmt.Sprintf("src-%d", iter)
		dst := fmt.Sprintf("dst-%d", iter)
		c.Set(src, []byte("renamed"), 0)
		c.Set(dst, []byte("original"), 0)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				c.Set(dst, []byte("concurrent"), 0)
			}
		}()

		ok := c.Rename(src, dst)
		wg.Wait()

		if !ok {
			t.Fatalf("iter %d: Rename returned false for existing key", iter)
		}
		if c.Exists(src) {
			t.Fatalf("iter %d: source %q still exists after Rename", iter, src)
		}
		if !c.Exists(dst) {
			t.Fatalf("iter %d: destination %q absent after Rename + concurrent Set", iter, dst)
		}

		// Clean up for next iteration.
		c.Del(dst)
	}
}
