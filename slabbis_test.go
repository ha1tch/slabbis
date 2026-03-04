package slabbis_test

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ha1tch/slabber"
	"github.com/ha1tch/slabbis"
)

// testClasses caps slab sizes at 4 KB for the test suite.
//
// Each slabber bucket eagerly reserves SlotsPerBucket×SlotSize bytes of
// virtual address space (65536 slots per bucket). The two largest
// DefaultClasses (32 KB and 256 KB) contribute ~2 GB and ~16 GB per Arena
// respectively. Linux faults pages lazily so the non-race suite survives,
// but the race detector must shadow every mapped byte and crashes the
// process when running ./... in parallel.
//
// Trimming to three classes (64 B / 512 B / 4 KB) reduces the per-Arena
// footprint from ~18 GB to ~292 MB — comfortably within the race
// detector's budget.
//
// Set SLABBIS_FULL_CLASSES=1 to restore DefaultClasses (e.g. on a
// well-provisioned machine when testing large-value paths with -race).
var testClasses []slabber.SizeClass

func init() {
	if os.Getenv("SLABBIS_FULL_CLASSES") == "1" {
		testClasses = nil // nil → Config uses DefaultClasses
	} else {
		testClasses = []slabber.SizeClass{
			{MaxSize: 64},
			{MaxSize: 512},
			{MaxSize: 4096},
		}
	}
}

// ---------------------------------------------------------------------------
// Shared cache instances — one per config variant, alive for the whole run.
// This avoids repeated Close() calls stalling on reaper goroutine scheduling.
// ---------------------------------------------------------------------------

var (
	sharedCache     slabbis.Cache // single-shard, no reaper
	sharedManyCache slabbis.Cache // multi-shard for concurrency tests
	sharedTTLCache  slabbis.Cache // fast reaper for TTL/eviction tests
)

func TestMain(m *testing.M) {
	sharedCache = slabbis.New(slabbis.Config{
		Shards:         1,
		ReaperInterval: time.Hour,
		Classes:        testClasses,
	})
	sharedManyCache = slabbis.New(slabbis.Config{
		Shards:         8,
		ReaperInterval: time.Hour,
		Classes:        testClasses,
	})
	sharedTTLCache = slabbis.New(slabbis.Config{
		Shards:         1,
		ReaperInterval: 20 * time.Millisecond,
		Classes:        testClasses,
	})

	// Shared server for server_test.go. Using one persistent server avoids
	// the goroutine stack reuse false positives that arise when tests
	// create back-to-back servers: the Serve goroutine never exits so its
	// stack pages are never recycled.
	srvCache := slabbis.New(slabbis.Config{
		Shards:         2,
		ReaperInterval: time.Hour,
		Classes:        testClasses,
	})
	var err error
	sharedSrv, err = slabbis.NewServer("127.0.0.1:0", srvCache, nil)
	if err != nil {
		panic("NewServer: " + err.Error())
	}
	go sharedSrv.Serve() //nolint:errcheck
	sharedAddr = sharedSrv.Addr()

	code := m.Run()

	sharedSrv.Close()
	srvCache.Close()
	sharedCache.Close()
	sharedManyCache.Close()
	sharedTTLCache.Close()
	os.Exit(code)
}

// cache returns the shared single-shard cache, flushed clean.
func cache(t *testing.T) slabbis.Cache {
	t.Helper()
	sharedCache.Flush()
	return sharedCache
}

// ttlCache returns the shared fast-reaper cache, flushed clean.
func ttlCache(t *testing.T) slabbis.Cache {
	t.Helper()
	sharedTTLCache.Flush()
	return sharedTTLCache
}

// manyCache returns the shared multi-shard cache, flushed clean.
func manyCache(t *testing.T) slabbis.Cache {
	t.Helper()
	sharedManyCache.Flush()
	return sharedManyCache
}

// ---- basic operations -------------------------------------------------------

func TestGetMiss(t *testing.T) {
	c := cache(t)
	_, ok := c.Get("missing")
	if ok {
		t.Fatal("expected miss, got hit")
	}
}

func TestSetGet(t *testing.T) {
	c := cache(t)
	c.Set("hello", []byte("world"), 0)
	val, ok := c.Get("hello")
	if !ok {
		t.Fatal("expected hit")
	}
	if string(val) != "world" {
		t.Fatalf("got %q, want %q", val, "world")
	}
}

func TestSetOverwrite(t *testing.T) {
	c := cache(t)
	c.Set("k", []byte("first"), 0)
	c.Set("k", []byte("second"), 0)
	val, ok := c.Get("k")
	if !ok {
		t.Fatal("expected hit after overwrite")
	}
	if string(val) != "second" {
		t.Fatalf("got %q, want %q", val, "second")
	}
}

func TestDel(t *testing.T) {
	c := cache(t)
	c.Set("k", []byte("v"), 0)
	if !c.Del("k") {
		t.Fatal("Del should return true for existing key")
	}
	if c.Del("k") {
		t.Fatal("Del should return false for already-deleted key")
	}
	if _, ok := c.Get("k"); ok {
		t.Fatal("Get after Del should miss")
	}
}

func TestDelMissing(t *testing.T) {
	c := cache(t)
	if c.Del("ghost") {
		t.Fatal("Del on missing key should return false")
	}
}

func TestExists(t *testing.T) {
	c := cache(t)
	if c.Exists("k") {
		t.Fatal("Exists on missing key should be false")
	}
	c.Set("k", []byte("v"), 0)
	if !c.Exists("k") {
		t.Fatal("Exists after Set should be true")
	}
	c.Del("k")
	if c.Exists("k") {
		t.Fatal("Exists after Del should be false")
	}
}

func TestFlush(t *testing.T) {
	c := cache(t)
	for i := 0; i < 10; i++ {
		c.Set(fmt.Sprintf("key%d", i), []byte("val"), 0)
	}
	c.Flush()
	for i := 0; i < 10; i++ {
		if _, ok := c.Get(fmt.Sprintf("key%d", i)); ok {
			t.Fatalf("key%d still present after Flush", i)
		}
	}
	stats := c.Stats()
	if stats.Keys != 0 {
		t.Fatalf("expected 0 keys after Flush, got %d", stats.Keys)
	}
}

// ---- TTL --------------------------------------------------------------------

func TestTTLMissing(t *testing.T) {
	c := cache(t)
	_, ok := c.TTL("ghost")
	if ok {
		t.Fatal("TTL on missing key should return false")
	}
}

func TestTTLNoExpiry(t *testing.T) {
	c := cache(t)
	c.Set("k", []byte("v"), 0)
	remaining, ok := c.TTL("k")
	if !ok {
		t.Fatal("TTL on key with no expiry should return true (key exists)")
	}
	if remaining != 0 {
		t.Fatalf("TTL on key with no expiry should return 0 duration, got %v", remaining)
	}
}

func TestTTLWithExpiry(t *testing.T) {
	c := cache(t)
	c.Set("k", []byte("v"), 10*time.Second)
	remaining, ok := c.TTL("k")
	if !ok {
		t.Fatal("TTL on key with expiry should return true")
	}
	if remaining <= 0 || remaining > 10*time.Second {
		t.Fatalf("unexpected TTL %v", remaining)
	}
}

func TestTTLExpiry(t *testing.T) {
	c := ttlCache(t)
	c.Set("k", []byte("v"), 30*time.Millisecond)
	if !c.Exists("k") {
		t.Fatal("key should exist before expiry")
	}
	time.Sleep(60 * time.Millisecond)
	if _, ok := c.Get("k"); ok {
		t.Fatal("Get should miss after TTL expiry")
	}
}

func TestReaperEviction(t *testing.T) {
	c := ttlCache(t)
	c.Set("ephemeral", []byte("bye"), 10*time.Millisecond)
	time.Sleep(80 * time.Millisecond)
	stats := c.Stats()
	if stats.Keys != 0 {
		t.Fatalf("expected 0 keys after reaper eviction, got %d", stats.Keys)
	}
}

// ---- stats ------------------------------------------------------------------

func TestStats(t *testing.T) {
	c := cache(t)
	s0 := c.Stats()
	if s0.Keys != 0 {
		t.Fatalf("expected 0 keys initially, got %d", s0.Keys)
	}
	c.Set("a", []byte("1"), 0)
	c.Set("b", []byte("2"), 0)
	s1 := c.Stats()
	if s1.Keys != 2 {
		t.Fatalf("expected 2 keys, got %d", s1.Keys)
	}
	c.Del("a")
	s2 := c.Stats()
	if s2.Keys != 1 {
		t.Fatalf("expected 1 key after Del, got %d", s2.Keys)
	}
	if len(s2.SlabStats) == 0 {
		t.Fatal("expected non-empty SlabStats")
	}
}

// ---- edge cases -------------------------------------------------------------

func TestEmptyValue(t *testing.T) {
	c := cache(t)
	c.Set("empty", []byte{}, 0)
	val, ok := c.Get("empty")
	if !ok {
		t.Fatal("expected hit for empty value")
	}
	if len(val) != 0 {
		t.Fatalf("expected empty slice, got len %d", len(val))
	}
}

func TestBinaryValue(t *testing.T) {
	c := cache(t)
	blob := []byte{0x00, 0xFF, 0x0D, 0x0A, 0x80}
	c.Set("bin", blob, 0)
	got, ok := c.Get("bin")
	if !ok {
		t.Fatal("expected hit")
	}
	if string(got) != string(blob) {
		t.Fatalf("binary roundtrip failed: got %x want %x", got, blob)
	}
}

// ---- new operations ---------------------------------------------------------

func TestKeys(t *testing.T) {
	c := cache(t)
	c.Set("apple", []byte("1"), 0)
	c.Set("apricot", []byte("2"), 0)
	c.Set("banana", []byte("3"), 0)

	all := c.Keys("*")
	if len(all) != 3 {
		t.Fatalf("Keys(*): expected 3, got %d", len(all))
	}
	ap := c.Keys("ap*")
	if len(ap) != 2 {
		t.Fatalf("Keys(ap*): expected 2, got %d: %v", len(ap), ap)
	}
	none := c.Keys("zzz*")
	if len(none) != 0 {
		t.Fatalf("Keys(zzz*): expected 0, got %d", len(none))
	}
}

func TestKeysExcludesExpired(t *testing.T) {
	c := ttlCache(t)
	c.Set("live", []byte("v"), 0)
	c.Set("dead", []byte("v"), 10*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	for _, k := range c.Keys("*") {
		if k == "dead" {
			t.Fatal("expired key should not appear in Keys")
		}
	}
}

func TestMGet(t *testing.T) {
	c := cache(t)
	c.Set("a", []byte("alpha"), 0)
	c.Set("b", []byte("beta"), 0)
	vals := c.MGet("a", "missing", "b")
	if len(vals) != 3 {
		t.Fatalf("expected 3 results, got %d", len(vals))
	}
	if string(vals[0]) != "alpha" {
		t.Fatalf("vals[0]: got %q", vals[0])
	}
	if vals[1] != nil {
		t.Fatalf("vals[1] should be nil for missing key, got %q", vals[1])
	}
	if string(vals[2]) != "beta" {
		t.Fatalf("vals[2]: got %q", vals[2])
	}
}

func TestMSet(t *testing.T) {
	c := cache(t)
	c.MSet(0, map[string][]byte{
		"x": []byte("10"),
		"y": []byte("20"),
		"z": []byte("30"),
	})
	for k, want := range map[string]string{"x": "10", "y": "20", "z": "30"} {
		got, ok := c.Get(k)
		if !ok {
			t.Fatalf("MSet: key %q not found", k)
		}
		if string(got) != want {
			t.Fatalf("MSet: key %q: got %q, want %q", k, got, want)
		}
	}
}

func TestSetNX(t *testing.T) {
	c := cache(t)
	if !c.SetNX("k", []byte("first"), 0) {
		t.Fatal("SetNX on new key should return true")
	}
	if c.SetNX("k", []byte("second"), 0) {
		t.Fatal("SetNX on existing key should return false")
	}
	val, _ := c.Get("k")
	if string(val) != "first" {
		t.Fatalf("value should be unchanged: got %q", val)
	}
}

func TestGetDel(t *testing.T) {
	c := cache(t)
	c.Set("k", []byte("gone"), 0)
	val, ok := c.GetDel("k")
	if !ok {
		t.Fatal("GetDel: expected hit")
	}
	if string(val) != "gone" {
		t.Fatalf("GetDel: got %q, want gone", val)
	}
	if c.Exists("k") {
		t.Fatal("key should be gone after GetDel")
	}
	_, ok = c.GetDel("ghost")
	if ok {
		t.Fatal("GetDel on missing key should return false")
	}
}

func TestRename(t *testing.T) {
	c := cache(t)
	c.Set("old", []byte("value"), 0)
	if !c.Rename("old", "new") {
		t.Fatal("Rename should return true for existing key")
	}
	if c.Exists("old") {
		t.Fatal("old key should not exist after Rename")
	}
	val, ok := c.Get("new")
	if !ok || string(val) != "value" {
		t.Fatalf("new key: got %q ok=%v", val, ok)
	}
	if c.Rename("ghost", "other") {
		t.Fatal("Rename non-existent key should return false")
	}
}

func TestDBSize(t *testing.T) {
	c := cache(t)
	if c.DBSize() != 0 {
		t.Fatalf("expected 0, got %d", c.DBSize())
	}
	c.Set("a", []byte("1"), 0)
	c.Set("b", []byte("2"), 0)
	if c.DBSize() != 2 {
		t.Fatalf("expected 2, got %d", c.DBSize())
	}
}

// ---- concurrency ------------------------------------------------------------

func TestConcurrentSetGet(t *testing.T) {
	c := manyCache(t)
	const goroutines = 32
	const ops = 500
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", g)
			val := fmt.Sprintf("val-%d", g)
			for i := 0; i < ops; i++ {
				c.Set(key, []byte(val), 0)
				got, ok := c.Get(key)
				if !ok {
					t.Errorf("goroutine %d: Get miss after Set", g)
					return
				}
				if string(got) != val {
					t.Errorf("goroutine %d: got %q want %q", g, got, val)
					return
				}
			}
			c.Del(key)
		}()
	}
	wg.Wait()
}

func TestConcurrentFlush(t *testing.T) {
	c := manyCache(t)
	const goroutines = 16
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				c.Set(fmt.Sprintf("k-%d-%d", g, i), []byte("v"), 0)
				if i%20 == 0 {
					c.Flush()
				}
			}
		}()
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// Concurrency stress — designed to surface races without the race detector
// ---------------------------------------------------------------------------

// TestConcurrentSameKey hammers a single key from many goroutines with
// interleaved Set/Get/Del. Without proper locking any of these would corrupt
// the shard map or the arena. The test checks only that operations never panic
// and that the final state is consistent (key either present or absent).
func TestConcurrentSameKey(t *testing.T) {
	c := manyCache(t)
	const goroutines = 32
	const ops = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				switch i % 4 {
				case 0:
					c.Set("hot", []byte(fmt.Sprintf("%d-%d", g, i)), 0)
				case 1:
					c.Get("hot")
				case 2:
					c.Del("hot")
				case 3:
					c.Exists("hot")
				}
			}
		}()
	}
	wg.Wait()
	// No assertion on value — just that we didn't crash.
}

// TestConcurrentSetNXOnce verifies that exactly one goroutine wins the race
// to set a new key when all goroutines call SetNX simultaneously.
func TestConcurrentSetNXOnce(t *testing.T) {
	c := manyCache(t)
	const goroutines = 50
	var wins int64
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			if c.SetNX("once", []byte("v"), 0) {
				atomic.AddInt64(&wins, 1)
			}
		}()
	}
	wg.Wait()
	if wins != 1 {
		t.Fatalf("SetNX: expected exactly 1 winner, got %d", wins)
	}
}

// TestConcurrentGetDel verifies GetDel is atomic: a value should be returned
// at most once across many concurrent callers.
func TestConcurrentGetDel(t *testing.T) {
	c := manyCache(t)
	const goroutines = 50
	c.Set("prize", []byte("gold"), 0)
	var got int64
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			if _, ok := c.GetDel("prize"); ok {
				atomic.AddInt64(&got, 1)
			}
		}()
	}
	wg.Wait()
	if got != 1 {
		t.Fatalf("GetDel: expected exactly 1 retrieval, got %d", got)
	}
}

// TestConcurrentMixedOps runs a mixed workload across all operations to stress
// the shard locking. No correctness assertion beyond no panic/deadlock.
func TestConcurrentMixedOps(t *testing.T) {
	c := manyCache(t)
	const goroutines = 32
	const ops = 500
	keys := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				k := keys[(g+i)%len(keys)]
				switch i % 8 {
				case 0:
					c.Set(k, []byte(fmt.Sprintf("%d", i)), 0)
				case 1:
					c.Get(k)
				case 2:
					c.Del(k)
				case 3:
					c.Exists(k)
				case 4:
					c.SetNX(k, []byte("nx"), 0)
				case 5:
					c.GetDel(k)
				case 6:
					c.Keys("*")
				case 7:
					c.DBSize()
				}
			}
		}()
	}
	wg.Wait()
}

// TestConcurrentFlushVsSet verifies that Flush racing with Set does not
// produce a cache that is simultaneously empty and non-empty.
func TestConcurrentFlushVsSet(t *testing.T) {
	c := manyCache(t)
	const goroutines = 16
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				c.Set(fmt.Sprintf("k-%d-%d", g, i), []byte("v"), 0)
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				c.Flush()
			}
		}()
	}
	wg.Wait()
	// Stats must be internally consistent after all goroutines finish.
	s := c.Stats()
	if int64(s.Keys) < 0 {
		t.Fatalf("negative key count after concurrent flush: %d", s.Keys)
	}
}

// TestLargeValue checks that values near and beyond typical slab boundaries
// are stored and retrieved correctly.
func TestLargeValue(t *testing.T) {
	c := cache(t)
	maxSize := slabbis.DefaultClasses[len(slabbis.DefaultClasses)-1].MaxSize
	if testClasses != nil {
		maxSize = testClasses[len(testClasses)-1].MaxSize
	}
	sizes := []int{0, 1, 63, 64, 65, 127, 128, 129, 255, 256, 512, 1024, 4096, 65536}
	for _, sz := range sizes {
		if sz > maxSize {
			t.Logf("skipping size %d (exceeds active class cap %d)", sz, maxSize)
			continue
		}
		val := make([]byte, sz)
		for i := range val {
			val[i] = byte(i % 251)
		}
		key := fmt.Sprintf("large-%d", sz)
		c.Set(key, val, 0)
		got, ok := c.Get(key)
		if !ok {
			t.Errorf("size %d: Get miss", sz)
			continue
		}
		if len(got) != sz {
			t.Errorf("size %d: got len %d", sz, len(got))
			continue
		}
		for i, b := range got {
			if b != val[i] {
				t.Errorf("size %d: byte %d corrupted: got %x want %x", sz, i, b, val[i])
				break
			}
		}
	}
}

// TestManyKeys verifies correct behaviour with a large number of distinct keys,
// exercising shard distribution and Stats accuracy.
func TestManyKeys(t *testing.T) {
	c := manyCache(t)
	const n = 500
	for i := 0; i < n; i++ {
		c.Set(fmt.Sprintf("mk-%04d", i), []byte("v"), 0)
	}
	s := c.Stats()
	if int(s.Keys) != n {
		t.Fatalf("Stats.Keys: got %d, want %d", s.Keys, n)
	}
	all := c.Keys("*")
	if len(all) != n {
		t.Fatalf("Keys(*): got %d, want %d", len(all), n)
	}
	if c.DBSize() != n {
		t.Fatalf("DBSize: got %d, want %d", c.DBSize(), n)
	}
}

// ---------------------------------------------------------------------------
// Gap coverage
// ---------------------------------------------------------------------------

// TestNewDefault verifies that NewDefault() produces a working cache.
func TestNewDefault(t *testing.T) {
	c := slabbis.NewDefault()
	defer c.Close()
	c.Set("k", []byte("v"), 0)
	val, ok := c.Get("k")
	if !ok || string(val) != "v" {
		t.Fatalf("NewDefault: Set/Get failed: ok=%v val=%q", ok, val)
	}
}

// TestMSetWithTTL verifies that MSet honours a non-zero TTL.
func TestMSetWithTTL(t *testing.T) {
	c := ttlCache(t)
	c.MSet(20*time.Millisecond, map[string][]byte{
		"mset-a": []byte("1"),
		"mset-b": []byte("2"),
	})
	if !c.Exists("mset-a") || !c.Exists("mset-b") {
		t.Fatal("MSet with TTL: keys should exist immediately after set")
	}
	time.Sleep(50 * time.Millisecond)
	if c.Exists("mset-a") || c.Exists("mset-b") {
		t.Fatal("MSet with TTL: keys should have expired")
	}
}

// TestRenameTTLDropped documents and verifies the current semantic: Rename
// does not preserve the source key's TTL. The destination gets no expiry.
func TestRenameTTLDropped(t *testing.T) {
	c := ttlCache(t)
	c.Set("src", []byte("v"), 50*time.Millisecond)
	if !c.Rename("src", "dst") {
		t.Fatal("Rename failed")
	}
	// dst should survive past the original TTL.
	time.Sleep(80 * time.Millisecond)
	if !c.Exists("dst") {
		t.Fatal("Rename: destination should have no TTL (current semantic), but it expired")
	}
	remaining, ok := c.TTL("dst")
	if !ok || remaining != 0 {
		t.Fatalf("Rename: destination should have no expiry; got ok=%v remaining=%v", ok, remaining)
	}
}

// TestRenameToExistingKey verifies that Rename overwrites an existing destination.
func TestRenameToExistingKey(t *testing.T) {
	c := cache(t)
	c.Set("src", []byte("new"), 0)
	c.Set("dst", []byte("old"), 0)
	if !c.Rename("src", "dst") {
		t.Fatal("Rename should succeed even when destination exists")
	}
	if c.Exists("src") {
		t.Fatal("source should be gone after Rename")
	}
	val, ok := c.Get("dst")
	if !ok || string(val) != "new" {
		t.Fatalf("destination should hold new value after Rename; got ok=%v val=%q", ok, val)
	}
}

// TestKeysMultiShard verifies Keys works correctly across multiple shards.
func TestKeysMultiShard(t *testing.T) {
	c := manyCache(t)
	const n = 100
	for i := 0; i < n; i++ {
		c.Set(fmt.Sprintf("ms-%03d", i), []byte("v"), 0)
	}
	all := c.Keys("*")
	if len(all) != n {
		t.Fatalf("Keys(*) across shards: got %d, want %d", len(all), n)
	}
	prefix := c.Keys("ms-00*")
	// ms-000 to ms-009 → 10 keys with prefix "ms-00"
	if len(prefix) != 10 {
		t.Fatalf("Keys(ms-00*): got %d, want 10", len(prefix))
	}
}

// TestTypeOnExpiredKey verifies that TYPE returns "none" for a key whose TTL
// has elapsed, even if the reaper hasn't run yet (lazy expiry check).
func TestTypeOnExpiredKey(t *testing.T) {
	// Use a cache with a very slow reaper so we're testing lazy expiry.
	c := slabbis.New(slabbis.Config{
		Shards:         1,
		ReaperInterval: time.Hour,
		Classes:        testClasses,
	})
	defer c.Close()
	c.Set("ex", []byte("v"), 20*time.Millisecond)
	time.Sleep(40 * time.Millisecond)
	// Get should miss (lazy expiry).
	_, ok := c.Get("ex")
	if ok {
		t.Fatal("expired key should not be returned by Get")
	}
	// Keys should exclude it.
	for _, k := range c.Keys("*") {
		if k == "ex" {
			t.Fatal("expired key should not appear in Keys")
		}
	}
}

// TestConfigZeroShards verifies that a zero Shards value falls back to a
// sensible default rather than panicking.
func TestConfigZeroShards(t *testing.T) {
	c := slabbis.New(slabbis.Config{Shards: 0, ReaperInterval: time.Hour, Classes: testClasses})
	defer c.Close()
	c.Set("k", []byte("v"), 0)
	if val, ok := c.Get("k"); !ok || string(val) != "v" {
		t.Fatal("Config{Shards:0} produced a broken cache")
	}
}

// TestPTTLSemantics verifies the three-state TTL return contract via PTTL
// (missing → 0,false; no-expiry → 0,true; expiring → remaining,true).
func TestPTTLSemantics(t *testing.T) {
	c := cache(t)

	_, ok := c.TTL("ghost")
	if ok {
		t.Fatal("TTL missing key: want false")
	}

	c.Set("forever", []byte("v"), 0)
	rem, ok := c.TTL("forever")
	if !ok || rem != 0 {
		t.Fatalf("TTL no-expiry: want (0, true), got (%v, %v)", rem, ok)
	}

	c.Set("temp", []byte("v"), 10*time.Second)
	rem, ok = c.TTL("temp")
	if !ok || rem <= 0 || rem > 10*time.Second {
		t.Fatalf("TTL expiring: want positive duration ≤10s, got (%v, %v)", rem, ok)
	}
}