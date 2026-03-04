package slabbis_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ha1tch/slabbis"
)

// testConfig returns a deterministic single-shard config for unit tests.
// A single shard keeps test behaviour predictable; concurrency tests use
// a wider config explicitly.
func testConfig() slabbis.Config {
	return slabbis.Config{
		Shards:         1,
		ReaperInterval: 50 * time.Millisecond,
	}
}

// ---- basic operations -------------------------------------------------------

func TestGetMiss(t *testing.T) {
	c := slabbis.New(testConfig())
	defer c.Close()

	_, ok := c.Get("missing")
	if ok {
		t.Fatal("expected miss, got hit")
	}
}

func TestSetGet(t *testing.T) {
	c := slabbis.New(testConfig())
	defer c.Close()

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
	c := slabbis.New(testConfig())
	defer c.Close()

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
	c := slabbis.New(testConfig())
	defer c.Close()

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
	c := slabbis.New(testConfig())
	defer c.Close()

	if c.Del("ghost") {
		t.Fatal("Del on missing key should return false")
	}
}

func TestExists(t *testing.T) {
	c := slabbis.New(testConfig())
	defer c.Close()

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
	c := slabbis.New(testConfig())
	defer c.Close()

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
	c := slabbis.New(testConfig())
	defer c.Close()

	_, ok := c.TTL("ghost")
	if ok {
		t.Fatal("TTL on missing key should return false")
	}
}

func TestTTLNoExpiry(t *testing.T) {
	c := slabbis.New(testConfig())
	defer c.Close()

	c.Set("k", []byte("v"), 0)
	_, ok := c.TTL("k")
	if ok {
		t.Fatal("TTL on key with no expiry should return false")
	}
}

func TestTTLWithExpiry(t *testing.T) {
	c := slabbis.New(testConfig())
	defer c.Close()

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
	c := slabbis.New(testConfig())
	defer c.Close()

	c.Set("k", []byte("v"), 30*time.Millisecond)

	// Key should be present immediately.
	if !c.Exists("k") {
		t.Fatal("key should exist before expiry")
	}

	// Wait for expiry.
	time.Sleep(60 * time.Millisecond)

	if _, ok := c.Get("k"); ok {
		t.Fatal("Get should miss after TTL expiry")
	}
	if c.Exists("k"); c.Exists("k") {
		t.Fatal("Exists should be false after TTL expiry")
	}
}

func TestReaperEviction(t *testing.T) {
	c := slabbis.New(slabbis.Config{
		Shards:         1,
		ReaperInterval: 20 * time.Millisecond,
	})
	defer c.Close()

	c.Set("ephemeral", []byte("bye"), 10*time.Millisecond)

	// Wait for reaper to run.
	time.Sleep(80 * time.Millisecond)

	stats := c.Stats()
	if stats.Keys != 0 {
		t.Fatalf("expected 0 keys after reaper eviction, got %d", stats.Keys)
	}
}

// ---- stats ------------------------------------------------------------------

func TestStats(t *testing.T) {
	c := slabbis.New(testConfig())
	defer c.Close()

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

// ---- empty value ------------------------------------------------------------

func TestEmptyValue(t *testing.T) {
	c := slabbis.New(testConfig())
	defer c.Close()

	c.Set("empty", []byte{}, 0)
	val, ok := c.Get("empty")
	if !ok {
		t.Fatal("expected hit for empty value")
	}
	if len(val) != 0 {
		t.Fatalf("expected empty slice, got len %d", len(val))
	}
}

// ---- binary safety ----------------------------------------------------------

func TestBinaryValue(t *testing.T) {
	c := slabbis.New(testConfig())
	defer c.Close()

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

// ---- concurrency ------------------------------------------------------------

func TestConcurrentSetGet(t *testing.T) {
	c := slabbis.New(slabbis.Config{
		Shards:         8,
		ReaperInterval: time.Second,
	})
	defer c.Close()

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
	c := slabbis.New(slabbis.Config{
		Shards:         4,
		ReaperInterval: time.Second,
	})
	defer c.Close()

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
	// No assertion — we are testing that concurrent Flush + Set does not panic
	// or deadlock. The race detector will catch any data races.
}

// ---- new operations ---------------------------------------------------------

func TestKeys(t *testing.T) {
	c := slabbis.New(testConfig())
	defer c.Close()

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
	c := slabbis.New(testConfig())
	defer c.Close()

	c.Set("live", []byte("v"), 0)
	c.Set("dead", []byte("v"), 10*time.Millisecond)
	time.Sleep(30 * time.Millisecond)

	keys := c.Keys("*")
	for _, k := range keys {
		if k == "dead" {
			t.Fatal("expired key should not appear in Keys")
		}
	}
}

func TestMGet(t *testing.T) {
	c := slabbis.New(testConfig())
	defer c.Close()

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
	c := slabbis.New(testConfig())
	defer c.Close()

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
	c := slabbis.New(testConfig())
	defer c.Close()

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
	c := slabbis.New(testConfig())
	defer c.Close()

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
	c := slabbis.New(testConfig())
	defer c.Close()

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
	c := slabbis.New(testConfig())
	defer c.Close()

	if c.DBSize() != 0 {
		t.Fatalf("expected 0, got %d", c.DBSize())
	}
	c.Set("a", []byte("1"), 0)
	c.Set("b", []byte("2"), 0)
	if c.DBSize() != 2 {
		t.Fatalf("expected 2, got %d", c.DBSize())
	}
}
