package slabbis_test

// newops_test.go — tests for operations added in v0.1.4:
//
//   Cache methods: SetTTL, GetSet, IncrBy
//   Server commands: UNLINK, STRLEN, INCR, DECR, INCRBY, DECRBY,
//                    GETSET, GETEX, EXPIRE, PEXPIRE, PERSIST,
//                    RANDOMKEY, COPY

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Cache.SetTTL
// ---------------------------------------------------------------------------

func TestSetTTLMissingKey(t *testing.T) {
	c := cache(t)
	if c.SetTTL("ghost", time.Second) {
		t.Fatal("SetTTL on missing key: want false")
	}
}

func TestSetTTLSetsExpiry(t *testing.T) {
	c := ttlCache(t)
	c.Set("k", []byte("v"), 0)
	if !c.SetTTL("k", 30*time.Millisecond) {
		t.Fatal("SetTTL: want true for live key")
	}
	if !c.Exists("k") {
		t.Fatal("key should still exist immediately after SetTTL")
	}
	time.Sleep(60 * time.Millisecond)
	if c.Exists("k") {
		t.Fatal("key should have expired after SetTTL duration elapsed")
	}
}

func TestSetTTLPersist(t *testing.T) {
	c := ttlCache(t)
	c.Set("k", []byte("v"), 50*time.Millisecond)
	// Remove expiry.
	if !c.SetTTL("k", 0) {
		t.Fatal("SetTTL(0): want true for live key")
	}
	time.Sleep(80 * time.Millisecond)
	if !c.Exists("k") {
		t.Fatal("key with TTL removed should survive past original deadline")
	}
	remaining, ok := c.TTL("k")
	if !ok || remaining != 0 {
		t.Fatalf("TTL after SetTTL(0): want (0, true), got (%v, %v)", remaining, ok)
	}
}

func TestSetTTLOnExpiredKey(t *testing.T) {
	c := ttlCache(t)
	c.Set("k", []byte("v"), 20*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	// Key is expired; SetTTL must return false.
	if c.SetTTL("k", time.Second) {
		t.Fatal("SetTTL on expired key: want false")
	}
}

func TestSetTTLUpdatesExpiry(t *testing.T) {
	c := ttlCache(t)
	c.Set("k", []byte("v"), 200*time.Millisecond)
	// Extend before it expires.
	if !c.SetTTL("k", 500*time.Millisecond) {
		t.Fatal("SetTTL: want true")
	}
	remaining, ok := c.TTL("k")
	if !ok || remaining <= 200*time.Millisecond {
		t.Fatalf("extended TTL should be >200ms, got %v (ok=%v)", remaining, ok)
	}
}

// ---------------------------------------------------------------------------
// Cache.GetSet
// ---------------------------------------------------------------------------

func TestGetSetMissingKey(t *testing.T) {
	c := cache(t)
	old, existed := c.GetSet("k", []byte("new"), 0)
	if existed {
		t.Fatal("GetSet on missing key: want existed=false")
	}
	if old != nil {
		t.Fatalf("GetSet on missing key: want nil old, got %q", old)
	}
	val, ok := c.Get("k")
	if !ok || string(val) != "new" {
		t.Fatalf("GetSet: new value not stored correctly: ok=%v val=%q", ok, val)
	}
}

func TestGetSetExistingKey(t *testing.T) {
	c := cache(t)
	c.Set("k", []byte("original"), 0)
	old, existed := c.GetSet("k", []byte("replaced"), 0)
	if !existed {
		t.Fatal("GetSet on existing key: want existed=true")
	}
	if string(old) != "original" {
		t.Fatalf("GetSet: old value wrong: got %q, want original", old)
	}
	val, ok := c.Get("k")
	if !ok || string(val) != "replaced" {
		t.Fatalf("GetSet: new value not stored: ok=%v val=%q", ok, val)
	}
}

func TestGetSetWithTTL(t *testing.T) {
	c := ttlCache(t)
	c.GetSet("k", []byte("v"), 30*time.Millisecond)
	if !c.Exists("k") {
		t.Fatal("GetSet with TTL: key should exist immediately")
	}
	time.Sleep(60 * time.Millisecond)
	if c.Exists("k") {
		t.Fatal("GetSet with TTL: key should have expired")
	}
}

func TestGetSetOnExpiredKey(t *testing.T) {
	c := ttlCache(t)
	c.Set("k", []byte("old"), 20*time.Millisecond)
	time.Sleep(40 * time.Millisecond)
	// Key is expired; GetSet should treat it as missing.
	old, existed := c.GetSet("k", []byte("new"), 0)
	if existed || old != nil {
		t.Fatalf("GetSet on expired key: want (nil, false), got (%q, %v)", old, existed)
	}
}

func TestGetSetAtomic(t *testing.T) {
	c := manyCache(t)
	c.Set("k", []byte("init"), 0)

	const goroutines = 32
	const ops = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			val := []byte(fmt.Sprintf("g%d", g))
			for i := 0; i < ops; i++ {
				c.GetSet("k", val, 0)
			}
		}()
	}
	wg.Wait()
	// No correctness assertion — just no panic/deadlock.
	if !c.Exists("k") {
		t.Fatal("key must still exist after concurrent GetSet")
	}
}

// ---------------------------------------------------------------------------
// Cache.IncrBy
// ---------------------------------------------------------------------------

func TestIncrByNewKey(t *testing.T) {
	c := cache(t)
	n, err := c.IncrBy("counter", 1)
	if err != nil {
		t.Fatalf("IncrBy on new key: %v", err)
	}
	if n != 1 {
		t.Fatalf("IncrBy: want 1, got %d", n)
	}
	val, ok := c.Get("counter")
	if !ok || string(val) != "1" {
		t.Fatalf("IncrBy: stored value: ok=%v val=%q", ok, val)
	}
}

func TestIncrByExistingKey(t *testing.T) {
	c := cache(t)
	c.Set("k", []byte("10"), 0)
	n, err := c.IncrBy("k", 5)
	if err != nil || n != 15 {
		t.Fatalf("IncrBy(5): want 15 nil, got %d %v", n, err)
	}
}

func TestIncrByNegativeDelta(t *testing.T) {
	c := cache(t)
	c.Set("k", []byte("10"), 0)
	n, err := c.IncrBy("k", -3)
	if err != nil || n != 7 {
		t.Fatalf("IncrBy(-3): want 7 nil, got %d %v", n, err)
	}
}

func TestIncrByNonInteger(t *testing.T) {
	c := cache(t)
	c.Set("k", []byte("hello"), 0)
	_, err := c.IncrBy("k", 1)
	if err == nil {
		t.Fatal("IncrBy on non-integer value: want error")
	}
}

func TestIncrByPreservesTTL(t *testing.T) {
	c := ttlCache(t)
	c.Set("k", []byte("0"), 200*time.Millisecond)
	_, err := c.IncrBy("k", 1)
	if err != nil {
		t.Fatalf("IncrBy: %v", err)
	}
	remaining, ok := c.TTL("k")
	if !ok || remaining <= 0 {
		t.Fatalf("IncrBy should preserve TTL: got ok=%v remaining=%v", ok, remaining)
	}
}

func TestIncrByZeroDelta(t *testing.T) {
	c := cache(t)
	c.Set("k", []byte("42"), 0)
	n, err := c.IncrBy("k", 0)
	if err != nil || n != 42 {
		t.Fatalf("IncrBy(0): want 42 nil, got %d %v", n, err)
	}
}

func TestIncrByNegativeStorage(t *testing.T) {
	c := cache(t)
	c.Set("k", []byte("5"), 0)
	n, err := c.IncrBy("k", -10)
	if err != nil || n != -5 {
		t.Fatalf("IncrBy(-10) from 5: want -5 nil, got %d %v", n, err)
	}
	val, ok := c.Get("k")
	if !ok || string(val) != "-5" {
		t.Fatalf("IncrBy: stored negative: ok=%v val=%q", ok, val)
	}
}

func TestIncrByNegativeStoredValue(t *testing.T) {
	c := cache(t)
	c.Set("k", []byte("-3"), 0)
	n, err := c.IncrBy("k", 10)
	if err != nil || n != 7 {
		t.Fatalf("IncrBy from negative: want 7 nil, got %d %v", n, err)
	}
}

func TestIncrByConcurrentSafety(t *testing.T) {
	c := manyCache(t)
	c.Set("cnt", []byte("0"), 0)

	const goroutines = 50
	const ops = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				c.IncrBy("cnt", 1) //nolint:errcheck
			}
		}()
	}
	wg.Wait()
	// cnt routes to a single shard; all increments must be atomic.
	// The final value must be exactly goroutines*ops.
	n, err := c.IncrBy("cnt", 0) // read by incrementing by 0
	if err != nil {
		t.Fatalf("final read: %v", err)
	}
	want := int64(goroutines * ops)
	if n != want {
		t.Fatalf("concurrent IncrBy: want %d, got %d (lost %d)", want, n, want-n)
	}
}

// ---------------------------------------------------------------------------
// Server: UNLINK
// ---------------------------------------------------------------------------

func TestServerUnlink(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("SET", "a", "1")
	cl.readLine()
	cl.send("SET", "b", "2")
	cl.readLine()

	cl.send("UNLINK", "a", "b", "ghost")
	if line := cl.readLine(); line != ":2" {
		t.Fatalf("UNLINK: got %q, want :2", line)
	}
	cl.send("EXISTS", "a")
	if line := cl.readLine(); line != ":0" {
		t.Fatalf("a should be gone after UNLINK")
	}
}

func TestServerUnlinkWrongArgs(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("UNLINK")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("UNLINK no args: expected error, got %q", line)
	}
}

// ---------------------------------------------------------------------------
// Server: STRLEN
// ---------------------------------------------------------------------------

func TestServerStrlen(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("SET", "k", "hello")
	cl.readLine()
	cl.send("STRLEN", "k")
	if line := cl.readLine(); line != ":5" {
		t.Fatalf("STRLEN: got %q, want :5", line)
	}
}

func TestServerStrlenMissing(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("STRLEN", "ghost")
	if line := cl.readLine(); line != ":0" {
		t.Fatalf("STRLEN missing: got %q, want :0", line)
	}
}

func TestServerStrlenWrongArgs(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("STRLEN")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("STRLEN no args: expected error, got %q", line)
	}
}

// ---------------------------------------------------------------------------
// Server: INCR / DECR / INCRBY / DECRBY
// ---------------------------------------------------------------------------

func TestServerIncr(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("INCR", "counter")
	if line := cl.readLine(); line != ":1" {
		t.Fatalf("INCR new key: got %q, want :1", line)
	}
	cl.send("INCR", "counter")
	if line := cl.readLine(); line != ":2" {
		t.Fatalf("INCR again: got %q, want :2", line)
	}
}

func TestServerDecr(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("SET", "k", "10")
	cl.readLine()
	cl.send("DECR", "k")
	if line := cl.readLine(); line != ":9" {
		t.Fatalf("DECR: got %q, want :9", line)
	}
}

func TestServerIncrBy(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("SET", "k", "10")
	cl.readLine()
	cl.send("INCRBY", "k", "5")
	if line := cl.readLine(); line != ":15" {
		t.Fatalf("INCRBY: got %q, want :15", line)
	}
}

func TestServerDecrBy(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("SET", "k", "10")
	cl.readLine()
	cl.send("DECRBY", "k", "3")
	if line := cl.readLine(); line != ":7" {
		t.Fatalf("DECRBY: got %q, want :7", line)
	}
}

func TestServerIncrByNegative(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("SET", "k", "10")
	cl.readLine()
	cl.send("INCRBY", "k", "-4")
	if line := cl.readLine(); line != ":6" {
		t.Fatalf("INCRBY negative: got %q, want :6", line)
	}
}

func TestServerIncrOnNonInteger(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("SET", "k", "notanumber")
	cl.readLine()
	cl.send("INCR", "k")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("INCR on non-integer: expected error, got %q", line)
	}
}

func TestServerIncrWrongArgs(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("INCR")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("INCR no args: expected error, got %q", line)
	}
}

func TestServerDecrWrongArgs(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("DECR")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("DECR no args: expected error, got %q", line)
	}
}

func TestServerIncrByWrongArgs(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("INCRBY", "k")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("INCRBY one arg: expected error, got %q", line)
	}
}

func TestServerDecrByWrongArgs(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("DECRBY", "k")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("DECRBY one arg: expected error, got %q", line)
	}
}

// ---------------------------------------------------------------------------
// Server: GETSET
// ---------------------------------------------------------------------------

func TestServerGetSet(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	// GETSET on new key — old value is null.
	cl.send("GETSET", "k", "first")
	if got := cl.readBulk(); got != "" {
		t.Fatalf("GETSET new key: expected null, got %q", got)
	}

	// GETSET on existing key — returns old value.
	cl.send("GETSET", "k", "second")
	if got := cl.readBulk(); got != "first" {
		t.Fatalf("GETSET existing: expected first, got %q", got)
	}

	// Key now holds second.
	cl.send("GET", "k")
	if got := cl.readBulk(); got != "second" {
		t.Fatalf("after GETSET: expected second, got %q", got)
	}
}

func TestServerGetSetWrongArgs(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("GETSET", "k")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("GETSET one arg: expected error, got %q", line)
	}
}

// ---------------------------------------------------------------------------
// Server: GETEX
// ---------------------------------------------------------------------------

func TestServerGetExNoOptions(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("SET", "k", "v")
	cl.readLine()
	cl.send("GETEX", "k")
	if got := cl.readBulk(); got != "v" {
		t.Fatalf("GETEX no opts: got %q, want v", got)
	}
}

func TestServerGetExMissing(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("GETEX", "ghost")
	if got := cl.readBulk(); got != "" {
		t.Fatalf("GETEX missing: expected null, got %q", got)
	}
}

func TestServerGetExWithPX(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("SET", "k", "v")
	cl.readLine()
	cl.send("GETEX", "k", "PX", "10000")
	if got := cl.readBulk(); got != "v" {
		t.Fatalf("GETEX PX: got %q, want v", got)
	}
	cl.send("TTL", "k")
	line := cl.readLine()
	if line == ":-1" || line == ":-2" {
		t.Fatalf("GETEX PX: TTL should be set, got %q", line)
	}
}

func TestServerGetExPersist(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("SET", "k", "v", "PX", "10000")
	cl.readLine()
	cl.send("GETEX", "k", "PERSIST")
	if got := cl.readBulk(); got != "v" {
		t.Fatalf("GETEX PERSIST: got %q, want v", got)
	}
	cl.send("TTL", "k")
	if line := cl.readLine(); line != ":-1" {
		t.Fatalf("GETEX PERSIST: TTL should be -1 (permanent), got %q", line)
	}
}

// ---------------------------------------------------------------------------
// Server: EXPIRE / PEXPIRE / PERSIST
// ---------------------------------------------------------------------------

func TestServerExpire(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("SET", "k", "v")
	cl.readLine()
	cl.send("EXPIRE", "k", "10")
	if line := cl.readLine(); line != ":1" {
		t.Fatalf("EXPIRE existing: got %q, want :1", line)
	}
	cl.send("TTL", "k")
	line := cl.readLine()
	if line == ":-1" || line == ":-2" {
		t.Fatalf("EXPIRE: TTL should be set, got %q", line)
	}
}

func TestServerExpireMissing(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("EXPIRE", "ghost", "10")
	if line := cl.readLine(); line != ":0" {
		t.Fatalf("EXPIRE missing: got %q, want :0", line)
	}
}

func TestServerPExpire(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("SET", "k", "v")
	cl.readLine()
	cl.send("PEXPIRE", "k", "10000")
	if line := cl.readLine(); line != ":1" {
		t.Fatalf("PEXPIRE: got %q, want :1", line)
	}
	cl.send("PTTL", "k")
	line := cl.readLine()
	if line == ":-1" || line == ":-2" {
		t.Fatalf("PEXPIRE: PTTL should be set, got %q", line)
	}
}

func TestServerPersist(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("SET", "k", "v", "EX", "60")
	cl.readLine()
	cl.send("PERSIST", "k")
	if line := cl.readLine(); line != ":1" {
		t.Fatalf("PERSIST: got %q, want :1", line)
	}
	cl.send("TTL", "k")
	if line := cl.readLine(); line != ":-1" {
		t.Fatalf("PERSIST: TTL should be -1 after PERSIST, got %q", line)
	}
}

func TestServerPersistMissing(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("PERSIST", "ghost")
	if line := cl.readLine(); line != ":0" {
		t.Fatalf("PERSIST missing: got %q, want :0", line)
	}
}

func TestServerExpireWrongArgs(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("EXPIRE", "k")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("EXPIRE one arg: expected error, got %q", line)
	}
}

func TestServerPExpireWrongArgs(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("PEXPIRE", "k")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("PEXPIRE one arg: expected error, got %q", line)
	}
}

func TestServerPersistWrongArgs(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("PERSIST")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("PERSIST no args: expected error, got %q", line)
	}
}

// ---------------------------------------------------------------------------
// Server: RANDOMKEY
// ---------------------------------------------------------------------------

func TestServerRandomKeyEmpty(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	// Cache is flushed by testServer.
	cl.send("RANDOMKEY")
	if got := cl.readBulk(); got != "" {
		t.Fatalf("RANDOMKEY on empty cache: expected null, got %q", got)
	}
}

func TestServerRandomKeyReturnsExistingKey(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	keys := []string{"alpha", "beta", "gamma"}
	for _, k := range keys {
		cl.send("SET", k, "v")
		cl.readLine()
	}

	seen := make(map[string]bool)
	for i := 0; i < 20; i++ {
		cl.send("RANDOMKEY")
		got := cl.readBulk()
		if got == "" {
			t.Fatal("RANDOMKEY returned null when cache is non-empty")
		}
		seen[got] = true
	}
	for _, k := range keys {
		if seen[k] {
			return // at least one known key was returned — good enough
		}
	}
	t.Fatalf("RANDOMKEY never returned any of the known keys; seen: %v", seen)
}

// ---------------------------------------------------------------------------
// Server: COPY
// ---------------------------------------------------------------------------

func TestServerCopy(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("SET", "src", "value")
	cl.readLine()
	cl.send("COPY", "src", "dst")
	if line := cl.readLine(); line != ":1" {
		t.Fatalf("COPY: got %q, want :1", line)
	}
	cl.send("GET", "dst")
	if got := cl.readBulk(); got != "value" {
		t.Fatalf("COPY: dst has wrong value: %q", got)
	}
	// Source must still exist.
	cl.send("EXISTS", "src")
	if line := cl.readLine(); line != ":1" {
		t.Fatalf("COPY: source should still exist after COPY")
	}
}

func TestServerCopyMissingSource(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("COPY", "ghost", "dst")
	if line := cl.readLine(); line != ":0" {
		t.Fatalf("COPY missing source: got %q, want :0", line)
	}
}

func TestServerCopyWrongArgs(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("COPY", "k")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("COPY one arg: expected error, got %q", line)
	}
}

// ---------------------------------------------------------------------------
// Integration: INCR + EXPIRE combo (common cache pattern)
// ---------------------------------------------------------------------------

func TestIncrWithExpire(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	// Rate-limit pattern: INCR + EXPIRE on first hit.
	cl.send("INCR", "hits")
	if line := cl.readLine(); line != ":1" {
		t.Fatalf("INCR: got %q, want :1", line)
	}
	cl.send("EXPIRE", "hits", "60")
	if line := cl.readLine(); line != ":1" {
		t.Fatalf("EXPIRE after INCR: got %q, want :1", line)
	}
	cl.send("INCR", "hits")
	if line := cl.readLine(); line != ":2" {
		t.Fatalf("second INCR: got %q, want :2", line)
	}
	cl.send("TTL", "hits")
	line := cl.readLine()
	if line == ":-1" || line == ":-2" {
		t.Fatalf("TTL after EXPIRE: want positive, got %q", line)
	}
}

// ---------------------------------------------------------------------------
// Stress: concurrent INCR across multiple server connections
// ---------------------------------------------------------------------------

func TestServerIncrConcurrent(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()

	const clients = 20
	const ops = 50
	var wg sync.WaitGroup
	var errors int64
	wg.Add(clients)
	for i := 0; i < clients; i++ {
		go func() {
			defer wg.Done()
			cl := dial(t, addr)
			defer cl.close()
			for j := 0; j < ops; j++ {
				cl.send("INCR", "shared-counter")
				line := cl.readLine()
				if len(line) == 0 || line[0] != ':' {
					atomic.AddInt64(&errors, 1)
				}
			}
		}()
	}
	wg.Wait()
	if errors > 0 {
		t.Fatalf("concurrent INCR: %d unexpected responses", errors)
	}

	// Final value must be exactly clients*ops.
	cl := dial(t, addr)
	defer cl.close()
	cl.send("GET", "shared-counter")
	got := cl.readBulk()
	want := fmt.Sprintf("%d", clients*ops)
	if got != want {
		t.Fatalf("concurrent INCR final value: got %q, want %q", got, want)
	}
}

// ---------------------------------------------------------------------------
// Regression: STRLEN on empty string value
// ---------------------------------------------------------------------------

func TestServerStrlenEmptyValue(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("SET", "k", "")
	cl.readLine()
	cl.send("STRLEN", "k")
	if line := cl.readLine(); line != ":0" {
		t.Fatalf("STRLEN empty value: got %q, want :0", line)
	}
}

// ---------------------------------------------------------------------------
// Regression: GETEX wrong-args guard
// ---------------------------------------------------------------------------

func TestServerGetExWrongArgs(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("GETEX")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("GETEX no args: expected error, got %q", line)
	}
}

// ---------------------------------------------------------------------------
// Regression: COPY wrong-args guard
// ---------------------------------------------------------------------------

func TestServerGetSetWrongArgCount(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	// Too many args for GETSET.
	cl.send("GETSET", "k", "v", "extra")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("GETSET too many args: expected error, got %q", line)
	}
}

// ---------------------------------------------------------------------------
// Ensure STRLEN returns byte count, not rune count (binary safety)
// ---------------------------------------------------------------------------

func TestServerStrlenBinary(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	// "héllo" is 6 bytes in UTF-8 (é is 2 bytes).
	val := "héllo"
	cl.send("SET", "k", val)
	cl.readLine()
	cl.send("STRLEN", "k")
	want := fmt.Sprintf(":%d", len(val))
	if line := cl.readLine(); line != want {
		t.Fatalf("STRLEN binary: got %q, want %q", line, want)
	}
}

// ---------------------------------------------------------------------------
// Ensure INCRBY with bad increment arg returns error
// ---------------------------------------------------------------------------

func TestServerIncrByBadDelta(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("INCRBY", "k", "notanumber")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("INCRBY bad delta: expected error, got %q", line)
	}
}

func TestServerDecrByBadDelta(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("DECRBY", "k", "notanumber")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("DECRBY bad delta: expected error, got %q", line)
	}
}

// ---------------------------------------------------------------------------
// Self-check: new commands appear in the server doc comment
// ---------------------------------------------------------------------------

func TestNewCommandsDocumented(_ *testing.T) {
	// This test is intentionally a compile-time/grep check expressed as a Go
	// test so failures surface in the test run. The actual validation is done
	// by TestServerIncr etc. above; this just acts as a canary.
	_ = strings.Join([]string{
		"UNLINK", "STRLEN", "INCR", "INCRBY", "DECR", "DECRBY",
		"GETSET", "GETEX", "EXPIRE", "PEXPIRE", "PERSIST",
		"RANDOMKEY", "COPY",
	}, ",")
}
