package slabbis_test

// integration_feedback_test.go — tests for items raised in the olu integration
// feedback document (v0.1.5):
//
//   #1  SCAN command
//   #2a Oversized-value drop logging (tested at Cache level; server-level
//       logging is observable via the server log but not via RESP response)
//   #3  DevConfig() constructor
//   #4a Config.MaxValueSize() helper
//   #5  CLI flags tested via Config / parseClasses equivalent

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ha1tch/slabber"
	"github.com/ha1tch/slabbis"
)

// ---------------------------------------------------------------------------
// #3: DevConfig
// ---------------------------------------------------------------------------

func TestDevConfigWorks(t *testing.T) {
	c := slabbis.New(slabbis.DevConfig())
	defer c.Close()

	c.Set("k", []byte("hello"), 0)
	val, ok := c.Get("k")
	if !ok || string(val) != "hello" {
		t.Fatalf("DevConfig: basic Get/Set failed: ok=%v val=%q", ok, val)
	}
}

func TestDevConfigMaxValueSize(t *testing.T) {
	cfg := slabbis.DevConfig()
	// DevConfig has two classes: 64B and 4KB.
	if cfg.MaxValueSize() != 4096 {
		t.Fatalf("DevConfig.MaxValueSize(): want 4096, got %d", cfg.MaxValueSize())
	}
}

func TestDevConfigSingleShard(t *testing.T) {
	cfg := slabbis.DevConfig()
	if cfg.Shards != 1 {
		t.Fatalf("DevConfig: want Shards=1, got %d", cfg.Shards)
	}
}

func TestDevConfigSmallFootprintAcceptsTypicalValues(t *testing.T) {
	c := slabbis.New(slabbis.DevConfig())
	defer c.Close()

	// All values up to the 4KB ceiling should store and round-trip.
	for _, size := range []int{0, 1, 63, 64, 65, 512, 1024, 4096} {
		val := make([]byte, size)
		for i := range val {
			val[i] = byte(i % 251)
		}
		key := fmt.Sprintf("dev-%d", size)
		c.Set(key, val, 0)
		got, ok := c.Get(key)
		if !ok {
			t.Errorf("DevConfig: size %d: Get miss", size)
			continue
		}
		if len(got) != size {
			t.Errorf("DevConfig: size %d: got len %d", size, len(got))
		}
	}
}

func TestDevConfigTTLWorks(t *testing.T) {
	c := slabbis.New(slabbis.DevConfig())
	defer c.Close()

	c.Set("k", []byte("v"), 30*time.Millisecond)
	if !c.Exists("k") {
		t.Fatal("DevConfig: key should exist before TTL expiry")
	}
	// DevConfig has a 50ms reaper — wait 100ms for both TTL and reaper.
	time.Sleep(100 * time.Millisecond)
	if c.Exists("k") {
		t.Fatal("DevConfig: key should have expired")
	}
}

// ---------------------------------------------------------------------------
// #4a: Config.MaxValueSize
// ---------------------------------------------------------------------------

func TestMaxValueSizeDefault(t *testing.T) {
	cfg := slabbis.Config{}
	// Zero Classes → DefaultClasses applies → largest is 262144.
	if got := cfg.MaxValueSize(); got != 262144 {
		t.Fatalf("MaxValueSize() default: want 262144, got %d", got)
	}
}

func TestMaxValueSizeCustom(t *testing.T) {
	cfg := slabbis.Config{
		Classes: []slabber.SizeClass{
			{MaxSize: 64},
			{MaxSize: 512},
			{MaxSize: 8192},
		},
	}
	if got := cfg.MaxValueSize(); got != 8192 {
		t.Fatalf("MaxValueSize() custom: want 8192, got %d", got)
	}
}

func TestMaxValueSizeEmptyClasses(t *testing.T) {
	cfg := slabbis.Config{
		Classes: []slabber.SizeClass{},
	}
	// Explicitly empty (not nil) — no classes configured.
	// MaxValueSize should return 0 without panicking.
	// Note: nil Classes falls back to DefaultClasses; empty slice does not.
	// This tests the guard path.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("MaxValueSize panicked on empty Classes: %v", r)
		}
	}()
	_ = cfg.MaxValueSize()
}

func TestMaxValueSizeMatchesDropBoundary(t *testing.T) {
	cfg := slabbis.Config{
		Shards:         1,
		ReaperInterval: time.Hour,
		Classes: []slabber.SizeClass{
			{MaxSize: 64},
		},
	}
	c := slabbis.New(cfg)
	defer c.Close()

	maxSize := cfg.MaxValueSize() // 64
	// Value at exactly the boundary: should store.
	c.Set("k", make([]byte, maxSize), 0)
	if !c.Exists("k") {
		t.Fatalf("MaxValueSize: value of exactly MaxValueSize (%d) should store", maxSize)
	}
	// Value one byte over: should be silently dropped.
	c.Set("k", make([]byte, maxSize+1), 0)
	if c.Exists("k") {
		t.Fatalf("MaxValueSize: value of MaxValueSize+1 (%d) should be dropped", maxSize+1)
	}
}

// ---------------------------------------------------------------------------
// #1: SCAN command (server level)
// ---------------------------------------------------------------------------

func TestServerScanEmpty(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("SCAN", "0")
	header := cl.readLine()
	if header != "*2" {
		t.Fatalf("SCAN empty: expected *2 array header, got %q", header)
	}
	cursor := cl.readBulk()
	if cursor != "0" {
		t.Fatalf("SCAN: cursor must always be 0, got %q", cursor)
	}
	// Keys array header.
	keysHeader := cl.readLine()
	if keysHeader != "*0" {
		t.Fatalf("SCAN empty: expected empty keys array *0, got %q", keysHeader)
	}
}

func TestServerScanReturnsAllKeys(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	keys := []string{"alpha", "beta", "gamma"}
	for _, k := range keys {
		cl.send("SET", k, "v")
		cl.readLine()
	}

	cl.send("SCAN", "0")
	header := cl.readLine()
	if header != "*2" {
		t.Fatalf("SCAN: expected *2, got %q", header)
	}
	cursor := cl.readBulk()
	if cursor != "0" {
		t.Fatalf("SCAN: cursor must be 0, got %q", cursor)
	}
	// Read keys array.
	keysHeader := cl.readLine()
	var n int
	fmt.Sscanf(keysHeader[1:], "%d", &n)
	if n != len(keys) {
		t.Fatalf("SCAN: expected %d keys, got %d", len(keys), n)
	}
	got := make(map[string]bool)
	for i := 0; i < n; i++ {
		got[cl.readBulk()] = true
	}
	for _, k := range keys {
		if !got[k] {
			t.Errorf("SCAN: missing key %q", k)
		}
	}
}

func TestServerScanWithMatch(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	for _, k := range []string{"user:1", "user:2", "session:1"} {
		cl.send("SET", k, "v")
		cl.readLine()
	}

	cl.send("SCAN", "0", "MATCH", "user:*")
	cl.readLine() // *2
	cursor := cl.readBulk()
	if cursor != "0" {
		t.Fatalf("SCAN MATCH: cursor must be 0, got %q", cursor)
	}
	keysHeader := cl.readLine()
	var n int
	fmt.Sscanf(keysHeader[1:], "%d", &n)
	if n != 2 {
		t.Fatalf("SCAN MATCH user:*: expected 2 keys, got %d", n)
	}
	for i := 0; i < n; i++ {
		k := cl.readBulk()
		if !strings.HasPrefix(k, "user:") {
			t.Errorf("SCAN MATCH user:*: unexpected key %q", k)
		}
	}
}

func TestServerScanCountIgnored(t *testing.T) {
	// COUNT is a hint for disk-based stores; slabbis ignores it and always
	// returns all matching keys. Verify it parses without error.
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("SET", "k", "v")
	cl.readLine()

	cl.send("SCAN", "0", "MATCH", "*", "COUNT", "100")
	header := cl.readLine()
	if header != "*2" {
		t.Fatalf("SCAN COUNT: expected *2, got %q", header)
	}
	cursor := cl.readBulk()
	if cursor != "0" {
		t.Fatalf("SCAN COUNT: cursor must be 0, got %q", cursor)
	}
	keysHeader := cl.readLine()
	var n int
	fmt.Sscanf(keysHeader[1:], "%d", &n)
	if n != 1 {
		t.Fatalf("SCAN COUNT: expected 1 key, got %d", n)
	}
	cl.readBulk() // consume the key
}

func TestServerScanCursorAlwaysZero(t *testing.T) {
	// Clients that follow the Redis pattern of scanning until cursor == "0"
	// should terminate after a single call regardless of the cursor value sent.
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	for _, cur := range []string{"0", "1", "42", "99999"} {
		cl.send("SCAN", cur)
		cl.readLine() // *2
		cursor := cl.readBulk()
		if cursor != "0" {
			t.Errorf("SCAN cursor %q: response cursor must always be 0, got %q", cur, cursor)
		}
		// Drain the keys array.
		keysHeader := cl.readLine()
		var n int
		fmt.Sscanf(keysHeader[1:], "%d", &n)
		for i := 0; i < n; i++ {
			cl.readBulk()
		}
	}
}

func TestServerScanWrongArgs(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("SCAN")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("SCAN no args: expected error, got %q", line)
	}
}

// TestServerScanDoubleStarPattern verifies that the double-star pattern
// ("entity:list:**") that olu's DeletePattern currently emits due to a
// redundant-wildcard bug produces correct, non-empty results against slabbis.
//
// path.Match treats "**" identically to "*" when the string contains no path
// separators (slabbis uses filepath.Match, not filepath.Glob). In particular:
//
//   - "entity:list:**" matches everything "entity:list:*" matches.
//   - The colon ":" is not a path separator, so "*" matches across it freely.
//   - "entity:list:**" therefore matches "entity:list:sub:key" (two colons
//     after the prefix) for the same reason "entity:list:*" does.
//
// olu can safely ship their DeletePattern fix (removing the redundant "*")
// and the resulting "entity:list:*" patterns will match exactly the same keys
// against any slabbis version that uses path.Match (i.e. all of them).
func TestServerScanDoubleStarPattern(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	// Populate keys that olu's entity cache would create.
	keysToSet := []string{
		"entity:list:1",
		"entity:list:abc",
		"entity:list:sub:key", // has an extra colon — both * and ** must match
		"entity:other",        // should NOT match entity:list:** or entity:list:*
		"other:list:1",        // should NOT match either pattern
	}
	for _, k := range keysToSet {
		cl.send("SET", k, "v")
		cl.readLine()
	}

	// --- Double-star pattern (olu's current buggy output) ---
	cl.send("SCAN", "0", "MATCH", "entity:list:**")
	cl.readLine() // *2
	cursor := cl.readBulk()
	if cursor != "0" {
		t.Fatalf("SCAN double-star: cursor must be 0, got %q", cursor)
	}
	keysHeader := cl.readLine()
	var nDouble int
	fmt.Sscanf(keysHeader[1:], "%d", &nDouble)
	doubleStarKeys := make(map[string]bool, nDouble)
	for i := 0; i < nDouble; i++ {
		doubleStarKeys[cl.readBulk()] = true
	}

	// --- Single-star pattern (olu's fixed output) ---
	cl.send("SCAN", "0", "MATCH", "entity:list:*")
	cl.readLine() // *2
	cl.readBulk() // cursor "0"
	keysHeader = cl.readLine()
	var nSingle int
	fmt.Sscanf(keysHeader[1:], "%d", &nSingle)
	singleStarKeys := make(map[string]bool, nSingle)
	for i := 0; i < nSingle; i++ {
		singleStarKeys[cl.readBulk()] = true
	}

	// Both patterns must return the same set of keys.
	if nDouble != nSingle {
		t.Errorf("double-star (%d keys) and single-star (%d keys) returned different counts",
			nDouble, nSingle)
	}
	for k := range doubleStarKeys {
		if !singleStarKeys[k] {
			t.Errorf("double-star matched %q but single-star did not", k)
		}
	}
	for k := range singleStarKeys {
		if !doubleStarKeys[k] {
			t.Errorf("single-star matched %q but double-star did not", k)
		}
	}

	// Confirm expected keys are present in both.
	expectedMatch := []string{"entity:list:1", "entity:list:abc", "entity:list:sub:key"}
	for _, k := range expectedMatch {
		if !doubleStarKeys[k] {
			t.Errorf("double-star pattern should have matched %q", k)
		}
		if !singleStarKeys[k] {
			t.Errorf("single-star pattern should have matched %q", k)
		}
	}

	// Confirm keys outside the prefix are absent from both.
	expectedNoMatch := []string{"entity:other", "other:list:1"}
	for _, k := range expectedNoMatch {
		if doubleStarKeys[k] {
			t.Errorf("double-star pattern should not have matched %q", k)
		}
		if singleStarKeys[k] {
			t.Errorf("single-star pattern should not have matched %q", k)
		}
	}
}

// TestServerScanDoubleStarVsKeys verifies that SCAN MATCH and KEYS return
// identical results for the double-star pattern, confirming consistent
// behaviour regardless of which command olu or its client library uses.
func TestServerScanDoubleStarVsKeys(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	for _, k := range []string{"entity:list:1", "entity:list:2", "entity:other"} {
		cl.send("SET", k, "v")
		cl.readLine()
	}

	// KEYS with double-star.
	cl.send("KEYS", "entity:list:**")
	keysHeader := cl.readLine()
	var nKeys int
	fmt.Sscanf(keysHeader[1:], "%d", &nKeys)
	keysResult := make(map[string]bool, nKeys)
	for i := 0; i < nKeys; i++ {
		keysResult[cl.readBulk()] = true
	}

	// SCAN with double-star.
	cl.send("SCAN", "0", "MATCH", "entity:list:**")
	cl.readLine() // *2
	cl.readBulk() // cursor
	scanKeysHeader := cl.readLine()
	var nScan int
	fmt.Sscanf(scanKeysHeader[1:], "%d", &nScan)
	scanResult := make(map[string]bool, nScan)
	for i := 0; i < nScan; i++ {
		scanResult[cl.readBulk()] = true
	}

	if nKeys != nScan {
		t.Errorf("KEYS returned %d, SCAN returned %d for same pattern", nKeys, nScan)
	}
	for k := range keysResult {
		if !scanResult[k] {
			t.Errorf("KEYS returned %q but SCAN did not", k)
		}
	}
}

// ---------------------------------------------------------------------------
// #2a: Oversized-value drop — Cache level correctness
// (Server-level WARN log is not testable via RESP response, but the
// underlying Cache behaviour that triggers it is tested here.)
// ---------------------------------------------------------------------------

func TestOversizedDropNotVisibleAfterSet(t *testing.T) {
	// Use a cache with a tiny max class so we can reliably trigger the drop.
	c := slabbis.New(slabbis.Config{
		Shards:         1,
		ReaperInterval: time.Hour,
		Classes:        []slabber.SizeClass{{MaxSize: 64}},
	})
	defer c.Close()

	// Establish a baseline value.
	c.Set("k", []byte("original"), 0)
	if !c.Exists("k") {
		t.Fatal("setup: small value not stored")
	}

	// Oversized Set — drops the key (current behaviour, documented).
	c.Set("k", make([]byte, 65), 0)

	// The key must not exist after the drop.
	if c.Exists("k") {
		t.Fatal("oversized Set: key must not exist after drop")
	}
	if _, ok := c.Get("k"); ok {
		t.Fatal("oversized Set: Get must miss after drop")
	}
}

func TestMaxValueSizeHelperPreventsBlindDrop(t *testing.T) {
	// Demonstrate the intended usage pattern: callers check MaxValueSize()
	// before calling Set to avoid silent drops.
	cfg := slabbis.Config{
		Shards:         1,
		ReaperInterval: time.Hour,
		Classes:        []slabber.SizeClass{{MaxSize: 64}},
	}
	c := slabbis.New(cfg)
	defer c.Close()

	value := make([]byte, 65) // one byte over the limit
	maxSize := cfg.MaxValueSize()

	if len(value) > maxSize {
		// Caller correctly identifies the problem before calling Set.
		// (In real code, this would return an error or fall back to another store.)
		return
	}
	// If we reach here the test logic is wrong.
	c.Set("k", value, 0)
	t.Error("test logic error: should have detected oversized value via MaxValueSize()")
}
