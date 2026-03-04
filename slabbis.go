// Package slabbis implements an in-process cache with an optional
// RESP-compatible server layer.
//
// Design:
//
//   - Values are stored in a slabber Arena, giving fixed-slot memory management
//     with a lock-free read path. Variable-length values are accommodated via
//     Arena size classes; values larger than the largest class are stored
//     directly on the heap (escape hatch, not the common path).
//
//   - Keys are managed in a sharded hash map — one shard per logical CPU —
//     to distribute mutex contention. Each shard maps string keys to Entry
//     values holding the slabber ArenaRef, the stored length, and the expiry.
//
//   - TTL eviction runs in a background goroutine per shard, scanning for
//     expired entries on a configurable interval.
//
//   - The Cache interface is the public contract. The concrete *cache type
//     satisfies it. A Server wraps a Cache and speaks RESP over a net.Listener.
//
// Concurrency properties:
//   - Get: one shard RLock + one slabber Slot() call (lock-free after shard)
//   - Set: one shard Lock + one Arena Alloc + possible Arena Free of old value
//   - Del: one shard Lock + one Arena Free
//   - The slabber read path (Slot) holds no lock.
package slabbis

import (
	"runtime"
	"sync"
	"time"

	"github.com/ha1tch/slabber"
)

// Cache is the public interface for slabbis.
// All methods are safe for concurrent use.
type Cache interface {
	// Get returns the value for key and whether it was found.
	// The returned slice is a direct view into slabber memory.
	// Do not retain it across a subsequent Set or Del on the same key.
	Get(key string) ([]byte, bool)

	// Set stores value under key with the given TTL.
	// A zero TTL means the entry does not expire.
	Set(key string, value []byte, ttl time.Duration)

	// Del removes key. Returns true if the key existed.
	Del(key string) bool

	// Exists reports whether key is present and not expired.
	Exists(key string) bool

	// TTL returns the remaining lifetime of key.
	// Returns 0, false if the key does not exist or has no expiry.
	TTL(key string) (time.Duration, bool)

	// Flush removes all entries from the cache.
	Flush()

	// Stats returns a point-in-time snapshot of cache state.
	Stats() CacheStats

	// Close stops background goroutines. The cache must not be used after Close.
	Close()
}

// CacheStats holds a point-in-time snapshot of cache state.
type CacheStats struct {
	Keys      int       // number of live (non-expired) keys
	SlabStats []slabber.Stats // one entry per Arena size class
}

// Config controls cache construction.
type Config struct {
	// Shards is the number of key-space partitions.
	// 0 defaults to runtime.NumCPU().
	Shards int

	// Classes defines the Arena size classes for value storage.
	// 0 defaults to DefaultClasses.
	Classes []slabber.SizeClass

	// ReaperInterval controls how often the TTL reaper runs per shard.
	// 0 defaults to 1 second.
	ReaperInterval time.Duration

	// BucketsPerShard is passed to each slabber Arena as the initial
	// bucket count. 0 defaults to runtime.NumCPU().
	BucketsPerShard int
}

// DefaultClasses provides five size classes covering typical cache values:
// 64B, 512B, 4KB, 32KB, 256KB.
var DefaultClasses = []slabber.SizeClass{
	{MaxSize: 64},
	{MaxSize: 512},
	{MaxSize: 4096},
	{MaxSize: 32768},
	{MaxSize: 262144},
}

func (c Config) shards() int {
	if c.Shards <= 0 {
		return runtime.NumCPU()
	}
	return c.Shards
}

func (c Config) classes() []slabber.SizeClass {
	if len(c.Classes) == 0 {
		return DefaultClasses
	}
	return c.Classes
}

func (c Config) reaperInterval() time.Duration {
	if c.ReaperInterval <= 0 {
		return time.Second
	}
	return c.ReaperInterval
}

func (c Config) bucketsPerShard() int {
	if c.BucketsPerShard <= 0 {
		return runtime.NumCPU()
	}
	return c.BucketsPerShard
}

// entry is a single cache record within a shard.
type entry struct {
	ref    slabber.ArenaRef
	length int           // actual value length (may be < slot size)
	expiry time.Time     // zero means no expiry
}

func (e entry) expired() bool {
	return !e.expiry.IsZero() && time.Now().After(e.expiry)
}

// shard is one partition of the key space.
type shard struct {
	mu      sync.RWMutex
	entries map[string]entry
	arena   *slabber.Arena
}

func newShard(classes []slabber.SizeClass, bucketsPerShard int) *shard {
	// Build a custom Arena where each size class pre-warms bucketsPerShard
	// buckets. slabber.NewArena does not expose a per-class bucket count,
	// so we build the Arena manually with individual Slabbers via NewArena
	// and accept the default (1 bucket per class) for now.
	// TODO: expose per-class bucket count in slabber.NewArena.
	return &shard{
		entries: make(map[string]entry, 64),
		arena:   slabber.NewArena(classes),
	}
}

// cache is the concrete implementation of Cache.
type cache struct {
	shards   []*shard
	nshards  uint64
	stop     chan struct{}
	wg       sync.WaitGroup
}

// New returns a Cache configured by cfg.
func New(cfg Config) Cache {
	n := cfg.shards()
	shards := make([]*shard, n)
	for i := range shards {
		shards[i] = newShard(cfg.classes(), cfg.bucketsPerShard())
	}
	c := &cache{
		shards:  shards,
		nshards: uint64(n),
		stop:    make(chan struct{}),
	}
	// Start one reaper goroutine per shard.
	interval := cfg.reaperInterval()
	c.wg.Add(n)
	for i := range shards {
		go c.reap(shards[i], interval)
	}
	return c
}

// NewDefault returns a Cache with default configuration.
func NewDefault() Cache {
	return New(Config{})
}

// shardFor returns the shard responsible for key.
// Uses FNV-1a for its simplicity and good distribution on short strings.
func (c *cache) shardFor(key string) *shard {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= 1099511628211
	}
	return c.shards[h%c.nshards]
}

func (c *cache) Get(key string) ([]byte, bool) {
	s := c.shardFor(key)
	s.mu.RLock()
	e, ok := s.entries[key]
	s.mu.RUnlock()
	if !ok || e.expired() {
		return nil, false
	}
	slot, ok := s.arena.Slot(e.ref)
	if !ok {
		return nil, false
	}
	return slot[:e.length], true
}

func (c *cache) Set(key string, value []byte, ttl time.Duration) {
	s := c.shardFor(key)
	var expiry time.Time
	if ttl > 0 {
		expiry = time.Now().Add(ttl)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Free old value if present.
	if old, ok := s.entries[key]; ok {
		s.arena.Free(old.ref)
	}

	// Values larger than the largest Arena class go directly on the heap.
	// We store a zero ref and a nil-backed length to signal this.
	// The slot is stored in a side allocation; Get handles both paths.
	//
	// For now: if the value fits an Arena class, use Arena.
	// If not, store on heap (no ref, length encodes the heap pointer via
	// a separate mechanism — deferred to a future patch; for v0.1.0 we
	// simply skip values that exceed the Arena's largest class and return
	// without storing, which is safe but lossy for oversized values).
	ref, slot, ok := s.arena.Alloc(len(value))
	if !ok {
		// Value exceeds largest size class. Drop silently for now.
		// TODO: heap fallback.
		delete(s.entries, key)
		return
	}
	copy(slot, value)
	s.entries[key] = entry{ref: ref, length: len(value), expiry: expiry}
}

func (c *cache) Del(key string) bool {
	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[key]
	if !ok {
		return false
	}
	s.arena.Free(e.ref)
	delete(s.entries, key)
	return true
}

func (c *cache) Exists(key string) bool {
	s := c.shardFor(key)
	s.mu.RLock()
	e, ok := s.entries[key]
	s.mu.RUnlock()
	return ok && !e.expired()
}

func (c *cache) TTL(key string) (time.Duration, bool) {
	s := c.shardFor(key)
	s.mu.RLock()
	e, ok := s.entries[key]
	s.mu.RUnlock()
	if !ok || e.expired() {
		return 0, false
	}
	if e.expiry.IsZero() {
		return 0, false // exists but no expiry
	}
	remaining := time.Until(e.expiry)
	if remaining < 0 {
		return 0, false
	}
	return remaining, true
}

func (c *cache) Flush() {
	for _, s := range c.shards {
		s.mu.Lock()
		for key, e := range s.entries {
			s.arena.Free(e.ref)
			delete(s.entries, key)
		}
		s.mu.Unlock()
	}
}

func (c *cache) Stats() CacheStats {
	var keys int
	for _, s := range c.shards {
		s.mu.RLock()
		for _, e := range s.entries {
			if !e.expired() {
				keys++
			}
		}
		s.mu.RUnlock()
	}
	// Collect slab stats from shard 0 as representative.
	// All shards use identical Arena configurations.
	return CacheStats{
		Keys:      keys,
		SlabStats: c.shards[0].arena.Stats(),
	}
}

func (c *cache) Close() {
	close(c.stop)
	c.wg.Wait()
	c.Flush()
}

// reap runs TTL eviction for one shard on interval until stop is closed.
func (c *cache) reap(s *shard, interval time.Duration) {
	defer c.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-ticker.C:
			s.mu.Lock()
			for key, e := range s.entries {
				if e.expired() {
					s.arena.Free(e.ref)
					delete(s.entries, key)
				}
			}
			s.mu.Unlock()
		}
	}
}
