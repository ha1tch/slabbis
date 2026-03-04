package slabbis_test

// ---------------------------------------------------------------------------
// Benchmark: Get (zero-copy) vs GetCopy (copy under lock)
//
// Run with:
//
//	go test -run ^$ -bench . -benchmem -benchtime=5s -count=6 -cpu=1,4,8 .
//
// Then pipe through benchstat for a proper comparison:
//
//	go test -run ^$ -bench . -benchmem -benchtime=5s -count=6 -cpu=1 . \
//	    | tee bench.txt
//	# edit bench.txt to split Get vs GetCopy results, then:
//	benchstat get.txt getcopy.txt
//
// What we measure
// ---------------
//   - BenchmarkGet / BenchmarkGetCopy
//     Sequential single-goroutine throughput. Isolates the raw cost of the
//     copy + extra lock scope, with no contention.
//
//   - BenchmarkGetParallel / BenchmarkGetCopyParallel
//     Read-only parallel throughput at GOMAXPROCS goroutines.
//     Both use RLock so there is no write contention; this shows how the
//     slightly-longer critical section in GetCopy affects parallel reads.
//
//   - BenchmarkGetMixedParallel / BenchmarkGetCopyMixedParallel
//     80 % reads, 20 % writes at GOMAXPROCS goroutines.
//     Closest to real server workload. The write path takes a full Lock,
//     which makes the RLock hold-time difference between Get and GetCopy
//     more visible because writers have to wait for all concurrent readers
//     to release.
//
// Interpreting results
// --------------------
// The key question is not the absolute ns/op difference but the *relative*
// penalty. A 10 ns overhead on a 30 ns operation is severe; the same 10 ns
// on a 300 ns network round-trip is noise.
//
// B/op and allocs/op will always be higher for GetCopy (one allocation per
// call, size = value length). That is expected and acceptable — it is
// exactly the allocation that makes the slice safe to retain.
// ---------------------------------------------------------------------------

import (
	"fmt"
	"testing"
	"time"

	"github.com/ha1tch/slabber"
	"github.com/ha1tch/slabbis"
)

// benchClasses mirrors testClasses: capped at 4 KB to keep arena virtual
// address space within race-detector budget. Benchmarks that want to measure
// the large-value path should run with SLABBIS_FULL_CLASSES=1.
var benchClasses = []slabber.SizeClass{
	{MaxSize: 64},
	{MaxSize: 512},
	{MaxSize: 4096},
}

// benchSizes covers the full range of the active size classes.
var benchSizes = []struct {
	name string
	size int
}{
	{"8B", 8},
	{"64B", 64},
	{"512B", 512},
	{"4KB", 4096},
}

func newBenchCache(b *testing.B, shards int) slabbis.Cache {
	b.Helper()
	c := slabbis.New(slabbis.Config{
		Shards:         shards,
		ReaperInterval: time.Hour,
		Classes:        benchClasses,
	})
	b.Cleanup(func() { c.Close() })
	return c
}

// seedKeys writes n keys of valueSize bytes and returns the key list.
func seedKeys(b *testing.B, c slabbis.Cache, n, valueSize int) []string {
	b.Helper()
	val := make([]byte, valueSize)
	for i := range val {
		val[i] = byte(i % 251)
	}
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("bench-%06d", i)
		c.Set(keys[i], val, 0)
	}
	return keys
}

// ---------------------------------------------------------------------------
// Sequential
// ---------------------------------------------------------------------------

// BenchmarkGet measures zero-copy Get: shardFor + RLock + map lookup +
// RUnlock + arena.Slot — no allocation.
func BenchmarkGet(b *testing.B) {
	for _, sz := range benchSizes {
		b.Run(sz.name, func(b *testing.B) {
			c := newBenchCache(b, 1)
			keys := seedKeys(b, c, 1000, sz.size)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = c.Get(keys[i%len(keys)])
			}
		})
	}
}

// BenchmarkGetCopy measures copy-under-lock GetCopy: same as Get, plus
// make([]byte, n) + copy — one allocation, copy proportional to value size.
func BenchmarkGetCopy(b *testing.B) {
	for _, sz := range benchSizes {
		b.Run(sz.name, func(b *testing.B) {
			c := newBenchCache(b, 1)
			keys := seedKeys(b, c, 1000, sz.size)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = c.GetCopy(keys[i%len(keys)])
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Parallel read-only (RLock contention only)
// ---------------------------------------------------------------------------

// BenchmarkGetParallel: concurrent zero-copy reads, no writes.
func BenchmarkGetParallel(b *testing.B) {
	for _, sz := range benchSizes {
		b.Run(sz.name, func(b *testing.B) {
			c := newBenchCache(b, 1)
			keys := seedKeys(b, c, 1000, sz.size)
			b.ResetTimer()
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					_, _ = c.Get(keys[i%len(keys)])
					i++
				}
			})
		})
	}
}

// BenchmarkGetCopyParallel: concurrent copy-under-lock reads, no writes.
func BenchmarkGetCopyParallel(b *testing.B) {
	for _, sz := range benchSizes {
		b.Run(sz.name, func(b *testing.B) {
			c := newBenchCache(b, 1)
			keys := seedKeys(b, c, 1000, sz.size)
			b.ResetTimer()
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					_, _ = c.GetCopy(keys[i%len(keys)])
					i++
				}
			})
		})
	}
}

// ---------------------------------------------------------------------------
// Mixed read/write parallel — closest to server workload
// 80 % reads, 20 % writes.
// ---------------------------------------------------------------------------

// BenchmarkGetMixedParallel: zero-copy reads against a live write stream.
func BenchmarkGetMixedParallel(b *testing.B) {
	for _, sz := range benchSizes {
		b.Run(sz.name, func(b *testing.B) {
			c := newBenchCache(b, 1)
			keys := seedKeys(b, c, 1000, sz.size)
			val := make([]byte, sz.size)
			b.ResetTimer()
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					k := keys[i%len(keys)]
					if i%5 == 0 {
						c.Set(k, val, 0)
					} else {
						_, _ = c.Get(k)
					}
					i++
				}
			})
		})
	}
}

// BenchmarkGetCopyMixedParallel: copy-under-lock reads against a live write
// stream. This is the most representative server workload benchmark.
func BenchmarkGetCopyMixedParallel(b *testing.B) {
	for _, sz := range benchSizes {
		b.Run(sz.name, func(b *testing.B) {
			c := newBenchCache(b, 1)
			keys := seedKeys(b, c, 1000, sz.size)
			val := make([]byte, sz.size)
			b.ResetTimer()
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					k := keys[i%len(keys)]
					if i%5 == 0 {
						c.Set(k, val, 0)
					} else {
						_, _ = c.GetCopy(k)
					}
					i++
				}
			})
		})
	}
}
