# Changelog

All notable changes to slabbis will be documented here.

Format: [Semantic Versioning](https://semver.org/spec/v2.0.0.html)

---

## [0.1.5] - 2026-06-12

### Added

This release addresses all actionable items from the olu integration feedback
report. Item #2b (adding an error return to `Cache.Set`) is deferred to v0.2.0
as a deliberate breaking-interface change.

**`SCAN cursor [MATCH pattern] [COUNT count]`** (item #1 — was blocking for
`go-redis` and most Redis client libraries). slabbis holds all keys in memory
and can always enumerate them in a single pass; `SCAN` therefore always returns
cursor `"0"` (iteration complete) alongside all matching keys. `COUNT` is
accepted and ignored. Clients that loop until cursor `"0"` terminate correctly
after one call. This unblocks all client libraries that issue `SCAN` for key
enumeration rather than `KEYS`.

**`Config.MaxValueSize() int`** (item #4a). Returns the `MaxSize` of the
largest configured size class — the hard ceiling above which `Set` silently
drops values. Callers can check before calling `Set` to avoid the silent-drop
hazard: `if len(value) > cfg.MaxValueSize() { /* handle */ }`.

**`DevConfig() Config`** (item #3). A named constructor for development,
testing, and memory-constrained environments (CI, sandboxes). Two size classes
(64B / 4KB), one shard, one bucket per shard, 50ms reaper. Total virtual
address footprint ~8MB. Saves every integrator from deriving the same config
independently.

**CLI flags `-buckets`, `-max-value`, `-classes`, `-dev`** (item #5):
- `-buckets int` — sets `BucketsPerShard`. Use `-buckets 1` to reduce startup
  memory footprint in constrained environments without changing semantics.
- `-max-value bytes` — creates a single size class with the given ceiling; slot
  size derived as `nextPow2(max_value)`. Common case: `-max-value 1048576`.
- `-classes string` — comma-separated list of size class ceilings, e.g.
  `64,4096,65536`; takes precedence over `-max-value`.
- `-dev` — activates `DevConfig()` from the CLI; overrides all other Config
  flags.

**Enriched startup log line** (item #4b):
```
slabbis: listening on 127.0.0.1:6379 (shards=8, classes=5, max_value=262144B)
```
Lets operators immediately confirm the server is configured as intended.

### Changed

**SET handler now logs a WARN on oversized-value drop** (item #2a). When a
`Set` call silently discards a value because it exceeds the largest size class,
the server now logs:
```
slabbis: WARN SET "key": value (N bytes) exceeds largest size class — dropped; resize -max-value or -classes
```
One extra `Exists` call per dropped value. On the non-drop path (overwhelmingly
the common case) there is no overhead.

**README: KEYS vs SCAN design note** (item #4c). A new section explains that
`SCAN` always completes in a single round-trip in slabbis, why this is correct
for an in-memory store, and how it differs from disk-based Redis behaviour.
Server usage examples updated with new flags.

### Deferred

**`Cache.Set` error return** (item #2b). Adding an error return to `Set` is the
right long-term fix for surfacing oversized-value drops to callers. It is a
breaking interface change and will be taken at v0.2.0.

---

## [0.1.4] - 2026-06-12

### Added

**13 new RESP commands** bringing slabbis substantially closer to full Redis
string/key-management parity for caching workloads.

Three new `Cache` interface methods underpin the atomic operations:

- **`Cache.SetTTL(key string, ttl time.Duration) bool`**: updates the expiry of
  an existing live key without touching its value. A zero `ttl` removes the
  expiry (PERSIST semantics). Returns false if the key does not exist or is
  already expired.

- **`Cache.GetSet(key string, newVal []byte, ttl time.Duration) ([]byte, bool)`**:
  atomically replaces a key's value and returns the old one. Returns `(nil,
  false)` if the key did not previously exist. The new entry respects the
  provided TTL.

- **`Cache.IncrBy(key string, delta int64) (int64, error)`**: atomically
  increments (or decrements, with negative delta) the integer value stored at
  key, preserving any existing TTL. The value is stored and returned as a
  decimal string, matching Redis semantics. Returns an error if the value is not
  a valid integer or if the result would overflow `int64`. Missing keys are
  treated as `"0"`.

New server commands built on the above:

- **`UNLINK key [key ...]`**: alias for `DEL`; synchronous in slabbis (no async
  reclaim needed — the slab allocator handles that).
- **`STRLEN key`**: returns the byte length of the value, or 0 if the key is
  missing. Uses the pooled `GetInto` buffer; zero allocations in steady state.
- **`INCR key`** / **`DECR key`**: increment or decrement by 1.
- **`INCRBY key n`** / **`DECRBY key n`**: increment or decrement by `n`.
  `INCRBY` accepts a signed `n`; `DECRBY` negates `n` before calling
  `IncrBy`.
- **`GETSET key value`**: atomic get-then-set; returns null bulk if key did not
  exist.
- **`GETEX key [EX s | PX ms | PERSIST]`**: get value and optionally update TTL
  in the same command.
- **`EXPIRE key seconds`** / **`PEXPIRE key milliseconds`**: set TTL on existing
  key; returns 1 if key existed, 0 otherwise.
- **`PERSIST key`**: remove expiry from key; returns 1 if key existed, 0
  otherwise.
- **`RANDOMKEY`**: returns a random live key, or null bulk on empty cache.
- **`COPY source destination`**: copies the value of `source` to `destination`;
  non-atomic (implemented as `GetCopy` + `Set`); returns 1 on success, 0 if
  source does not exist.

New helpers in `server.go`:

- `parseSignedInt`: parses a signed decimal integer from a RESP argument for
  use by `INCRBY` and `DECRBY`.

**64 new tests** in `newops_test.go` covering all new methods and commands,
including concurrency stress tests for `IncrBy` atomicity (50 goroutines ×
100 ops each) and the `INCR` + `EXPIRE` rate-limit pattern.

---

## [0.1.3] - 2026-06-12

### Fixed

- **`Server.serveWg` was never incremented**: `Serve()` declared a `serveWg`
  WaitGroup intended to track the accept-loop goroutine lifetime, but never
  called `serveWg.Add(1)` or `serveWg.Done()`. `Close()` waited on it, which
  was always a no-op. `Serve()` now increments the WaitGroup on entry and
  decrements it on return, so `Close()` correctly waits for the accept loop to
  exit before returning. Callers that run `Serve()` in a goroutine and call
  `Close()` concurrently now have a proper happens-before guarantee.

- **`parseInt` rejected empty input silently**: an empty byte slice produced a
  zero return value with a nil error, which `parseSetOptions` would then accept
  as a valid `0`-second TTL before the `n <= 0` guard rejected it. Empty input
  now returns an explicit `"empty integer"` error. Error message for non-digit
  bytes changed from `"not an integer"` to `"not a non-negative integer"` to
  accurately describe the constraint (the function only accepts unsigned decimal
  strings; negative sign is not valid syntax here).

### Added

- **`internal/resp`: direct unit tests for `WriteArrayHeader`**: three new tests
  cover the streaming array write path — equivalence with `WriteArray`, a
  zero-element header, and the MGET pattern of header followed by a mix of bulk
  and null bulk strings. `WriteArrayHeader` was previously exercised only
  indirectly through the server-level MGET test.

### Removed

- **README: phantom `bench/` entry**: the architecture table referenced
  `bench/main.go` (comparative benchmark against Redis) which was never
  created. The entry has been removed to eliminate confusion.

---

## [0.1.2] - 2026-03-04

### Added

- **`Cache.GetInto(key string, dst []byte) ([]byte, bool)`**: zero-allocation
  companion to `GetCopy`. Copies the value into a caller-supplied buffer,
  growing it only when `cap(dst) < len(value)`. Callers that pool or reuse
  `dst` (e.g. one buffer per server connection) achieve zero per-call heap
  allocations in steady state.
- **`resp.Writer.WriteArrayHeader(n int) error`**: writes the `*N\r\n` RESP
  array prefix without requiring all elements to be buffered first. Enables
  streaming MGET responses over a single pooled buffer.

### Changed

- **Server GET handler**: switched from `GetCopy` (heap allocation per request)
  to `GetInto` with a `sync.Pool` of `*[]byte` buffers. In steady state, each
  pooled buffer grows to the high-water mark of values on its connection and
  is reused with zero heap allocations.
- **Server MGET handler**: replaced the `[][]byte` vals slice + per-key
  `GetCopy` pattern with a streaming approach: write the array header first,
  then loop `GetInto` + `WriteBulk` per key using a single pooled buffer.
  Eliminates the vals allocation and all per-key value allocations.
- **Benchmark in-process GET and MGET**: updated to use `GetInto` with a
  per-goroutine scratch buffer, matching the server's zero-alloc strategy.
  Prior numbers for these two operations were artificially penalised by
  `GetCopy` allocations.

---

## [0.1.1] - 2026-03-04

### Fixed

- **Data race in server GET handler**: `Get` returns a direct view into slabber
  memory; the server was releasing the shard read lock before copying the value,
  creating a window where a concurrent `Set` on the same key could mutate the
  slot while the server was reading it. Fixed by adding `GetCopy` to the `Cache`
  interface: it copies under the shard read lock so the returned slice is safe to
  retain indefinitely. The server's `GET` and `MGET` handlers now use `GetCopy`.
- **Data race in server MGET handler**: same root cause — `MGet` returns live
  slabber views which the server held across the `WriteArray` call. Fixed by
  calling `GetCopy` per key in the handler instead of `MGet`.
- **`//go:norace` removed from `handleConn`**: the annotation was suppressing
  race detector instrumentation across the entire connection handler, masking
  the above races. Removed now that the underlying races are fixed.

### Added

- **`Cache.GetCopy(key string) ([]byte, bool)`**: safe-to-retain copy of a
  cache value, made while holding the shard read lock.
- **`testClasses` in test suite**: the test suite now defaults to three size
  classes (64B / 512B / 4KB) instead of the full five, reducing per-Arena
  virtual address space from ~18 GB to ~292 MB. This allows `make test-race`
  to run without exhausting memory on the race detector's shadow memory.
  Set `SLABBIS_FULL_CLASSES=1` to restore `DefaultClasses` for large-value
  testing.
- **`gaps_test.go`**: 20 new tests covering five previously untested areas:
  `GetCopy` correctness and race safety; oversized-value silent drop behaviour
  in `Set`; `BucketsPerShard` config field; `Stats` field accuracy
  (`SlotSize`, `UsedSlots`, `TotalSlots`, `FreeSlots`, `MemoryMB`,
  `Buckets`); and `Rename` non-atomicity under concurrent writes.
- **`get_copy_bench_test.go`**: benchmark suite comparing `Get` and `GetCopy`
  across value sizes (8B–4KB) and concurrency patterns (sequential, parallel
  read-only, 80/20 mixed). Run with `go test -bench . -benchmem -count=6 -cpu=1,4,8 .`

---

## [0.1.0] - 2026-03-03

### Added

- **`Cache` interface**: `Get`, `Set` (with optional `EX`/`PX` TTL), `Del`,
  `Exists`, `TTL`, `Flush`, `Stats`, `Close`.
- **`*cache` implementation**: sharded key map (one shard per CPU) over a
  slabber `Arena` per shard. FNV-1a key routing. Background TTL reaper per
  shard. Five default size classes: 64B, 512B, 4KB, 32KB, 256KB.
- **`Server`**: RESP2 server wrapping any `Cache`. Supports `GET`, `SET`
  `[EX|PX]`, `DEL`, `EXISTS`, `TTL`, `PTTL`, `FLUSH`/`FLUSHALL`/`FLUSHDB`,
  `PING`, `COMMAND`, `QUIT`. Listens on TCP or Unix socket.
- **`internal/resp`**: minimal RESP2 reader/writer covering exactly the
  commands slabbis exposes.
- **`cmd/slabbis`**: standalone server binary with `-addr`, `-shards`,
  `-reaper`, and `-v` flags. Graceful shutdown on SIGINT/SIGTERM.
- **`Config`**: `Shards`, `Classes`, `ReaperInterval`, `BucketsPerShard`.
- **`DefaultClasses`**: five Arena size classes covering typical cache values.

---
