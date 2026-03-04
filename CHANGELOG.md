# Changelog

All notable changes to slabbis will be documented here.

Format: [Semantic Versioning](https://semver.org/spec/v2.0.0.html)

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
