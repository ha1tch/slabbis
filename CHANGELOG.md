# Changelog

All notable changes to slabbis will be documented here.

Format: [Semantic Versioning](https://semver.org/spec/v2.0.0.html)

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
