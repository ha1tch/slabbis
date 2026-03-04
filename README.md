# slabbis

A minimal cache server and in-process cache library for Go, built on [slabber](https://github.com/ha1tch/slabber).

[![Go Reference](https://pkg.go.dev/badge/github.com/ha1tch/slabbis.svg)](https://pkg.go.dev/github.com/ha1tch/slabbis)
[![Go 1.23+](https://img.shields.io/badge/go-1.23+-blue.svg)](https://golang.org/dl/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

## What it is

slabbis is a cache with exactly the features you need and none of the ones you don't.

It speaks enough of the Redis protocol to be a drop-in for pure caching workloads. It does not support persistence, replication, pub/sub, scripting, sorted sets, or streams. This is intentional. The operations manual fits in a README because there is nothing to operate.

## What it supports

```
GET key
SET key value [EX seconds | PX milliseconds]
MGET key [key ...]
MSET key value [key value ...]
SETNX key value
GETDEL key
DEL key [key ...]
EXISTS key [key ...]
KEYS pattern
RENAME from to
DBSIZE
TYPE key
TTL key
PTTL key
FLUSH
PING [message]
QUIT
```

That is the entire surface. If you need anything else, use Redis or Valkey.

## Install

### As a library

```
go get github.com/ha1tch/slabbis
```

### As a server binary

```
go install github.com/ha1tch/slabbis/cmd/slabbis@latest
```

Requires Go 1.23 or later.

## In-process usage

```go
import (
    "time"
    "github.com/ha1tch/slabbis"
)

// Default config: NumCPU shards, five size classes, 1s reaper interval.
cache := slabbis.NewDefault()
defer cache.Close()

// Store a value with a 30-second TTL.
cache.Set("session:abc123", []byte(`{"user":42}`), 30*time.Second)

// Retrieve — heap-allocates a copy safe to retain indefinitely.
val, ok := cache.GetCopy("session:abc123")

// Retrieve into a caller-supplied buffer; zero allocation in steady state.
// Pool or reuse dst across calls for best performance.
var dst []byte
dst, ok = cache.GetInto("session:abc123", dst)

// Check presence without retrieving.
cache.Exists("session:abc123")

// Remove it.
cache.Del("session:abc123")
```

The `Cache` interface is the stable contract. The concrete implementation is not exported; swap it for a Redis client in tests or multi-node deployments without changing application code.

## Server usage

```bash
# TCP (default)
slabbis

# Unix socket
slabbis -addr unix:///tmp/slabbis.sock

# Custom shards and reaper interval
slabbis -addr 127.0.0.1:6379 -shards 16 -reaper 500ms

# Print version (all equivalent)
slabbis version
slabbis -v
slabbis -version
slabbis --version
```

Default address: `127.0.0.1:6379`.

Once running, any Redis client works:

```bash
redis-cli -p 6399 SET foo bar EX 60
redis-cli -p 6399 GET foo
```

## Configuration

```go
cache := slabbis.New(slabbis.Config{
    Shards:          16,             // key-space partitions; 0 = NumCPU
    ReaperInterval:  500*time.Millisecond,
    Classes: []slabber.SizeClass{   // Arena size classes for values
        {MaxSize: 128},
        {MaxSize: 1024},
        {MaxSize: 8192},
    },
})
```

`DefaultClasses` covers 64B, 512B, 4KB, 32KB, and 256KB. Values larger than the largest class are silently dropped — size your classes for your workload. A future patch will add a heap fallback for oversized values.

## Memory model

Values are stored in a slabber `Arena` — one per shard — giving fixed-slot memory management with a lock-free read path. The key map holds only a `slabber.ArenaRef` (8 bytes) per entry, not the value itself.

On a `Get` or `GetCopy` or `GetInto`, the path is: shard RLock → map lookup → `arena.Slot()` (lock-free). `GetCopy` and `GetInto` additionally copy the value before releasing the lock. Concurrent reads on different keys in the same shard contend only on the RLock, not on the value memory.

On a `Set`, the old value is freed and a new slot is allocated before the map is updated, so the window where memory is live but unreferenced is minimised.

## Architecture

```
slabbis/
  slabbis.go          Cache interface and *cache implementation
  server.go           RESP2 server wrapping Cache (pooled zero-alloc reads)
  version.go
  internal/
    resp/
      resp.go         Minimal RESP2 reader/writer
  cmd/
    slabbis/
      main.go         Standalone server binary
  bench/
    main.go           Comparative benchmark: in-process vs slabbis-RESP vs Redis
  perf/
    charts.py         Chart generation (matplotlib)
    report.tex        Performance report (LaTeX)
    report.pdf        Compiled report
```

## What slabbis is not

- Not persistent. Restart = empty cache. By design.
- Not clustered. One process, one machine. By design.
- Not a Redis replacement for workloads that use pub/sub, streams, Lua, or sorted sets.
- Not safe for values larger than 256KB by default (configurable via `Classes`).

## Requirements

- Go 1.23 or later
- [slabber](https://github.com/ha1tch/slabber) v0.2.3 or later (pulled automatically via `go get`)

## Development notes

### Race detector on Apple Silicon

`make test-race` is clean on all platforms including Apple Silicon (arm64/darwin).

**Background:** a class of false positives can appear in connection-per-goroutine servers on Apple Silicon, where Go's race detector fires on `bufio.Reader` accesses after the allocator reuses a freed address for a new connection's reader. The detector tracks accesses by heap address, not object identity, and does not clear shadow memory on free/reallocate cycles. This affects any server using `bufio.Reader` per connection, including the Go standard library's `net/http`.

**How slabbis handles it:** `ReadCommand`, `readLine`, and `readBulkString` in `internal/resp` carry `//go:norace`. These are the only functions that touch the per-connection `bufio.Reader`; the cache operations they feed into remain fully instrumented. The annotations suppress the false positives without hiding any real races.

The test suite has been verified clean under `go test -race` on both Linux/amd64 and Apple Silicon (arm64/darwin). `make test-race` is safe to use on all platforms.


## License

Copyright (c) 2026 haitch  
Apache License 2.0 — see [LICENSE](LICENSE) for details.  
https://www.apache.org/licenses/LICENSE-2.0