// Command slabbis is a minimal cache server speaking a Redis-compatible
// protocol subset over TCP or a Unix socket.
//
// Usage:
//
//	slabbis [flags]
//	slabbis version
//
// Flags:
//
//	-addr string
//	      Listen address. TCP: "127.0.0.1:6379". Unix: "unix:///tmp/slabbis.sock".
//	      (default "127.0.0.1:6379")
//	-shards int
//	      Number of key-space shards. 0 = runtime.NumCPU(). (default 0)
//	-buckets int
//	      Arena buckets per shard. 0 = runtime.NumCPU(). Lower values reduce
//	      startup memory footprint; useful in constrained environments. (default 0)
//	-reaper duration
//	      TTL reaper interval per shard. (default 1s)
//	-max-value bytes
//	      Maximum storable value size in bytes. Creates a single size class with
//	      this ceiling. Ignored when -classes is provided. (default 0, uses
//	      DefaultClasses: 64B/512B/4KB/32KB/256KB)
//	-classes string
//	      Comma-separated list of size class ceilings, e.g. "64,4096,65536".
//	      Slot sizes are derived as the next power of two >= ceiling.
//	      Takes precedence over -max-value. (default "", uses DefaultClasses)
//	-dev
//	      Development mode: two small size classes (64B/4KB), one shard, one
//	      bucket, fast reaper. Equivalent to slabbis.DevConfig(). Overrides all
//	      other Config flags.
//	-v, -version, --version
//	      Print version and exit.
//
// Example:
//
//	slabbis -addr unix:///tmp/slabbis.sock -shards 16
//	slabbis -max-value 1048576
//	slabbis -classes 128,4096,65536
//	slabbis -dev
//
// slabbis speaks RESP2. It supports: GET, SET [EX|PX], GETSET, GETEX,
// MGET, MSET, SETNX, GETDEL, DEL, UNLINK, EXISTS, STRLEN, INCR, INCRBY,
// DECR, DECRBY, KEYS, SCAN, RANDOMKEY, COPY, RENAME, DBSIZE, TYPE, TTL,
// PTTL, EXPIRE, PEXPIRE, PERSIST, FLUSH, PING, COMMAND, QUIT.
//
// It does not support persistence, replication, pub/sub, scripting, or any
// other Redis feature outside that list. This is by design.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ha1tch/slabber"
	"github.com/ha1tch/slabbis"
)

func printVersion() {
	fmt.Printf("slabbis %s\n", slabbis.Version)
}

// nextPow2 returns the smallest power of two >= n. Mirrors slabber's internal
// derivation so that omitting SlotSize from a SizeClass produces the same
// result as specifying it explicitly.
func nextPow2(n int) int {
	if n <= 1 {
		return 1
	}
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}

// parseClasses parses a comma-separated list of size class ceilings (e.g.
// "64,4096,65536") into a []slabber.SizeClass. Slot sizes are derived as
// nextPow2(ceiling).
func parseClasses(s string) ([]slabber.SizeClass, error) {
	parts := strings.Split(s, ",")
	classes := make([]slabber.SizeClass, 0, len(parts))
	prev := 0
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		n, err := strconv.Atoi(p)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("invalid size class ceiling %q: must be a positive integer", p)
		}
		if n <= prev {
			return nil, fmt.Errorf("size class ceilings must be strictly ascending: %d <= %d", n, prev)
		}
		classes = append(classes, slabber.SizeClass{
			MaxSize:  n,
			SlotSize: nextPow2(n),
		})
		prev = n
	}
	if len(classes) == 0 {
		return nil, fmt.Errorf("-classes: at least one size class required")
	}
	return classes, nil
}

func main() {
	// Handle bare "version" subcommand before flag.Parse so it works
	// even when other flags are also registered.
	if len(os.Args) > 1 && os.Args[1] == "version" {
		printVersion()
		os.Exit(0)
	}

	addr := flag.String("addr", "127.0.0.1:6379", "listen address (TCP or unix://path)")
	shards := flag.Int("shards", 0, "key-space shards (0 = NumCPU)")
	buckets := flag.Int("buckets", 0, "arena buckets per shard (0 = NumCPU; use 1 for constrained environments)")
	reaper := flag.Duration("reaper", time.Second, "TTL reaper interval")
	maxValue := flag.Int("max-value", 0, "maximum value size in bytes; creates a single size class (ignored when -classes is set)")
	classesStr := flag.String("classes", "", "comma-separated size class ceilings, e.g. 64,4096,65536")
	dev := flag.Bool("dev", false, "development mode: small footprint config (overrides other Config flags)")

	// All of -v, -version, --version write to the same variable.
	var showVersion bool
	flag.BoolVar(&showVersion, "v", false, "print version and exit")
	flag.BoolVar(&showVersion, "version", false, "print version and exit")

	flag.Parse()

	if showVersion {
		printVersion()
		os.Exit(0)
	}

	logger := log.New(os.Stderr, "slabbis: ", log.LstdFlags)

	// Build Config.
	var cfg slabbis.Config
	if *dev {
		cfg = slabbis.DevConfig()
	} else {
		cfg = slabbis.Config{
			Shards:          *shards,
			BucketsPerShard: *buckets,
			ReaperInterval:  *reaper,
		}
		switch {
		case *classesStr != "":
			classes, err := parseClasses(*classesStr)
			if err != nil {
				logger.Fatalf("-classes: %v", err)
			}
			cfg.Classes = classes
		case *maxValue > 0:
			cfg.Classes = []slabber.SizeClass{
				{MaxSize: *maxValue, SlotSize: nextPow2(*maxValue)},
			}
		}
		// Zero Classes → New() will apply DefaultClasses.
	}

	cache := slabbis.New(cfg)
	defer cache.Close()

	srv, err := slabbis.NewServer(*addr, cache, logger)
	if err != nil {
		logger.Fatalf("failed to start: %v", err)
	}
	defer srv.Close()

	// Enriched startup line: address + effective slab configuration.
	// SlabStats has one entry per size class (from shard 0's arena).
	// Effective shard count comes from cfg.MaxValueSize() via the resolved classes.
	stats := cache.Stats()
	nClasses := len(stats.SlabStats)
	maxVal := cfg.MaxValueSize()
	effectiveShards := *shards
	if *dev {
		effectiveShards = 1
	} else if effectiveShards <= 0 {
		effectiveShards = runtime.NumCPU()
	}
	logger.Printf("listening on %s (shards=%d, classes=%d, max_value=%dB)",
		srv.Addr(), effectiveShards, nClasses, maxVal)

	// Graceful shutdown on SIGINT / SIGTERM.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		logger.Println("shutting down")
		srv.Close()
	}()

	if err := srv.Serve(); err != nil {
		// Normal shutdown via Close() returns a non-nil error from Accept.
		// Log only unexpected errors.
		select {
		case <-quit:
		default:
			logger.Fatalf("serve error: %v", err)
		}
	}
}
