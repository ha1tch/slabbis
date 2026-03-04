// Command slabbis is a minimal cache server speaking a Redis-compatible
// protocol subset over TCP or a Unix socket.
//
// Usage:
//
//	slabbis [flags]
//
// Flags:
//
//	-addr string
//	      Listen address. TCP: "127.0.0.1:6379". Unix: "unix:///tmp/slabbis.sock".
//	      (default "127.0.0.1:6379")
//	-shards int
//	      Number of key-space shards. 0 = runtime.NumCPU(). (default 0)
//	-reaper duration
//	      TTL reaper interval per shard. (default 1s)
//	-v    Print version and exit.
//
// Example:
//
//	slabbis -addr unix:///tmp/slabbis.sock -shards 16
//
// slabbis speaks RESP2. It supports: GET, MGET, SET [EX|PX], MSET, SETNX,
// GETDEL, DEL, EXISTS, KEYS, RENAME, DBSIZE, TYPE, TTL, PTTL, FLUSH, PING,
// COMMAND, QUIT.
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
	"syscall"
	"time"

	"github.com/ha1tch/slabbis"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:6379", "listen address (TCP or unix://path)")
	shards := flag.Int("shards", 0, "key-space shards (0 = NumCPU)")
	reaper := flag.Duration("reaper", time.Second, "TTL reaper interval")
	version := flag.Bool("v", false, "print version and exit")
	flag.Parse()

	if *version {
		fmt.Printf("slabbis %s\n", slabbis.Version)
		os.Exit(0)
	}

	logger := log.New(os.Stderr, "slabbis: ", log.LstdFlags)

	cache := slabbis.New(slabbis.Config{
		Shards:         *shards,
		ReaperInterval: *reaper,
	})
	defer cache.Close()

	srv, err := slabbis.NewServer(*addr, cache, logger)
	if err != nil {
		logger.Fatalf("failed to start: %v", err)
	}
	defer srv.Close()

	logger.Printf("listening on %s", srv.Addr())

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
