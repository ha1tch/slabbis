// Command bench measures slabbis performance across three execution targets
// and a representative set of cache operations.
//
// Targets:
//
//	in-process   — slabbis.Cache called directly, no network overhead
//	slabbis-RESP — slabbis server over TCP, RESP2 protocol
//	Redis        — Redis server over TCP, RESP2 protocol
//
// The RESP targets must be running before this program is invoked.
// runbenchmark.sh handles starting them automatically.
//
// Usage:
//
//	go run . [flags]
//
// Flags:
//
//	-duration      duration of each individual benchmark run (default 5s)
//	-concurrency   goroutines per benchmark (default 10)
//	-key-space     number of distinct keys (default 10000)
//	-value-size    value size in bytes (default 64)
//	-batch-size    keys per MGET / MSET operation (default 10)
//	-slabbis-addr  slabbis RESP server address (default 127.0.0.1:6399)
//	-redis-addr    Redis server address (default 127.0.0.1:6379)
package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ha1tch/slabber"
	"github.com/ha1tch/slabbis"
)

// ─── configuration ────────────────────────────────────────────────────────────

type config struct {
	duration    time.Duration
	concurrency int
	keySpace    int
	valueSize   int
	batchSize   int
	slabbisAddr string
	redisAddr   string
}

// ─── minimal RESP2 client ─────────────────────────────────────────────────────

// respConn is a synchronous, single-connection RESP2 client.
// Each benchmark goroutine owns its own respConn to avoid lock contention.
type respConn struct {
	conn net.Conn
	br   *bufio.Reader
	bw   *bufio.Writer
}

func dialRESP(addr string) (*respConn, error) {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return nil, err
	}
	return &respConn{
		conn: conn,
		br:   bufio.NewReaderSize(conn, 32*1024),
		bw:   bufio.NewWriterSize(conn, 32*1024),
	}, nil
}

func (c *respConn) close() { c.conn.Close() }

// do sends a command and discards the response. Only the error matters
// for throughput benchmarking; actual response values are not inspected.
func (c *respConn) do(args ...string) error {
	fmt.Fprintf(c.bw, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(c.bw, "$%d\r\n%s\r\n", len(a), a)
	}
	if err := c.bw.Flush(); err != nil {
		return err
	}
	return c.drain()
}

// drain reads and discards one complete RESP2 value.
func (c *respConn) drain() error {
	line, err := c.br.ReadString('\n')
	if err != nil {
		return err
	}
	switch line[0] {
	case '+', '-', ':':
		return nil
	case '$':
		n, _ := strconv.Atoi(strings.TrimSpace(line[1:]))
		if n < 0 {
			return nil // null bulk string
		}
		_, err = c.br.Discard(n + 2) // data + \r\n
		return err
	case '*':
		n, _ := strconv.Atoi(strings.TrimSpace(line[1:]))
		if n < 0 {
			return nil // null array
		}
		for i := 0; i < n; i++ {
			if err := c.drain(); err != nil {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("unexpected RESP prefix 0x%02x in %q", line[0], line)
}

// ─── benchmark runner ─────────────────────────────────────────────────────────

// maxSamplesPerGoroutine caps per-goroutine latency recording.
// At 10 goroutines this gives up to 1 M samples total — fine to sort.
const maxSamplesPerGoroutine = 100_000

type benchResult struct {
	wall    time.Duration
	ops     int64
	errs    int64
	samples []time.Duration // sorted ascending after run
}

func (r benchResult) opsPerSec() float64 {
	if r.wall == 0 {
		return 0
	}
	return float64(r.ops) / r.wall.Seconds()
}

// percentile returns the p-th percentile latency (0–100).
func (r benchResult) percentile(p float64) time.Duration {
	if len(r.samples) == 0 {
		return 0
	}
	idx := int(float64(len(r.samples)) * p / 100.0)
	if idx >= len(r.samples) {
		idx = len(r.samples) - 1
	}
	return r.samples[idx]
}

// factory is called once per goroutine.
// It returns the per-operation function and an optional cleanup.
// Returning a nil fn signals that the goroutine could not be initialised
// (e.g. the server is unreachable); it is counted as an error.
type factory func(goroutineID int) (fn func() error, cleanup func())

func runBench(dur time.Duration, concurrency int, f factory) benchResult {
	done := make(chan struct{})
	time.AfterFunc(dur, func() { close(done) })

	var (
		mu      sync.Mutex
		allOps  int64
		allErrs int64
		allSamp []time.Duration
	)

	wallStart := time.Now()
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for g := 0; g < concurrency; g++ {
		go func(g int) {
			defer wg.Done()
			fn, cleanup := f(g)
			if cleanup != nil {
				defer cleanup()
			}
			if fn == nil {
				mu.Lock()
				allErrs++
				mu.Unlock()
				return
			}

			samp := make([]time.Duration, 0, maxSamplesPerGoroutine)
			var ops, errs int64
			recording := true

			for {
				select {
				case <-done:
					goto finished
				default:
				}
				t := time.Now()
				if err := fn(); err != nil {
					errs++
				}
				if recording {
					samp = append(samp, time.Since(t))
					if len(samp) == maxSamplesPerGoroutine {
						recording = false
					}
				}
				ops++
			}
		finished:
			mu.Lock()
			allOps += ops
			allErrs += errs
			allSamp = append(allSamp, samp...)
			mu.Unlock()
		}(g)
	}

	wg.Wait()
	wall := time.Since(wallStart)

	sort.Slice(allSamp, func(i, j int) bool { return allSamp[i] < allSamp[j] })

	return benchResult{
		wall:    wall,
		ops:     allOps,
		errs:    allErrs,
		samples: allSamp,
	}
}

// ─── key / value generation ───────────────────────────────────────────────────

func key(i int) string              { return fmt.Sprintf("bench:k%07d", i) }
func randKey(r *rand.Rand, n int) string { return key(r.Intn(n)) }
func val(size int) string           { return strings.Repeat("x", size) }
func valBytes(size int) []byte      { return []byte(val(size)) }

// ─── seeding and flushing ─────────────────────────────────────────────────────

// seedInProc flushes the cache and fills keySpace entries.
func seedInProc(c slabbis.Cache, keySpace, valueSize int) {
	c.Flush()
	v := valBytes(valueSize)
	pairs := make(map[string][]byte, 200)
	for i := 0; i < keySpace; i++ {
		pairs[key(i)] = v
		if len(pairs) == 200 {
			c.MSet(0, pairs)
			pairs = make(map[string][]byte, 200)
		}
	}
	if len(pairs) > 0 {
		c.MSet(0, pairs)
	}
}

// seedRESP flushes the server and fills keySpace entries via MSET batches.
func seedRESP(addr string, keySpace, valueSize int) error {
	c, err := dialRESP(addr)
	if err != nil {
		return err
	}
	defer c.close()
	if err := c.do("FLUSHALL"); err != nil {
		return err
	}
	v := val(valueSize)
	const batch = 200
	for start := 0; start < keySpace; start += batch {
		end := start + batch
		if end > keySpace {
			end = keySpace
		}
		args := make([]string, 0, 1+2*(end-start))
		args = append(args, "MSET")
		for i := start; i < end; i++ {
			args = append(args, key(i), v)
		}
		if err := c.do(args...); err != nil {
			return err
		}
	}
	return nil
}

func flushRESP(addr string) error {
	c, err := dialRESP(addr)
	if err != nil {
		return err
	}
	defer c.close()
	return c.do("FLUSHALL")
}

// ─── operations ───────────────────────────────────────────────────────────────

type opKind int

const (
	opGET opKind = iota
	opSET
	opDEL
	opEXISTS
	opMixed8020
	opMGET
	opMSET
)

var allOps = []opKind{opGET, opSET, opDEL, opEXISTS, opMixed8020, opMGET, opMSET}

func (o opKind) String() string {
	switch o {
	case opGET:
		return "GET"
	case opSET:
		return "SET"
	case opDEL:
		return "DEL"
	case opEXISTS:
		return "EXISTS"
	case opMixed8020:
		return "Mixed 80/20"
	case opMGET:
		return "MGET"
	case opMSET:
		return "MSET"
	}
	return "?"
}

// needsSeed reports whether this operation requires a pre-populated key space.
func (o opKind) needsSeed() bool {
	switch o {
	case opGET, opEXISTS, opMixed8020, opMGET:
		return true
	}
	return false
}

// ─── factory constructors ─────────────────────────────────────────────────────

// inProcFactory builds a factory for the in-process slabbis.Cache target.
// GET and MGET use GetCopy, matching the behaviour of the RESP server after
// the v0.1.1 race fix.
func inProcFactory(op opKind, cfg config, c slabbis.Cache) factory {
	v := valBytes(cfg.valueSize)
	return func(id int) (func() error, func()) {
		rng := rand.New(rand.NewSource(int64(id + 1)))
		switch op {
		case opGET:
			return func() error {
				c.GetCopy(randKey(rng, cfg.keySpace))
				return nil
			}, nil
		case opSET:
			return func() error {
				c.Set(randKey(rng, cfg.keySpace), v, 0)
				return nil
			}, nil
		case opDEL:
			return func() error {
				c.Del(randKey(rng, cfg.keySpace))
				return nil
			}, nil
		case opEXISTS:
			return func() error {
				c.Exists(randKey(rng, cfg.keySpace))
				return nil
			}, nil
		case opMixed8020:
			return func() error {
				if rng.Intn(100) < 80 {
					c.GetCopy(randKey(rng, cfg.keySpace))
				} else {
					c.Set(randKey(rng, cfg.keySpace), v, 0)
				}
				return nil
			}, nil
		case opMGET:
			// Mirror the server's per-key GetCopy implementation.
			keys := make([]string, cfg.batchSize)
			return func() error {
				for i := range keys {
					keys[i] = randKey(rng, cfg.keySpace)
				}
				for _, k := range keys {
					c.GetCopy(k)
				}
				return nil
			}, nil
		case opMSET:
			pairs := make(map[string][]byte, cfg.batchSize)
			return func() error {
				for i := 0; i < cfg.batchSize; i++ {
					pairs[randKey(rng, cfg.keySpace)] = v
				}
				c.MSet(0, pairs)
				return nil
			}, nil
		}
		return nil, nil
	}
}

// respFactory builds a factory for a RESP server target (slabbis or Redis).
func respFactory(op opKind, cfg config, addr string) factory {
	v := val(cfg.valueSize)
	return func(id int) (func() error, func()) {
		conn, err := dialRESP(addr)
		if err != nil {
			return nil, nil
		}
		rng := rand.New(rand.NewSource(int64(id + 1)))
		switch op {
		case opGET:
			return func() error {
				return conn.do("GET", randKey(rng, cfg.keySpace))
			}, conn.close
		case opSET:
			return func() error {
				return conn.do("SET", randKey(rng, cfg.keySpace), v)
			}, conn.close
		case opDEL:
			return func() error {
				return conn.do("DEL", randKey(rng, cfg.keySpace))
			}, conn.close
		case opEXISTS:
			return func() error {
				return conn.do("EXISTS", randKey(rng, cfg.keySpace))
			}, conn.close
		case opMixed8020:
			return func() error {
				if rng.Intn(100) < 80 {
					return conn.do("GET", randKey(rng, cfg.keySpace))
				}
				return conn.do("SET", randKey(rng, cfg.keySpace), v)
			}, conn.close
		case opMGET:
			args := make([]string, 1+cfg.batchSize)
			args[0] = "MGET"
			return func() error {
				for i := 1; i <= cfg.batchSize; i++ {
					args[i] = randKey(rng, cfg.keySpace)
				}
				return conn.do(args...)
			}, conn.close
		case opMSET:
			args := make([]string, 1+2*cfg.batchSize)
			args[0] = "MSET"
			return func() error {
				for i := 0; i < cfg.batchSize; i++ {
					args[1+2*i] = randKey(rng, cfg.keySpace)
					args[2+2*i] = v
				}
				return conn.do(args...)
			}, conn.close
		}
		return nil, conn.close
	}
}

// ─── output ───────────────────────────────────────────────────────────────────

func fmtDur(d time.Duration) string {
	switch {
	case d == 0:
		return "—"
	case d < time.Microsecond:
		return fmt.Sprintf("%dns", d.Nanoseconds())
	case d < time.Millisecond:
		return fmt.Sprintf("%.1fµs", float64(d.Nanoseconds())/1e3)
	default:
		return fmt.Sprintf("%.2fms", float64(d.Nanoseconds())/1e6)
	}
}

func fmtOps(f float64) string {
	switch {
	case f >= 1e6:
		return fmt.Sprintf("%.2fM/s", f/1e6)
	case f >= 1e3:
		return fmt.Sprintf("%.1fk/s", f/1e3)
	default:
		return fmt.Sprintf("%.0f/s", f)
	}
}

type row struct {
	op      string
	target  string
	result  benchResult
	skipped bool
	skipMsg string
}

func printTable(rows []row) {
	const (
		wOp     = 12
		wTarget = 14
		wOps    = 10
		wLat    = 9
		wErr    = 6
	)
	width := wOp + wTarget + wOps + 3*wLat + wErr + 14
	sep := strings.Repeat("─", width)

	fmt.Println(sep)
	fmt.Printf("  %-*s  %-*s  %*s  %*s  %*s  %*s  %s\n",
		wOp, "Operation",
		wTarget, "Target",
		wOps, "ops/s",
		wLat, "p50",
		wLat, "p99",
		wLat, "p999",
		"errors",
	)
	fmt.Println(sep)

	prevOp := ""
	for _, r := range rows {
		if r.op != prevOp && prevOp != "" {
			fmt.Println()
		}
		prevOp = r.op

		if r.skipped {
			fmt.Printf("  %-*s  %-*s  %s\n", wOp, r.op, wTarget, r.target, r.skipMsg)
			continue
		}
		res := r.result
		fmt.Printf("  %-*s  %-*s  %*s  %*s  %*s  %*s  %d\n",
			wOp, r.op,
			wTarget, r.target,
			wOps, fmtOps(res.opsPerSec()),
			wLat, fmtDur(res.percentile(50)),
			wLat, fmtDur(res.percentile(99)),
			wLat, fmtDur(res.percentile(99.9)),
			res.errs,
		)
	}
	fmt.Println(sep)
}

// ─── availability probe ───────────────────────────────────────────────────────

func probe(addr string) bool {
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return false
	}
	c.Close()
	return true
}

// ─── main ─────────────────────────────────────────────────────────────────────

func main() {
	var cfg config
	flag.DurationVar(&cfg.duration, "duration", 5*time.Second, "duration of each benchmark run")
	flag.IntVar(&cfg.concurrency, "concurrency", 10, "goroutines per benchmark")
	flag.IntVar(&cfg.keySpace, "key-space", 10_000, "number of distinct keys")
	flag.IntVar(&cfg.valueSize, "value-size", 64, "value size in bytes")
	flag.IntVar(&cfg.batchSize, "batch-size", 10, "keys per MGET / MSET call")
	flag.StringVar(&cfg.slabbisAddr, "slabbis-addr", "127.0.0.1:6399", "slabbis RESP server address")
	flag.StringVar(&cfg.redisAddr, "redis-addr", "127.0.0.1:6379", "Redis server address")
	flag.Parse()

	slabbisOK := probe(cfg.slabbisAddr)
	redisOK := probe(cfg.redisAddr)

	// In-process cache: same size classes the slabbis server uses by default,
	// minus the two largest to keep the memory footprint sane.
	inProcCache := slabbis.New(slabbis.Config{
		Shards: 0, // NumCPU
		Classes: []slabber.SizeClass{
			{MaxSize: 64},
			{MaxSize: 512},
			{MaxSize: 4096},
			{MaxSize: 32768},
		},
	})
	defer inProcCache.Close()

	// ── header ────────────────────────────────────────────────────────────────
	fmt.Printf("\n  slabbis benchmark\n")
	fmt.Printf("  %d goroutines · %d-byte values · %s per run · key space %d · batch %d\n\n",
		cfg.concurrency, cfg.valueSize, cfg.duration, cfg.keySpace, cfg.batchSize)

	if !slabbisOK {
		fmt.Fprintf(os.Stderr, "  ! slabbis-RESP not reachable at %s (skipping)\n\n", cfg.slabbisAddr)
	}
	if !redisOK {
		fmt.Fprintf(os.Stderr, "  ! Redis not reachable at %s (skipping)\n\n", cfg.redisAddr)
	}

	type target struct {
		name   string
		ok     bool
		isRESP bool
		addr   string
	}
	targets := []target{
		{"in-process", true, false, ""},
		{"slabbis-RESP", slabbisOK, true, cfg.slabbisAddr},
		{"Redis", redisOK, true, cfg.redisAddr},
	}

	var rows []row

	for _, op := range allOps {
		opStr := op.String()

		// ── prepare key space ─────────────────────────────────────────────────
		if op.needsSeed() {
			fmt.Printf("  seeding %d keys for %-12s ...", cfg.keySpace, opStr)
			seedInProc(inProcCache, cfg.keySpace, cfg.valueSize)
			if slabbisOK {
				if err := seedRESP(cfg.slabbisAddr, cfg.keySpace, cfg.valueSize); err != nil {
					fmt.Fprintf(os.Stderr, "\n  ! seed slabbis: %v\n", err)
				}
			}
			if redisOK {
				if err := seedRESP(cfg.redisAddr, cfg.keySpace, cfg.valueSize); err != nil {
					fmt.Fprintf(os.Stderr, "\n  ! seed redis: %v\n", err)
				}
			}
			fmt.Println(" done")
		} else {
			inProcCache.Flush()
			if slabbisOK {
				_ = flushRESP(cfg.slabbisAddr)
			}
			if redisOK {
				_ = flushRESP(cfg.redisAddr)
			}
		}

		// ── run each target ───────────────────────────────────────────────────
		for _, tgt := range targets {
			if !tgt.ok {
				rows = append(rows, row{
					op:      opStr,
					target:  tgt.name,
					skipped: true,
					skipMsg: "(not running)",
				})
				continue
			}

			var f factory
			if !tgt.isRESP {
				f = inProcFactory(op, cfg, inProcCache)
			} else {
				f = respFactory(op, cfg, tgt.addr)
			}

			fmt.Printf("  %-12s  %-14s  running ... ", opStr, tgt.name)
			os.Stdout.Sync()

			res := runBench(cfg.duration, cfg.concurrency, f)
			fmt.Printf("%s\n", fmtOps(res.opsPerSec()))

			rows = append(rows, row{
				op:     opStr,
				target: tgt.name,
				result: res,
			})
		}

		fmt.Println()
	}

	// ── final table ───────────────────────────────────────────────────────────
	fmt.Println()
	printTable(rows)
	fmt.Println()
}
