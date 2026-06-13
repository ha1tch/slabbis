package slabbis

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ha1tch/slabbis/internal/resp"
)

// valBufPool is a pool of value-copy buffers shared across server connections.
// GET and MGET handlers borrow a buffer, call GetInto to copy under the shard
// read lock, write the result to the RESP wire buffer (which copies the bytes),
// then return the buffer to the pool. In steady state each buffer grows to the
// high-water mark of values seen on its connection and is reused with zero
// heap allocations.
var valBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 512)
		return &b
	},
}

// Server listens on a network address and dispatches RESP commands to a Cache.
// It supports exactly the commands slabbis exposes; anything else returns an
// error response rather than a panic.
//
// Supported commands:
//
//	GET key
//	SET key value [EX seconds | PX milliseconds]
//	GETSET key value
//	GETEX key [EX seconds | PX milliseconds | PERSIST]
//	MGET key [key ...]
//	MSET key value [key value ...]
//	SETNX key value
//	GETDEL key
//	DEL key [key ...]
//	UNLINK key [key ...]
//	EXISTS key [key ...]
//	STRLEN key
//	INCR key
//	INCRBY key increment
//	DECR key
//	DECRBY key decrement
//	KEYS pattern
//	SCAN cursor [MATCH pattern] [COUNT count]
//	RANDOMKEY
//	COPY source destination
//	RENAME from to
//	DBSIZE
//	TYPE key
//	TTL key
//	PTTL key
//	EXPIRE key seconds
//	PEXPIRE key milliseconds
//	PERSIST key
//	FLUSH (non-standard; equivalent to FLUSHALL)
//	PING [message]
//	COMMAND (returns empty array — satisfies redis-cli startup probe)
//	QUIT
type Server struct {
	cache    Cache
	listener net.Listener
	log      *log.Logger
	serveWg  sync.WaitGroup // tracks the Serve goroutine lifetime
	connWg   sync.WaitGroup // tracks all handleConn goroutine lifetimes
}

// NewServer returns a Server bound to addr using the provided Cache.
// addr may be a TCP address ("127.0.0.1:6399") or a Unix socket path
// ("unix:///tmp/slabbis.sock" — the "unix://" prefix is stripped).
func NewServer(addr string, c Cache, logger *log.Logger) (*Server, error) {
	var l net.Listener
	var err error
	if strings.HasPrefix(addr, "unix://") {
		path := strings.TrimPrefix(addr, "unix://")
		l, err = net.Listen("unix", path)
	} else {
		l, err = net.Listen("tcp", addr)
	}
	if err != nil {
		return nil, fmt.Errorf("slabbis: listen %s: %w", addr, err)
	}
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	return &Server{cache: c, listener: l, log: logger}, nil
}

// Addr returns the address the server is listening on.
func (s *Server) Addr() string {
	return s.listener.Addr().String()
}

// Serve accepts connections until the listener is closed.
// It returns the listener's close error, which is typically non-nil only
// when Close() has been called.
//
// Serve registers itself with serveWg so that Close() can wait for the
// accept loop to return before declaring shutdown complete.
func (s *Server) Serve() error {
	s.serveWg.Add(1)
	defer s.serveWg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return err
		}
		s.connWg.Add(1)
		go s.handleConn(conn)
	}
}

// Close stops the server and waits for all goroutines to return.
// It closes the listener first (causing Serve to return), then waits for
// Serve itself and all active connection handlers to finish.
func (s *Server) Close() error {
	err := s.listener.Close()
	s.serveWg.Wait()
	s.connWg.Wait()
	return err
}

// handleConn serves a single client connection.
func (s *Server) handleConn(conn net.Conn) {
	defer s.connWg.Done()
	defer conn.Close()
	rd := resp.NewReader(conn)
	wr := resp.NewWriter(conn)

	for {
		cmd, err := rd.ReadCommand()
		if err != nil {
			return // EOF or broken pipe — close silently
		}
		if quit := s.dispatch(cmd, wr); quit {
			_ = wr.Flush()
			return
		}
		if err := wr.Flush(); err != nil {
			return
		}
	}
}

// dispatch executes one command, writes the response, and returns true if the
// connection should be closed (QUIT command).
func (s *Server) dispatch(cmd *resp.Command, wr *resp.Writer) bool {
	switch cmd.Name() {

	case "PING":
		if len(cmd.Args) >= 2 {
			_ = wr.WriteBulk(cmd.Args[1])
		} else {
			_ = wr.WriteSimpleString("PONG")
		}

	case "GET":
		if len(cmd.Args) != 2 {
			_ = wr.WriteError("wrong number of arguments for GET")
			return false
		}
		// Borrow a pooled buffer, copy the value into it under the shard read
		// lock via GetInto, write to the wire buffer (which copies the bytes),
		// then return. Zero heap allocations in steady state.
		bp := valBufPool.Get().(*[]byte)
		dst, ok := s.cache.GetInto(string(cmd.Args[1]), *bp)
		*bp = dst
		if !ok {
			_ = wr.WriteBulk(nil)
		} else {
			_ = wr.WriteBulk(dst)
		}
		valBufPool.Put(bp)

	case "SET":
		if len(cmd.Args) < 3 {
			_ = wr.WriteError("wrong number of arguments for SET")
			return false
		}
		key := string(cmd.Args[1])
		value := cmd.Args[2]
		ttl, err := parseSetOptions(cmd.Args[3:])
		if err != nil {
			_ = wr.WriteError(err.Error())
			return false
		}
		s.cache.Set(key, value, ttl)
		// Detect silent drop: if the value is non-empty but the key is now
		// absent, it exceeded the largest Arena size class and was discarded.
		// This is an expected limitation of the current slab-only storage, but
		// silently losing data is extremely hard to debug. Log a warning so
		// operators know immediately which keys and sizes are affected.
		if len(value) > 0 && !s.cache.Exists(key) {
			s.log.Printf("WARN SET %q: value (%d bytes) exceeds largest size class — dropped; resize -max-value or -classes",
				key, len(value))
		}
		_ = wr.WriteSimpleString("OK")

	case "DEL":
		if len(cmd.Args) < 2 {
			_ = wr.WriteError("wrong number of arguments for DEL")
			return false
		}
		var n int64
		for _, arg := range cmd.Args[1:] {
			if s.cache.Del(string(arg)) {
				n++
			}
		}
		_ = wr.WriteInt(n)

	case "EXISTS":
		if len(cmd.Args) < 2 {
			_ = wr.WriteError("wrong number of arguments for EXISTS")
			return false
		}
		var n int64
		for _, arg := range cmd.Args[1:] {
			if s.cache.Exists(string(arg)) {
				n++
			}
		}
		_ = wr.WriteInt(n)

	case "TTL":
		if len(cmd.Args) != 2 {
			_ = wr.WriteError("wrong number of arguments for TTL")
			return false
		}
		remaining, ok := s.cache.TTL(string(cmd.Args[1]))
		if !ok {
			_ = wr.WriteInt(-2) // key does not exist (Redis convention)
		} else if remaining == 0 {
			_ = wr.WriteInt(-1) // key exists but has no expiry
		} else {
			_ = wr.WriteInt(int64(remaining.Seconds()))
		}

	case "PTTL":
		if len(cmd.Args) != 2 {
			_ = wr.WriteError("wrong number of arguments for PTTL")
			return false
		}
		remaining, ok := s.cache.TTL(string(cmd.Args[1]))
		if !ok {
			_ = wr.WriteInt(-2)
		} else if remaining == 0 {
			_ = wr.WriteInt(-1)
		} else {
			_ = wr.WriteInt(remaining.Milliseconds())
		}

	case "KEYS":
		if len(cmd.Args) != 2 {
			_ = wr.WriteError("wrong number of arguments for KEYS")
			return false
		}
		keys := s.cache.Keys(string(cmd.Args[1]))
		sort.Strings(keys)
		items := make([][]byte, len(keys))
		for i, k := range keys {
			items[i] = []byte(k)
		}
		_ = wr.WriteArray(items)

	case "SCAN":
		// SCAN cursor [MATCH pattern] [COUNT count]
		//
		// slabbis is an in-memory store: all matching keys are always available
		// in a single pass. We return cursor "0" on every call, which per the
		// RESP spec signals that the full iteration is complete. Clients that
		// loop until cursor "0" will terminate correctly after the first call.
		//
		// COUNT is accepted but ignored — the count hint controls batch size in
		// a persistent store; here there is no cost to returning all keys at once.
		//
		// SCAN requires at least a cursor argument; extra args are MATCH/COUNT pairs.
		if len(cmd.Args) < 2 {
			_ = wr.WriteError("wrong number of arguments for SCAN")
			return false
		}
		pattern := "*"
		args := cmd.Args[2:] // skip verb and cursor
		for i := 0; i+1 < len(args); i += 2 {
			switch strings.ToUpper(string(args[i])) {
			case "MATCH":
				pattern = string(args[i+1])
			case "COUNT":
				// accepted and ignored
			}
		}
		keys := s.cache.Keys(pattern)
		sort.Strings(keys)
		// RESP response: *2 [ bulk_string("0"), array_of_keys ]
		_ = wr.WriteArrayHeader(2)
		_ = wr.WriteBulk([]byte("0"))
		items := make([][]byte, len(keys))
		for i, k := range keys {
			items[i] = []byte(k)
		}
		_ = wr.WriteArray(items)

	case "MGET":
		if len(cmd.Args) < 2 {
			_ = wr.WriteError("wrong number of arguments for MGET")
			return false
		}
		// Write the array header first, then stream each value directly using
		// a single pooled buffer. This avoids allocating the vals [][]byte
		// slice and per-key GetCopy allocations. The buffer is borrowed once
		// for the whole batch and returned after the last key.
		n := len(cmd.Args) - 1
		_ = wr.WriteArrayHeader(n)
		bp := valBufPool.Get().(*[]byte)
		for _, arg := range cmd.Args[1:] {
			dst, ok := s.cache.GetInto(string(arg), *bp)
			*bp = dst
			if ok {
				_ = wr.WriteBulk(dst)
			} else {
				_ = wr.WriteBulk(nil)
			}
		}
		valBufPool.Put(bp)

	case "MSET":
		if len(cmd.Args) < 3 || len(cmd.Args)%2 == 0 {
			_ = wr.WriteError("wrong number of arguments for MSET")
			return false
		}
		pairs := make(map[string][]byte, (len(cmd.Args)-1)/2)
		for i := 1; i < len(cmd.Args); i += 2 {
			pairs[string(cmd.Args[i])] = cmd.Args[i+1]
		}
		s.cache.MSet(0, pairs)
		_ = wr.WriteSimpleString("OK")

	case "SETNX":
		if len(cmd.Args) != 3 {
			_ = wr.WriteError("wrong number of arguments for SETNX")
			return false
		}
		set := s.cache.SetNX(string(cmd.Args[1]), cmd.Args[2], 0)
		if set {
			_ = wr.WriteInt(1)
		} else {
			_ = wr.WriteInt(0)
		}

	case "GETDEL":
		if len(cmd.Args) != 2 {
			_ = wr.WriteError("wrong number of arguments for GETDEL")
			return false
		}
		val, ok := s.cache.GetDel(string(cmd.Args[1]))
		if !ok {
			_ = wr.WriteBulk(nil)
		} else {
			_ = wr.WriteBulk(val)
		}

	case "RENAME":
		if len(cmd.Args) != 3 {
			_ = wr.WriteError("wrong number of arguments for RENAME")
			return false
		}
		if !s.cache.Rename(string(cmd.Args[1]), string(cmd.Args[2])) {
			_ = wr.WriteError("no such key")
			return false
		}
		_ = wr.WriteSimpleString("OK")

	case "DBSIZE":
		_ = wr.WriteInt(int64(s.cache.DBSize()))

	case "TYPE":
		if len(cmd.Args) != 2 {
			_ = wr.WriteError("wrong number of arguments for TYPE")
			return false
		}
		if s.cache.Exists(string(cmd.Args[1])) {
			_ = wr.WriteSimpleString("string")
		} else {
			_ = wr.WriteSimpleString("none")
		}

	case "FLUSH", "FLUSHALL", "FLUSHDB":
		s.cache.Flush()
		_ = wr.WriteSimpleString("OK")

	case "COMMAND":
		// redis-cli sends COMMAND DOCS or COMMAND COUNT on startup.
		// Return an empty array to satisfy the probe without implementing
		// the full COMMAND introspection surface.
		_ = wr.WriteArray(nil)

	case "QUIT":
		_ = wr.WriteSimpleString("OK")
		return true

	case "UNLINK":
		// UNLINK is an async DEL in Redis; for slabbis it is synchronous DEL.
		if len(cmd.Args) < 2 {
			_ = wr.WriteError("wrong number of arguments for UNLINK")
			return false
		}
		var n int64
		for _, arg := range cmd.Args[1:] {
			if s.cache.Del(string(arg)) {
				n++
			}
		}
		_ = wr.WriteInt(n)

	case "STRLEN":
		if len(cmd.Args) != 2 {
			_ = wr.WriteError("wrong number of arguments for STRLEN")
			return false
		}
		bp := valBufPool.Get().(*[]byte)
		dst, ok := s.cache.GetInto(string(cmd.Args[1]), *bp)
		*bp = dst
		valBufPool.Put(bp)
		if !ok {
			_ = wr.WriteInt(0)
		} else {
			_ = wr.WriteInt(int64(len(dst)))
		}

	case "INCR":
		if len(cmd.Args) != 2 {
			_ = wr.WriteError("wrong number of arguments for INCR")
			return false
		}
		n, err := s.cache.IncrBy(string(cmd.Args[1]), 1)
		if err != nil {
			_ = wr.WriteError(err.Error())
		} else {
			_ = wr.WriteInt(n)
		}

	case "DECR":
		if len(cmd.Args) != 2 {
			_ = wr.WriteError("wrong number of arguments for DECR")
			return false
		}
		n, err := s.cache.IncrBy(string(cmd.Args[1]), -1)
		if err != nil {
			_ = wr.WriteError(err.Error())
		} else {
			_ = wr.WriteInt(n)
		}

	case "INCRBY":
		if len(cmd.Args) != 3 {
			_ = wr.WriteError("wrong number of arguments for INCRBY")
			return false
		}
		delta, err := parseSignedInt(cmd.Args[2])
		if err != nil {
			_ = wr.WriteError("value is not an integer or out of range")
			return false
		}
		n, err := s.cache.IncrBy(string(cmd.Args[1]), delta)
		if err != nil {
			_ = wr.WriteError(err.Error())
		} else {
			_ = wr.WriteInt(n)
		}

	case "DECRBY":
		if len(cmd.Args) != 3 {
			_ = wr.WriteError("wrong number of arguments for DECRBY")
			return false
		}
		delta, err := parseSignedInt(cmd.Args[2])
		if err != nil {
			_ = wr.WriteError("value is not an integer or out of range")
			return false
		}
		n, err := s.cache.IncrBy(string(cmd.Args[1]), -delta)
		if err != nil {
			_ = wr.WriteError(err.Error())
		} else {
			_ = wr.WriteInt(n)
		}

	case "GETSET":
		if len(cmd.Args) != 3 {
			_ = wr.WriteError("wrong number of arguments for GETSET")
			return false
		}
		old, _ := s.cache.GetSet(string(cmd.Args[1]), cmd.Args[2], 0)
		_ = wr.WriteBulk(old) // nil → null bulk if key did not exist

	case "GETEX":
		// GETEX key [EX seconds | PX milliseconds | PERSIST]
		if len(cmd.Args) < 2 {
			_ = wr.WriteError("wrong number of arguments for GETEX")
			return false
		}
		key := string(cmd.Args[1])
		bp := valBufPool.Get().(*[]byte)
		dst, found := s.cache.GetInto(key, *bp)
		*bp = dst
		if !found {
			valBufPool.Put(bp)
			_ = wr.WriteBulk(nil)
			return false
		}
		// Return value before applying TTL change.
		_ = wr.WriteBulk(dst)
		valBufPool.Put(bp)
		// Apply optional TTL modifier.
		if len(cmd.Args) >= 3 {
			opt := strings.ToUpper(string(cmd.Args[2]))
			switch opt {
			case "PERSIST":
				s.cache.SetTTL(key, 0)
			case "EX":
				if len(cmd.Args) != 4 {
					// Already wrote the value; don't write another response.
					return false
				}
				n, err := parseInt(cmd.Args[3])
				if err != nil || n <= 0 {
					return false
				}
				s.cache.SetTTL(key, time.Duration(n)*time.Second)
			case "PX":
				if len(cmd.Args) != 4 {
					return false
				}
				n, err := parseInt(cmd.Args[3])
				if err != nil || n <= 0 {
					return false
				}
				s.cache.SetTTL(key, time.Duration(n)*time.Millisecond)
			}
		}

	case "EXPIRE":
		if len(cmd.Args) != 3 {
			_ = wr.WriteError("wrong number of arguments for EXPIRE")
			return false
		}
		n, err := parseInt(cmd.Args[2])
		if err != nil || n <= 0 {
			_ = wr.WriteError("invalid expire time in EXPIRE")
			return false
		}
		if s.cache.SetTTL(string(cmd.Args[1]), time.Duration(n)*time.Second) {
			_ = wr.WriteInt(1)
		} else {
			_ = wr.WriteInt(0)
		}

	case "PEXPIRE":
		if len(cmd.Args) != 3 {
			_ = wr.WriteError("wrong number of arguments for PEXPIRE")
			return false
		}
		n, err := parseInt(cmd.Args[2])
		if err != nil || n <= 0 {
			_ = wr.WriteError("invalid expire time in PEXPIRE")
			return false
		}
		if s.cache.SetTTL(string(cmd.Args[1]), time.Duration(n)*time.Millisecond) {
			_ = wr.WriteInt(1)
		} else {
			_ = wr.WriteInt(0)
		}

	case "PERSIST":
		if len(cmd.Args) != 2 {
			_ = wr.WriteError("wrong number of arguments for PERSIST")
			return false
		}
		// SetTTL with 0 removes the expiry.
		if s.cache.SetTTL(string(cmd.Args[1]), 0) {
			_ = wr.WriteInt(1)
		} else {
			_ = wr.WriteInt(0)
		}

	case "RANDOMKEY":
		keys := s.cache.Keys("*")
		if len(keys) == 0 {
			_ = wr.WriteBulk(nil)
		} else {
			_ = wr.WriteBulk([]byte(keys[rand.Intn(len(keys))]))
		}

	case "COPY":
		// COPY source destination — non-atomic: GetCopy + Set.
		// Returns 1 on success, 0 if source does not exist.
		if len(cmd.Args) != 3 {
			_ = wr.WriteError("wrong number of arguments for COPY")
			return false
		}
		val, ok := s.cache.GetCopy(string(cmd.Args[1]))
		if !ok {
			_ = wr.WriteInt(0)
		} else {
			s.cache.Set(string(cmd.Args[2]), val, 0)
			_ = wr.WriteInt(1)
		}

	default:
		_ = wr.WriteError(fmt.Sprintf("unknown command %q", cmd.Name()))
	}

	return false
}

// parseSetOptions parses the optional [EX seconds | PX milliseconds] tail of
// a SET command. Returns 0 duration for no expiry.
func parseSetOptions(args [][]byte) (time.Duration, error) {
	if len(args) == 0 {
		return 0, nil
	}
	if len(args) < 2 {
		return 0, fmt.Errorf("syntax error in SET options")
	}
	opt := strings.ToUpper(string(args[0]))
	n, err := parseInt(args[1])
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid expire time in SET")
	}
	switch opt {
	case "EX":
		return time.Duration(n) * time.Second, nil
	case "PX":
		return time.Duration(n) * time.Millisecond, nil
	default:
		return 0, fmt.Errorf("unsupported SET option %q", opt)
	}
}

func parseInt(b []byte) (int64, error) {
	if len(b) == 0 {
		return 0, fmt.Errorf("empty integer")
	}
	var n int64
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("not a non-negative integer")
		}
		n = n*10 + int64(c-'0')
	}
	return n, nil
}

// parseSignedInt parses a signed decimal integer from a RESP argument.
// Accepts an optional leading '-'. Used by INCRBY and DECRBY.
func parseSignedInt(b []byte) (int64, error) {
	if len(b) == 0 {
		return 0, fmt.Errorf("empty integer")
	}
	neg := false
	i := 0
	if b[0] == '-' {
		neg = true
		i = 1
	}
	if i >= len(b) {
		return 0, fmt.Errorf("not an integer")
	}
	var n uint64
	for ; i < len(b); i++ {
		c := b[i]
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("not an integer")
		}
		n = n*10 + uint64(c-'0')
	}
	const maxInt64 = uint64(1<<63 - 1)
	if neg {
		if n > maxInt64+1 {
			return 0, fmt.Errorf("value out of range")
		}
		return -int64(n), nil
	}
	if n > maxInt64 {
		return 0, fmt.Errorf("value out of range")
	}
	return int64(n), nil
}