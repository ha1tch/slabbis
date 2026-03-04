package slabbis

import (
	"fmt"
	"io"
	"log"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ha1tch/slabbis/internal/resp"
)

// Server listens on a network address and dispatches RESP commands to a Cache.
// It supports exactly the commands slabbis exposes; anything else returns an
// error response rather than a panic.
//
// Supported commands:
//
//	GET key
//	SET key value [EX seconds | PX milliseconds]
//	MGET key [key ...]
//	MSET key value [key value ...]
//	SETNX key value
//	GETDEL key
//	DEL key [key ...]
//	EXISTS key [key ...]
//	KEYS pattern
//	RENAME from to
//	DBSIZE
//	TYPE key
//	TTL key
//	PTTL key
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
func (s *Server) Serve() error {
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
		// GetCopy copies inside the shard read lock, so the slice is safe
		// to hold across concurrent Set/Del on the same key.
		val, ok := s.cache.GetCopy(string(cmd.Args[1]))
		if !ok {
			_ = wr.WriteBulk(nil)
		} else {
			_ = wr.WriteBulk(val)
		}

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

	case "MGET":
		if len(cmd.Args) < 2 {
			_ = wr.WriteError("wrong number of arguments for MGET")
			return false
		}
		// Use GetCopy per key rather than MGet: MGet returns live slabber
		// views that could be mutated by a concurrent Set before WriteArray
		// finishes reading them. GetCopy copies under the shard read lock.
		vals := make([][]byte, len(cmd.Args)-1)
		for i, arg := range cmd.Args[1:] {
			if v, ok := s.cache.GetCopy(string(arg)); ok {
				vals[i] = v
			}
		}
		_ = wr.WriteArray(vals)

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
	var n int64
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("not an integer")
		}
		n = n*10 + int64(c-'0')
	}
	return n, nil
}