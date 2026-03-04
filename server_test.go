package slabbis_test

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ha1tch/slabbis"
)

// testServer starts a server on a random local port and returns the address
// and a cleanup function. The cache uses a fast reaper for TTL tests.
func testServer(t *testing.T) (addr string, cleanup func()) {
	t.Helper()
	c := slabbis.New(slabbis.Config{
		Shards:         2,
		ReaperInterval: 20 * time.Millisecond,
	})
	srv, err := slabbis.NewServer("127.0.0.1:0", c, nil)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	go srv.Serve() //nolint:errcheck
	return srv.Addr(), func() {
		srv.Close()
		c.Close()
	}
}

// client is a minimal test RESP client over a single TCP connection.
type client struct {
	conn net.Conn
	r    *bufio.Reader
}

func dial(t *testing.T, addr string) *client {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return &client{conn: conn, r: bufio.NewReader(conn)}
}

func (c *client) close() { c.conn.Close() }

// send writes a RESP array command to the server.
func (c *client) send(args ...string) {
	var sb strings.Builder
	fmt.Fprintf(&sb, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&sb, "$%d\r\n%s\r\n", len(a), a)
	}
	c.conn.Write([]byte(sb.String())) //nolint:errcheck
}

// readLine reads one RESP line (including the type prefix).
func (c *client) readLine() string {
	line, _ := c.r.ReadString('\n')
	return strings.TrimRight(line, "\r\n")
}

// readBulk reads a bulk string response, returning the value or "" for null.
func (c *client) readBulk() string {
	header := c.readLine()
	if header == "$-1" {
		return ""
	}
	if len(header) == 0 || header[0] != '$' {
		return header // return as-is for error messages in tests
	}
	var n int
	fmt.Sscanf(header[1:], "%d", &n)
	buf := make([]byte, n+2)
	c.r.Read(buf) //nolint:errcheck
	return string(buf[:n])
}

// ---- server tests -----------------------------------------------------------

func TestServerPing(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()

	cl := dial(t, addr)
	defer cl.close()

	cl.send("PING")
	if line := cl.readLine(); line != "+PONG" {
		t.Fatalf("got %q, want +PONG", line)
	}
}

func TestServerPingWithMessage(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()

	cl := dial(t, addr)
	defer cl.close()

	cl.send("PING", "hello")
	if got := cl.readBulk(); got != "hello" {
		t.Fatalf("got %q, want %q", got, "hello")
	}
}

func TestServerSetGet(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()

	cl := dial(t, addr)
	defer cl.close()

	cl.send("SET", "mykey", "myvalue")
	if line := cl.readLine(); line != "+OK" {
		t.Fatalf("SET: got %q", line)
	}

	cl.send("GET", "mykey")
	if got := cl.readBulk(); got != "myvalue" {
		t.Fatalf("GET: got %q, want %q", got, "myvalue")
	}
}

func TestServerGetMiss(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()

	cl := dial(t, addr)
	defer cl.close()

	cl.send("GET", "nosuchkey")
	if got := cl.readBulk(); got != "" {
		t.Fatalf("expected null bulk, got %q", got)
	}
}

func TestServerSetGetWithEX(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()

	cl := dial(t, addr)
	defer cl.close()

	cl.send("SET", "ttlkey", "val", "EX", "10")
	cl.readLine() // +OK

	cl.send("TTL", "ttlkey")
	line := cl.readLine()
	if line == ":-2" || line == ":-1" {
		t.Fatalf("expected positive TTL, got %q", line)
	}
	if len(line) == 0 || line[0] != ':' {
		t.Fatalf("expected integer response, got %q", line)
	}
}

func TestServerSetGetWithPX(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()

	cl := dial(t, addr)
	defer cl.close()

	cl.send("SET", "pxkey", "val", "PX", "10000")
	cl.readLine() // +OK

	cl.send("PTTL", "pxkey")
	line := cl.readLine()
	if line == ":-2" || line == ":-1" {
		t.Fatalf("expected positive PTTL, got %q", line)
	}
}

func TestServerDel(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()

	cl := dial(t, addr)
	defer cl.close()

	cl.send("SET", "a", "1")
	cl.readLine()
	cl.send("SET", "b", "2")
	cl.readLine()

	cl.send("DEL", "a", "b", "missing")
	if line := cl.readLine(); line != ":2" {
		t.Fatalf("DEL: got %q, want :2", line)
	}

	cl.send("GET", "a")
	if got := cl.readBulk(); got != "" {
		t.Fatalf("expected miss after DEL, got %q", got)
	}
}

func TestServerExists(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()

	cl := dial(t, addr)
	defer cl.close()

	cl.send("EXISTS", "ghost")
	if line := cl.readLine(); line != ":0" {
		t.Fatalf("EXISTS missing: got %q, want :0", line)
	}

	cl.send("SET", "x", "1")
	cl.readLine()

	cl.send("EXISTS", "x", "x")
	if line := cl.readLine(); line != ":2" {
		t.Fatalf("EXISTS x x: got %q, want :2", line)
	}
}

func TestServerTTLMissing(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()

	cl := dial(t, addr)
	defer cl.close()

	cl.send("TTL", "ghost")
	if line := cl.readLine(); line != ":-2" {
		t.Fatalf("TTL missing: got %q, want :-2", line)
	}
}

func TestServerTTLNoExpiry(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()

	cl := dial(t, addr)
	defer cl.close()

	cl.send("SET", "k", "v")
	cl.readLine()
	cl.send("TTL", "k")
	if line := cl.readLine(); line != ":-1" {
		t.Fatalf("TTL no-expiry: got %q, want :-1", line)
	}
}

func TestServerFlush(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()

	cl := dial(t, addr)
	defer cl.close()

	for _, k := range []string{"a", "b", "c"} {
		cl.send("SET", k, "v")
		cl.readLine()
	}

	cl.send("FLUSH")
	if line := cl.readLine(); line != "+OK" {
		t.Fatalf("FLUSH: got %q", line)
	}

	cl.send("GET", "a")
	if got := cl.readBulk(); got != "" {
		t.Fatalf("expected miss after FLUSH, got %q", got)
	}
}

func TestServerCommand(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()

	cl := dial(t, addr)
	defer cl.close()

	cl.send("COMMAND")
	// Should return *0\r\n — empty array.
	if line := cl.readLine(); line != "*0" {
		t.Fatalf("COMMAND: got %q, want *0", line)
	}
}

func TestServerUnknownCommand(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()

	cl := dial(t, addr)
	defer cl.close()

	cl.send("HSET", "myhash", "field", "value")
	line := cl.readLine()
	if len(line) == 0 || line[0] != '-' {
		t.Fatalf("expected error response for unsupported command, got %q", line)
	}
}

func TestServerQuit(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()

	cl := dial(t, addr)
	defer cl.close()

	cl.send("QUIT")
	if line := cl.readLine(); line != "+OK" {
		t.Fatalf("QUIT: got %q, want +OK", line)
	}
	// Connection should now be closed by the server.
	cl.send("PING")
	_, err := cl.r.ReadString('\n')
	if err == nil {
		t.Fatal("expected connection closed after QUIT")
	}
}

func TestServerMultipleClients(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()

	const n = 20
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			cl := dial(t, addr)
			defer cl.close()
			key := fmt.Sprintf("client-%d", i)
			val := fmt.Sprintf("value-%d", i)
			cl.send("SET", key, val)
			cl.readLine()
			cl.send("GET", key)
			got := cl.readBulk()
			if got != val {
				t.Errorf("client %d: got %q, want %q", i, got, val)
			}
		}()
	}
	wg.Wait()
}

func TestServerKeys(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()

	cl := dial(t, addr)
	defer cl.close()

	for _, k := range []string{"foo", "foobar", "bar", "baz"} {
		cl.send("SET", k, "v")
		cl.readLine()
	}

	// KEYS foo* should match foo and foobar.
	cl.send("KEYS", "foo*")
	header := cl.readLine()
	var n int
	fmt.Sscanf(header[1:], "%d", &n)
	got := make([]string, n)
	for i := range got {
		got[i] = cl.readBulk()
	}
	if n != 2 {
		t.Fatalf("KEYS foo*: expected 2 results, got %d: %v", n, got)
	}

	// KEYS * should return all four.
	cl.send("KEYS", "*")
	header = cl.readLine()
	fmt.Sscanf(header[1:], "%d", &n)
	if n != 4 {
		t.Fatalf("KEYS *: expected 4 results, got %d", n)
	}
	for i := 0; i < n; i++ {
		cl.readBulk()
	}
}

func TestServerMGet(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()

	cl := dial(t, addr)
	defer cl.close()

	cl.send("SET", "a", "alpha")
	cl.readLine()
	cl.send("SET", "b", "beta")
	cl.readLine()

	cl.send("MGET", "a", "missing", "b")
	header := cl.readLine() // *3
	if header != "*3" {
		t.Fatalf("MGET header: got %q, want *3", header)
	}
	if got := cl.readBulk(); got != "alpha" {
		t.Fatalf("MGET a: got %q", got)
	}
	if got := cl.readBulk(); got != "" {
		t.Fatalf("MGET missing: expected null, got %q", got)
	}
	if got := cl.readBulk(); got != "beta" {
		t.Fatalf("MGET b: got %q", got)
	}
}

func TestServerMSet(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()

	cl := dial(t, addr)
	defer cl.close()

	cl.send("MSET", "x", "1", "y", "2", "z", "3")
	if line := cl.readLine(); line != "+OK" {
		t.Fatalf("MSET: got %q", line)
	}
	cl.send("GET", "y")
	if got := cl.readBulk(); got != "2" {
		t.Fatalf("GET y after MSET: got %q, want 2", got)
	}
}

func TestServerSetNX(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()

	cl := dial(t, addr)
	defer cl.close()

	cl.send("SETNX", "nx", "first")
	if line := cl.readLine(); line != ":1" {
		t.Fatalf("SETNX new key: got %q, want :1", line)
	}

	cl.send("SETNX", "nx", "second")
	if line := cl.readLine(); line != ":0" {
		t.Fatalf("SETNX existing key: got %q, want :0", line)
	}

	cl.send("GET", "nx")
	if got := cl.readBulk(); got != "first" {
		t.Fatalf("value should be unchanged: got %q", got)
	}
}

func TestServerGetDel(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()

	cl := dial(t, addr)
	defer cl.close()

	cl.send("SET", "gd", "gone")
	cl.readLine()

	cl.send("GETDEL", "gd")
	if got := cl.readBulk(); got != "gone" {
		t.Fatalf("GETDEL: got %q, want gone", got)
	}

	cl.send("EXISTS", "gd")
	if line := cl.readLine(); line != ":0" {
		t.Fatalf("key should be gone after GETDEL: got %q", line)
	}

	// GETDEL on missing key returns null.
	cl.send("GETDEL", "ghost")
	if got := cl.readBulk(); got != "" {
		t.Fatalf("GETDEL missing: expected null, got %q", got)
	}
}

func TestServerRename(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()

	cl := dial(t, addr)
	defer cl.close()

	cl.send("SET", "old", "value")
	cl.readLine()

	cl.send("RENAME", "old", "new")
	if line := cl.readLine(); line != "+OK" {
		t.Fatalf("RENAME: got %q", line)
	}

	cl.send("GET", "new")
	if got := cl.readBulk(); got != "value" {
		t.Fatalf("GET after RENAME: got %q, want value", got)
	}

	cl.send("EXISTS", "old")
	if line := cl.readLine(); line != ":0" {
		t.Fatalf("old key should be gone after RENAME")
	}

	// RENAME non-existent key returns error.
	cl.send("RENAME", "ghost", "other")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("expected error for missing key, got %q", line)
	}
}

func TestServerDBSize(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()

	cl := dial(t, addr)
	defer cl.close()

	cl.send("FLUSH")
	cl.readLine()

	cl.send("DBSIZE")
	if line := cl.readLine(); line != ":0" {
		t.Fatalf("DBSIZE empty: got %q, want :0", line)
	}

	cl.send("SET", "a", "1")
	cl.readLine()
	cl.send("SET", "b", "2")
	cl.readLine()

	cl.send("DBSIZE")
	if line := cl.readLine(); line != ":2" {
		t.Fatalf("DBSIZE after 2 sets: got %q, want :2", line)
	}
}

func TestServerType(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()

	cl := dial(t, addr)
	defer cl.close()

	cl.send("TYPE", "ghost")
	if line := cl.readLine(); line != "+none" {
		t.Fatalf("TYPE missing: got %q, want +none", line)
	}

	cl.send("SET", "k", "v")
	cl.readLine()

	cl.send("TYPE", "k")
	if line := cl.readLine(); line != "+string" {
		t.Fatalf("TYPE existing: got %q, want +string", line)
	}
}
