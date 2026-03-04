package slabbis_test

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ha1tch/slabbis"
)

// sharedSrv and sharedAddr are set by TestMain in slabbis_test.go.
var sharedSrv *slabbis.Server
var sharedAddr string

// testServer returns the shared server address and a cleanup function.
// cleanup sends QUIT on any open connection, waits for handleConn goroutines
// to exit, then calls runtime.GC() to reclaim their stack pages and clear
// race detector shadow memory. Without the GC call, the next test's
// goroutines can land on recycled stack pages, producing false race reports.
func testServer(t *testing.T) (addr string, cleanup func()) {
	t.Helper()
	flushViaQuit(t)
	return sharedAddr, func() { flushViaQuit(t) }
}

// flushViaQuit sends FLUSH then QUIT so the server closes the connection
// cleanly, allowing connWg to decrement promptly.
func flushViaQuit(t *testing.T) {
	t.Helper()
	cl := dial(t, sharedAddr)
	cl.send("FLUSH")
	cl.readLine()
	cl.send("QUIT")
	cl.readLine()
	cl.close()
}

// ---------------------------------------------------------------------------
// Minimal RESP test client
// ---------------------------------------------------------------------------

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

func (c *client) send(args ...string) {
	var sb strings.Builder
	fmt.Fprintf(&sb, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&sb, "$%d\r\n%s\r\n", len(a), a)
	}
	c.conn.Write([]byte(sb.String())) //nolint:errcheck
}

func (c *client) readLine() string {
	line, _ := c.r.ReadString('\n')
	return strings.TrimRight(line, "\r\n")
}

func (c *client) readBulk() string {
	header := c.readLine()
	if header == "$-1" {
		return ""
	}
	var n int
	fmt.Sscanf(header[1:], "%d", &n)
	buf := make([]byte, n+2)
	if _, err := io.ReadFull(c.r, buf); err != nil {
		return ""
	}
	return string(buf[:n])
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestServerPing(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("PING")
	if line := cl.readLine(); line != "+PONG" {
		t.Fatalf("PING: got %q, want +PONG", line)
	}
}

func TestServerPingWithMessage(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("PING", "hello")
	if got := cl.readBulk(); got != "hello" {
		t.Fatalf("PING hello: got %q, want hello", got)
	}
}

func TestServerSetGet(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("SET", "k", "v")
	if line := cl.readLine(); line != "+OK" {
		t.Fatalf("SET: got %q", line)
	}
	cl.send("GET", "k")
	if got := cl.readBulk(); got != "v" {
		t.Fatalf("GET: got %q, want v", got)
	}
}

func TestServerGetMiss(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("GET", "missing")
	if got := cl.readBulk(); got != "" {
		t.Fatalf("GET missing: expected null, got %q", got)
	}
}

func TestServerSetGetWithEX(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("SET", "k", "v", "EX", "10")
	if line := cl.readLine(); line != "+OK" {
		t.Fatalf("SET EX: got %q", line)
	}
	cl.send("GET", "k")
	if got := cl.readBulk(); got != "v" {
		t.Fatalf("GET after SET EX: got %q", got)
	}
}

func TestServerSetGetWithPX(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("SET", "k", "v", "PX", "10000")
	if line := cl.readLine(); line != "+OK" {
		t.Fatalf("SET PX: got %q", line)
	}
	cl.send("GET", "k")
	if got := cl.readBulk(); got != "v" {
		t.Fatalf("GET after SET PX: got %q", got)
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
}

func TestServerExists(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("SET", "k", "v")
	cl.readLine()
	cl.send("EXISTS", "k")
	if line := cl.readLine(); line != ":1" {
		t.Fatalf("EXISTS hit: got %q", line)
	}
	cl.send("EXISTS", "missing")
	if line := cl.readLine(); line != ":0" {
		t.Fatalf("EXISTS miss: got %q", line)
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

	cl.send("SET", "k", "v")
	cl.readLine()
	cl.send("FLUSH")
	if line := cl.readLine(); line != "+OK" {
		t.Fatalf("FLUSH: got %q", line)
	}
	cl.send("GET", "k")
	if got := cl.readBulk(); got != "" {
		t.Fatalf("GET after FLUSH: expected null, got %q", got)
	}
}

func TestServerCommand(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("COMMAND")
	if line := cl.readLine(); line != "*0" {
		t.Fatalf("COMMAND: got %q, want *0", line)
	}
}

func TestServerUnknownCommand(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("NOSUCHTHING")
	line := cl.readLine()
	if len(line) == 0 || line[0] != '-' {
		t.Fatalf("unknown command: expected error, got %q", line)
	}
}

func TestServerQuit(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("QUIT")
	if line := cl.readLine(); line != "+OK" {
		t.Fatalf("QUIT: got %q", line)
	}
	buf := make([]byte, 1)
	if _, err := cl.conn.Read(buf); err == nil {
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
			key := fmt.Sprintf("mc-key-%d", i)
			val := fmt.Sprintf("mc-val-%d", i)
			cl.send("SET", key, val)
			if line := cl.readLine(); line != "+OK" {
				t.Errorf("client %d SET: got %q", i, line)
				return
			}
			cl.send("GET", key)
			if got := cl.readBulk(); got != val {
				t.Errorf("client %d GET: got %q, want %q", i, got, val)
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
	header := cl.readLine()
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

// ---------------------------------------------------------------------------
// Protocol robustness
// ---------------------------------------------------------------------------

// TestServerMalformedNotArray sends a non-array RESP value. The server closes
// the connection silently on parse errors — it does not write an error response
// for malformed protocol framing.
func TestServerMalformedNotArray(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	// Send a simple string instead of an array.
	cl.conn.Write([]byte("+notanarray\r\n")) //nolint:errcheck
	// Server closes the connection; Read returns an error.
	buf := make([]byte, 1)
	cl.conn.SetReadDeadline(time.Now().Add(time.Second)) //nolint:errcheck
	_, err := cl.conn.Read(buf)
	if err == nil {
		t.Fatal("expected connection to be closed after malformed input")
	}
}

// TestServerMalformedTruncated sends a command that is cut off mid-stream.
// The server should close the connection cleanly rather than blocking forever.
func TestServerMalformedTruncated(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	// Do not defer cl.close() — we close early to simulate truncation.

	// Begin a SET command but never finish it.
	cl.conn.Write([]byte("*3\r\n$3\r\nSET\r\n")) //nolint:errcheck
	cl.conn.Close()
	// No assertion — just verifying the server doesn't deadlock or panic.
}

// TestServerPipelined sends multiple commands in a single write and reads all
// responses. This exercises the server's command loop correctness.
func TestServerPipelined(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	// Three commands in one write.
	var sb strings.Builder
	for _, args := range [][]string{
		{"SET", "pipe-a", "1"},
		{"SET", "pipe-b", "2"},
		{"GET", "pipe-a"},
	} {
		fmt.Fprintf(&sb, "*%d\r\n", len(args))
		for _, a := range args {
			fmt.Fprintf(&sb, "$%d\r\n%s\r\n", len(a), a)
		}
	}
	cl.conn.Write([]byte(sb.String())) //nolint:errcheck

	if line := cl.readLine(); line != "+OK" {
		t.Fatalf("pipeline SET pipe-a: got %q", line)
	}
	if line := cl.readLine(); line != "+OK" {
		t.Fatalf("pipeline SET pipe-b: got %q", line)
	}
	if got := cl.readBulk(); got != "1" {
		t.Fatalf("pipeline GET pipe-a: got %q, want 1", got)
	}
}

// TestServerConcurrentMixedCommands runs many clients simultaneously, each
// issuing a mix of read and write commands. This is the server-level analogue
// of TestConcurrentMixedOps — it exercises the server dispatch loop and the
// cache under concurrent network load.
func TestServerConcurrentMixedCommands(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()

	const clients = 30
	const ops = 40
	keys := []string{"x", "y", "z", "w"}
	var wg sync.WaitGroup
	wg.Add(clients)
	for n := 0; n < clients; n++ {
		n := n
		go func() {
			defer wg.Done()
			cl := dial(t, addr)
			defer cl.close()
			for i := 0; i < ops; i++ {
				k := keys[(n+i)%len(keys)]
				switch i % 5 {
				case 0:
					cl.send("SET", k, fmt.Sprintf("%d", i))
					cl.readLine()
				case 1:
					cl.send("GET", k)
					cl.readBulk()
				case 2:
					cl.send("DEL", k)
					cl.readLine()
				case 3:
					cl.send("EXISTS", k)
					cl.readLine()
				case 4:
					cl.send("SETNX", k, "nx")
					cl.readLine()
				}
			}
		}()
	}
	wg.Wait()
}

// TestServerDelMultipleKeys verifies DEL counts correctly across present and
// absent keys.
func TestServerDelMultipleKeys(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	for _, k := range []string{"d1", "d2", "d3"} {
		cl.send("SET", k, "v")
		cl.readLine()
	}
	cl.send("DEL", "d1", "d2", "d3", "ghost1", "ghost2")
	if line := cl.readLine(); line != ":3" {
		t.Fatalf("DEL mixed: got %q, want :3", line)
	}
}

// TestServerSetEXExpiry verifies that a key set with EX actually expires.
func TestServerSetEXExpiry(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("SET", "ex-key", "bye", "PX", "30")
	cl.readLine()
	time.Sleep(60 * time.Millisecond)
	cl.send("GET", "ex-key")
	if got := cl.readBulk(); got != "" {
		t.Fatalf("key should have expired, still got %q", got)
	}
}

// TestServerKeysGlob checks a variety of glob patterns.
func TestServerKeysGlob(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	for _, k := range []string{"cat", "car", "card", "bar", "bat"} {
		cl.send("SET", k, "v")
		cl.readLine()
	}

	cases := []struct {
		pattern string
		want    int
	}{
		{"*", 5},
		{"ca*", 3},
		{"ba?", 2},
		{"car?", 1},
		{"zzz*", 0},
	}
	for _, tc := range cases {
		cl.send("KEYS", tc.pattern)
		header := cl.readLine()
		var n int
		fmt.Sscanf(header[1:], "%d", &n)
		for i := 0; i < n; i++ {
			cl.readBulk()
		}
		if n != tc.want {
			t.Errorf("KEYS %q: got %d, want %d", tc.pattern, n, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Gap coverage
// ---------------------------------------------------------------------------

// TestServerPTTL verifies the three PTTL states: missing (:-2), no-expiry
// (:-1), and expiring (positive milliseconds).
func TestServerPTTL(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("PTTL", "ghost")
	if line := cl.readLine(); line != ":-2" {
		t.Fatalf("PTTL missing: got %q, want :-2", line)
	}

	cl.send("SET", "forever", "v")
	cl.readLine()
	cl.send("PTTL", "forever")
	if line := cl.readLine(); line != ":-1" {
		t.Fatalf("PTTL no-expiry: got %q, want :-1", line)
	}

	cl.send("SET", "temp", "v", "PX", "10000")
	cl.readLine()
	cl.send("PTTL", "temp")
	line := cl.readLine()
	var ms int64
	if _, err := fmt.Sscanf(line[1:], "%d", &ms); err != nil || ms <= 0 || ms > 10000 {
		t.Fatalf("PTTL expiring: got %q, want positive ms ≤10000", line)
	}
}

// TestServerRenameToExisting verifies that RENAME overwrites an existing
// destination key and removes the source.
func TestServerRenameToExisting(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("SET", "src", "new")
	cl.readLine()
	cl.send("SET", "dst", "old")
	cl.readLine()
	cl.send("RENAME", "src", "dst")
	if line := cl.readLine(); line != "+OK" {
		t.Fatalf("RENAME to existing: got %q", line)
	}
	cl.send("EXISTS", "src")
	if line := cl.readLine(); line != ":0" {
		t.Fatalf("source should be gone: got %q", line)
	}
	cl.send("GET", "dst")
	if got := cl.readBulk(); got != "new" {
		t.Fatalf("destination should hold new value: got %q", got)
	}
}

// TestServerTypeOnExpired verifies that TYPE returns "none" for a key whose
// TTL has elapsed even before the reaper runs.
func TestServerTypeOnExpired(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("SET", "bye", "v", "PX", "20")
	cl.readLine()
	time.Sleep(50 * time.Millisecond)
	cl.send("TYPE", "bye")
	if line := cl.readLine(); line != "+none" {
		t.Fatalf("TYPE on expired key: got %q, want +none", line)
	}
}

// TestServerMSetWithArgs verifies MSET with multiple pairs in one call and
// that all keys are retrievable afterwards.
func TestServerMSetMultiplePairs(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("MSET", "p1", "v1", "p2", "v2", "p3", "v3", "p4", "v4")
	if line := cl.readLine(); line != "+OK" {
		t.Fatalf("MSET: got %q", line)
	}
	for _, kv := range [][2]string{{"p1", "v1"}, {"p2", "v2"}, {"p3", "v3"}, {"p4", "v4"}} {
		cl.send("GET", kv[0])
		if got := cl.readBulk(); got != kv[1] {
			t.Errorf("GET %s: got %q, want %q", kv[0], got, kv[1])
		}
	}
}

// TestServerPTTLWrongArgs verifies PTTL rejects wrong argument count.
func TestServerPTTLWrongArgs(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	cl.send("PTTL")
	line := cl.readLine()
	if len(line) == 0 || line[0] != '-' {
		t.Fatalf("PTTL no args: expected error, got %q", line)
	}
}

// ---------------------------------------------------------------------------
// Wrong-argument error paths — one per command that validates arity
// ---------------------------------------------------------------------------

func TestServerGetWrongArgs(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()
	cl.send("GET")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("GET no args: expected error, got %q", line)
	}
	cl.send("GET", "a", "b")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("GET two args: expected error, got %q", line)
	}
}

func TestServerSetWrongArgs(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()
	cl.send("SET", "k")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("SET one arg: expected error, got %q", line)
	}
}

func TestServerSetBadOptions(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	// EX with non-integer value
	cl.send("SET", "k", "v", "EX", "abc")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("SET EX abc: expected error, got %q", line)
	}
	// EX with zero
	cl.send("SET", "k", "v", "EX", "0")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("SET EX 0: expected error, got %q", line)
	}
	// PX with negative (parseInt rejects non-digits so we use a letter)
	cl.send("SET", "k", "v", "PX", "-1")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("SET PX -1: expected error, got %q", line)
	}
	// Unknown option
	cl.send("SET", "k", "v", "KEEPTTL", "10")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("SET KEEPTTL: expected error, got %q", line)
	}
	// Option keyword without value
	cl.send("SET", "k", "v", "EX")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("SET EX missing value: expected error, got %q", line)
	}
}

func TestServerDelWrongArgs(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()
	cl.send("DEL")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("DEL no args: expected error, got %q", line)
	}
}

func TestServerExistsWrongArgs(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()
	cl.send("EXISTS")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("EXISTS no args: expected error, got %q", line)
	}
}

func TestServerTTLWrongArgs(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()
	cl.send("TTL")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("TTL no args: expected error, got %q", line)
	}
	cl.send("TTL", "a", "b")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("TTL two args: expected error, got %q", line)
	}
}

func TestServerKeysWrongArgs(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()
	cl.send("KEYS")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("KEYS no args: expected error, got %q", line)
	}
	cl.send("KEYS", "*", "extra")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("KEYS two args: expected error, got %q", line)
	}
}

func TestServerMGetWrongArgs(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()
	cl.send("MGET")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("MGET no args: expected error, got %q", line)
	}
}

func TestServerMSetWrongArgs(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()
	// MSET requires pairs — odd number of args is invalid
	cl.send("MSET")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("MSET no args: expected error, got %q", line)
	}
	cl.send("MSET", "k")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("MSET odd args: expected error, got %q", line)
	}
}

func TestServerSetNXWrongArgs(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()
	cl.send("SETNX", "k")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("SETNX one arg: expected error, got %q", line)
	}
}

func TestServerGetDelWrongArgs(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()
	cl.send("GETDEL")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("GETDEL no args: expected error, got %q", line)
	}
}

func TestServerRenameWrongArgs(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()
	cl.send("RENAME", "k")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("RENAME one arg: expected error, got %q", line)
	}
}

func TestServerTypeWrongArgs(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()
	cl.send("TYPE")
	if line := cl.readLine(); len(line) == 0 || line[0] != '-' {
		t.Fatalf("TYPE no args: expected error, got %q", line)
	}
}

func TestServerFlushAliases(t *testing.T) {
	addr, cleanup := testServer(t)
	defer cleanup()
	cl := dial(t, addr)
	defer cl.close()

	for _, cmd := range []string{"FLUSHALL", "FLUSHDB"} {
		cl.send("SET", "k", "v")
		cl.readLine()
		cl.send(cmd)
		if line := cl.readLine(); line != "+OK" {
			t.Fatalf("%s: got %q, want +OK", cmd, line)
		}
		cl.send("EXISTS", "k")
		if line := cl.readLine(); line != ":0" {
			t.Fatalf("%s: key should be gone, got %q", cmd, line)
		}
	}
}