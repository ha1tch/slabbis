package resp_test

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/ha1tch/slabbis/internal/resp"
)

// ---- Reader -----------------------------------------------------------------

func TestReadCommandBasic(t *testing.T) {
	// *2\r\n$3\r\nGET\r\n$5\r\nhello\r\n
	input := "*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n"
	rd := resp.NewReader(strings.NewReader(input))
	cmd, err := rd.ReadCommand()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cmd.Name() != "GET" {
		t.Fatalf("expected GET, got %q", cmd.Name())
	}
	if len(cmd.Args) != 2 {
		t.Fatalf("expected 2 args, got %d", len(cmd.Args))
	}
	if string(cmd.Args[1]) != "hello" {
		t.Fatalf("expected %q, got %q", "hello", cmd.Args[1])
	}
}

func TestReadCommandLowercaseNormalised(t *testing.T) {
	input := "*1\r\n$4\r\nping\r\n"
	rd := resp.NewReader(strings.NewReader(input))
	cmd, err := rd.ReadCommand()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cmd.Name() != "PING" {
		t.Fatalf("expected PING after normalisation, got %q", cmd.Name())
	}
}

func TestReadCommandMixedCase(t *testing.T) {
	input := "*1\r\n$4\r\nQuit\r\n"
	rd := resp.NewReader(strings.NewReader(input))
	cmd, err := rd.ReadCommand()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cmd.Name() != "QUIT" {
		t.Fatalf("expected QUIT, got %q", cmd.Name())
	}
}

func TestReadCommandSetWithTTL(t *testing.T) {
	// SET mykey myvalue EX 30
	input := "*5\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n$2\r\nEX\r\n$2\r\n30\r\n"
	rd := resp.NewReader(strings.NewReader(input))
	cmd, err := rd.ReadCommand()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cmd.Name() != "SET" {
		t.Fatalf("expected SET, got %q", cmd.Name())
	}
	if len(cmd.Args) != 5 {
		t.Fatalf("expected 5 args, got %d", len(cmd.Args))
	}
	if string(cmd.Args[4]) != "30" {
		t.Fatalf("expected TTL %q, got %q", "30", cmd.Args[4])
	}
}

func TestReadCommandBinaryValue(t *testing.T) {
	// Bulk string containing null bytes and \r\n.
	payload := []byte{0x00, 0xFF, 0x0D, 0x0A}
	var buf bytes.Buffer
	buf.WriteString("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n")
	buf.WriteString("$4\r\n")
	buf.Write(payload)
	buf.WriteString("\r\n")

	rd := resp.NewReader(&buf)
	cmd, err := rd.ReadCommand()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(cmd.Args[2], payload) {
		t.Fatalf("binary roundtrip failed: got %x want %x", cmd.Args[2], payload)
	}
}

func TestReadCommandEOF(t *testing.T) {
	rd := resp.NewReader(strings.NewReader(""))
	_, err := rd.ReadCommand()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

func TestReadCommandMalformedNotArray(t *testing.T) {
	rd := resp.NewReader(strings.NewReader("+OK\r\n"))
	_, err := rd.ReadCommand()
	if !errors.Is(err, resp.ErrProtocol) {
		t.Fatalf("expected ErrProtocol, got %v", err)
	}
}

func TestReadCommandMalformedBadLength(t *testing.T) {
	rd := resp.NewReader(strings.NewReader("*abc\r\n"))
	_, err := rd.ReadCommand()
	if !errors.Is(err, resp.ErrProtocol) {
		t.Fatalf("expected ErrProtocol, got %v", err)
	}
}

func TestReadCommandMalformedBadBulkHeader(t *testing.T) {
	// Array says 1 element but element is not a bulk string.
	rd := resp.NewReader(strings.NewReader("*1\r\n+notbulk\r\n"))
	_, err := rd.ReadCommand()
	if !errors.Is(err, resp.ErrProtocol) {
		t.Fatalf("expected ErrProtocol, got %v", err)
	}
}

func TestReadMultipleCommands(t *testing.T) {
	input := "*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n"
	rd := resp.NewReader(strings.NewReader(input))
	for i := 0; i < 2; i++ {
		cmd, err := rd.ReadCommand()
		if err != nil {
			t.Fatalf("command %d: unexpected error: %v", i, err)
		}
		if cmd.Name() != "PING" {
			t.Fatalf("command %d: expected PING, got %q", i, cmd.Name())
		}
	}
	_, err := rd.ReadCommand()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF after two commands, got %v", err)
	}
}

// ---- Writer -----------------------------------------------------------------

func TestWriteSimpleString(t *testing.T) {
	var buf bytes.Buffer
	wr := resp.NewWriter(&buf)
	_ = wr.WriteSimpleString("OK")
	_ = wr.Flush()
	if buf.String() != "+OK\r\n" {
		t.Fatalf("got %q", buf.String())
	}
}

func TestWriteError(t *testing.T) {
	var buf bytes.Buffer
	wr := resp.NewWriter(&buf)
	_ = wr.WriteError("something went wrong")
	_ = wr.Flush()
	if buf.String() != "-ERR something went wrong\r\n" {
		t.Fatalf("got %q", buf.String())
	}
}

func TestWriteInt(t *testing.T) {
	var buf bytes.Buffer
	wr := resp.NewWriter(&buf)
	_ = wr.WriteInt(42)
	_ = wr.Flush()
	if buf.String() != ":42\r\n" {
		t.Fatalf("got %q", buf.String())
	}
}

func TestWriteIntNegative(t *testing.T) {
	var buf bytes.Buffer
	wr := resp.NewWriter(&buf)
	_ = wr.WriteInt(-2)
	_ = wr.Flush()
	if buf.String() != ":-2\r\n" {
		t.Fatalf("got %q", buf.String())
	}
}

func TestWriteBulk(t *testing.T) {
	var buf bytes.Buffer
	wr := resp.NewWriter(&buf)
	_ = wr.WriteBulk([]byte("hello"))
	_ = wr.Flush()
	if buf.String() != "$5\r\nhello\r\n" {
		t.Fatalf("got %q", buf.String())
	}
}

func TestWriteBulkNull(t *testing.T) {
	var buf bytes.Buffer
	wr := resp.NewWriter(&buf)
	_ = wr.WriteBulk(nil)
	_ = wr.Flush()
	if buf.String() != "$-1\r\n" {
		t.Fatalf("got %q", buf.String())
	}
}

func TestWriteBulkEmpty(t *testing.T) {
	var buf bytes.Buffer
	wr := resp.NewWriter(&buf)
	_ = wr.WriteBulk([]byte{})
	_ = wr.Flush()
	if buf.String() != "$0\r\n\r\n" {
		t.Fatalf("got %q", buf.String())
	}
}

func TestWriteArray(t *testing.T) {
	var buf bytes.Buffer
	wr := resp.NewWriter(&buf)
	_ = wr.WriteArray([][]byte{[]byte("foo"), []byte("bar")})
	_ = wr.Flush()
	want := "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
	if buf.String() != want {
		t.Fatalf("got %q want %q", buf.String(), want)
	}
}

func TestWriteArrayEmpty(t *testing.T) {
	var buf bytes.Buffer
	wr := resp.NewWriter(&buf)
	_ = wr.WriteArray(nil)
	_ = wr.Flush()
	if buf.String() != "*0\r\n" {
		t.Fatalf("got %q", buf.String())
	}
}

// ---- roundtrip --------------------------------------------------------------

func TestRoundtrip(t *testing.T) {
	// Write a SET command as a client would, then read it back as a server would.
	var buf bytes.Buffer
	wr := resp.NewWriter(&buf)
	// Manually write a RESP array: SET foo bar
	_ = wr.WriteArray([][]byte{[]byte("SET"), []byte("foo"), []byte("bar")})
	_ = wr.Flush()

	rd := resp.NewReader(&buf)
	cmd, err := rd.ReadCommand()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// WriteArray writes bulk strings, ReadCommand upper-cases Args[0].
	if cmd.Name() != "SET" {
		t.Fatalf("expected SET, got %q", cmd.Name())
	}
	if string(cmd.Args[1]) != "foo" || string(cmd.Args[2]) != "bar" {
		t.Fatalf("unexpected args: %q %q", cmd.Args[1], cmd.Args[2])
	}
}
