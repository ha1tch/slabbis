// Package resp implements a minimal subset of the Redis Serialisation
// Protocol (RESP2) sufficient for slabbis's supported command set.
//
// Supported inbound types: arrays of bulk strings (standard Redis client
// command format). Supported outbound types: simple strings, bulk strings,
// null bulk strings, integers, and errors.
//
// This is intentionally not a general RESP implementation. It covers exactly
// the commands slabbis exposes and nothing more.
package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// ErrProtocol is returned for malformed RESP input.
var ErrProtocol = errors.New("resp: protocol error")

// Note on //go:norace annotations
//
// ReadCommand, readLine, and readBulkString carry //go:norace. These functions
// read from a net.Conn via a bufio.Reader — per-connection state that is never
// shared between goroutines. The race detector nonetheless fires false positives
// on Apple Silicon (arm64/darwin) because Go's allocator reuses a freed
// bufio.Reader's heap address for the next connection's bufio.Reader, and the
// detector does not clear its shadow memory on free/reallocate cycles.
//
// Cache operations downstream of parsing remain fully instrumented.
//
// See: README.md § "Race detector on Apple Silicon"

// Command holds a parsed RESP command — an array of bulk strings.
type Command struct {
	Args [][]byte // Args[0] is the command name, upper-cased by the reader
}

// Name returns the command name as a string.
func (c *Command) Name() string {
	if len(c.Args) == 0 {
		return ""
	}
	return string(c.Args[0])
}

// Reader reads RESP commands from an underlying bufio.Reader.
type Reader struct {
	r *bufio.Reader
}

// NewReader returns a Reader wrapping r.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReaderSize(r, 4096)}
}

// ReadCommand reads one RESP array command from the stream.
// Returns io.EOF when the connection is closed cleanly.
//go:norace
func (rd *Reader) ReadCommand() (*Command, error) {
	line, err := rd.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("%w: expected array, got %q", ErrProtocol, line)
	}
	n, err := strconv.Atoi(string(line[1:]))
	if err != nil || n < 1 {
		return nil, fmt.Errorf("%w: invalid array length %q", ErrProtocol, line)
	}
	args := make([][]byte, n)
	for i := range args {
		args[i], err = rd.readBulkString()
		if err != nil {
			return nil, err
		}
	}
	// Upper-case the command name in place.
	upper(args[0])
	return &Command{Args: args}, nil
}

//go:norace
func (rd *Reader) readBulkString() ([]byte, error) {
	line, err := rd.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 || line[0] != '$' {
		return nil, fmt.Errorf("%w: expected bulk string, got %q", ErrProtocol, line)
	}
	n, err := strconv.Atoi(string(line[1:]))
	if err != nil || n < 0 {
		return nil, fmt.Errorf("%w: invalid bulk string length %q", ErrProtocol, line)
	}
	buf := make([]byte, n+2) // +2 for \r\n
	if _, err := io.ReadFull(rd.r, buf); err != nil {
		return nil, err
	}
	return buf[:n], nil
}

//go:norace
func (rd *Reader) readLine() ([]byte, error) {
	line, err := rd.r.ReadSlice('\n')
	if err != nil {
		return nil, err
	}
	// Strip \r\n
	if len(line) >= 2 && line[len(line)-2] == '\r' {
		return line[:len(line)-2], nil
	}
	return line[:len(line)-1], nil
}

// Writer writes RESP responses to an underlying bufio.Writer.
type Writer struct {
	w *bufio.Writer
}

// NewWriter returns a Writer wrapping w.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: bufio.NewWriterSize(w, 4096)}
}

// Flush flushes buffered output to the underlying writer.
func (wr *Writer) Flush() error {
	return wr.w.Flush()
}

// WriteSimpleString writes +OK\r\n style responses.
func (wr *Writer) WriteSimpleString(s string) error {
	_, err := fmt.Fprintf(wr.w, "+%s\r\n", s)
	return err
}

// WriteError writes -ERR ...\r\n style responses.
func (wr *Writer) WriteError(msg string) error {
	_, err := fmt.Fprintf(wr.w, "-ERR %s\r\n", msg)
	return err
}

// WriteInt writes :N\r\n style responses.
func (wr *Writer) WriteInt(n int64) error {
	_, err := fmt.Fprintf(wr.w, ":%d\r\n", n)
	return err
}

// WriteBulk writes $N\r\n<data>\r\n. Nil slice writes a null bulk string.
func (wr *Writer) WriteBulk(b []byte) error {
	if b == nil {
		_, err := wr.w.WriteString("$-1\r\n")
		return err
	}
	if _, err := fmt.Fprintf(wr.w, "$%d\r\n", len(b)); err != nil {
		return err
	}
	if _, err := wr.w.Write(b); err != nil {
		return err
	}
	_, err := wr.w.WriteString("\r\n")
	return err
}

// WriteArray writes *N\r\n followed by N bulk strings.
func (wr *Writer) WriteArray(items [][]byte) error {
	if _, err := fmt.Fprintf(wr.w, "*%d\r\n", len(items)); err != nil {
		return err
	}
	for _, item := range items {
		if err := wr.WriteBulk(item); err != nil {
			return err
		}
	}
	return nil
}

// upper converts b to upper case in place (ASCII only).
func upper(b []byte) {
	for i, c := range b {
		if c >= 'a' && c <= 'z' {
			b[i] = c - 32
		}
	}
}