package web

import (
	"bufio"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type socketWriter struct {
	http.ResponseWriter
	conn net.Conn
}

func (w socketWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.conn, bufio.NewReadWriter(bufio.NewReader(w.conn), bufio.NewWriter(w.conn)), nil
}

func TestCaptureWriterOptionalCapabilities(t *testing.T) {
	plain := &captureWriter{ResponseWriter: httptest.NewRecorder(), max: 5}
	if _, _, err := plain.Hijack(); !errors.Is(err, http.ErrNotSupported) {
		t.Fatalf("unsupported hijack: %v", err)
	}
	if err := plain.Push("/x", nil); !errors.Is(err, http.ErrNotSupported) {
		t.Fatalf("unsupported push: %v", err)
	}
	if plain.Unwrap() == nil {
		t.Fatal("missing unwrap")
	}
	if n, err := plain.ReadFrom(strings.NewReader("abcdef")); err != nil || n != 6 || plain.total != 6 || plain.body.String() != "abcde" {
		t.Fatalf("captured n=%d err=%v total=%d body=%q", n, err, plain.total, plain.body.String())
	}
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	socket := &captureWriter{ResponseWriter: socketWriter{ResponseWriter: httptest.NewRecorder(), conn: left}}
	conn, rw, err := socket.Hijack()
	if err != nil || conn != left || rw == nil || socket.status != http.StatusSwitchingProtocols {
		t.Fatalf("hijack conn=%v err=%v status=%d", conn, err, socket.status)
	}
}
