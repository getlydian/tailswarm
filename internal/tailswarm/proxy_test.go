package tailswarm

import (
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// startEcho stands up a TCP echo server on 127.0.0.1 and returns its
// host:port. It runs until the test finishes.
func startEcho(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				_, _ = io.Copy(c, c)
			}(c)
		}
	}()
	return ln.Addr().String()
}

func TestForwardTCPRoundTrip(t *testing.T) {
	upstream := startEcho(t)

	srv := &fakeTsnetServer{}
	cfg := ProxyConfig{
		Hostname: "tester",
		Target:   strings.Split(upstream, ":")[0],
		Ports:    []Port{{Target: 0}},
		StateDir: t.TempDir(),
	}
	// We need the proxy to dial upstream, not "127.0.0.1:0" — override
	// the target after Listen by pointing forwardTCP directly.
	// Easier: use the upstream port as the listen port.
	parts := strings.Split(upstream, ":")
	cfg.Target = parts[0]
	cfg.Ports = []Port{{Target: portFromAddr(t, upstream)}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, err := startProxyOn(ctx, srv, cfg, nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = p.Close() }()

	// Dial the loopback listener that the fake tsnet handed us.
	addrs := srv.addrs()
	if len(addrs) != 1 {
		t.Fatalf("listeners: %d", len(addrs))
	}
	conn, err := net.Dial("tcp", addrs[0].String())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != "ping" {
		t.Fatalf("got %q want ping", buf)
	}
}

func TestProxyCloseStopsListeners(t *testing.T) {
	srv := &fakeTsnetServer{}
	cfg := ProxyConfig{
		Hostname: "tester",
		Target:   "127.0.0.1",
		Ports:    []Port{{Target: 1}},
		StateDir: t.TempDir(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, err := startProxyOn(ctx, srv, cfg, nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Subsequent dials to the closed listener should fail.
	addrs := srv.addrs()
	if _, err := net.DialTimeout("tcp", addrs[0].String(), 100*time.Millisecond); err == nil {
		t.Fatal("expected dial to fail after Close")
	}
}

func portFromAddr(t *testing.T, addr string) uint32 {
	t.Helper()
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	tcpAddr, err := net.ResolveTCPAddr("tcp", net.JoinHostPort("127.0.0.1", portStr))
	if err != nil {
		t.Fatal(err)
	}
	return uint32(tcpAddr.Port)
}
