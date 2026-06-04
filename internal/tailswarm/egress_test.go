package tailswarm

import (
	"context"
	"io"
	"net"
	"testing"
	"time"
)

// recordingListen wraps listenLoopback to capture the listeners
// startEgressProxyWith opens, so the test can dial them.
func recordingListen() (func(EgressTarget) (net.Listener, error), func() []net.Listener) {
	var lns []net.Listener
	listen := func(t EgressTarget) (net.Listener, error) {
		ln, err := listenLoopback(t)
		if err != nil {
			return nil, err
		}
		lns = append(lns, ln)
		return ln, nil
	}
	return listen, func() []net.Listener { return lns }
}

func TestEgressForwardRoundTrip(t *testing.T) {
	upstream := startEcho(t)

	srv := &fakeTsnetDialer{upstream: upstream}
	cfg := EgressConfig{
		Hostname: "gw-tester",
		Targets:  []EgressTarget{{Host: "db-mysql", Port: 3306}},
		StateDir: t.TempDir(),
	}

	listen, listeners := recordingListen()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, err := startEgressProxyWith(ctx, srv, cfg, listen, nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = p.Close() }()

	lns := listeners()
	if len(lns) != 1 {
		t.Fatalf("listeners: %d want 1", len(lns))
	}

	conn, err := net.Dial("tcp", lns[0].Addr().String())
	if err != nil {
		t.Fatalf("dial overlay listener: %v", err)
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

	// The gateway must have dialed the *real* MagicDNS target name over
	// the tailnet, not the loopback upstream the fake actually connects
	// to. This is the property the alias model depends on.
	if got := srv.dialedAddrs(); len(got) != 1 || got[0] != "db-mysql:3306" {
		t.Fatalf("dialed %v want [db-mysql:3306]", got)
	}
}

func TestEgressCloseStopsListeners(t *testing.T) {
	srv := &fakeTsnetDialer{upstream: "127.0.0.1:1"}
	cfg := EgressConfig{
		Hostname: "gw-tester",
		Targets:  []EgressTarget{{Host: "db-mysql", Port: 3306}},
		StateDir: t.TempDir(),
	}

	listen, listeners := recordingListen()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, err := startEgressProxyWith(ctx, srv, cfg, listen, nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	addr := listeners()[0].Addr().String()
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if !srv.closed {
		t.Fatal("Close did not close the tsnet server")
	}
	// The overlay listener must be closed after Close — dials should fail.
	if c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond); err == nil {
		_ = c.Close()
		t.Fatal("expected dial to fail after Close")
	}
}

func TestEgressMultipleTargets(t *testing.T) {
	upstream := startEcho(t)
	srv := &fakeTsnetDialer{upstream: upstream}
	cfg := EgressConfig{
		Hostname: "gw-tester",
		Targets: []EgressTarget{
			{Host: "db-mysql", Port: 3306},
			{Host: "analytics-mysql", Port: 3306},
		},
		StateDir: t.TempDir(),
	}

	listen, listeners := recordingListen()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, err := startEgressProxyWith(ctx, srv, cfg, listen, nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = p.Close() }()

	if got := len(listeners()); got != 2 {
		t.Fatalf("listeners: %d want 2", got)
	}
}
