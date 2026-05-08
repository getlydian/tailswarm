package tailswarm

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/docker/docker/api/types/swarm"
)

// fakeDocker is the in-memory DockerClient used by reconciler tests. It
// records every call and supports per-method error injection.
type fakeDocker struct {
	mu sync.Mutex

	services map[string]*swarm.Service
	networks []swarm.Network

	idSeq atomic.Uint64

	errInspect error
	errList    error
	errNets    error

	missing map[string]struct{}

	calls []dockerCall
}

type dockerCallKind int

const (
	dCallList dockerCallKind = iota
	dCallInspect
	dCallListNets
)

type dockerCall struct {
	Kind      dockerCallKind
	Filter    LabelFilter
	ServiceID string
}

func newFakeDocker() *fakeDocker {
	return &fakeDocker{
		services: map[string]*swarm.Service{},
		missing:  map[string]struct{}{},
	}
}

func (f *fakeDocker) addService(svc swarm.Service) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if svc.ID == "" {
		svc.ID = "svc-" + strconv.FormatUint(f.idSeq.Add(1), 10)
	}
	if svc.Version.Index == 0 {
		svc.Version.Index = 1
	}
	cp := svc
	f.services[svc.ID] = &cp
	return svc.ID
}

func (f *fakeDocker) markMissing(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.missing[id] = struct{}{}
	delete(f.services, id)
}

func (f *fakeDocker) ListServices(_ context.Context, filter LabelFilter) ([]swarm.Service, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, dockerCall{Kind: dCallList, Filter: filter})

	if err := f.errList; err != nil {
		f.errList = nil
		return nil, err
	}

	out := make([]swarm.Service, 0, len(f.services))
	for _, s := range f.services {
		if filter.Key != "" {
			v, ok := s.Spec.Labels[filter.Key]
			if !ok {
				continue
			}
			if filter.Value != "" && v != filter.Value {
				continue
			}
		}
		out = append(out, *s)
	}
	return out, nil
}

func (f *fakeDocker) InspectService(_ context.Context, serviceID string) (swarm.Service, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, dockerCall{Kind: dCallInspect, ServiceID: serviceID})

	if err := f.errInspect; err != nil {
		f.errInspect = nil
		return swarm.Service{}, err
	}

	if _, gone := f.missing[serviceID]; gone {
		return swarm.Service{}, ErrServiceNotFound
	}
	s, ok := f.services[serviceID]
	if !ok {
		return swarm.Service{}, ErrServiceNotFound
	}
	return *s, nil
}

func (f *fakeDocker) ListNetworks(_ context.Context) ([]swarm.Network, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, dockerCall{Kind: dCallListNets})

	if err := f.errNets; err != nil {
		f.errNets = nil
		return nil, err
	}
	out := make([]swarm.Network, len(f.networks))
	copy(out, f.networks)
	return out, nil
}

var _ DockerClient = (*fakeDocker)(nil)

var errInjected = errors.New("injected fake error")

// fakeController records every CreateEphemeralKey/ExpireKey call and
// hands out predictable key IDs so tests can match them up.
type fakeController struct {
	mu sync.Mutex

	idSeq atomic.Uint64

	created []KeyRequest
	expired []string

	errCreate error
	errExpire error
}

func newFakeController() *fakeController { return &fakeController{} }

func (c *fakeController) CreateEphemeralKey(_ context.Context, req KeyRequest) (Key, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.created = append(c.created, req)
	if err := c.errCreate; err != nil {
		c.errCreate = nil
		return Key{}, err
	}
	id := "key-" + strconv.FormatUint(c.idSeq.Add(1), 10)
	return Key{ID: id, Secret: "secret-" + id, ExpiresAt: time.Now().Add(req.Expiration)}, nil
}

func (c *fakeController) ExpireKey(_ context.Context, keyID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.expired = append(c.expired, keyID)
	if err := c.errExpire; err != nil {
		c.errExpire = nil
		return err
	}
	return nil
}

var _ Controller = (*fakeController)(nil)

// fakeTsnetServer is a loopback stand-in for tsnet.Server. Its Listen
// returns real net.Listeners on 127.0.0.1, so forwardTCP can be exercised
// without bringing up a real tailnet.
type fakeTsnetServer struct {
	mu        sync.Mutex
	listeners []net.Listener
	closed    bool
}

func (s *fakeTsnetServer) Listen(network, addr string) (net.Listener, error) {
	if network != "tcp" {
		return nil, fmt.Errorf("fakeTsnetServer: unsupported network %q", network)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.listeners = append(s.listeners, ln)
	s.mu.Unlock()
	return ln, nil
}

func (s *fakeTsnetServer) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	for _, ln := range s.listeners {
		_ = ln.Close()
	}
	return nil
}

func (s *fakeTsnetServer) addrs() []net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]net.Addr, len(s.listeners))
	for i, ln := range s.listeners {
		out[i] = ln.Addr()
	}
	return out
}

// fakeProxy is what the fake ProxyFactory returns. It wraps a
// fakeTsnetServer and the real startProxyOn so the reconciler exercises
// the same Listen/forward plumbing it would in production.
type fakeProxyHandle struct {
	proxy  *Proxy
	server *fakeTsnetServer
	cfg    ProxyConfig
}

func (h *fakeProxyHandle) closed() bool {
	h.server.mu.Lock()
	defer h.server.mu.Unlock()
	return h.server.closed
}

// newFakeProxyFactory returns a ProxyFactory whose history every test
// can inspect, plus a hook to inject a startup error on the next call.
type fakeProxyFactory struct {
	mu sync.Mutex

	handles  []*fakeProxyHandle
	errStart error
}

func (f *fakeProxyFactory) factory() ProxyFactory {
	return func(ctx context.Context, cfg ProxyConfig, log *slog.Logger) (*Proxy, error) {
		f.mu.Lock()
		if err := f.errStart; err != nil {
			f.errStart = nil
			f.mu.Unlock()
			return nil, err
		}
		f.mu.Unlock()

		srv := &fakeTsnetServer{}
		p, err := startProxyOn(ctx, srv, cfg, log)
		if err != nil {
			return nil, err
		}
		h := &fakeProxyHandle{proxy: p, server: srv, cfg: cfg}
		f.mu.Lock()
		f.handles = append(f.handles, h)
		f.mu.Unlock()
		return p, nil
	}
}

func (f *fakeProxyFactory) snapshot() []*fakeProxyHandle {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*fakeProxyHandle, len(f.handles))
	copy(out, f.handles)
	return out
}
