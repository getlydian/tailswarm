package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types/swarm"
	"github.com/getlydian/tailswarm/internal/tailswarm"
)

type fakeRunDocker struct {
	mu       sync.Mutex
	services map[string]swarm.Service
}

func (f *fakeRunDocker) ListServices(_ context.Context, filter tailswarm.LabelFilter) ([]swarm.Service, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []swarm.Service{}
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
		out = append(out, s)
	}
	return out, nil
}
func (f *fakeRunDocker) InspectService(_ context.Context, id string) (swarm.Service, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.services[id]
	if !ok {
		return swarm.Service{}, tailswarm.ErrServiceNotFound
	}
	return s, nil
}
func (f *fakeRunDocker) ListNetworks(_ context.Context) ([]swarm.Network, error) {
	return nil, nil
}

type silentEvents struct{}

func (silentEvents) Subscribe(ctx context.Context) (<-chan tailswarm.Event, error) {
	ch := make(chan tailswarm.Event)
	go func() { <-ctx.Done(); close(ch) }()
	return ch, nil
}

type stubController struct {
	mu      sync.Mutex
	created int
	expired int
}

func (s *stubController) CreateEphemeralKey(_ context.Context, _ tailswarm.KeyRequest) (tailswarm.Key, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.created++
	return tailswarm.Key{ID: "k-" + strconv.Itoa(s.created), Secret: "x", ExpiresAt: time.Now().Add(time.Minute)}, nil
}
func (s *stubController) ExpireKey(_ context.Context, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expired++
	return nil
}

// failingProxyFactory is used in run()'s integration test where no
// services are present, so Reconcile never calls it. If it ever does,
// the failure makes the test loud rather than silent.
func failingProxyFactory(_ context.Context, _ tailswarm.ProxyConfig, _ *slog.Logger) (*tailswarm.Proxy, error) {
	return nil, errors.New("proxy factory should not be called in this integration test")
}

func TestRunBootsAndShutsDown(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "tailswarm.yml")
	cfgBody := `headscale:
  url: https://hs.example
  user: swarm
  key_expiration: 1m
reconcile:
  full_resync_interval: 1h
  rate_limit_rps: 5
tsnet:
  state_dir: ` + filepath.Join(dir, "state") + `
network: tailswarm-overlay
`
	if err := os.WriteFile(cfgPath, []byte(cfgBody), 0o600); err != nil {
		t.Fatal(err)
	}

	docker := &fakeRunDocker{services: map[string]swarm.Service{}}
	started := make(chan struct{}, 1)
	deps := &runDeps{
		Docker:     docker,
		Events:     silentEvents{},
		Controller: &stubController{},
		NewProxy:   failingProxyFactory,
		Started:    started,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	envMap := map[string]string{"TAILSWARM_HEADSCALE_API_KEY": "x"}
	env := func(k string) string { return envMap[k] }

	var out, errBuf bytes.Buffer
	done := make(chan error, 1)
	go func() { done <- run(ctx, []string{"-config", cfgPath}, env, &out, &errBuf, deps) }()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("run did not signal Started")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("run returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run did not return after cancel")
	}
}
