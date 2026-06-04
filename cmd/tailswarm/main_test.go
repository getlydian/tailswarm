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
	tasks    []swarm.Task
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

func (f *fakeRunDocker) ListTasks(_ context.Context) ([]swarm.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tasks, nil
}

// seedOwnTask wires the fake so DiscoverGatewayImage at startup finds the
// daemon's own task (matched by container ID == this host's name) and
// resolves it to a service carrying the given image. Egress gateway image
// discovery is fatal, so run() needs this to boot.
func (f *fakeRunDocker) seedOwnTask(t *testing.T, image string) {
	t.Helper()
	host, err := os.Hostname()
	if err != nil {
		t.Fatalf("hostname: %v", err)
	}
	const svcID = "tailswarm-self"
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tasks = append(f.tasks, swarm.Task{
		ServiceID: svcID,
		Status:    swarm.TaskStatus{ContainerStatus: &swarm.ContainerStatus{ContainerID: host}},
	})
	f.services[svcID] = swarm.Service{
		ID: svcID,
		Spec: swarm.ServiceSpec{
			TaskTemplate: swarm.TaskSpec{
				ContainerSpec: &swarm.ContainerSpec{Image: image},
			},
		},
	}
}

func (f *fakeRunDocker) CreateService(_ context.Context, _ swarm.ServiceSpec) (string, error) {
	return "", nil
}

func (f *fakeRunDocker) UpdateService(_ context.Context, _ string, _ uint64, _ swarm.ServiceSpec) error {
	return nil
}

func (f *fakeRunDocker) RemoveService(_ context.Context, _ string) error {
	return nil
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
	docker.seedOwnTask(t, "ghcr.io/getlydian/tailswarm:test")
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

// Egress gateway image discovery is fatal: if the daemon can't find its
// own Swarm task at startup (no matching task — e.g. socket proxy missing
// the SWARM/NODES/TASKS read scopes), run() must return an error rather
// than boot with egress silently disabled.
func TestRunFailsWhenGatewayImageUndiscovered(t *testing.T) {
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

	// No tasks seeded → discovery finds no matching own-task.
	docker := &fakeRunDocker{services: map[string]swarm.Service{}}
	deps := &runDeps{
		Docker:     docker,
		Events:     silentEvents{},
		Controller: &stubController{},
		NewProxy:   failingProxyFactory,
		Started:    make(chan struct{}, 1),
	}

	env := func(k string) string {
		return map[string]string{"TAILSWARM_HEADSCALE_API_KEY": "x"}[k]
	}
	var out, errBuf bytes.Buffer
	err := run(context.Background(), []string{"-config", cfgPath}, env, &out, &errBuf, deps)
	if err == nil {
		t.Fatal("expected run to fail when gateway image is undiscovered, got nil")
	}
}
