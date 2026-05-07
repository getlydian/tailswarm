package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/docker/docker/api/types/swarm"

	"github.com/getlydian/tailswarm/internal/tailswarm"
)

// TestRun_SmokeCreatesSidecar wires run() with fake Docker and Headscale,
// drops a labeled service into the fake Docker, fires a service event,
// and asserts that a sidecar was created and a key minted. This is the
// end-to-end happy path: watcher → queue → reconciler → controller.
func TestRun_SmokeCreatesSidecar(t *testing.T) {
	t.Parallel()

	cfgPath := writeConfig(t, `
headscale:
  url: http://headscale.test
  user: swarm
sidecar:
  image: tailscale/tailscale:v1.78
reconcile:
  full_resync_interval: 10s
  rate_limit_rps: 100
`)

	fakeDoc := newFakeDocker()
	fakeDoc.addNetwork(swarm.Network{
		ID: "net-1",
		Spec: swarm.NetworkSpec{
			Annotations: swarm.Annotations{Name: "app-overlay"},
		},
	})
	target := swarm.Service{
		ID: "target-1",
		Spec: swarm.ServiceSpec{
			Annotations: swarm.Annotations{
				Name: "billing",
				Labels: map[string]string{
					"tailswarm.enable":  "true",
					"tailswarm.network": "app-overlay",
				},
			},
			TaskTemplate: swarm.TaskSpec{
				Networks: []swarm.NetworkAttachmentConfig{{Target: "net-1"}},
			},
		},
		Meta: swarm.Meta{Version: swarm.Version{Index: 1}},
	}
	fakeDoc.addService(target)

	fakeEvents := newFakeEventStream()
	fakeCtrl := newFakeController()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	started := make(chan struct{})
	deps := &runDeps{
		Docker:     fakeDoc,
		Events:     fakeEvents,
		Controller: fakeCtrl,
		Started:    started,
	}

	runCtx, runCancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() {
		var stdout, stderr bytes.Buffer
		errCh <- run(runCtx, []string{"-config", cfgPath}, func(string) string { return "" }, &stdout, &stderr, deps)
	}()

	// Wait for run() to finish startup wiring.
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatalf("run never reached steady state: %v", ctx.Err())
	}

	// Push a service event for the target. The watcher's first fullList
	// already enqueues it, but firing an event exercises the
	// EventStream → queue path explicitly.
	fakeEvents.send(tailswarm.Event{ServiceID: "target-1", Action: "create"})

	// Poll until a sidecar shows up. The reconciler is async, so we
	// can't observe synchronously.
	if !waitFor(2*time.Second, func() bool {
		return fakeDoc.sidecarCount() == 1 && fakeCtrl.createdCount() == 1
	}) {
		t.Fatalf("sidecar=%d keys=%d after wait; want 1/1", fakeDoc.sidecarCount(), fakeCtrl.createdCount())
	}

	runCancel()
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("run returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("run did not exit after cancel")
	}
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "tailswarm.yml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func waitFor(d time.Duration, pred func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if pred() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return pred()
}

// --- Fakes -----------------------------------------------------------
//
// The reconciler/watcher tests under internal/tailswarm have their own
// in-package fakes; those aren't exported, so this file ships its own
// minimal pair just for the cmd-level smoke test.

type fakeDocker struct {
	mu       sync.Mutex
	services map[string]*swarm.Service
	networks []swarm.Network
	idSeq    atomic.Uint64
}

func newFakeDocker() *fakeDocker {
	return &fakeDocker{services: map[string]*swarm.Service{}}
}

func (f *fakeDocker) addService(svc swarm.Service) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := svc
	f.services[svc.ID] = &cp
}

func (f *fakeDocker) addNetwork(n swarm.Network) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.networks = append(f.networks, n)
}

func (f *fakeDocker) sidecarCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, s := range f.services {
		if s.Spec.Annotations.Labels["tailswarm.managed"] == "true" {
			n++
		}
	}
	return n
}

func (f *fakeDocker) ListServices(_ context.Context, filter tailswarm.LabelFilter) ([]swarm.Service, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]swarm.Service, 0, len(f.services))
	for _, s := range f.services {
		if filter.Key != "" {
			v, ok := s.Spec.Annotations.Labels[filter.Key]
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
	s, ok := f.services[serviceID]
	if !ok {
		return swarm.Service{}, tailswarm.ErrServiceNotFound
	}
	return *s, nil
}

func (f *fakeDocker) CreateService(_ context.Context, spec tailswarm.SidecarSpec) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := "sidecar-" + strconv.FormatUint(f.idSeq.Add(1), 10)
	envSlice := make([]string, 0, len(spec.Env))
	for k, v := range spec.Env {
		envSlice = append(envSlice, k+"="+v)
	}
	f.services[id] = &swarm.Service{
		ID:   id,
		Meta: swarm.Meta{Version: swarm.Version{Index: 1}},
		Spec: swarm.ServiceSpec{
			Annotations: swarm.Annotations{Name: spec.Name, Labels: spec.Labels},
			TaskTemplate: swarm.TaskSpec{
				ContainerSpec: &swarm.ContainerSpec{
					Image:    spec.Image,
					Hostname: spec.Hostname,
					Env:      envSlice,
				},
			},
		},
	}
	return id, nil
}

func (f *fakeDocker) UpdateService(_ context.Context, serviceID string, version uint64, spec tailswarm.SidecarSpec) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cur, ok := f.services[serviceID]
	if !ok {
		return errors.New("unknown service")
	}
	cur.Version.Index = version + 1
	cur.Spec.Annotations.Labels = spec.Labels
	return nil
}

func (f *fakeDocker) RemoveService(_ context.Context, serviceID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.services, serviceID)
	return nil
}

func (f *fakeDocker) ListNetworks(_ context.Context) ([]swarm.Network, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]swarm.Network, len(f.networks))
	copy(out, f.networks)
	return out, nil
}

type fakeEventStream struct {
	mu     sync.Mutex
	subs   []chan tailswarm.Event
	closed bool
}

func newFakeEventStream() *fakeEventStream { return &fakeEventStream{} }

func (e *fakeEventStream) Subscribe(ctx context.Context) (<-chan tailswarm.Event, error) {
	ch := make(chan tailswarm.Event, 16)
	e.mu.Lock()
	e.subs = append(e.subs, ch)
	e.mu.Unlock()
	go func() {
		<-ctx.Done()
		e.mu.Lock()
		defer e.mu.Unlock()
		// Drop the subscriber; tests don't recreate them.
		for i, c := range e.subs {
			if c == ch {
				e.subs = append(e.subs[:i], e.subs[i+1:]...)
				break
			}
		}
	}()
	return ch, nil
}

func (e *fakeEventStream) send(ev tailswarm.Event) {
	e.mu.Lock()
	subs := append([]chan tailswarm.Event(nil), e.subs...)
	e.mu.Unlock()
	for _, ch := range subs {
		select {
		case ch <- ev:
		default:
		}
	}
}

type fakeController struct {
	mu      sync.Mutex
	keys    map[string]tailswarm.Key
	nodes   map[string]tailswarm.Node
	created atomic.Uint64
	keySeq  atomic.Uint64
}

func newFakeController() *fakeController {
	return &fakeController{
		keys:  map[string]tailswarm.Key{},
		nodes: map[string]tailswarm.Node{},
	}
}

func (f *fakeController) createdCount() int { return int(f.created.Load()) }

func (f *fakeController) CreateEphemeralKey(_ context.Context, req tailswarm.KeyRequest) (tailswarm.Key, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := "key-" + strconv.FormatUint(f.keySeq.Add(1), 10)
	k := tailswarm.Key{ID: id, Secret: "secret-" + id, ExpiresAt: time.Now().Add(req.Expiration)}
	f.keys[id] = k
	f.created.Add(1)
	return k, nil
}

func (f *fakeController) ExpireKey(_ context.Context, keyID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.keys, keyID)
	return nil
}

func (f *fakeController) DeleteNode(_ context.Context, nodeID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.nodes, nodeID)
	return nil
}

func (f *fakeController) ListNodes(_ context.Context, _ string) ([]tailswarm.Node, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]tailswarm.Node, 0, len(f.nodes))
	for _, n := range f.nodes {
		out = append(out, n)
	}
	return out, nil
}
