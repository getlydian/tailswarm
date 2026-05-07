package tailswarm

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/docker/docker/api/types/swarm"
)

// fakeDocker is an in-memory DockerClient used by reconciler tests.
// It records every mutating call and supports per-method error
// injection. Safe for concurrent use.
type fakeDocker struct {
	mu sync.Mutex

	services map[string]*swarm.Service
	networks []swarm.Network

	idSeq atomic.Uint64

	// Per-call error injection. Each is one-shot: set, fires once,
	// clears. This matches fakeController's ergonomics so tests can
	// freely mix the two.
	errInspect error
	errCreate  error
	errUpdate  error
	errRemove  error
	errList    error
	errNets    error

	// missing IDs pretend to not exist on Inspect (returns
	// ErrServiceNotFound). Useful for "target removed" cases.
	missing map[string]struct{}

	calls []dockerCall
}

type dockerCallKind int

const (
	dCallList dockerCallKind = iota
	dCallInspect
	dCallCreate
	dCallUpdate
	dCallRemove
	dCallListNets
)

type dockerCall struct {
	Kind      dockerCallKind
	Filter    LabelFilter
	ServiceID string
	Version   uint64
	Spec      SidecarSpec
}

func newFakeDocker() *fakeDocker {
	return &fakeDocker{
		services: map[string]*swarm.Service{},
		missing:  map[string]struct{}{},
	}
}

// addService inserts a target service so the reconciler can inspect it.
// The returned ID is what tests pass to Reconcile.
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

// addNetwork adds a network usable by ListNetworks.
func (f *fakeDocker) addNetwork(n swarm.Network) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.networks = append(f.networks, n)
}

// markMissing makes Inspect for the given ID return ErrServiceNotFound,
// simulating a deleted target without disturbing other state.
func (f *fakeDocker) markMissing(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.missing[id] = struct{}{}
	delete(f.services, id)
}

func (f *fakeDocker) callLog() []dockerCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]dockerCall, len(f.calls))
	copy(out, f.calls)
	return out
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

func (f *fakeDocker) CreateService(_ context.Context, spec SidecarSpec) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, dockerCall{Kind: dCallCreate, Spec: spec})

	if err := f.errCreate; err != nil {
		f.errCreate = nil
		return "", err
	}

	id := "sidecar-" + strconv.FormatUint(f.idSeq.Add(1), 10)
	f.services[id] = sidecarToService(id, spec, 1)
	return id, nil
}

func (f *fakeDocker) UpdateService(_ context.Context, serviceID string, version uint64, spec SidecarSpec) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, dockerCall{Kind: dCallUpdate, ServiceID: serviceID, Version: version, Spec: spec})

	if err := f.errUpdate; err != nil {
		f.errUpdate = nil
		return err
	}

	cur, ok := f.services[serviceID]
	if !ok {
		return fmt.Errorf("fakeDocker: unknown service %q", serviceID)
	}
	if cur.Version.Index != version {
		return fmt.Errorf("fakeDocker: version mismatch (have %d, got %d)",
			cur.Version.Index, version)
	}
	f.services[serviceID] = sidecarToService(serviceID, spec, cur.Version.Index+1)
	return nil
}

func (f *fakeDocker) RemoveService(_ context.Context, serviceID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, dockerCall{Kind: dCallRemove, ServiceID: serviceID})

	if err := f.errRemove; err != nil {
		f.errRemove = nil
		return err
	}
	if _, ok := f.services[serviceID]; !ok {
		return fmt.Errorf("fakeDocker: unknown service %q", serviceID)
	}
	delete(f.services, serviceID)
	return nil
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

// sidecarToService converts our SidecarSpec back into a swarm.Service so
// InspectService on the sidecar's own ID returns something sensible
// (used by the version lookup before UpdateService).
func sidecarToService(id string, spec SidecarSpec, version uint64) *swarm.Service {
	envSlice := make([]string, 0, len(spec.Env))
	for k, v := range spec.Env {
		envSlice = append(envSlice, k+"="+v)
	}
	return &swarm.Service{
		ID: id,
		Meta: swarm.Meta{
			Version: swarm.Version{Index: version},
		},
		Spec: swarm.ServiceSpec{
			Annotations: swarm.Annotations{
				Name:   spec.Name,
				Labels: spec.Labels,
			},
			TaskTemplate: swarm.TaskSpec{
				ContainerSpec: &swarm.ContainerSpec{
					Image:    spec.Image,
					Hostname: spec.Hostname,
					Env:      envSlice,
				},
			},
		},
	}
}

// Compile-time check that the fake satisfies the interface.
var _ DockerClient = (*fakeDocker)(nil)

// Sentinel error reused across reconciler tests.
var errInjected = errors.New("injected fakeDocker error")
