package tailswarm

import (
	"sync"
	"time"
)

// GatewayRef tracks one egress gateway Docker service. An egressing app
// gets one gateway per target (each target needs its own Swarm service, so
// its own VIP/task IP — multiple same-port targets behind one container IP
// would be indistinguishable at L4 and would collide on bind). Target is
// the target's "host:port" addr, the stable key that survives reconciles;
// ServiceID is the Swarm service ID (to update or remove it); KeyID is the
// ephemeral preauth key minted under the app's egress.tag (to expire it);
// Hash is the per-gateway spec hash (gatewayHash) so a surviving gateway
// whose image/tag/network drifted is updated in place rather than left
// stale — notably across a daemon image upgrade.
type GatewayRef struct {
	Target    string
	ServiceID string
	KeyID     string
	Hash      string
}

// Entry is the per-service bookkeeping the reconciler keeps between
// ticks: the live tsnet proxy, the spec hash that produced it, and the
// preauth key ID so we can expire it on rotation or teardown.
//
// For ingress there is no Docker-side artefact to track — the proxy *is*
// the artefact, and lives entirely in this process. Egress is different:
// each gateway is a managed Docker service, so its fields (below) record
// the external artefacts we must update and tear down.
type Entry struct {
	Proxy           *Proxy
	LastSpecHash    string
	PreAuthKeyID    string
	LastReconcileAt time.Time

	// Egress gateway bookkeeping. Unlike the ingress Proxy (which lives in
	// this process), each gateway is a managed Docker service. Gateways holds
	// one GatewayRef per egress target; GatewayHash is the hash over the whole
	// desired target set (so we can detect drift and fast-path no-ops). Both
	// are zero/nil for a pure-ingress service.
	Gateways    []GatewayRef
	GatewayHash string
}

// Store is a concurrency-safe map keyed by Swarm service ID. It owns
// the live Proxy pointers, but it is intentionally just storage — the
// reconciler is responsible for Close()ing proxies before removing
// their entry.
type Store struct {
	mu      sync.RWMutex
	entries map[string]*Entry
}

func NewStore() *Store {
	return &Store{entries: make(map[string]*Entry)}
}

// Get returns a copy of the entry for serviceID, or the zero Entry and
// false if none exists. The returned Entry shares the *Proxy pointer
// with the store; callers must not Close it without going through the
// reconciler.
func (s *Store) Get(serviceID string) (Entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.entries[serviceID]
	if !ok {
		return Entry{}, false
	}
	return *e, true
}

// Put writes the entry for serviceID, replacing any previous value.
// The caller is responsible for closing any displaced proxy.
func (s *Store) Put(serviceID string, e Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := e
	s.entries[serviceID] = &cp
}

// Delete removes the entry for serviceID without closing its proxy.
// The reconciler closes first, then deletes.
func (s *Store) Delete(serviceID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, serviceID)
}

// Snapshot returns a copy of every entry keyed by service ID.
func (s *Store) Snapshot() map[string]Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]Entry, len(s.entries))
	for k, v := range s.entries {
		out[k] = *v
	}
	return out
}

func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.entries))
	for k := range s.entries {
		out = append(out, k)
	}
	return out
}
