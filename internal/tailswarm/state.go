package tailswarm

import (
	"sync"
	"time"
)

// Entry is the per-service bookkeeping the reconciler keeps between
// ticks: the live tsnet proxy, the spec hash that produced it, and the
// preauth key ID so we can expire it on rotation or teardown.
//
// Unlike the sidecar design there is no Docker-side artefact to track
// — the proxy *is* the artefact, and lives entirely in this process.
type Entry struct {
	Proxy           *Proxy
	LastSpecHash    string
	PreAuthKeyID    string
	LastReconcileAt time.Time
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
