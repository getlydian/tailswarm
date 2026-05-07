package tailswarm

import (
	"sync"
	"time"
)

// Entry is the per-service bookkeeping the reconciler keeps between
// ticks: enough state to detect a no-op (LastSpecHash), tear down the
// right artefacts on label removal (SidecarID, HeadscaleNodeID), and
// expire the previous key when rotating (PreAuthKeyID).
//
// Per DESIGN.md §5.1 the authoritative source is always Docker +
// Headscale; this struct is a cache rebuilt on startup by the
// reconciler's resync path.
type Entry struct {
	SidecarID        string
	LastSpecHash     string
	PreAuthKeyID     string
	HeadscaleNodeID  string
	LastReconcileAt  time.Time
}

// Store is a concurrency-safe map keyed by Swarm service ID. It is
// intentionally just storage: no persistence, no eviction, no
// invalidation policy. The reconciler owns all interpretation of what
// the entries mean and is responsible for rebuilding the store on
// startup from live Docker + Headscale state.
type Store struct {
	mu      sync.RWMutex
	entries map[string]*Entry
}

// NewStore returns an empty Store ready for concurrent use.
func NewStore() *Store {
	return &Store{entries: make(map[string]*Entry)}
}

// Get returns a copy of the entry for serviceID, or the zero Entry and
// false if none exists. Returning a copy keeps callers from mutating
// the stored value through a shared pointer.
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
func (s *Store) Put(serviceID string, e Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := e
	s.entries[serviceID] = &cp
}

// Delete removes the entry for serviceID. It is a no-op when no entry
// exists, so the reconciler's teardown path can call it
// unconditionally.
func (s *Store) Delete(serviceID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, serviceID)
}

// Snapshot returns a copy of every entry keyed by service ID. The
// returned map and its values are decoupled from the store, so callers
// may iterate without holding the lock.
func (s *Store) Snapshot() map[string]Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]Entry, len(s.entries))
	for k, v := range s.entries {
		out[k] = *v
	}
	return out
}

// Keys returns the set of service IDs currently in the store. Used by
// the reconciler's resync path to walk known entries.
func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.entries))
	for k := range s.entries {
		out = append(out, k)
	}
	return out
}
