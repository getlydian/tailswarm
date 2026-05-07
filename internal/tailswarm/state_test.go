package tailswarm

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestStore_PutGetDelete(t *testing.T) {
	t.Parallel()

	s := NewStore()

	if _, ok := s.Get("missing"); ok {
		t.Fatalf("Get on empty store: ok = true, want false")
	}

	now := time.Now()
	s.Put("svc-1", Entry{
		SidecarID:       "sc-1",
		LastSpecHash:    "hash-1",
		PreAuthKeyID:    "key-1",
		HeadscaleNodeID: "node-1",
		LastReconcileAt: now,
	})

	got, ok := s.Get("svc-1")
	if !ok {
		t.Fatalf("Get(svc-1): ok = false, want true")
	}
	if got.SidecarID != "sc-1" || got.LastSpecHash != "hash-1" ||
		got.PreAuthKeyID != "key-1" || got.HeadscaleNodeID != "node-1" ||
		!got.LastReconcileAt.Equal(now) {
		t.Fatalf("Get(svc-1) = %+v, want round-trip of put value", got)
	}

	s.Delete("svc-1")
	if _, ok := s.Get("svc-1"); ok {
		t.Fatalf("Get after Delete: ok = true, want false")
	}
}

func TestStore_DeleteIdempotent(t *testing.T) {
	t.Parallel()

	s := NewStore()
	s.Delete("never-existed")
	s.Put("svc-1", Entry{SidecarID: "sc-1"})
	s.Delete("svc-1")
	s.Delete("svc-1")

	if _, ok := s.Get("svc-1"); ok {
		t.Fatalf("Get after double Delete: ok = true, want false")
	}
}

func TestStore_GetReturnsCopy(t *testing.T) {
	t.Parallel()

	s := NewStore()
	s.Put("svc-1", Entry{SidecarID: "sc-1"})

	got, _ := s.Get("svc-1")
	got.SidecarID = "mutated"

	again, _ := s.Get("svc-1")
	if again.SidecarID != "sc-1" {
		t.Fatalf("Get returned a live pointer; mutation leaked: %+v", again)
	}
}

func TestStore_PutOverwrites(t *testing.T) {
	t.Parallel()

	s := NewStore()
	s.Put("svc-1", Entry{SidecarID: "sc-1", LastSpecHash: "h1"})
	s.Put("svc-1", Entry{SidecarID: "sc-2", LastSpecHash: "h2"})

	got, ok := s.Get("svc-1")
	if !ok {
		t.Fatalf("Get(svc-1): ok = false, want true")
	}
	if got.SidecarID != "sc-2" || got.LastSpecHash != "h2" {
		t.Fatalf("Get(svc-1) = %+v, want second Put to win", got)
	}
}

func TestStore_SnapshotIsDecoupled(t *testing.T) {
	t.Parallel()

	s := NewStore()
	s.Put("svc-1", Entry{SidecarID: "sc-1"})
	s.Put("svc-2", Entry{SidecarID: "sc-2"})

	snap := s.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("Snapshot len = %d, want 2", len(snap))
	}

	// Mutating the snapshot must not affect the store.
	snap["svc-1"] = Entry{SidecarID: "mutated"}
	delete(snap, "svc-2")

	got1, _ := s.Get("svc-1")
	if got1.SidecarID != "sc-1" {
		t.Fatalf("Snapshot mutation leaked into store: svc-1 = %+v", got1)
	}
	if _, ok := s.Get("svc-2"); !ok {
		t.Fatalf("Snapshot delete leaked into store: svc-2 missing")
	}
}

func TestStore_Keys(t *testing.T) {
	t.Parallel()

	s := NewStore()
	if got := s.Keys(); len(got) != 0 {
		t.Fatalf("Keys on empty store = %v, want empty", got)
	}

	s.Put("svc-1", Entry{})
	s.Put("svc-2", Entry{})
	s.Put("svc-3", Entry{})

	keys := s.Keys()
	if len(keys) != 3 {
		t.Fatalf("Keys len = %d, want 3", len(keys))
	}

	want := map[string]bool{"svc-1": true, "svc-2": true, "svc-3": true}
	for _, k := range keys {
		if !want[k] {
			t.Fatalf("Keys returned unexpected %q", k)
		}
		delete(want, k)
	}
	if len(want) != 0 {
		t.Fatalf("Keys missing entries: %v", want)
	}
}

// TestStore_Concurrent hammers the store from many goroutines doing
// Put/Get/Snapshot/Delete in parallel. With -race this catches missing
// locking around the map and around the *Entry pointers Snapshot
// dereferences.
func TestStore_Concurrent(t *testing.T) {
	t.Parallel()

	s := NewStore()

	const writers = 16
	const readers = 16
	const opsPerGoroutine = 500

	var wg sync.WaitGroup
	wg.Add(writers + readers)

	for w := 0; w < writers; w++ {
		w := w
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := fmt.Sprintf("svc-%d-%d", w, i%8)
				s.Put(key, Entry{
					SidecarID:       fmt.Sprintf("sc-%d-%d", w, i),
					LastSpecHash:    fmt.Sprintf("h-%d-%d", w, i),
					LastReconcileAt: time.Now(),
				})
				if i%4 == 0 {
					s.Delete(key)
				}
			}
		}()
	}

	for r := 0; r < readers; r++ {
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				_, _ = s.Get(fmt.Sprintf("svc-%d-%d", i%writers, i%8))
				if i%16 == 0 {
					snap := s.Snapshot()
					for _, e := range snap {
						_ = e.SidecarID
					}
					_ = s.Keys()
				}
			}
		}()
	}

	wg.Wait()
}
