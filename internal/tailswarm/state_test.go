package tailswarm

import (
	"testing"
	"time"
)

func TestStorePutGetDelete(t *testing.T) {
	s := NewStore()
	if _, ok := s.Get("missing"); ok {
		t.Fatal("expected miss")
	}
	now := time.Now()
	s.Put("svc", Entry{LastSpecHash: "h", LastReconcileAt: now})
	got, ok := s.Get("svc")
	if !ok || got.LastSpecHash != "h" {
		t.Fatalf("get: %+v ok=%v", got, ok)
	}
	s.Delete("svc")
	if _, ok := s.Get("svc"); ok {
		t.Fatal("expected delete")
	}
}

func TestStoreSnapshotIsDecoupled(t *testing.T) {
	s := NewStore()
	s.Put("a", Entry{LastSpecHash: "1"})
	snap := s.Snapshot()
	snap["a"] = Entry{LastSpecHash: "mutated"}
	got, _ := s.Get("a")
	if got.LastSpecHash != "1" {
		t.Fatalf("snapshot leaked back into store: %+v", got)
	}
}
