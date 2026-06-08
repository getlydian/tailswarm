package tailswarm

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types/swarm"
)

type fakeEvents struct {
	mu sync.Mutex
	ch chan Event
}

func newFakeEvents() *fakeEvents { return &fakeEvents{ch: make(chan Event, 8)} }

func (f *fakeEvents) Subscribe(ctx context.Context) (<-chan Event, error) {
	return f.ch, nil
}

func (f *fakeEvents) push(e Event) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ch <- e
}

func TestWatcherFullListEnqueuesEnabled(t *testing.T) {
	d := newFakeDocker()
	d.addService(swarm.Service{
		ID: "svc-on",
		Spec: swarm.ServiceSpec{
			Annotations: swarm.Annotations{Labels: map[string]string{"tailswarm.enable": "true"}},
		},
	})
	d.addService(swarm.Service{
		ID: "svc-off",
		Spec: swarm.ServiceSpec{
			Annotations: swarm.Annotations{Labels: map[string]string{}},
		},
	})

	out := make(chan string, 4)
	w := &Watcher{
		Docker:         d,
		Events:         newFakeEvents(),
		Out:            out,
		FullResync:     50 * time.Millisecond,
		LabelNamespace: defaultNamespace,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() { defer close(done); _ = w.Run(ctx) }()

	got := map[string]int{}
loop:
	for {
		select {
		case id := <-out:
			got[id]++
		case <-ctx.Done():
			break loop
		}
	}
	<-done
	if got["svc-on"] == 0 {
		t.Errorf("expected svc-on to be enqueued at least once: %+v", got)
	}
	if got["svc-off"] != 0 {
		t.Errorf("svc-off enqueued: %+v", got)
	}
}

// An egress-only service sets tailswarm.egress.enable=true but no plain
// tailswarm.enable. The resync must still enqueue it: its only other path
// to a reconcile is a one-shot Docker event, so if that event-driven
// reconcile failed (e.g. its Headscale user didn't exist yet), resync is
// the sole retry — and an ingress-only filter would never re-list it.
func TestWatcherFullListEnqueuesEgressOnly(t *testing.T) {
	d := newFakeDocker()
	d.addService(swarm.Service{
		ID: "svc-egress-only",
		Spec: swarm.ServiceSpec{
			Annotations: swarm.Annotations{Labels: map[string]string{"tailswarm.egress.enable": "true"}},
		},
	})
	d.addService(swarm.Service{
		ID: "svc-both",
		Spec: swarm.ServiceSpec{
			Annotations: swarm.Annotations{Labels: map[string]string{
				"tailswarm.enable":        "true",
				"tailswarm.egress.enable": "true",
			}},
		},
	})
	d.addService(swarm.Service{
		ID: "svc-off",
		Spec: swarm.ServiceSpec{
			Annotations: swarm.Annotations{Labels: map[string]string{}},
		},
	})

	out := make(chan string, 8)
	w := &Watcher{
		Docker:         d,
		Events:         newFakeEvents(),
		Out:            out,
		FullResync:     50 * time.Millisecond,
		LabelNamespace: defaultNamespace,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() { defer close(done); _ = w.Run(ctx) }()

	got := map[string]int{}
loop:
	for {
		select {
		case id := <-out:
			got[id]++
		case <-ctx.Done():
			break loop
		}
	}
	<-done
	if got["svc-egress-only"] == 0 {
		t.Errorf("expected egress-only service to be enqueued at least once: %+v", got)
	}
	if got["svc-off"] != 0 {
		t.Errorf("svc-off enqueued: %+v", got)
	}
	// A service opting into BOTH must be enqueued once per resync, not
	// twice — fullList unions the two label lists by ID.
	if n := got["svc-both"]; n > 0 {
		// One full resync fires at startup plus possibly more within the
		// window; assert it never double-counts within a single tick by
		// checking the count tracks the egress-only service (both appear in
		// every resync). The window can end mid-tick — after one of the two
		// is drained from the channel but before the other — so allow the
		// counts to differ by one; a per-tick dupe would diverge by the
		// number of ticks, not by one.
		if diff := n - got["svc-egress-only"]; diff < -1 || diff > 1 {
			t.Errorf("svc-both enqueued %d times, expected within 1 of egress-only %d (no per-tick dupes): %+v",
				n, got["svc-egress-only"], got)
		}
	}
}

func TestWatcherForwardsEvents(t *testing.T) {
	d := newFakeDocker()
	ev := newFakeEvents()
	out := make(chan string, 4)

	w := &Watcher{
		Docker:         d,
		Events:         ev,
		Out:            out,
		FullResync:     time.Hour, // disable slow path for this test
		LabelNamespace: defaultNamespace,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	ev.push(Event{ServiceID: "abc", Action: "update"})

	select {
	case id := <-out:
		if id != "abc" {
			t.Errorf("got %q want abc", id)
		}
	case <-time.After(time.Second):
		t.Fatal("event not delivered")
	}
}
