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
