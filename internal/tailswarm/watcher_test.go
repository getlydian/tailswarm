package tailswarm

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/docker/docker/api/types/swarm"
)

// fakeEventStream is a programmable EventStream. Each Subscribe call
// pulls the next scripted result off subs; tests use this to inject
// events, force errors, or close the stream to exercise the reconnect
// path. Safe for concurrent Subscribe/push from the test goroutine.
type fakeEventStream struct {
	mu sync.Mutex

	// subs is the queue of scripted Subscribe outcomes. Each call to
	// Subscribe pops the front; if the queue is empty, Subscribe
	// returns errSubExhausted so a buggy test that loops forever will
	// fail loudly instead of hanging.
	subs []subResult

	// subscribeCount records how many times Subscribe has been called;
	// reconnect tests assert this grows past 1.
	subscribeCount atomic.Int32
}

type subResult struct {
	// If err is non-nil, Subscribe returns it.
	err error
	// Otherwise Subscribe returns ch. The test can push events on push
	// (which is the same chan, exposed under a send-friendly type).
	ch   chan Event
	push chan<- Event
}

var errSubExhausted = errors.New("fakeEventStream: no more scripted Subscribe results")

func newFakeEventStream() *fakeEventStream {
	return &fakeEventStream{}
}

// scriptChannel queues up a Subscribe result that returns a fresh
// channel; the returned send-side is what the test pushes events on,
// and closing it simulates the stream ending.
func (f *fakeEventStream) scriptChannel(buffer int) chan<- Event {
	ch := make(chan Event, buffer)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.subs = append(f.subs, subResult{ch: ch, push: ch})
	return ch
}

// scriptError queues up a Subscribe result that returns an error
// without ever yielding a channel. Used to exercise the backoff path.
func (f *fakeEventStream) scriptError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.subs = append(f.subs, subResult{err: err})
}

func (f *fakeEventStream) Subscribe(_ context.Context) (<-chan Event, error) {
	f.subscribeCount.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.subs) == 0 {
		return nil, errSubExhausted
	}
	res := f.subs[0]
	f.subs = f.subs[1:]
	if res.err != nil {
		return nil, res.err
	}
	return res.ch, nil
}

// quietLogger discards watcher log output so test runs stay clean.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// drainStrings reads up to n strings from ch with the given timeout per
// receive. Returns however many it got; the caller asserts on the
// shape.
func drainStrings(t *testing.T, ch <-chan string, n int, timeout time.Duration) []string {
	t.Helper()
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		select {
		case s := <-ch:
			out = append(out, s)
		case <-time.After(timeout):
			return out
		}
	}
	return out
}

// makeEnabledService builds a swarm.Service the fake docker can return
// from ListServices when filtered by tailswarm.enable=true.
func makeEnabledService(id string) swarm.Service {
	return swarm.Service{
		ID: id,
		Spec: swarm.ServiceSpec{
			Annotations: swarm.Annotations{
				Labels: map[string]string{"tailswarm.enable": "true"},
			},
		},
	}
}

func TestWatcherEventsFlowToOut(t *testing.T) {
	docker := newFakeDocker()
	stream := newFakeEventStream()
	push := stream.scriptChannel(4)

	out := make(chan string, 8)
	w := &Watcher{
		Docker:     docker,
		Events:     stream,
		Out:        out,
		FullResync: time.Hour, // disable the slow path for this test
		Log:        quietLogger(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	push <- Event{ServiceID: "svc-A", Action: "create"}
	push <- Event{ServiceID: "svc-B", Action: "update"}
	push <- Event{ServiceID: "", Action: "noise"} // empty ID is dropped
	push <- Event{ServiceID: "svc-C", Action: "remove"}

	got := drainStrings(t, out, 3, 500*time.Millisecond)
	want := []string{"svc-A", "svc-B", "svc-C"}
	if !equalSlices(got, want) {
		t.Fatalf("event IDs mismatch: got %v want %v", got, want)
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled", err)
	}
}

func TestWatcherTickerProducesIDsWithoutEvents(t *testing.T) {
	docker := newFakeDocker()
	docker.addService(makeEnabledService("tick-1"))
	docker.addService(makeEnabledService("tick-2"))
	// A non-enabled service should not be enqueued.
	docker.addService(swarm.Service{
		ID: "ignored",
		Spec: swarm.ServiceSpec{
			Annotations: swarm.Annotations{Labels: map[string]string{"other": "label"}},
		},
	})

	stream := newFakeEventStream()
	stream.scriptChannel(1) // live but silent

	out := make(chan string, 16)
	w := &Watcher{
		Docker:     docker,
		Events:     stream,
		Out:        out,
		FullResync: 25 * time.Millisecond,
		Log:        quietLogger(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// First fullList runs immediately on Run start; that's two IDs.
	first := drainStrings(t, out, 2, 200*time.Millisecond)
	if !sameSet(first, []string{"tick-1", "tick-2"}) {
		t.Fatalf("first tick IDs mismatch: got %v", first)
	}

	// At least one more tick should fire before the deadline; we expect
	// the same two IDs again.
	second := drainStrings(t, out, 2, 200*time.Millisecond)
	if !sameSet(second, []string{"tick-1", "tick-2"}) {
		t.Fatalf("second tick IDs mismatch: got %v", second)
	}

	// "ignored" must never appear because the LabelFilter is enforced
	// by the fake.
	cancel()
	<-done
	for _, c := range docker.callLog() {
		if c.Kind == dCallList && c.Filter.Key != "tailswarm.enable" {
			t.Fatalf("expected enable filter, got %+v", c.Filter)
		}
	}
}

func TestWatcherResubscribesAfterStreamError(t *testing.T) {
	docker := newFakeDocker()
	stream := newFakeEventStream()

	// Initial Subscribe fails; watcher should back off and try again.
	stream.scriptError(errors.New("boom"))
	push := stream.scriptChannel(2)

	out := make(chan string, 4)
	w := &Watcher{
		Docker:           docker,
		Events:           stream,
		Out:              out,
		FullResync:       time.Hour,
		ReconnectBackoff: 10 * time.Millisecond,
		Log:              quietLogger(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// Wait for the second Subscribe to succeed, then send an event and
	// confirm Run is still alive and forwarding.
	if !waitFor(func() bool { return stream.subscribeCount.Load() >= 2 }, 500*time.Millisecond) {
		t.Fatalf("watcher did not resubscribe; count=%d", stream.subscribeCount.Load())
	}

	push <- Event{ServiceID: "after-reconnect"}
	got := drainStrings(t, out, 1, 200*time.Millisecond)
	if len(got) != 1 || got[0] != "after-reconnect" {
		t.Fatalf("post-reconnect event not delivered: %v", got)
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled", err)
	}
}

func TestWatcherResubscribesAfterStreamClose(t *testing.T) {
	docker := newFakeDocker()
	stream := newFakeEventStream()

	// First subscription gets closed mid-flight; second one delivers.
	first := stream.scriptChannel(1)
	second := stream.scriptChannel(1)

	out := make(chan string, 4)
	w := &Watcher{
		Docker:           docker,
		Events:           stream,
		Out:              out,
		FullResync:       time.Hour,
		ReconnectBackoff: 10 * time.Millisecond,
		Log:              quietLogger(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// Deliver one event on the first stream, then close it to simulate
	// the docker daemon dropping the connection.
	first <- Event{ServiceID: "before-drop"}
	close(first)

	// Watcher should resubscribe (count >= 2) and keep delivering.
	if !waitFor(func() bool { return stream.subscribeCount.Load() >= 2 }, 500*time.Millisecond) {
		t.Fatalf("watcher did not resubscribe after close; count=%d",
			stream.subscribeCount.Load())
	}
	second <- Event{ServiceID: "after-drop"}

	got := drainStrings(t, out, 2, 300*time.Millisecond)
	if !equalSlices(got, []string{"before-drop", "after-drop"}) {
		t.Fatalf("post-close delivery mismatch: got %v", got)
	}

	cancel()
	<-done
}

func TestWatcherFullListErrorDoesNotExitRun(t *testing.T) {
	docker := newFakeDocker()
	docker.errList = errors.New("list boom")

	stream := newFakeEventStream()
	push := stream.scriptChannel(2)

	out := make(chan string, 4)
	w := &Watcher{
		Docker:     docker,
		Events:     stream,
		Out:        out,
		FullResync: 20 * time.Millisecond,
		Log:        quietLogger(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// First fullList fails (one-shot error), but subsequent calls and
	// events should still flow. Push an event to confirm liveness.
	push <- Event{ServiceID: "alive"}
	got := drainStrings(t, out, 1, 300*time.Millisecond)
	if len(got) == 0 || got[0] != "alive" {
		t.Fatalf("watcher did not deliver event after list error: %v", got)
	}

	cancel()
	<-done
}

func TestWatcherRunRequiresFields(t *testing.T) {
	cases := map[string]Watcher{
		"no Out":    {Docker: newFakeDocker(), Events: newFakeEventStream()},
		"no Docker": {Events: newFakeEventStream(), Out: make(chan<- string)},
		"no Events": {Docker: newFakeDocker(), Out: make(chan<- string)},
	}
	for name, w := range cases {
		w := w
		t.Run(name, func(t *testing.T) {
			err := w.Run(context.Background())
			if err == nil {
				t.Fatal("expected error for missing field, got nil")
			}
		})
	}
}

func TestNextBackoffCaps(t *testing.T) {
	d := defaultReconnectBackoff
	for i := 0; i < 100; i++ {
		d = nextBackoff(d)
	}
	if d != maxReconnectBackoff {
		t.Fatalf("backoff did not saturate: %v", d)
	}
	if got := nextBackoff(0); got != defaultReconnectBackoff {
		t.Fatalf("zero backoff should reset to default; got %v", got)
	}
}

// --- helpers ---

func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sameSet(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	m := map[string]int{}
	for _, s := range got {
		m[s]++
	}
	for _, s := range want {
		if m[s] == 0 {
			return false
		}
		m[s]--
	}
	return true
}

func waitFor(cond func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}
