package tailswarm

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types/swarm"
)

// blockingStarter is a tsnetStarter whose Start never returns until
// Close is called — the real-world failure mode where Headscale accepts
// the connection but never completes registration.
type blockingStarter struct {
	release   chan struct{}
	closeOnce sync.Once
	closed    chan struct{}
}

func newBlockingStarter() *blockingStarter {
	return &blockingStarter{
		release: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

func (b *blockingStarter) Start() error {
	<-b.release
	return errors.New("aborted")
}

func (b *blockingStarter) Close() error {
	b.closeOnce.Do(func() {
		close(b.release)
		close(b.closed)
	})
	return nil
}

// A tsnet bring-up that never completes registration must fail on a
// deadline rather than parking the caller forever. This is the bug that
// wedged every reconcile worker in production.
func TestStartTsnetBoundedTimesOut(t *testing.T) {
	srv := newBlockingStarter()

	done := make(chan error, 1)
	go func() {
		done <- startTsnetBounded(context.Background(), srv, "wedged", 50*time.Millisecond, nil)
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("startTsnetBounded returned nil, want timeout error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("startTsnetBounded blocked past its timeout")
	}

	// The abandoned Start must be unblocked via Close so its goroutine exits.
	select {
	case <-srv.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("timed-out bring-up did not Close the server")
	}
}

// Cancelling the context must abort a pending bring-up too, so shutdown
// isn't held up by a registration that will never finish.
func TestStartTsnetBoundedHonoursContext(t *testing.T) {
	srv := newBlockingStarter()
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- startTsnetBounded(ctx, srv, "wedged", time.Hour, nil)
	}()

	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("startTsnetBounded ignored context cancellation")
	}
}

// A successful Start must pass straight through, unchanged.
type okStarter struct{}

func (okStarter) Start() error { return nil }
func (okStarter) Close() error { return nil }

func TestStartTsnetBoundedPassesThrough(t *testing.T) {
	if err := startTsnetBounded(context.Background(), okStarter{}, "fine", time.Second, nil); err != nil {
		t.Fatalf("startTsnetBounded: %v", err)
	}
}

// A full shard must refuse the send instead of blocking the caller, and
// must not leave the refused ID marked pending — a stuck pending entry
// would make dedupe suppress every later attempt for that service.
func TestQueueEnqueueDoesNotBlockWhenFull(t *testing.T) {
	q := NewQueue(1, 1)

	if !q.Enqueue("first") {
		t.Fatal("first enqueue refused, want accepted")
	}

	done := make(chan bool, 1)
	go func() { done <- q.Enqueue("second") }()

	select {
	case ok := <-done:
		if ok {
			t.Fatal("enqueue onto a full shard reported success")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Enqueue blocked on a full shard")
	}

	if got := q.pendingCount(); got != 1 {
		t.Fatalf("pendingCount = %d, want 1 (refused ID must not stay pending)", got)
	}
}

// A refused ID must be enqueueable again once the shard drains, proving
// the drop is a delay rather than a permanent loss.
func TestQueueEnqueueRetriableAfterDrop(t *testing.T) {
	q := NewQueue(1, 1)
	q.Enqueue("first")
	if q.Enqueue("second") {
		t.Fatal("expected the second enqueue to be refused")
	}

	s := q.shardFor("first")
	<-s.ch // drain, as a worker would
	s.mu.Lock()
	delete(s.pending, "first")
	s.mu.Unlock()

	if !q.Enqueue("second") {
		t.Fatal("previously refused ID could not be re-enqueued")
	}
}

// The core liveness property: even when nothing ever drains Out, the
// watcher's periodic full resync must keep running. Previously a full
// Out blocked fullList on the Run goroutine, so ticker.C was never
// serviced again and resync died for the life of the process.
func TestWatcherKeepsResyncingWhenConsumerIsWedged(t *testing.T) {
	d := newFakeDocker()
	d.addService(swarm.Service{
		ID: "svc-on",
		Spec: swarm.ServiceSpec{
			Annotations: swarm.Annotations{Labels: map[string]string{"tailswarm.enable": "true"}},
		},
	})

	// Unbuffered and never drained: every send must be refused.
	out := make(chan string)

	w := &Watcher{
		Docker:         d,
		Events:         newFakeEvents(),
		Out:            out,
		FullResync:     20 * time.Millisecond,
		LabelNamespace: defaultNamespace,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() { defer close(done); _ = w.Run(ctx) }()

	<-done

	// fullList issues one ListServices per opt-in label key, so count
	// list calls as the proxy for "the resync ticker is still firing".
	d.mu.Lock()
	got := 0
	for _, c := range d.calls {
		if c.Kind == dCallList {
			got++
		}
	}
	d.mu.Unlock()

	// One immediate list plus several ticks; without the fix this stalls
	// on the very first blocking send and never ticks again.
	if got < 3 {
		t.Fatalf("full resync ran %d times, want >= 3 (ticker stalled behind a wedged consumer)", got)
	}
}
