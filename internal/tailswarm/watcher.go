package tailswarm

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// Event is a Docker service event flattened to the bits the watcher
// cares about. Concrete EventStream implementations translate the
// docker SDK's events.Message into this shape so the watcher (and its
// tests) don't depend on the SDK's event encoding.
type Event struct {
	// ServiceID is the affected service's ID (Actor.ID for service
	// events). Empty events are skipped.
	ServiceID string
	// Action is the docker action ("create", "update", "remove").
	// Currently informational — the watcher enqueues on every action
	// because the reconciler is responsible for diffing.
	Action string
}

// EventStream is the watcher's source of fast-path service change
// notifications. It is a thin abstraction over Docker's
// /events?type=service so the watcher can be tested without a real
// Docker daemon and so the docker-socket-proxy plumbing in DESIGN.md §8
// stays out of this package.
//
// Subscribe returns a channel that closes when the underlying stream
// ends (cleanly or via error); on error, the watcher is expected to
// back off and resubscribe.
type EventStream interface {
	Subscribe(ctx context.Context) (<-chan Event, error)
}

// Watcher bridges the Docker event stream and a periodic full list into
// a single stream of per-service reconcile requests on Out. It is the
// only producer for the queue feeding the reconciler (DESIGN.md §5.1).
//
// Out is intentionally a send-only channel typed as serviceID. Dedupe
// is the queue's job — sending the same ID twice is fine and expected
// (an event and a tick can fire for the same service).
type Watcher struct {
	Docker     DockerClient
	Events     EventStream
	Out        chan<- string
	FullResync time.Duration

	// LabelNamespace mirrors Reconciler config. The periodic full list
	// filters services by "<namespace>.enable" so the watcher only
	// enqueues opted-in targets; sidecars and unrelated services are
	// ignored on the slow path. (Events on the fast path are not
	// filtered — the reconciler is cheap on services it doesn't
	// manage.)
	LabelNamespace string

	// ReconnectBackoff is the minimum gap between Subscribe attempts
	// when the stream drops. Defaults to one second; capped internally.
	ReconnectBackoff time.Duration

	Log *slog.Logger
}

const (
	defaultFullResync       = 30 * time.Second
	defaultReconnectBackoff = 1 * time.Second
	maxReconnectBackoff     = 30 * time.Second
)

// Run subscribes to the event stream, ticks a periodic full list, and
// pushes service IDs onto Out until ctx is cancelled. The returned
// error is ctx.Err on clean shutdown; transient stream failures are
// handled internally with exponential backoff.
func (w *Watcher) Run(ctx context.Context) error {
	if w.Out == nil {
		return errors.New("tailswarm: Watcher.Out is nil")
	}
	if w.Docker == nil {
		return errors.New("tailswarm: Watcher.Docker is nil")
	}
	if w.Events == nil {
		return errors.New("tailswarm: Watcher.Events is nil")
	}
	resync := w.FullResync
	if resync <= 0 {
		resync = defaultFullResync
	}
	ns := w.LabelNamespace
	if ns == "" {
		ns = defaultNamespace
	}
	log := w.Log
	if log == nil {
		log = slog.Default()
	}

	ticker := time.NewTicker(resync)
	defer ticker.Stop()

	// Run the slow path once immediately so we have a known good state
	// before any events arrive — matches the reconciler's Resync model.
	w.fullList(ctx, ns, log)

	// The event loop keeps a live subscription in eventsCh. On error or
	// stream close, eventsCh is set to nil and a deadline is scheduled
	// in resubAt; the main select uses time.After(remaining) to wake up
	// for the next subscribe attempt without busy-looping.
	var (
		eventsCh <-chan Event
		backoff  = w.ReconnectBackoff
		resubAt  time.Time // zero when we have a live subscription
	)
	if backoff <= 0 {
		backoff = defaultReconnectBackoff
	}

	subscribe := func() {
		ch, err := w.Events.Subscribe(ctx)
		if err != nil {
			log.Warn("event subscribe failed", "err", err, "retry_in", backoff)
			resubAt = time.Now().Add(backoff)
			backoff = nextBackoff(backoff)
			return
		}
		eventsCh = ch
		resubAt = time.Time{}
		backoff = w.ReconnectBackoff
		if backoff <= 0 {
			backoff = defaultReconnectBackoff
		}
	}

	subscribe()

	for {
		var resubTimer <-chan time.Time
		if eventsCh == nil && !resubAt.IsZero() {
			d := time.Until(resubAt)
			if d < 0 {
				d = 0
			}
			resubTimer = time.After(d)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ticker.C:
			w.fullList(ctx, ns, log)

		case ev, ok := <-eventsCh:
			if !ok {
				// Stream ended; schedule a resubscribe. Use the current
				// backoff and bump it for the next attempt.
				log.Info("event stream closed; resubscribing", "retry_in", backoff)
				eventsCh = nil
				resubAt = time.Now().Add(backoff)
				backoff = nextBackoff(backoff)
				continue
			}
			if ev.ServiceID == "" {
				continue
			}
			w.send(ctx, ev.ServiceID)

		case <-resubTimer:
			subscribe()
		}
	}
}

// fullList enqueues every service carrying the enable label. Errors are
// logged but never returned: a failed periodic list is recoverable and
// shouldn't tear down the watcher (the next tick will try again, and
// events keep flowing).
func (w *Watcher) fullList(ctx context.Context, namespace string, log *slog.Logger) {
	filter := LabelFilter{Key: namespace + ".enable", Value: "true"}
	svcs, err := w.Docker.ListServices(ctx, filter)
	if err != nil {
		log.Warn("full resync list failed", "err", err)
		return
	}
	for _, s := range svcs {
		if s.ID == "" {
			continue
		}
		w.send(ctx, s.ID)
	}
}

// send pushes id to Out, respecting ctx so a cancelled watcher doesn't
// block forever on a full channel.
func (w *Watcher) send(ctx context.Context, id string) {
	select {
	case <-ctx.Done():
	case w.Out <- id:
	}
}

func nextBackoff(d time.Duration) time.Duration {
	n := d * 2
	if n > maxReconnectBackoff {
		n = maxReconnectBackoff
	}
	if n <= 0 {
		n = defaultReconnectBackoff
	}
	return n
}
