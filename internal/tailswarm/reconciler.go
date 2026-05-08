package tailswarm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/docker/docker/api/types/swarm"
	"golang.org/x/time/rate"
)

// ErrServiceNotFound is what DockerClient.InspectService returns when
// the target service no longer exists. The reconciler treats this as a
// teardown trigger.
var ErrServiceNotFound = errors.New("tailswarm: docker service not found")

// LabelFilter narrows ListServices to services carrying a particular
// label. Empty Value matches the label's presence regardless of value.
type LabelFilter struct {
	Key   string
	Value string
}

// DockerClient is the read-only Docker API surface tailswarm uses in
// the tsnet design. Service mutation is gone — there are no sidecars to
// create, update, or remove.
type DockerClient interface {
	ListServices(ctx context.Context, filter LabelFilter) ([]swarm.Service, error)
	InspectService(ctx context.Context, serviceID string) (swarm.Service, error)
	ListNetworks(ctx context.Context) ([]swarm.Network, error)
}

// Reconciler converges Docker Swarm services into a set of in-process
// tsnet proxies. Each opted-in service gets one Proxy; the reconciler
// is responsible for the create/rotate/destroy lifecycle and for
// expiring Headscale preauth keys when proxies come and go.
type Reconciler struct {
	Docker  DockerClient
	Ctrl    Controller
	Store   *Store
	Cfg     Config
	Limiter *rate.Limiter
	Log     *slog.Logger

	// NewProxy is the factory used to start a tsnet proxy. Tests inject
	// a fake; production wires NewTsnetProxy.
	NewProxy ProxyFactory
}

// NewReconciler returns a Reconciler with sane defaults for any
// optional fields. NewProxy still has to be set explicitly; the wiring
// in cmd/tailswarm does that.
func NewReconciler(d DockerClient, c Controller, s *Store, cfg Config) *Reconciler {
	r := &Reconciler{
		Docker: d,
		Ctrl:   c,
		Store:  s,
		Cfg:    cfg,
	}
	rps := r.Cfg.Reconcile.RateLimitRPS
	if rps <= 0 {
		rps = 5
	}
	r.Limiter = rate.NewLimiter(rate.Limit(rps), int(rps))
	r.Log = slog.Default()
	if r.Cfg.Headscale.KeyExpiration == 0 {
		r.Cfg.Headscale.KeyExpiration = 5 * time.Minute
	}
	if r.Cfg.LabelNamespace == "" {
		r.Cfg.LabelNamespace = defaultNamespace
	}
	return r
}

// Reconcile drives one service ID through the proxy lifecycle:
//
//  1. Inspect target. Gone or disabled → close the proxy and expire its key.
//  2. Parse labels + ports. Malformed → tear down.
//  3. Hash the desired ProxyConfig; compare to last applied. No-op on match.
//  4. Mint a fresh ephemeral key (rate-limited).
//  5. Start a new tsnet proxy. On success, close the previous one and
//     expire its key.
func (r *Reconciler) Reconcile(ctx context.Context, serviceID string) error {
	target, err := r.Docker.InspectService(ctx, serviceID)
	if errors.Is(err, ErrServiceNotFound) {
		return r.teardown(ctx, serviceID)
	}
	if err != nil {
		return fmt.Errorf("inspect service %s: %w", serviceID, err)
	}

	networks, err := r.Docker.ListNetworks(ctx)
	if err != nil {
		return fmt.Errorf("list networks: %w", err)
	}

	parser := Labels{
		Namespace:          r.Cfg.LabelNamespace,
		AllowedTagPrefixes: r.Cfg.AllowedTagPrefixes,
		DefaultNetwork:     r.Cfg.Network,
	}
	tgt, enabled, err := parser.Parse(target, networks)
	if err != nil {
		r.Log.Warn("label parse error; tearing down",
			"service_id", serviceID, "err", err)
		if tdErr := r.teardown(ctx, serviceID); tdErr != nil {
			return tdErr
		}
		return err
	}
	if !enabled {
		return r.teardown(ctx, serviceID)
	}

	desired := proxyConfigFor(tgt, r.Cfg)
	desiredHash := proxyHash(desired)

	prev, hadPrev := r.Store.Get(serviceID)
	if hadPrev && prev.LastSpecHash == desiredHash && prev.Proxy != nil {
		prev.LastReconcileAt = time.Now()
		r.Store.Put(serviceID, prev)
		return nil
	}

	if err := r.Limiter.Wait(ctx); err != nil {
		return fmt.Errorf("rate limit: %w", err)
	}
	key, err := r.Ctrl.CreateEphemeralKey(ctx, KeyRequest{
		User:       r.Cfg.Headscale.User,
		Tags:       []string{tgt.Tag},
		Ephemeral:  true,
		Reusable:   false,
		Expiration: r.Cfg.Headscale.KeyExpiration,
	})
	if err != nil {
		return fmt.Errorf("mint key: %w", err)
	}

	proxyCfg := desired
	proxyCfg.AuthKey = key.Secret

	proxy, err := r.NewProxy(ctx, proxyCfg, r.Log.With("hostname", proxyCfg.Hostname))
	if err != nil {
		r.expireOrLog(ctx, key.ID, "rollback after proxy start failure")
		return fmt.Errorf("start proxy %s: %w", proxyCfg.Hostname, err)
	}

	// Swap in the new proxy. Close the old one (if any) and expire its
	// previous key after the new one is healthy.
	r.Store.Put(serviceID, Entry{
		Proxy:           proxy,
		LastSpecHash:    desiredHash,
		PreAuthKeyID:    key.ID,
		LastReconcileAt: time.Now(),
	})
	if hadPrev && prev.Proxy != nil {
		if err := prev.Proxy.Close(); err != nil {
			r.Log.Warn("close previous proxy", "service_id", serviceID, "err", err)
		}
		if prev.PreAuthKeyID != "" {
			r.expireOrLog(ctx, prev.PreAuthKeyID, "rotated key")
		}
	}
	r.Log.Info("reconciled",
		"service_id", serviceID,
		"hostname", proxyCfg.Hostname,
		"hash", desiredHash)
	return nil
}

// teardown closes any proxy we know about for serviceID and expires its
// preauth key. Each step is best-effort.
func (r *Reconciler) teardown(ctx context.Context, serviceID string) error {
	prev, ok := r.Store.Get(serviceID)
	if !ok {
		return nil
	}

	var firstErr error
	if prev.Proxy != nil {
		if err := prev.Proxy.Close(); err != nil {
			r.Log.Warn("close proxy", "service_id", serviceID, "err", err)
			firstErr = err
		}
	}
	if prev.PreAuthKeyID != "" {
		if err := r.Limiter.Wait(ctx); err != nil {
			return err
		}
		if err := r.Ctrl.ExpireKey(ctx, prev.PreAuthKeyID); err != nil {
			r.Log.Warn("expire key", "service_id", serviceID,
				"key_id", prev.PreAuthKeyID, "err", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	r.Store.Delete(serviceID)
	return firstErr
}

// CloseAll tears down every proxy on shutdown. Keys are not expired
// here — they're already ephemeral and will lapse on their own once the
// tsnet servers disconnect.
func (r *Reconciler) CloseAll() {
	for id, e := range r.Store.Snapshot() {
		if e.Proxy != nil {
			if err := e.Proxy.Close(); err != nil {
				r.Log.Warn("close proxy on shutdown", "service_id", id, "err", err)
			}
		}
		r.Store.Delete(id)
	}
}

// expireOrLog is the rollback helper.
func (r *Reconciler) expireOrLog(ctx context.Context, keyID, reason string) {
	if keyID == "" {
		return
	}
	if err := r.Ctrl.ExpireKey(ctx, keyID); err != nil {
		r.Log.Warn("expire key in rollback", "key_id", keyID, "reason", reason, "err", err)
	}
}

// proxyConfigFor is the pure (Target, Config) → ProxyConfig translation.
func proxyConfigFor(t Target, cfg Config) ProxyConfig {
	return ProxyConfig{
		Hostname: t.Hostname,
		Target:   t.ServiceName,
		Ports:    t.Ports,
		StateDir: cfg.Tsnet.StateDir,
		LoginURL: cfg.Headscale.URL,
		Tags:     []string{t.Tag},
	}
}

// proxyHash is a stable hash over the diff-relevant subset of a
// ProxyConfig — everything except the auth key, which rotates on every
// reconcile. Map iteration order does not affect the result.
func proxyHash(c ProxyConfig) string {
	ports := make([]uint32, 0, len(c.Ports))
	for _, p := range c.Ports {
		ports = append(ports, p.Target)
	}
	sort.Slice(ports, func(i, j int) bool { return ports[i] < ports[j] })

	tags := append([]string(nil), c.Tags...)
	sort.Strings(tags)

	payload := struct {
		Hostname string
		Target   string
		Ports    []uint32
		StateDir string
		LoginURL string
		Tags     []string
	}{
		Hostname: c.Hostname,
		Target:   c.Target,
		Ports:    ports,
		StateDir: c.StateDir,
		LoginURL: c.LoginURL,
		Tags:     tags,
	}
	b, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// Queue is a sharded per-serviceID work queue with dedupe. Unchanged
// from the sidecar design — its semantics are identical for proxy
// lifecycle work.
type Queue struct {
	shards []*shard
	stride uint32
}

type shard struct {
	mu      sync.Mutex
	pending map[string]struct{}
	ch      chan string
}

func NewQueue(workers, buffer int) *Queue {
	if workers < 1 {
		workers = 1
	}
	if buffer < 1 {
		buffer = 64
	}
	q := &Queue{
		shards: make([]*shard, workers),
		stride: uint32(workers),
	}
	for i := range q.shards {
		q.shards[i] = &shard{
			pending: make(map[string]struct{}),
			ch:      make(chan string, buffer),
		}
	}
	return q
}

func (q *Queue) Enqueue(serviceID string) {
	s := q.shardFor(serviceID)
	s.mu.Lock()
	if _, dup := s.pending[serviceID]; dup {
		s.mu.Unlock()
		return
	}
	s.pending[serviceID] = struct{}{}
	s.mu.Unlock()

	s.ch <- serviceID
}

func (q *Queue) Run(ctx context.Context, fn func(ctx context.Context, serviceID string)) {
	var wg sync.WaitGroup
	wg.Add(len(q.shards))
	for _, s := range q.shards {
		s := s
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case id, ok := <-s.ch:
					if !ok {
						return
					}
					s.mu.Lock()
					delete(s.pending, id)
					s.mu.Unlock()
					fn(ctx, id)
				}
			}
		}()
	}
	wg.Wait()
}

func (q *Queue) shardFor(serviceID string) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(serviceID))
	return q.shards[h.Sum32()%q.stride]
}

// pendingCount is exposed for tests; production code never needs it.
func (q *Queue) pendingCount() int {
	n := 0
	for _, s := range q.shards {
		s.mu.Lock()
		n += len(s.pending)
		s.mu.Unlock()
	}
	return n
}
