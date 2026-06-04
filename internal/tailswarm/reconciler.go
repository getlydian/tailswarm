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

// DockerClient is the Docker API surface tailswarm uses. The read paths
// (List/Inspect/ListNetworks) drive both ingress and egress reconcile;
// the write paths (Create/Update/Remove) are used only to manage egress
// gateway companion services. Ingress touches no write path.
type DockerClient interface {
	ListServices(ctx context.Context, filter LabelFilter) ([]swarm.Service, error)
	InspectService(ctx context.Context, serviceID string) (swarm.Service, error)
	ListNetworks(ctx context.Context) ([]swarm.Network, error)
	ListTasks(ctx context.Context) ([]swarm.Task, error)

	CreateService(ctx context.Context, spec swarm.ServiceSpec) (string, error)
	UpdateService(ctx context.Context, serviceID string, version uint64, spec swarm.ServiceSpec) error
	RemoveService(ctx context.Context, serviceID string) error
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

	// GatewayImage is the image egress gateway companions run — tailswarm's
	// own image in "gateway" mode. The wiring in cmd/tailswarm discovers it
	// from the daemon's own Swarm service (see DiscoverGatewayImage) and
	// sets it here. Empty means egress is unavailable: an egressing service
	// reconciles to errGatewayImageUnset.
	GatewayImage string

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

// Reconcile drives one service ID through the ingress proxy and egress
// gateway lifecycles:
//
//  1. Inspect target. Gone or disabled → tear down both and expire keys.
//  2. Parse labels. Malformed → tear down.
//  3. Run the ingress reconcile (no-op when the service doesn't ingress)
//     and the egress reconcile (no-op when it doesn't egress). Each is
//     independent; a service may use either, both, or neither.
func (r *Reconciler) Reconcile(ctx context.Context, serviceID string) error {
	target, err := r.Docker.InspectService(ctx, serviceID)
	if errors.Is(err, ErrServiceNotFound) {
		return r.teardown(ctx, serviceID)
	}
	if err != nil {
		return fmt.Errorf("inspect service %s: %w", serviceID, err)
	}

	// Egress gateways are tailswarm's own creations, not opted-in apps.
	// Skip them outright so a stray event on a gateway never drives the
	// app-reconcile path.
	if _, isGateway := target.Spec.Labels[gatewayForLabel]; isGateway {
		return nil
	}

	networks, err := r.Docker.ListNetworks(ctx)
	if err != nil {
		return fmt.Errorf("list networks: %w", err)
	}

	parser := Labels{
		Namespace:      r.Cfg.LabelNamespace,
		AllowedTags:    r.Cfg.AllowedTags,
		DefaultNetwork: r.Cfg.Network,
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

	// Ingress and egress are independent. Run both; each tears down its own
	// stale artefact if the service no longer opts into that direction
	// (e.g. egress labels removed from a service that keeps ingress). The
	// errors are joined so one direction's failure doesn't silently mask
	// the other's progress.
	ingErr := r.reconcileIngress(ctx, serviceID, tgt)
	egrErr := r.reconcileEgress(ctx, serviceID, tgt)
	return errors.Join(ingErr, egrErr)
}

// reconcileIngress drives the in-process tsnet proxy lifecycle for a
// service. When the service does not opt into ingress it tears down any
// stale proxy (without touching the egress gateway) and returns.
func (r *Reconciler) reconcileIngress(ctx context.Context, serviceID string, tgt Target) error {
	if !tgt.Ingress {
		return r.teardownIngress(ctx, serviceID)
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
	// previous key after the new one is healthy. Carry forward the egress
	// gateway fields untouched — reconcileEgress owns those.
	next := prev
	next.Proxy = proxy
	next.LastSpecHash = desiredHash
	next.PreAuthKeyID = key.ID
	next.LastReconcileAt = time.Now()
	r.Store.Put(serviceID, next)
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

// reconcileEgress drives the egress gateway lifecycle for a service:
//
//  1. Not egressing → tear down any stale gateway (without touching the
//     ingress proxy) and return.
//  2. Hash the desired gateway spec; matching the live one → no-op.
//  3. Mint a fresh ephemeral key under the app's egress.tag (rate-limited).
//  4. Create the gateway (first time) or update it in place (label change),
//     then expire the previous key.
//
// The gateway is a managed Docker service, so unlike ingress this writes to
// the Docker socket (Create/Update/Remove) — the only place tailswarm does.
func (r *Reconciler) reconcileEgress(ctx context.Context, serviceID string, tgt Target) error {
	if tgt.Egress == nil {
		return r.teardownEgress(ctx, serviceID)
	}
	if r.GatewayImage == "" {
		return errGatewayImageUnset
	}

	eg := tgt.Egress
	desiredHash := gatewayHash(eg, r.GatewayImage)

	prev, hadPrev := r.Store.Get(serviceID)
	if hadPrev && prev.GatewayHash == desiredHash && prev.GatewayServiceID != "" {
		prev.LastReconcileAt = time.Now()
		r.Store.Put(serviceID, prev)
		return nil
	}

	if err := r.Limiter.Wait(ctx); err != nil {
		return fmt.Errorf("rate limit: %w", err)
	}
	key, err := r.Ctrl.CreateEphemeralKey(ctx, KeyRequest{
		User:       r.Cfg.Headscale.User,
		Tags:       []string{eg.Tag},
		Ephemeral:  true,
		Reusable:   false,
		Expiration: r.Cfg.Headscale.KeyExpiration,
	})
	if err != nil {
		return fmt.Errorf("mint egress key: %w", err)
	}

	spec := gatewaySpec(eg, serviceID, r.Cfg, r.GatewayImage, key.Secret)

	gatewayID := prev.GatewayServiceID
	if gatewayID == "" {
		// No gateway tracked in the store. After a daemon restart the
		// gateway may still exist on the swarm (it outlives the daemon);
		// adopt it by its marker label rather than creating a duplicate
		// (Docker rejects a second service with the same name).
		if adopted, err := r.findGateway(ctx, serviceID); err != nil {
			r.expireOrLog(ctx, key.ID, "rollback after gateway lookup failure")
			return fmt.Errorf("find gateway for %s: %w", serviceID, err)
		} else if adopted != "" {
			gatewayID = adopted
		}
	}
	if gatewayID != "" {
		// Update the existing gateway in place. Swarm rejects stale writes,
		// so inspect for the current version first.
		gw, err := r.Docker.InspectService(ctx, gatewayID)
		if errors.Is(err, ErrServiceNotFound) {
			// Gateway vanished out from under us — fall back to recreate.
			gatewayID = ""
		} else if err != nil {
			r.expireOrLog(ctx, key.ID, "rollback after gateway inspect failure")
			return fmt.Errorf("inspect gateway %s: %w", gatewayID, err)
		} else if err := r.Docker.UpdateService(ctx, gatewayID, gw.Version.Index, spec); err != nil {
			r.expireOrLog(ctx, key.ID, "rollback after gateway update failure")
			return fmt.Errorf("update gateway %s: %w", gatewayID, err)
		}
	}
	if gatewayID == "" {
		id, err := r.Docker.CreateService(ctx, spec)
		if err != nil {
			r.expireOrLog(ctx, key.ID, "rollback after gateway create failure")
			return fmt.Errorf("create gateway for %s: %w", serviceID, err)
		}
		gatewayID = id
	}

	// Swap in the new gateway state, carrying ingress fields untouched.
	next := prev
	next.GatewayServiceID = gatewayID
	next.GatewayHash = desiredHash
	next.GatewayKeyID = key.ID
	next.LastReconcileAt = time.Now()
	r.Store.Put(serviceID, next)

	if hadPrev && prev.GatewayKeyID != "" && prev.GatewayKeyID != key.ID {
		r.expireOrLog(ctx, prev.GatewayKeyID, "rotated egress key")
	}
	r.Log.Info("reconciled egress",
		"service_id", serviceID,
		"gateway_id", gatewayID,
		"hostname", eg.Hostname,
		"hash", desiredHash)
	return nil
}

// findGateway returns the service ID of an existing gateway fronting
// appServiceID, or "" if none exists. It matches on the gatewayForLabel
// marker so a daemon restart adopts a still-running gateway instead of
// creating a duplicate.
func (r *Reconciler) findGateway(ctx context.Context, appServiceID string) (string, error) {
	gws, err := r.Docker.ListServices(ctx, LabelFilter{Key: gatewayForLabel, Value: appServiceID})
	if err != nil {
		return "", err
	}
	for _, gw := range gws {
		if gw.Spec.Labels[gatewayForLabel] == appServiceID {
			return gw.ID, nil
		}
	}
	return "", nil
}

// teardown removes every artefact we know about for serviceID — both the
// ingress proxy and the egress gateway — expires their keys, and drops the
// store entry. Used when the service is gone, disabled, or malformed. Each
// step is best-effort; the first error is returned.
func (r *Reconciler) teardown(ctx context.Context, serviceID string) error {
	ingErr := r.teardownIngress(ctx, serviceID)
	egrErr := r.teardownEgress(ctx, serviceID)
	r.Store.Delete(serviceID)
	return errors.Join(ingErr, egrErr)
}

// teardownIngress closes the in-process proxy for serviceID and expires
// its preauth key, leaving any egress gateway untouched. It clears the
// ingress fields from the store entry (and deletes the entry if no egress
// artefact remains). Each step is best-effort.
func (r *Reconciler) teardownIngress(ctx context.Context, serviceID string) error {
	prev, ok := r.Store.Get(serviceID)
	if !ok || (prev.Proxy == nil && prev.PreAuthKeyID == "") {
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

	prev.Proxy = nil
	prev.LastSpecHash = ""
	prev.PreAuthKeyID = ""
	if prev.GatewayServiceID == "" {
		r.Store.Delete(serviceID)
	} else {
		r.Store.Put(serviceID, prev)
	}
	return firstErr
}

// teardownEgress removes the egress gateway service for serviceID and
// expires its preauth key, leaving any ingress proxy untouched. It clears
// the gateway fields from the store entry (and deletes the entry if no
// ingress artefact remains). Each step is best-effort.
func (r *Reconciler) teardownEgress(ctx context.Context, serviceID string) error {
	prev, ok := r.Store.Get(serviceID)
	if !ok || (prev.GatewayServiceID == "" && prev.GatewayKeyID == "") {
		return nil
	}

	var firstErr error
	if prev.GatewayServiceID != "" {
		if err := r.Limiter.Wait(ctx); err != nil {
			return err
		}
		if err := r.Docker.RemoveService(ctx, prev.GatewayServiceID); err != nil {
			r.Log.Warn("remove gateway", "service_id", serviceID,
				"gateway_id", prev.GatewayServiceID, "err", err)
			firstErr = err
		}
	}
	if prev.GatewayKeyID != "" {
		if err := r.Limiter.Wait(ctx); err != nil {
			return err
		}
		if err := r.Ctrl.ExpireKey(ctx, prev.GatewayKeyID); err != nil {
			r.Log.Warn("expire gateway key", "service_id", serviceID,
				"key_id", prev.GatewayKeyID, "err", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	prev.GatewayServiceID = ""
	prev.GatewayHash = ""
	prev.GatewayKeyID = ""
	if prev.Proxy == nil {
		r.Store.Delete(serviceID)
	} else {
		r.Store.Put(serviceID, prev)
	}
	return firstErr
}

// CloseAll tears down every in-process proxy on shutdown. Keys are not
// expired here — they're already ephemeral and will lapse on their own
// once the tsnet servers disconnect. Egress gateways are deliberately
// left running: they are independent Docker services that keep forwarding
// across a daemon restart, and the next reconcile re-adopts them by their
// marker label (see findGateway).
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
