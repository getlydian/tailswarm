package tailswarm

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"
	"time"

	"github.com/docker/docker/api/types/swarm"
	"golang.org/x/time/rate"
)

// ErrServiceNotFound is what DockerClient.InspectService returns when the
// target service no longer exists. The reconciler treats this as a
// teardown trigger; concrete Docker clients are expected to translate
// 404s into this sentinel.
var ErrServiceNotFound = errors.New("tailswarm: docker service not found")

// LabelFilter narrows ListServices to services carrying a particular
// label. Empty Value matches the label's presence regardless of value.
//
// Kept as a small struct (rather than docker's filters.Args) so the
// fake used by reconciler tests doesn't have to depend on the docker
// SDK's filter encoding.
type LabelFilter struct {
	Key   string
	Value string
}

// DockerClient is the minimal Docker API surface tailswarm uses. It
// matches the docker-socket-proxy section in DESIGN.md §8: services
// (list/inspect/create/update/remove) plus networks (list, to resolve
// tailswarm.network names to IDs).
type DockerClient interface {
	ListServices(ctx context.Context, filter LabelFilter) ([]swarm.Service, error)
	InspectService(ctx context.Context, serviceID string) (swarm.Service, error)
	CreateService(ctx context.Context, spec SidecarSpec) (string, error)
	UpdateService(ctx context.Context, serviceID string, version uint64, spec SidecarSpec) error
	RemoveService(ctx context.Context, serviceID string) error
	ListNetworks(ctx context.Context) ([]swarm.Network, error)
}

// Reconciler converges Docker Swarm services into a tailnet sidecar set.
// All external interactions go through DockerClient and Controller, so
// the reconciler is testable without real Docker or Headscale.
type Reconciler struct {
	Docker  DockerClient
	Ctrl    Controller
	Store   *Store
	Cfg     Config
	Limiter *rate.Limiter
	Log     *slog.Logger
}

// NewReconciler returns a Reconciler with sane defaults for any optional
// fields.
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
	if r.Limiter == nil {
		r.Limiter = rate.NewLimiter(rate.Limit(rps), int(rps))
	}
	if r.Log == nil {
		r.Log = slog.Default()
	}
	if r.Cfg.Headscale.KeyExpiration == 0 {
		r.Cfg.Headscale.KeyExpiration = 5 * time.Minute
	}
	if r.Cfg.LabelNamespace == "" {
		r.Cfg.LabelNamespace = defaultNamespace
	}
	return r
}

// Reconcile drives one service ID through the lifecycle table in
// DESIGN.md §4.3. Steps roughly:
//
//  1. Inspect the target. If gone → tear down the sidecar, expire the
//     key, delete the Headscale node.
//  2. Parse labels. If not enabled (or labels malformed) → tear down.
//  3. Plan the desired sidecar spec; diff against the cached hash. If
//     unchanged, no-op.
//  4. Mint a fresh ephemeral key (rate-limited). On any subsequent
//     failure, expire the freshly-minted key so we never leak it
//     (DESIGN.md §7).
//  5. Create or update the sidecar service; record the new state.
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
	}
	tgt, enabled, err := parser.Parse(target, networks)
	if err != nil {
		// Malformed labels: tear down anything we previously created
		// for this service so a rename-and-break doesn't leave a stale
		// sidecar wired to the tailnet.
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

	netID := networkIDByName(networks, tgt.Network)
	if netID == "" {
		return fmt.Errorf("resolve network %q: not found", tgt.Network)
	}

	plannerCfg := PlannerConfig{
		Image:        r.Cfg.Sidecar.Image,
		HeadscaleURL: r.Cfg.Headscale.URL,
		NetworkID:    netID,
	}

	prev, hadPrev := r.Store.Get(serviceID)

	// Plan once with a placeholder so we can hash-diff before minting a
	// new key. SpecHash strips TS_AUTHKEY anyway, so the placeholder
	// doesn't affect the hash.
	desired := Plan(tgt, plannerCfg, "")
	desiredHash := SpecHash(desired)

	if hadPrev && prev.LastSpecHash == desiredHash && prev.SidecarID != "" {
		// No-op: spec is unchanged from what we last applied. Refresh
		// the timestamp so observers can tell reconciles are firing.
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

	desired.Env["TS_AUTHKEY"] = key.Secret

	var sidecarID string
	if hadPrev && prev.SidecarID != "" {
		// Update in place. UpdateService needs the current spec
		// version, which we read from the live sidecar to avoid races
		// with manual edits.
		ver, verErr := r.sidecarVersion(ctx, prev.SidecarID)
		if verErr != nil {
			r.expireOrLog(ctx, key.ID, "rollback after version lookup")
			return fmt.Errorf("inspect sidecar %s: %w", prev.SidecarID, verErr)
		}
		if err := r.Docker.UpdateService(ctx, prev.SidecarID, ver, desired); err != nil {
			r.expireOrLog(ctx, key.ID, "rollback after update failure")
			return fmt.Errorf("update sidecar %s: %w", prev.SidecarID, err)
		}
		sidecarID = prev.SidecarID
		// Best-effort expire of the previous key now that the sidecar
		// has been re-pointed at the new one.
		if prev.PreAuthKeyID != "" {
			r.expireOrLog(ctx, prev.PreAuthKeyID, "rotated key")
		}
	} else {
		id, err := r.Docker.CreateService(ctx, desired)
		if err != nil {
			r.expireOrLog(ctx, key.ID, "rollback after create failure")
			return fmt.Errorf("create sidecar: %w", err)
		}
		sidecarID = id
	}

	r.Store.Put(serviceID, Entry{
		SidecarID:       sidecarID,
		LastSpecHash:    desiredHash,
		PreAuthKeyID:    key.ID,
		HeadscaleNodeID: prev.HeadscaleNodeID, // learned via Resync; not knowable at create time
		LastReconcileAt: time.Now(),
	})
	r.Log.Info("reconciled",
		"service_id", serviceID,
		"sidecar_id", sidecarID,
		"hash", desiredHash)
	return nil
}

// teardown removes any sidecar / key / node we know about for
// serviceID. Each step is best-effort: we want to free as many
// resources as possible even if one step fails.
func (r *Reconciler) teardown(ctx context.Context, serviceID string) error {
	prev, ok := r.Store.Get(serviceID)
	if !ok {
		// Nothing tracked. Could still be an orphan on the controller
		// side (a sidecar-less node), but that's Resync's job — per-ID
		// teardown without state is a no-op.
		return nil
	}

	var firstErr error
	if prev.SidecarID != "" {
		if err := r.Docker.RemoveService(ctx, prev.SidecarID); err != nil {
			r.Log.Warn("remove sidecar", "service_id", serviceID,
				"sidecar_id", prev.SidecarID, "err", err)
			if firstErr == nil {
				firstErr = err
			}
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
	if prev.HeadscaleNodeID != "" {
		if err := r.Limiter.Wait(ctx); err != nil {
			return err
		}
		if err := r.Ctrl.DeleteNode(ctx, prev.HeadscaleNodeID); err != nil {
			r.Log.Warn("delete node", "service_id", serviceID,
				"node_id", prev.HeadscaleNodeID, "err", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	r.Store.Delete(serviceID)
	return firstErr
}

// Resync is the startup path. It reconstructs the in-memory store from
// live Docker + Headscale state and removes orphans on both sides:
//   - sidecars whose target service no longer exists or no longer has
//     tailswarm.enable=true;
//   - Headscale nodes for our user that have no matching sidecar.
//
// Per DESIGN.md §7, this is what makes "tailswarm crashes mid-reconcile"
// safe: the Docker + Headscale state is authoritative.
func (r *Reconciler) Resync(ctx context.Context) error {
	sidecars, err := r.Docker.ListServices(ctx, LabelFilter{Key: ownerLabelManaged, Value: "true"})
	if err != nil {
		return fmt.Errorf("list managed sidecars: %w", err)
	}

	// Rebuild store from sidecar inventory. Map sidecar → target via
	// the owner label so we don't have to grep service names.
	live := make(map[string]swarm.Service, len(sidecars))
	for _, sc := range sidecars {
		targetID := sc.Spec.Labels[ownerLabelTarget]
		if targetID == "" {
			r.Log.Warn("sidecar without target label; removing",
				"sidecar_id", sc.ID)
			if err := r.Docker.RemoveService(ctx, sc.ID); err != nil {
				r.Log.Warn("remove orphan sidecar", "sidecar_id", sc.ID, "err", err)
			}
			continue
		}
		live[targetID] = sc
		r.Store.Put(targetID, Entry{
			SidecarID:       sc.ID,
			LastSpecHash:    "", // forces a re-plan on first reconcile
			LastReconcileAt: time.Now(),
		})
	}

	// Tear down sidecars whose target is gone or no longer enabled.
	networks, err := r.Docker.ListNetworks(ctx)
	if err != nil {
		return fmt.Errorf("list networks: %w", err)
	}
	parser := Labels{
		Namespace:          r.Cfg.LabelNamespace,
		AllowedTagPrefixes: r.Cfg.AllowedTagPrefixes,
	}
	for targetID := range live {
		svc, err := r.Docker.InspectService(ctx, targetID)
		if errors.Is(err, ErrServiceNotFound) {
			if tdErr := r.teardown(ctx, targetID); tdErr != nil {
				r.Log.Warn("teardown orphan", "service_id", targetID, "err", tdErr)
			}
			continue
		}
		if err != nil {
			r.Log.Warn("inspect target", "service_id", targetID, "err", err)
			continue
		}
		_, enabled, perr := parser.Parse(svc, networks)
		if perr != nil || !enabled {
			if tdErr := r.teardown(ctx, targetID); tdErr != nil {
				r.Log.Warn("teardown disabled", "service_id", targetID, "err", tdErr)
			}
		}
	}

	// Headscale-side orphans: any node owned by our user whose hostname
	// doesn't correspond to a live sidecar's target hostname.
	if err := r.Limiter.Wait(ctx); err != nil {
		return err
	}
	nodes, err := r.Ctrl.ListNodes(ctx, r.Cfg.Headscale.User)
	if err != nil {
		return fmt.Errorf("list nodes: %w", err)
	}
	wantedHostnames := make(map[string]string, len(live)) // hostname → targetID
	for targetID, sc := range live {
		// Hostname is stored in the sidecar's container env; pluck it
		// from the spec. Fall back to nothing if the spec is malformed.
		host := containerHostname(sc)
		if host != "" {
			wantedHostnames[host] = targetID
		}
	}
	for _, n := range nodes {
		targetID, kept := wantedHostnames[n.Hostname]
		if !kept {
			if err := r.Limiter.Wait(ctx); err != nil {
				return err
			}
			if err := r.Ctrl.DeleteNode(ctx, n.ID); err != nil {
				r.Log.Warn("delete orphan node", "node_id", n.ID, "err", err)
			}
			continue
		}
		// Backfill the node ID into the entry so future teardowns can
		// clean it up without another ListNodes call.
		entry, ok := r.Store.Get(targetID)
		if ok && entry.HeadscaleNodeID == "" {
			entry.HeadscaleNodeID = n.ID
			r.Store.Put(targetID, entry)
		}
	}

	return nil
}

// expireOrLog is the rollback helper. We don't want to surface the
// rollback error itself — the caller already has a more useful primary
// error — but a leaked key is worth logging.
func (r *Reconciler) expireOrLog(ctx context.Context, keyID, reason string) {
	if keyID == "" {
		return
	}
	if err := r.Ctrl.ExpireKey(ctx, keyID); err != nil {
		r.Log.Warn("expire key in rollback", "key_id", keyID, "reason", reason, "err", err)
	}
}

func (r *Reconciler) sidecarVersion(ctx context.Context, sidecarID string) (uint64, error) {
	sc, err := r.Docker.InspectService(ctx, sidecarID)
	if err != nil {
		return 0, err
	}
	return sc.Version.Index, nil
}

func networkIDByName(networks []swarm.Network, name string) string {
	for _, n := range networks {
		if n.Spec.Name == name {
			return n.ID
		}
	}
	return ""
}

func containerHostname(sc swarm.Service) string {
	if sc.Spec.TaskTemplate.ContainerSpec == nil {
		return ""
	}
	if h := sc.Spec.TaskTemplate.ContainerSpec.Hostname; h != "" {
		return h
	}
	for _, e := range sc.Spec.TaskTemplate.ContainerSpec.Env {
		const prefix = "TS_HOSTNAME="
		if len(e) > len(prefix) && e[:len(prefix)] == prefix {
			return e[len(prefix):]
		}
	}
	return ""
}

// Queue is a sharded per-serviceID work queue with dedupe. The watcher
// (step 7) feeds it via Enqueue; workers drain it via Run.
//
// Each shard is a goroutine that pulls from a buffered channel of
// pending IDs. Dedupe is per-shard: enqueueing an ID that's already
// pending is a no-op. This bounds the worst case to one in-flight
// reconcile per serviceID without serializing across all of them.
type Queue struct {
	shards []*shard
	stride uint32
}

type shard struct {
	mu      sync.Mutex
	pending map[string]struct{}
	ch      chan string
}

// NewQueue returns a Queue with workers shards. Buffer is the per-shard
// capacity before Enqueue blocks; sized to the expected fan-out from a
// single full-resync tick.
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

// Enqueue adds serviceID to its shard. If an ID is already pending in
// that shard the call is a cheap no-op.
func (q *Queue) Enqueue(serviceID string) {
	s := q.shardFor(serviceID)
	s.mu.Lock()
	if _, dup := s.pending[serviceID]; dup {
		s.mu.Unlock()
		return
	}
	s.pending[serviceID] = struct{}{}
	s.mu.Unlock()

	// The channel buffer is sized for typical fan-out; a full channel
	// means we're already swamped, so blocking here applies the
	// natural backpressure.
	s.ch <- serviceID
}

// Run drains every shard until ctx is cancelled, calling fn for each
// pulled serviceID. Returns when all shards have closed (which only
// happens after Stop or ctx cancellation).
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
