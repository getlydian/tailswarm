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
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types/swarm"
	"golang.org/x/time/rate"
)

// ErrServiceNotFound is what DockerClient.InspectService returns when
// the target service no longer exists. The reconciler treats this as a
// teardown trigger.
var ErrServiceNotFound = errors.New("tailswarm: docker service not found")

// ErrServiceExists is what DockerClient.CreateService returns when a
// service with the same name already exists (Docker replies 409). The
// reconciler treats this as "adopt the existing service and reconcile it"
// rather than a hard failure — a gateway whose owning app service was
// recreated under a new Swarm ID keeps its stable name, so its stale
// gatewayForLabel hides it from adoption and a blind create collides.
var ErrServiceExists = errors.New("tailswarm: docker service already exists")

// LabelFilter narrows ListServices to services carrying a particular
// label. Empty Value matches the label's presence regardless of value.
// Name, if set, additionally restricts the result to services with that
// exact name — used to adopt an existing gateway by its stable name after
// a create collides (the app-id marker label having gone stale).
type LabelFilter struct {
	Key   string
	Value string
	Name  string
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

// reconcileEgress drives the egress gateway lifecycle for a service. There
// is one gateway Docker service per egress target (see GatewayRef), so this
// is a set-reconcile keyed on each target's "host:port" addr:
//
//  1. Not egressing → tear down all stale gateways (without touching the
//     ingress proxy) and return.
//  2. Hash the desired target set; if it matches and every desired gateway
//     is already tracked → no-op.
//  3. Otherwise diff desired targets against the tracked gateways:
//     added targets get a fresh key + new gateway; removed targets are
//     deleted and their key expired; surviving targets are left running
//     untouched (no key churn on unrelated targets).
//
// The gateways are managed Docker services, so unlike ingress this writes to
// the Docker socket (Create/Remove) — the only place tailswarm does.
func (r *Reconciler) reconcileEgress(ctx context.Context, serviceID string, tgt Target) error {
	if tgt.Egress == nil {
		return r.teardownEgress(ctx, serviceID)
	}
	if r.GatewayImage == "" {
		return errGatewayImageUnset
	}

	eg := tgt.Egress
	desiredHash := gatewayHash(eg, r.GatewayImage)

	prev, _ := r.Store.Get(serviceID)

	// Index the gateways we already track by target addr, adopting any
	// still-running-but-untracked gateways (e.g. after a daemon restart)
	// before we diff so we don't recreate duplicates.
	tracked, err := r.trackedGateways(ctx, serviceID, eg, prev)
	if err != nil {
		return err
	}

	desired := make(map[string]EgressTarget, len(eg.Targets))
	for _, t := range eg.Targets {
		desired[t.addr()] = t
	}

	// Fast path: target set unchanged and every desired gateway is tracked.
	if prev.GatewayHash == desiredHash && len(tracked) == len(desired) {
		allTracked := true
		for addr := range desired {
			if _, ok := tracked[addr]; !ok {
				allTracked = false
				break
			}
		}
		if allTracked {
			prev.LastReconcileAt = time.Now()
			r.Store.Put(serviceID, prev)
			return nil
		}
	}

	var errs []error

	// Remove gateways whose target is no longer desired.
	for addr, ref := range tracked {
		if _, keep := desired[addr]; keep {
			continue
		}
		if err := r.removeGateway(ctx, serviceID, ref); err != nil {
			errs = append(errs, err)
			continue
		}
		delete(tracked, addr)
	}

	// Create gateways for newly desired targets; update surviving gateways
	// whose spec drifted (e.g. a new image after a daemon upgrade). A
	// surviving gateway whose hash still matches is left untouched — no churn
	// on unrelated targets.
	for addr, t := range desired {
		ref, exists := tracked[addr]
		if !exists {
			created, err := r.createGateway(ctx, serviceID, eg, t)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			tracked[addr] = created
			continue
		}
		if ref.Hash == targetGatewayHash(eg, t, r.GatewayImage) {
			continue // up to date
		}
		updated, found, err := r.updateGateway(ctx, serviceID, eg, t, ref)
		if err != nil {
			errs = append(errs, err)
			// Drop drifted-but-failed gateways from tracked so the count
			// check forces a retry next pass.
			delete(tracked, addr)
			continue
		}
		if !found {
			// Vanished out from under us — recreate.
			delete(tracked, addr)
			created, err := r.createGateway(ctx, serviceID, eg, t)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			tracked[addr] = created
			continue
		}
		tracked[addr] = updated
	}

	// Persist the surviving + new gateway set and the desired hash. The hash
	// is recorded even on partial failure: any target that failed is absent
	// from tracked, so the count check on the next pass forces a retry.
	next := prev
	next.Gateways = sortedGateways(tracked)
	next.GatewayHash = desiredHash
	next.LastReconcileAt = time.Now()
	r.Store.Put(serviceID, next)

	r.Log.Info("reconciled egress",
		"service_id", serviceID,
		"gateways", len(next.Gateways),
		"targets", len(desired),
		"hash", desiredHash)
	return errors.Join(errs...)
}

// trackedGateways returns the live gateways for serviceID keyed by target
// addr. It starts from the store's records and, if any are missing, adopts
// still-running gateways found by their marker label (a daemon restart
// leaves gateways running but drops the store). Adopted gateways are matched
// back to their target by the single addr in TAILSWARM_GATEWAY_TARGETS.
func (r *Reconciler) trackedGateways(ctx context.Context, serviceID string, eg *EgressSpec, prev Entry) (map[string]GatewayRef, error) {
	out := make(map[string]GatewayRef, len(prev.Gateways))
	for _, ref := range prev.Gateways {
		out[ref.Target] = ref
	}

	// Adopt any running gateway not already accounted for. We list once and
	// only fold in services whose target we aren't already tracking.
	gws, err := r.findGateways(ctx, serviceID)
	if err != nil {
		return nil, fmt.Errorf("find gateways for %s: %w", serviceID, err)
	}
	bySvc := make(map[string]struct{}, len(out))
	for _, ref := range out {
		bySvc[ref.ServiceID] = struct{}{}
	}
	for _, gw := range gws {
		if _, known := bySvc[gw.ID]; known {
			continue
		}
		addr := gatewayTargetAddr(gw)
		if addr == "" {
			continue
		}
		if _, dup := out[addr]; dup {
			continue
		}
		// Adopted without a tracked key: it was minted by a prior daemon and
		// is ephemeral, so it lapses on its own if this gateway is later
		// removed. KeyID stays empty; removeGateway tolerates that. Its Hash
		// is reconstructed from the running spec so the reconcile detects an
		// image/tag/network drift (e.g. after a daemon upgrade) and updates it.
		out[addr] = GatewayRef{Target: addr, ServiceID: gw.ID, Hash: liveGatewayHash(eg, gw)}
	}
	return out, nil
}

// createGateway mints an ephemeral key under the app's egress.tag and
// creates the gateway service for a single target, rolling the key back on
// failure.
func (r *Reconciler) createGateway(ctx context.Context, serviceID string, eg *EgressSpec, t EgressTarget) (GatewayRef, error) {
	if err := r.Limiter.Wait(ctx); err != nil {
		return GatewayRef{}, fmt.Errorf("rate limit: %w", err)
	}
	key, err := r.Ctrl.CreateEphemeralKey(ctx, KeyRequest{
		User:       r.Cfg.Headscale.User,
		Tags:       []string{eg.Tag},
		Ephemeral:  true,
		Reusable:   false,
		Expiration: r.Cfg.Headscale.KeyExpiration,
	})
	if err != nil {
		return GatewayRef{}, fmt.Errorf("mint egress key for %s: %w", t.addr(), err)
	}

	spec := gatewaySpec(eg, t, serviceID, r.Cfg, r.GatewayImage, key.Secret)
	id, err := r.Docker.CreateService(ctx, spec)
	if errors.Is(err, ErrServiceExists) {
		// A gateway with this (stable) name already runs, but adoption missed
		// it — its gatewayForLabel still points at a prior incarnation of the
		// app service (recreated under a new Swarm ID). Adopt it by name and
		// update it in place: the desired spec re-stamps the marker to the
		// current app ID and installs the freshly minted key.
		return r.adoptExistingGateway(ctx, serviceID, eg, t, spec, key)
	}
	if err != nil {
		r.expireOrLog(ctx, key.ID, "rollback after gateway create failure")
		return GatewayRef{}, fmt.Errorf("create gateway for %s target %s: %w", serviceID, t.addr(), err)
	}
	return GatewayRef{
		Target:    t.addr(),
		ServiceID: id,
		KeyID:     key.ID,
		Hash:      targetGatewayHash(eg, t, r.GatewayImage),
	}, nil
}

// adoptExistingGateway reconciles a gateway that already exists under its
// stable name but whose stale gatewayForLabel hid it from adoption. It
// finds the live service by name and updates it in place to spec, which
// re-stamps the marker label to the current app service ID and rotates in
// the freshly minted key. The key was already minted by the caller; on any
// failure it is rolled back so no orphan key survives.
func (r *Reconciler) adoptExistingGateway(ctx context.Context, serviceID string, eg *EgressSpec, t EgressTarget, spec swarm.ServiceSpec, key Key) (GatewayRef, error) {
	gws, err := r.Docker.ListServices(ctx, LabelFilter{Name: spec.Annotations.Name})
	if err != nil {
		r.expireOrLog(ctx, key.ID, "rollback after gateway adopt lookup failure")
		return GatewayRef{}, fmt.Errorf("adopt gateway %s: lookup: %w", spec.Annotations.Name, err)
	}
	if len(gws) == 0 {
		// It vanished between the create collision and this lookup — a plain
		// retry next pass will recreate it cleanly.
		r.expireOrLog(ctx, key.ID, "rollback after gateway adopt vanished")
		return GatewayRef{}, fmt.Errorf("adopt gateway %s: create reported exists but not found on lookup", spec.Annotations.Name)
	}
	existing := gws[0]

	if err := r.Docker.UpdateService(ctx, existing.ID, existing.Version.Index, spec); err != nil {
		r.expireOrLog(ctx, key.ID, "rollback after gateway adopt update failure")
		return GatewayRef{}, fmt.Errorf("adopt gateway %s: update: %w", existing.ID, err)
	}
	r.Log.Info("adopted existing gateway",
		"service_id", serviceID, "gateway_id", existing.ID,
		"name", spec.Annotations.Name, "target", t.addr())
	return GatewayRef{
		Target:    t.addr(),
		ServiceID: existing.ID,
		KeyID:     key.ID,
		Hash:      targetGatewayHash(eg, t, r.GatewayImage),
	}, nil
}

// updateGateway updates a drifted surviving gateway in place (rolling
// update, same service ID) to apply a new spec — e.g. a new image after a
// daemon upgrade. The update restarts the task, and because gateways keep no
// persistent tsnet state (no state volume mounted), the restarted container
// comes up NeedsLogin and must register again — so a fresh ephemeral key is
// minted and the old one rotated out. Swarm rejects stale writes, so it
// inspects for the current version first. A vanished gateway returns
// found=false so the caller recreates instead.
func (r *Reconciler) updateGateway(ctx context.Context, serviceID string, eg *EgressSpec, t EgressTarget, ref GatewayRef) (newRef GatewayRef, found bool, err error) {
	gw, err := r.Docker.InspectService(ctx, ref.ServiceID)
	if errors.Is(err, ErrServiceNotFound) {
		return GatewayRef{}, false, nil
	}
	if err != nil {
		return ref, true, fmt.Errorf("inspect gateway %s: %w", ref.ServiceID, err)
	}

	if err := r.Limiter.Wait(ctx); err != nil {
		return ref, true, fmt.Errorf("rate limit: %w", err)
	}
	key, err := r.Ctrl.CreateEphemeralKey(ctx, KeyRequest{
		User:       r.Cfg.Headscale.User,
		Tags:       []string{eg.Tag},
		Ephemeral:  true,
		Reusable:   false,
		Expiration: r.Cfg.Headscale.KeyExpiration,
	})
	if err != nil {
		return ref, true, fmt.Errorf("mint egress key for %s: %w", t.addr(), err)
	}

	spec := gatewaySpec(eg, t, serviceID, r.Cfg, r.GatewayImage, key.Secret)
	if err := r.Docker.UpdateService(ctx, ref.ServiceID, gw.Version.Index, spec); err != nil {
		r.expireOrLog(ctx, key.ID, "rollback after gateway update failure")
		return ref, true, fmt.Errorf("update gateway %s: %w", ref.ServiceID, err)
	}

	// Update succeeded — rotate out the old key (best effort).
	if ref.KeyID != "" && ref.KeyID != key.ID {
		r.expireOrLog(ctx, ref.KeyID, "rotated egress key on gateway update")
	}
	return GatewayRef{
		Target:    t.addr(),
		ServiceID: ref.ServiceID,
		KeyID:     key.ID,
		Hash:      targetGatewayHash(eg, t, r.GatewayImage),
	}, true, nil
}

// removeGateway deletes one gateway service and expires its key (best
// effort, rate-limited). A zero KeyID (an adopted gateway) is tolerated.
func (r *Reconciler) removeGateway(ctx context.Context, serviceID string, ref GatewayRef) error {
	if err := r.Limiter.Wait(ctx); err != nil {
		return err
	}
	if err := r.Docker.RemoveService(ctx, ref.ServiceID); err != nil && !errors.Is(err, ErrServiceNotFound) {
		r.Log.Warn("remove gateway", "service_id", serviceID,
			"gateway_id", ref.ServiceID, "target", ref.Target, "err", err)
		return fmt.Errorf("remove gateway %s: %w", ref.ServiceID, err)
	}
	if ref.KeyID != "" {
		if err := r.Limiter.Wait(ctx); err != nil {
			return err
		}
		r.expireOrLog(ctx, ref.KeyID, "removed egress target")
	}
	return nil
}

// sortedGateways flattens the keyed gateway set into a slice ordered by
// target addr, so the stored Entry is stable across reconciles.
func sortedGateways(m map[string]GatewayRef) []GatewayRef {
	out := make([]GatewayRef, 0, len(m))
	for _, ref := range m {
		out = append(out, ref)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Target < out[j].Target })
	return out
}

// findGateways returns every gateway service fronting appServiceID, matched
// on the gatewayForLabel marker, so a daemon restart adopts the whole set
// instead of creating duplicates.
func (r *Reconciler) findGateways(ctx context.Context, appServiceID string) ([]swarm.Service, error) {
	gws, err := r.Docker.ListServices(ctx, LabelFilter{Key: gatewayForLabel, Value: appServiceID})
	if err != nil {
		return nil, err
	}
	out := gws[:0]
	for _, gw := range gws {
		if gw.Spec.Labels[gatewayForLabel] == appServiceID {
			out = append(out, gw)
		}
	}
	return out, nil
}

// gatewayTargetAddr reads the single target addr a gateway fronts from its
// TAILSWARM_GATEWAY_TARGETS env, used to re-key an adopted gateway back to
// its target. Returns "" if the env is absent.
func gatewayTargetAddr(gw swarm.Service) string {
	cs := gw.Spec.TaskTemplate.ContainerSpec
	if cs == nil {
		return ""
	}
	prefix := envGatewayTargets + "="
	for _, e := range cs.Env {
		if strings.HasPrefix(e, prefix) {
			return strings.TrimPrefix(e, prefix)
		}
	}
	return ""
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
	if len(prev.Gateways) == 0 {
		r.Store.Delete(serviceID)
	} else {
		r.Store.Put(serviceID, prev)
	}
	return firstErr
}

// teardownEgress removes every egress gateway service for serviceID and
// expires their preauth keys, leaving any ingress proxy untouched. It clears
// the gateway fields from the store entry (and deletes the entry if no
// ingress artefact remains). Each step is best-effort; the first error wins.
func (r *Reconciler) teardownEgress(ctx context.Context, serviceID string) error {
	prev, ok := r.Store.Get(serviceID)
	if !ok || len(prev.Gateways) == 0 {
		return nil
	}

	var firstErr error
	for _, ref := range prev.Gateways {
		if err := r.removeGateway(ctx, serviceID, ref); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	prev.Gateways = nil
	prev.GatewayHash = ""
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
