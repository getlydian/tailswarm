package tailswarm

import (
	"context"
	"errors"
	"testing"

	"github.com/docker/docker/api/types/swarm"
)

const testGatewayImage = "ghcr.io/getlydian/tailswarm:test"

// testEgressReconciler is testReconciler with a gateway image configured,
// so the egress path actually creates gateways instead of erroring.
func testEgressReconciler(t *testing.T) (*Reconciler, *fakeDocker, *fakeController) {
	t.Helper()
	d := newFakeDocker()
	c := newFakeController()
	pf := &fakeProxyFactory{}
	cfg := Config{
		Headscale: HeadscaleConfig{URL: "https://hs", User: "swarm", KeyExpiration: testKeyExpiry},
		Tsnet:     TsnetConfig{StateDir: t.TempDir()},
		Reconcile: ReconcileConfig{FullResyncInterval: 1, RateLimitRPS: 100},
		Network:   defaultOverlay,
	}
	r := NewReconciler(d, c, NewStore(), cfg)
	r.GatewayImage = testGatewayImage
	r.NewProxy = pf.factory()
	return r, d, c
}

// egressService builds a swarm.Service opted into egress (only) with the
// given targets label. No ports — egress needs none.
func egressService(name, targets string) swarm.Service {
	return swarm.Service{
		Spec: swarm.ServiceSpec{
			Annotations: swarm.Annotations{
				Name: name,
				Labels: map[string]string{
					stackLabel:                 "admin",
					"tailswarm.egress.enable":  "true",
					"tailswarm.egress.targets": targets,
				},
			},
		},
	}
}

func TestReconcileCreatesGateway(t *testing.T) {
	r, d, c := testEgressReconciler(t)
	id := d.addService(egressService("admin_phpmyadmin", "db-mysql:3306"))

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if len(d.created) != 1 {
		t.Fatalf("gateways created: got %d want 1", len(d.created))
	}
	spec := d.created[0]
	if spec.TaskTemplate.ContainerSpec.Image != testGatewayImage {
		t.Errorf("gateway image: %q", spec.TaskTemplate.ContainerSpec.Image)
	}
	if got := spec.Labels[gatewayForLabel]; got != id {
		t.Errorf("marker label: got %q want %q", got, id)
	}
	if _, isApp := spec.Labels["tailswarm.enable"]; isApp {
		t.Error("gateway must not carry tailswarm.enable")
	}
	nets := spec.TaskTemplate.Networks
	if len(nets) != 1 || nets[0].Target != defaultOverlay {
		t.Fatalf("gateway network: %+v", nets)
	}
	if len(nets[0].Aliases) != 1 || nets[0].Aliases[0] != "db-mysql" {
		t.Errorf("gateway aliases: %+v", nets[0].Aliases)
	}

	// Key minted under the egress tag.
	if len(c.created) != 1 {
		t.Fatalf("keys minted: %d", len(c.created))
	}
	if got := c.created[0].Tags; len(got) != 1 || got[0] != "tag:swarm-phpmyadmin" {
		t.Errorf("egress key tags: %+v", got)
	}

	e, ok := r.Store.Get(id)
	if !ok || e.GatewayServiceID == "" || e.GatewayHash == "" || e.GatewayKeyID == "" {
		t.Errorf("store gateway bookkeeping not set: %+v", e)
	}
}

func TestReconcileGatewayNoOpOnUnchanged(t *testing.T) {
	r, d, c := testEgressReconciler(t)
	id := d.addService(egressService("admin_phpmyadmin", "db-mysql:3306"))

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if len(d.created) != 1 {
		t.Errorf("gateways: got %d want 1 (second reconcile should no-op)", len(d.created))
	}
	if len(d.updated) != 0 {
		t.Errorf("unexpected updates: %+v", d.updated)
	}
	if len(c.created) != 1 {
		t.Errorf("keys: got %d want 1", len(c.created))
	}
}

func TestReconcileGatewayUpdatesOnTargetChange(t *testing.T) {
	r, d, c := testEgressReconciler(t)
	id := d.addService(egressService("admin_phpmyadmin", "db-mysql:3306"))

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}

	// Add a second target. Replace the stored service.
	updated := egressService("admin_phpmyadmin", "db-mysql:3306, analytics-mysql:3306")
	updated.ID = id
	updated.Version.Index = 2
	d.mu.Lock()
	d.services[id] = &updated
	d.mu.Unlock()

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}

	if len(d.created) != 1 {
		t.Errorf("gateways created: got %d want 1 (update, not recreate)", len(d.created))
	}
	if len(d.updated) != 1 {
		t.Fatalf("gateways updated: got %d want 1", len(d.updated))
	}
	// Old egress key (key-1) expired on rotation.
	if len(c.expired) != 1 || c.expired[0] != "key-1" {
		t.Errorf("rotated key not expired: %+v", c.expired)
	}

	e, _ := r.Store.Get(id)
	gw := d.services[e.GatewayServiceID]
	aliases := gw.Spec.TaskTemplate.Networks[0].Aliases
	if len(aliases) != 2 {
		t.Errorf("updated aliases: %+v", aliases)
	}
}

func TestReconcileGatewayTeardownOnDisable(t *testing.T) {
	r, d, c := testEgressReconciler(t)
	id := d.addService(egressService("admin_phpmyadmin", "db-mysql:3306"))

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	e, _ := r.Store.Get(id)
	gwID := e.GatewayServiceID

	d.mu.Lock()
	d.services[id].Spec.Labels["tailswarm.egress.enable"] = "false"
	d.services[id].Version.Index = 2
	d.mu.Unlock()

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if len(d.removed) != 1 || d.removed[0] != gwID {
		t.Errorf("gateway not removed: %+v", d.removed)
	}
	if len(c.expired) != 1 || c.expired[0] != "key-1" {
		t.Errorf("egress key not expired: %+v", c.expired)
	}
	if _, ok := r.Store.Get(id); ok {
		t.Error("store entry not deleted after egress teardown")
	}
}

func TestReconcileGatewayTeardownOnDelete(t *testing.T) {
	r, d, c := testEgressReconciler(t)
	id := d.addService(egressService("admin_phpmyadmin", "db-mysql:3306"))

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	e, _ := r.Store.Get(id)
	gwID := e.GatewayServiceID

	d.markMissing(id)
	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if len(d.removed) != 1 || d.removed[0] != gwID {
		t.Errorf("gateway not removed on delete: %+v", d.removed)
	}
	if len(c.expired) != 1 {
		t.Errorf("egress key not expired: %+v", c.expired)
	}
}

// When a service drops its egress labels but keeps ingress, the gateway is
// torn down while the ingress proxy survives.
func TestReconcileEgressRemovedKeepsIngress(t *testing.T) {
	r, d, c := testEgressReconciler(t)

	svc := enabledService("admin_phpmyadmin", 80)
	svc.Spec.Labels["tailswarm.egress.enable"] = "true"
	svc.Spec.Labels["tailswarm.egress.targets"] = "db-mysql:3306"
	id := d.addService(svc)

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	e, _ := r.Store.Get(id)
	if e.Proxy == nil || e.GatewayServiceID == "" {
		t.Fatalf("expected both ingress and egress artefacts: %+v", e)
	}
	gwID := e.GatewayServiceID

	// Drop egress labels, keep ingress.
	d.mu.Lock()
	delete(d.services[id].Spec.Labels, "tailswarm.egress.enable")
	delete(d.services[id].Spec.Labels, "tailswarm.egress.targets")
	d.services[id].Version.Index = 2
	d.mu.Unlock()

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}

	if len(d.removed) != 1 || d.removed[0] != gwID {
		t.Errorf("gateway not removed: %+v", d.removed)
	}
	e, ok := r.Store.Get(id)
	if !ok {
		t.Fatal("store entry deleted but ingress should remain")
	}
	if e.Proxy == nil {
		t.Error("ingress proxy torn down when only egress was removed")
	}
	if e.GatewayServiceID != "" {
		t.Errorf("gateway fields not cleared: %+v", e)
	}
	// The egress key was expired; the ingress key was not.
	if len(c.expired) != 1 {
		t.Errorf("expected exactly the egress key expired: %+v", c.expired)
	}
}

func TestReconcileEgressMissingImageErrors(t *testing.T) {
	r, d, _ := testEgressReconciler(t)
	r.GatewayImage = ""
	id := d.addService(egressService("admin_phpmyadmin", "db-mysql:3306"))

	err := r.Reconcile(context.Background(), id)
	if !errors.Is(err, errGatewayImageUnset) {
		t.Fatalf("got %v want errGatewayImageUnset", err)
	}
	if len(d.created) != 0 {
		t.Errorf("no gateway should be created: %+v", d.created)
	}
}

// After a daemon restart the store is empty but the gateway still runs;
// the reconciler must adopt it by marker label, not create a duplicate.
func TestReconcileGatewayAdoptedAfterRestart(t *testing.T) {
	r, d, _ := testEgressReconciler(t)
	id := d.addService(egressService("admin_phpmyadmin", "db-mysql:3306"))

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	e, _ := r.Store.Get(id)
	gwID := e.GatewayServiceID

	// Simulate a restart: drop all in-memory state but keep the gateway
	// service alive on the swarm.
	r.Store.Delete(id)

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if len(d.created) != 1 {
		t.Errorf("gateway recreated instead of adopted: created=%d", len(d.created))
	}
	if len(d.updated) != 1 {
		t.Errorf("adopted gateway not updated in place: updated=%+v", d.updated)
	}
	e2, ok := r.Store.Get(id)
	if !ok || e2.GatewayServiceID != gwID {
		t.Errorf("adopted gateway id mismatch: got %q want %q", e2.GatewayServiceID, gwID)
	}
}

func TestReconcileGatewayCreateFailureExpiresKey(t *testing.T) {
	r, d, c := testEgressReconciler(t)
	d.errCreate = errInjected
	id := d.addService(egressService("admin_phpmyadmin", "db-mysql:3306"))

	err := r.Reconcile(context.Background(), id)
	if err == nil {
		t.Fatal("expected error")
	}
	if len(c.created) != 1 {
		t.Errorf("keys minted: %d", len(c.created))
	}
	if len(c.expired) != 1 || c.expired[0] != "key-1" {
		t.Errorf("create-failure rollback didn't expire key: %+v", c.expired)
	}
	if _, ok := r.Store.Get(id); ok {
		t.Error("no store entry should persist after failed create")
	}
}
