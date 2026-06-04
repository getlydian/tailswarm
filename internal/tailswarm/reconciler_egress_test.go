package tailswarm

import (
	"context"
	"errors"
	"sort"
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

// gatewayAliases returns the sorted set of all overlay aliases across an
// entry's gateways — each gateway holds exactly one. Used to assert the
// union of targets a service fronts.
func gatewayAliases(d *fakeDocker, e Entry) []string {
	var out []string
	for _, ref := range e.Gateways {
		gw := d.services[ref.ServiceID]
		out = append(out, gw.Spec.TaskTemplate.Networks[0].Aliases...)
	}
	sort.Strings(out)
	return out
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
	if !ok || len(e.Gateways) != 1 || e.GatewayHash == "" {
		t.Fatalf("store gateway bookkeeping not set: %+v", e)
	}
	if g := e.Gateways[0]; g.Target != "db-mysql:3306" || g.ServiceID == "" || g.KeyID == "" {
		t.Errorf("gateway ref not fully set: %+v", g)
	}
}

// Each egress target gets its own gateway service, so multiple same-port
// targets do not collide on bind (the production crash this fixes). This is
// the regression guard for the thanos-query case (5 targets on :10901).
func TestReconcileSamePortTargetsGetSeparateGateways(t *testing.T) {
	r, d, c := testEgressReconciler(t)
	const targets = "thanos-store-lydian:10901, thanos-sidecar-prd-lydian:10901, thanos-query-kulmin:10901"
	id := d.addService(egressService("ops_thanos-query", targets))

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if len(d.created) != 3 {
		t.Fatalf("gateways created: got %d want 3 (one per same-port target)", len(d.created))
	}
	// Distinct service names and hostnames per gateway — the duplicate
	// hostname/bind collision is exactly the bug.
	names := map[string]bool{}
	hostnames := map[string]bool{}
	for _, spec := range d.created {
		names[spec.Name] = true
		hostnames[parseEnvList(spec.TaskTemplate.ContainerSpec.Env)[envGatewayHostname]] = true
		// Each gateway forwards exactly one target.
		if got := parseEnvList(spec.TaskTemplate.ContainerSpec.Env)[envGatewayTargets]; got == "" ||
			len(spec.TaskTemplate.Networks[0].Aliases) != 1 {
			t.Errorf("gateway %q is not single-target: targets=%q aliases=%+v",
				spec.Name, got, spec.TaskTemplate.Networks[0].Aliases)
		}
	}
	if len(names) != 3 {
		t.Errorf("gateway service names not distinct: %v", names)
	}
	if len(hostnames) != 3 {
		t.Errorf("gateway hostnames not distinct: %v", hostnames)
	}
	// One ephemeral key per gateway, all under the same egress tag.
	if len(c.created) != 3 {
		t.Errorf("keys minted: got %d want 3", len(c.created))
	}
	e, _ := r.Store.Get(id)
	if len(e.Gateways) != 3 {
		t.Errorf("tracked gateways: got %d want 3", len(e.Gateways))
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
	if len(d.removed) != 0 {
		t.Errorf("unexpected removals: %+v", d.removed)
	}
	if len(c.created) != 1 {
		t.Errorf("keys: got %d want 1", len(c.created))
	}
}

// When the daemon's gateway image changes (an upgrade, e.g. rc.8 -> rc.9),
// a surviving gateway is updated in place to the new image and its key is
// rotated (the restart re-registers, since gateways keep no persistent tsnet
// state). This is the regression guard for gateways stuck on the old image.
func TestReconcileGatewayUpdatesOnImageDrift(t *testing.T) {
	r, d, c := testEgressReconciler(t)
	id := d.addService(egressService("admin_phpmyadmin", "db-mysql:3306"))

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	before, _ := r.Store.Get(id)
	gwID := before.Gateways[0].ServiceID
	oldKey := before.Gateways[0].KeyID

	// Daemon upgraded: a new gateway image is discovered.
	r.GatewayImage = "ghcr.io/getlydian/tailswarm:rc.9"

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}

	if len(d.created) != 1 {
		t.Errorf("gateway recreated instead of updated: created=%d", len(d.created))
	}
	if len(d.updated) != 1 || d.updated[0] != gwID {
		t.Fatalf("gateway not updated in place: updated=%+v want [%s]", d.updated, gwID)
	}
	// The updated service carries the new image.
	if got := d.services[gwID].Spec.TaskTemplate.ContainerSpec.Image; got != "ghcr.io/getlydian/tailswarm:rc.9" {
		t.Errorf("gateway image after update = %q, want rc.9", got)
	}
	// Old key rotated out; a fresh key minted for the restart.
	if len(c.expired) != 1 || c.expired[0] != oldKey {
		t.Errorf("old key not rotated: expired=%+v want [%s]", c.expired, oldKey)
	}
	e, _ := r.Store.Get(id)
	if e.Gateways[0].ServiceID != gwID {
		t.Errorf("service ID changed on in-place update: %q", e.Gateways[0].ServiceID)
	}
}

// After a daemon upgrade the store is empty but the old-image gateway still
// runs; the reconciler adopts it by marker label, sees the image drift via
// the reconstructed live hash, and updates it in place — without recreating.
func TestReconcileAdoptedGatewayUpdatedOnImageDrift(t *testing.T) {
	r, d, c := testEgressReconciler(t)
	id := d.addService(egressService("admin_phpmyadmin", "db-mysql:3306"))

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	before, _ := r.Store.Get(id)
	gwID := before.Gateways[0].ServiceID

	// Simulate an upgrade: drop in-memory state, bump the discovered image.
	r.Store.Delete(id)
	r.GatewayImage = "ghcr.io/getlydian/tailswarm:rc.9"

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}

	if len(d.created) != 1 {
		t.Errorf("adopted gateway recreated instead of updated: created=%d", len(d.created))
	}
	if len(d.updated) != 1 || d.updated[0] != gwID {
		t.Fatalf("adopted gateway not updated in place: updated=%+v want [%s]", d.updated, gwID)
	}
	if got := d.services[gwID].Spec.TaskTemplate.ContainerSpec.Image; got != "ghcr.io/getlydian/tailswarm:rc.9" {
		t.Errorf("adopted gateway image after update = %q, want rc.9", got)
	}
	// A fresh key was minted for the restart (the adopted gateway had no
	// tracked key to rotate).
	if len(c.created) != 2 {
		t.Errorf("keys minted: got %d want 2 (initial + update)", len(c.created))
	}
}

// Adding a target creates a new gateway and leaves the surviving gateway
// (and its key) untouched — no churn on unrelated targets.
func TestReconcileGatewayAddsTargetLeavesSurvivors(t *testing.T) {
	r, d, c := testEgressReconciler(t)
	id := d.addService(egressService("admin_phpmyadmin", "db-mysql:3306"))

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	before, _ := r.Store.Get(id)
	survivorSvc := before.Gateways[0].ServiceID

	// Add a second target.
	updated := egressService("admin_phpmyadmin", "db-mysql:3306, analytics-mysql:3306")
	updated.ID = id
	updated.Version.Index = 2
	d.mu.Lock()
	d.services[id] = &updated
	d.mu.Unlock()

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}

	if len(d.created) != 2 {
		t.Errorf("gateways created: got %d want 2 (one new per added target)", len(d.created))
	}
	if len(d.removed) != 0 {
		t.Errorf("survivor wrongly removed: %+v", d.removed)
	}
	// No key rotation: the survivor keeps key-1; only the new target mints.
	if len(c.expired) != 0 {
		t.Errorf("survivor key wrongly expired: %+v", c.expired)
	}
	if len(c.created) != 2 {
		t.Errorf("keys minted: got %d want 2", len(c.created))
	}

	e, _ := r.Store.Get(id)
	if want := []string{"analytics-mysql", "db-mysql"}; !equalStrings(gatewayAliases(d, e), want) {
		t.Errorf("aliases across gateways = %v, want %v", gatewayAliases(d, e), want)
	}
	// Survivor still tracked with its original service ID.
	found := false
	for _, g := range e.Gateways {
		if g.ServiceID == survivorSvc {
			found = true
		}
	}
	if !found {
		t.Errorf("survivor gateway %q dropped from store: %+v", survivorSvc, e.Gateways)
	}
}

// Dropping a target removes only that gateway and expires only its key;
// the surviving gateway is left running.
func TestReconcileGatewayDropsTarget(t *testing.T) {
	r, d, c := testEgressReconciler(t)
	id := d.addService(egressService("admin_phpmyadmin", "db-mysql:3306, analytics-mysql:3306"))

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	before, _ := r.Store.Get(id)
	var droppedSvc, droppedKey string
	for _, g := range before.Gateways {
		if g.Target == "analytics-mysql:3306" {
			droppedSvc, droppedKey = g.ServiceID, g.KeyID
		}
	}

	// Drop analytics-mysql.
	updated := egressService("admin_phpmyadmin", "db-mysql:3306")
	updated.ID = id
	updated.Version.Index = 2
	d.mu.Lock()
	d.services[id] = &updated
	d.mu.Unlock()

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}

	if len(d.removed) != 1 || d.removed[0] != droppedSvc {
		t.Errorf("dropped gateway not removed: removed=%+v want [%s]", d.removed, droppedSvc)
	}
	if len(c.expired) != 1 || c.expired[0] != droppedKey {
		t.Errorf("dropped key not expired: expired=%+v want [%s]", c.expired, droppedKey)
	}
	e, _ := r.Store.Get(id)
	if want := []string{"db-mysql"}; !equalStrings(gatewayAliases(d, e), want) {
		t.Errorf("aliases after drop = %v, want %v", gatewayAliases(d, e), want)
	}
}

func TestReconcileGatewayTeardownOnDisable(t *testing.T) {
	r, d, c := testEgressReconciler(t)
	id := d.addService(egressService("admin_phpmyadmin", "db-mysql:3306"))

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	e, _ := r.Store.Get(id)
	gwID := e.Gateways[0].ServiceID

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
	gwID := e.Gateways[0].ServiceID

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
	if e.Proxy == nil || len(e.Gateways) != 1 {
		t.Fatalf("expected both ingress and egress artefacts: %+v", e)
	}
	gwID := e.Gateways[0].ServiceID

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
	if len(e.Gateways) != 0 {
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

// After a daemon restart the store is empty but the gateways still run; the
// reconciler must adopt them by marker label, not create duplicates.
func TestReconcileGatewayAdoptedAfterRestart(t *testing.T) {
	r, d, _ := testEgressReconciler(t)
	id := d.addService(egressService("admin_phpmyadmin", "db-mysql:3306, analytics-mysql:3306"))

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	before, _ := r.Store.Get(id)
	wantSvcs := map[string]bool{}
	for _, g := range before.Gateways {
		wantSvcs[g.ServiceID] = true
	}

	// Simulate a restart: drop all in-memory state but keep the gateway
	// services alive on the swarm.
	r.Store.Delete(id)

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if len(d.created) != 2 {
		t.Errorf("gateways recreated instead of adopted: created=%d want 2 (the original create)", len(d.created))
	}
	if len(d.removed) != 0 {
		t.Errorf("adopted gateways wrongly removed: %+v", d.removed)
	}
	e2, ok := r.Store.Get(id)
	if !ok || len(e2.Gateways) != 2 {
		t.Fatalf("adopted gateway set wrong: %+v", e2.Gateways)
	}
	for _, g := range e2.Gateways {
		if !wantSvcs[g.ServiceID] {
			t.Errorf("adopted gateway id %q not among originals %v", g.ServiceID, wantSvcs)
		}
	}
}

// Migration: a legacy single gateway fronting multiple targets (the
// pre-per-target format, all targets in one TAILSWARM_GATEWAY_TARGETS) is
// adopted by marker label, found to match no single desired target, removed,
// and replaced by one gateway per target.
func TestReconcileLegacyMultiTargetGatewayMigrated(t *testing.T) {
	r, d, _ := testEgressReconciler(t)
	id := d.addService(egressService("admin_phpmyadmin", "db-a:3306, db-b:3306"))

	// Inject a legacy gateway: one service, both targets, marker label set.
	legacy := swarm.ServiceSpec{
		Annotations: swarm.Annotations{
			Name:   "tsgw-admin-phpmyadmin-egress",
			Labels: map[string]string{gatewayForLabel: id},
		},
		TaskTemplate: swarm.TaskSpec{
			ContainerSpec: &swarm.ContainerSpec{
				Image: testGatewayImage,
				Env:   []string{envGatewayTargets + "=db-a:3306,db-b:3306"},
			},
			Networks: []swarm.NetworkAttachmentConfig{
				{Target: defaultOverlay, Aliases: []string{"db-a", "db-b"}},
			},
		},
	}
	legacyID, err := d.CreateService(context.Background(), legacy)
	if err != nil {
		t.Fatal(err)
	}
	// Reset the create log so the assertion counts only reconcile-created
	// gateways, not this fixture.
	d.created = nil

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}

	if len(d.removed) != 1 || d.removed[0] != legacyID {
		t.Errorf("legacy gateway not removed: removed=%+v want [%s]", d.removed, legacyID)
	}
	if len(d.created) != 2 {
		t.Errorf("per-target gateways created: got %d want 2", len(d.created))
	}
	e, _ := r.Store.Get(id)
	if want := []string{"db-a", "db-b"}; !equalStrings(gatewayAliases(d, e), want) {
		t.Errorf("aliases after migration = %v, want %v", gatewayAliases(d, e), want)
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
	// No gateway was created, so nothing is tracked — but the store entry may
	// exist with an empty gateway set; the next pass retries.
	if e, ok := r.Store.Get(id); ok && len(e.Gateways) != 0 {
		t.Errorf("failed create left a tracked gateway: %+v", e.Gateways)
	}
}

func equalStrings(a, b []string) bool {
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
