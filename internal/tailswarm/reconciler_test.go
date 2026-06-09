package tailswarm

import (
	"context"
	"testing"

	"github.com/docker/docker/api/types/swarm"
)

// testReconciler builds a fully-wired Reconciler with fake dependencies.
func testReconciler(t *testing.T) (*Reconciler, *fakeDocker, *fakeController, *fakeProxyFactory) {
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
	r.NewProxy = pf.factory()
	return r, d, c, pf
}

const testKeyExpiry = 60 * 1_000_000_000 // 60s as a Duration

// enabledService returns a swarm.Service with the labels and ports
// needed to make Reconcile bring up a proxy.
func enabledService(name string, port uint32) swarm.Service {
	return swarm.Service{
		Spec: swarm.ServiceSpec{
			Annotations: swarm.Annotations{
				Name: name,
				Labels: map[string]string{
					stackLabel:         "billing",
					"tailswarm.enable": "true",
				},
			},
			EndpointSpec: &swarm.EndpointSpec{
				Ports: []swarm.PortConfig{
					{Protocol: swarm.PortConfigProtocolTCP, TargetPort: port},
				},
			},
		},
	}
}

func TestReconcileCreatesProxy(t *testing.T) {
	r, d, c, pf := testReconciler(t)
	id := d.addService(enabledService("billing_api", 8080))

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if got := len(c.created); got != 1 {
		t.Fatalf("keys minted: got %d want 1", got)
	}
	if got := len(pf.snapshot()); got != 1 {
		t.Fatalf("proxies: got %d want 1", got)
	}
	got := pf.snapshot()[0].cfg
	if got.Hostname != "billing-api" {
		t.Errorf("hostname: %q", got.Hostname)
	}
	if got.Target != "billing_api" {
		t.Errorf("target: %q", got.Target)
	}
	if len(got.Ports) != 1 || got.Ports[0].Target != 8080 {
		t.Errorf("ports: %+v", got.Ports)
	}
	if got.AuthKey == "" {
		t.Errorf("authkey not threaded into proxy config")
	}
}

func TestReconcileNoOpOnUnchangedSpec(t *testing.T) {
	r, d, c, pf := testReconciler(t)
	id := d.addService(enabledService("billing_api", 8080))

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if got := len(pf.snapshot()); got != 1 {
		t.Errorf("proxies: got %d want 1 (second reconcile should be a no-op)", got)
	}
	if got := len(c.created); got != 1 {
		t.Errorf("keys: got %d want 1", got)
	}
}

func TestReconcileRotatesOnSpecChange(t *testing.T) {
	r, d, c, pf := testReconciler(t)
	id := d.addService(enabledService("billing_api", 8080))

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}

	// Change the service: bump the port. Replace the stored service.
	updated := enabledService("billing_api", 9090)
	updated.ID = id
	updated.Version.Index = 2
	d.mu.Lock()
	d.services[id] = &updated
	d.mu.Unlock()

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	handles := pf.snapshot()
	if len(handles) != 2 {
		t.Fatalf("proxies: got %d want 2", len(handles))
	}
	if !handles[0].closed() {
		t.Errorf("first proxy not closed after rotation")
	}
	if handles[1].closed() {
		t.Errorf("second proxy unexpectedly closed")
	}
	if len(c.expired) != 1 || c.expired[0] != "key-1" {
		t.Errorf("expired keys: %+v", c.expired)
	}
}

func TestReconcileTeardownOnDelete(t *testing.T) {
	r, d, c, pf := testReconciler(t)
	id := d.addService(enabledService("billing_api", 8080))

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	d.markMissing(id)
	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if !pf.snapshot()[0].closed() {
		t.Error("proxy not closed on teardown")
	}
	if len(c.expired) != 1 {
		t.Errorf("expired keys: %+v", c.expired)
	}
	if _, ok := r.Store.Get(id); ok {
		t.Error("entry not deleted")
	}
}

func TestReconcileTeardownOnDisable(t *testing.T) {
	r, d, c, pf := testReconciler(t)
	id := d.addService(enabledService("billing_api", 8080))

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}

	d.mu.Lock()
	d.services[id].Spec.Labels["tailswarm.enable"] = "false"
	d.services[id].Version.Index = 2
	d.mu.Unlock()

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if !pf.snapshot()[0].closed() {
		t.Error("proxy not closed on disable")
	}
	if len(c.expired) != 1 {
		t.Errorf("expired: %+v", c.expired)
	}
}

func TestReconcileExpiresKeyOnProxyStartFailure(t *testing.T) {
	r, d, c, pf := testReconciler(t)
	pf.errStart = errInjected
	id := d.addService(enabledService("billing_api", 8080))

	err := r.Reconcile(context.Background(), id)
	if err == nil {
		t.Fatal("expected error")
	}
	if len(c.created) != 1 {
		t.Errorf("keys minted: %d", len(c.created))
	}
	if len(c.expired) != 1 || c.expired[0] != "key-1" {
		t.Errorf("rollback didn't expire key: expired=%+v", c.expired)
	}
}

func TestReconcileServiceMissingTeardownNoState(t *testing.T) {
	r, d, _, _ := testReconciler(t)
	d.markMissing("svc-unknown")
	// Nothing tracked; should be a no-op, no error.
	if err := r.Reconcile(context.Background(), "svc-unknown"); err != nil {
		t.Fatal(err)
	}
}

func TestCloseAllShutsDownProxies(t *testing.T) {
	r, d, _, pf := testReconciler(t)
	id1 := d.addService(enabledService("api1", 80))
	id2 := d.addService(enabledService("api2", 80))
	if err := r.Reconcile(context.Background(), id1); err != nil {
		t.Fatal(err)
	}
	if err := r.Reconcile(context.Background(), id2); err != nil {
		t.Fatal(err)
	}
	r.CloseAll()
	for i, h := range pf.snapshot() {
		if !h.closed() {
			t.Errorf("proxy %d not closed", i)
		}
	}
	if len(r.Store.Keys()) != 0 {
		t.Errorf("store not drained")
	}
}

func TestQueueDedupes(t *testing.T) {
	q := NewQueue(2, 4)
	for i := 0; i < 5; i++ {
		q.Enqueue("a")
	}
	if got := q.pendingCount(); got != 1 {
		t.Fatalf("pendingCount: %d want 1", got)
	}
}
