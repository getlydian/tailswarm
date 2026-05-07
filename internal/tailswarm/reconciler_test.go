package tailswarm

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types/swarm"
	"golang.org/x/time/rate"
)

// reconcilerHarness wires up a Reconciler against fakes with sane
// defaults for tests. Individual tests adjust fields after construction.
type reconcilerHarness struct {
	docker *fakeDocker
	ctrl   *fakeController
	store  *Store
	rec    *Reconciler
}

func newHarness(t *testing.T) *reconcilerHarness {
	t.Helper()
	d := newFakeDocker()
	c := newFakeController()
	s := NewStore()
	cfg := Config{
		LabelNamespace: "tailswarm",
		Headscale: HeadscaleConfig{
			URL:           "https://headscale.internal",
			User:          "swarm",
			KeyExpiration: 5 * time.Minute,
		},
		Sidecar: SidecarConfig{
			Image: "tailscale/tailscale:v1.78",
		},
	}
	r := NewReconciler(d, c, s, cfg)
	// Effectively unlimited rate so tests don't sleep.
	r.Limiter = rate.NewLimiter(rate.Inf, 1)
	r.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	return &reconcilerHarness{docker: d, ctrl: c, store: s, rec: r}
}

// addEnabledTarget seeds a labeled target service plus its overlay
// network and returns the service ID.
func (h *reconcilerHarness) addEnabledTarget(_ *testing.T, name, stack string, extra map[string]string) string {
	labels := map[string]string{"tailswarm.enable": "true"}
	for k, v := range extra {
		labels[k] = v
	}
	if stack != "" {
		labels[stackLabel] = stack
	}
	netID := "net-" + name
	h.docker.addNetwork(swarm.Network{
		ID: netID,
		Spec: swarm.NetworkSpec{
			Annotations: swarm.Annotations{Name: "app"},
		},
		DriverState: swarm.Driver{Name: "overlay"},
	})
	svc := swarm.Service{
		Meta: swarm.Meta{Version: swarm.Version{Index: 1}},
		Spec: swarm.ServiceSpec{
			Annotations: swarm.Annotations{
				Name:   name,
				Labels: labels,
			},
			TaskTemplate: swarm.TaskSpec{
				Networks: []swarm.NetworkAttachmentConfig{{Target: netID}},
			},
		},
	}
	return h.docker.addService(svc)
}

func TestReconcile_CreateFromScratch(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	id := h.addEnabledTarget(t, "billing_api", "billing", nil)

	if err := h.rec.Reconcile(context.Background(), id); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	entry, ok := h.store.Get(id)
	if !ok {
		t.Fatalf("expected store entry for %s", id)
	}
	if entry.SidecarID == "" {
		t.Fatalf("expected SidecarID set, got %+v", entry)
	}
	if entry.PreAuthKeyID == "" {
		t.Fatalf("expected PreAuthKeyID set, got %+v", entry)
	}
	if entry.LastSpecHash == "" {
		t.Fatalf("expected LastSpecHash set")
	}

	// Sidecar created carries owner labels.
	created := findCall(h.docker.callLog(), dCallCreate)
	if created == nil {
		t.Fatalf("expected a CreateService call")
	}
	if created.Spec.Labels[ownerLabelManaged] != "true" {
		t.Fatalf("missing managed label: %+v", created.Spec.Labels)
	}
	if created.Spec.Labels[ownerLabelTarget] != id {
		t.Fatalf("target label mismatch: %s", created.Spec.Labels[ownerLabelTarget])
	}
	if created.Spec.Env["TS_AUTHKEY"] == "" {
		t.Fatalf("expected TS_AUTHKEY set")
	}

	// Exactly one ephemeral key minted.
	keys := 0
	for _, c := range h.ctrl.callLog() {
		if c.Kind == callCreate {
			keys++
		}
	}
	if keys != 1 {
		t.Fatalf("expected 1 CreateEphemeralKey call, got %d", keys)
	}
}

func TestReconcile_NoOpWhenSpecUnchanged(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	id := h.addEnabledTarget(t, "api", "stack", nil)

	if err := h.rec.Reconcile(context.Background(), id); err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	keysBefore := countCalls(h.ctrl.callLog(), callCreate)
	createsBefore := countDockerCalls(h.docker.callLog(), dCallCreate)

	// Second reconcile with no change should not touch Docker or the
	// controller.
	if err := h.rec.Reconcile(context.Background(), id); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	if got := countCalls(h.ctrl.callLog(), callCreate); got != keysBefore {
		t.Fatalf("expected no new keys, before=%d after=%d", keysBefore, got)
	}
	if got := countDockerCalls(h.docker.callLog(), dCallCreate); got != createsBefore {
		t.Fatalf("expected no new CreateService, before=%d after=%d", createsBefore, got)
	}
}

func TestReconcile_LabelChangeUpdatesAndExpiresOldKey(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	id := h.addEnabledTarget(t, "api", "stack", nil)

	if err := h.rec.Reconcile(context.Background(), id); err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	first, _ := h.store.Get(id)

	// Bump the target's hostname and version: triggers a fresh plan.
	h.docker.mu.Lock()
	tgt := h.docker.services[id]
	tgt.Spec.Annotations.Labels["tailswarm.hostname"] = "renamed"
	tgt.Version.Index++
	h.docker.mu.Unlock()

	if err := h.rec.Reconcile(context.Background(), id); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}

	second, _ := h.store.Get(id)
	if second.SidecarID != first.SidecarID {
		t.Fatalf("sidecar should be updated in place, got new id %s", second.SidecarID)
	}
	if second.PreAuthKeyID == "" || second.PreAuthKeyID == first.PreAuthKeyID {
		t.Fatalf("expected fresh PreAuthKeyID, before=%s after=%s",
			first.PreAuthKeyID, second.PreAuthKeyID)
	}
	if second.LastSpecHash == first.LastSpecHash {
		t.Fatalf("expected new spec hash")
	}

	// Update path was used (not Create).
	updates := countDockerCalls(h.docker.callLog(), dCallUpdate)
	if updates < 1 {
		t.Fatalf("expected at least one UpdateService call")
	}

	// Old key was expired.
	expired := false
	for _, c := range h.ctrl.callLog() {
		if c.Kind == callExpire && c.KeyID == first.PreAuthKeyID {
			expired = true
			break
		}
	}
	if !expired {
		t.Fatalf("old key %s never expired; call log: %+v", first.PreAuthKeyID, h.ctrl.callLog())
	}
}

func TestReconcile_TargetRemovedTearsDown(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	id := h.addEnabledTarget(t, "api", "stack", nil)
	if err := h.rec.Reconcile(context.Background(), id); err != nil {
		t.Fatalf("seed Reconcile: %v", err)
	}
	entry, _ := h.store.Get(id)

	// Pre-seed a Headscale node so DeleteNode has something to find.
	h.ctrl.addNode(Node{ID: "node-7", Hostname: entry.SidecarID, User: "swarm"})
	entry.HeadscaleNodeID = "node-7"
	h.store.Put(id, entry)

	// Target service vanishes from Docker.
	h.docker.markMissing(id)

	if err := h.rec.Reconcile(context.Background(), id); err != nil {
		t.Fatalf("teardown Reconcile: %v", err)
	}

	if _, ok := h.store.Get(id); ok {
		t.Fatalf("store entry should be gone after teardown")
	}

	// Sidecar removed from Docker.
	if !sawDockerCall(h.docker.callLog(), dCallRemove, entry.SidecarID) {
		t.Fatalf("expected RemoveService(%s)", entry.SidecarID)
	}
	// Key expired and node deleted on the controller.
	if !sawCall(h.ctrl.callLog(), callExpire, entry.PreAuthKeyID, "") {
		t.Fatalf("expected ExpireKey(%s)", entry.PreAuthKeyID)
	}
	if !sawCall(h.ctrl.callLog(), callDelete, "", "node-7") {
		t.Fatalf("expected DeleteNode(node-7)")
	}
}

func TestReconcile_SidecarCreateFails_KeyExpired(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	id := h.addEnabledTarget(t, "api", "stack", nil)

	h.docker.errCreate = errInjected

	err := h.rec.Reconcile(context.Background(), id)
	if err == nil {
		t.Fatalf("expected reconcile to error")
	}
	if !errors.Is(err, errInjected) {
		t.Fatalf("error should wrap errInjected, got %v", err)
	}

	// Store wasn't updated — no leak of phantom state.
	if _, ok := h.store.Get(id); ok {
		t.Fatalf("store should be empty after rollback")
	}

	// Exactly one create + one expire on the controller side. The mint
	// call appears first; the rollback expire fires immediately after.
	creates := countCalls(h.ctrl.callLog(), callCreate)
	expires := countCalls(h.ctrl.callLog(), callExpire)
	if creates != 1 || expires != 1 {
		t.Fatalf("expected 1 create and 1 expire on controller, got create=%d expire=%d",
			creates, expires)
	}
}

func TestReconcile_ControllerDown_LeavesSidecarUntouched(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	id := h.addEnabledTarget(t, "api", "stack", nil)

	h.ctrl.errCreate = errInjected

	err := h.rec.Reconcile(context.Background(), id)
	if err == nil {
		t.Fatalf("expected reconcile to error")
	}

	// No CreateService call should have happened — the key mint failed
	// before we touched Docker.
	if sawDockerCall(h.docker.callLog(), dCallCreate, "") {
		t.Fatalf("Docker should not have been called when controller is down")
	}
	if sawDockerCall(h.docker.callLog(), dCallUpdate, "") {
		t.Fatalf("Docker should not have been called when controller is down")
	}
	if _, ok := h.store.Get(id); ok {
		t.Fatalf("store should be empty after controller failure")
	}
}

func TestReconcile_DisabledTargetTearsDown(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	id := h.addEnabledTarget(t, "api", "stack", nil)
	if err := h.rec.Reconcile(context.Background(), id); err != nil {
		t.Fatalf("seed Reconcile: %v", err)
	}
	first, _ := h.store.Get(id)

	// Operator drops tailswarm.enable.
	h.docker.mu.Lock()
	h.docker.services[id].Spec.Annotations.Labels["tailswarm.enable"] = "false"
	h.docker.mu.Unlock()

	if err := h.rec.Reconcile(context.Background(), id); err != nil {
		t.Fatalf("disable Reconcile: %v", err)
	}

	if _, ok := h.store.Get(id); ok {
		t.Fatalf("store entry should be gone")
	}
	if !sawDockerCall(h.docker.callLog(), dCallRemove, first.SidecarID) {
		t.Fatalf("expected RemoveService(%s)", first.SidecarID)
	}
}

func TestResync_RemovesOrphansBothSides(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	ctx := context.Background()

	// One live target with a sidecar.
	liveTargetID := h.addEnabledTarget(t, "live", "stack", nil)
	if err := h.rec.Reconcile(ctx, liveTargetID); err != nil {
		t.Fatalf("seed live Reconcile: %v", err)
	}
	liveEntry, _ := h.store.Get(liveTargetID)
	liveSidecarID := liveEntry.SidecarID

	// One sidecar whose target is gone (orphan).
	orphanTargetID := "ghost-target-id"
	orphanSidecarSpec := SidecarSpec{
		Name:  "tailswarm_ghost_api",
		Image: "tailscale/tailscale:v1.78",
		Labels: map[string]string{
			ownerLabelManaged: "true",
			ownerLabelTarget:  orphanTargetID,
		},
		Hostname: "ghost-host",
	}
	orphanSidecarID, err := h.docker.CreateService(ctx, orphanSidecarSpec)
	if err != nil {
		t.Fatalf("seed orphan sidecar: %v", err)
	}
	// orphan target is not in fakeDocker; Inspect will return
	// ErrServiceNotFound naturally.

	// Live node on the controller (matches live sidecar's hostname).
	liveHostname := "stack-live"
	h.ctrl.addNode(Node{ID: "n-live", Hostname: liveHostname, User: "swarm"})
	// Stale node nobody asked for.
	h.ctrl.addNode(Node{ID: "n-stale", Hostname: "long-gone", User: "swarm"})
	// Node belonging to a different user — must be left alone.
	h.ctrl.addNode(Node{ID: "n-other", Hostname: "other", User: "different-user"})

	// Drop store so Resync rebuilds it cold (simulating a restart).
	h.store = NewStore()
	h.rec.Store = h.store

	if err := h.rec.Resync(ctx); err != nil {
		t.Fatalf("Resync: %v", err)
	}

	// Orphan sidecar removed.
	if !sawDockerCall(h.docker.callLog(), dCallRemove, orphanSidecarID) {
		t.Fatalf("expected orphan sidecar %s to be removed", orphanSidecarID)
	}
	// Live sidecar preserved.
	h.docker.mu.Lock()
	_, liveStillThere := h.docker.services[liveSidecarID]
	h.docker.mu.Unlock()
	if !liveStillThere {
		t.Fatalf("live sidecar %s was incorrectly removed", liveSidecarID)
	}

	// Stale node deleted, others untouched.
	if !sawCall(h.ctrl.callLog(), callDelete, "", "n-stale") {
		t.Fatalf("expected DeleteNode(n-stale)")
	}
	if sawCall(h.ctrl.callLog(), callDelete, "", "n-live") {
		t.Fatalf("DeleteNode(n-live) should not have happened")
	}
	if sawCall(h.ctrl.callLog(), callDelete, "", "n-other") {
		t.Fatalf("DeleteNode(n-other) should not have happened")
	}

	// Store now has the live sidecar plus the headscale node ID
	// backfilled.
	got, ok := h.store.Get(liveTargetID)
	if !ok {
		t.Fatalf("expected live target to be in the rebuilt store")
	}
	if got.SidecarID != liveSidecarID {
		t.Fatalf("rebuilt SidecarID mismatch: got %s, want %s", got.SidecarID, liveSidecarID)
	}
	if got.HeadscaleNodeID != "n-live" {
		t.Fatalf("expected backfilled HeadscaleNodeID=n-live, got %q", got.HeadscaleNodeID)
	}
}

func TestQueue_Dedupe(t *testing.T) {
	t.Parallel()

	q := NewQueue(2, 16)

	// Enqueue the same ID a few times in a row before any worker drains
	// it: only one entry should be live.
	q.Enqueue("svc-1")
	q.Enqueue("svc-1")
	q.Enqueue("svc-1")

	if got := q.pendingCount(); got != 1 {
		t.Fatalf("pendingCount = %d, want 1", got)
	}

	// Drain via Run; expect exactly one fn call for svc-1, then more
	// after re-enqueue.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		mu   sync.Mutex
		seen []string
		done = make(chan struct{})
	)
	go func() {
		q.Run(ctx, func(_ context.Context, id string) {
			mu.Lock()
			seen = append(seen, id)
			n := len(seen)
			mu.Unlock()
			if n == 2 {
				close(done)
			}
		})
	}()

	// Wait until the first item is consumed, then enqueue another to
	// observe a second invocation.
	for i := 0; i < 100; i++ {
		mu.Lock()
		n := len(seen)
		mu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	q.Enqueue("svc-1")

	select {
	case <-done:
	case <-time.After(time.Second):
		mu.Lock()
		t.Fatalf("queue worker did not invoke fn twice; seen=%v", seen)
	}

	cancel()
	mu.Lock()
	defer mu.Unlock()
	if len(seen) < 2 {
		t.Fatalf("expected at least 2 invocations, got %v", seen)
	}
	for _, id := range seen {
		if id != "svc-1" {
			t.Fatalf("unexpected id in seen: %v", seen)
		}
	}
}

func TestQueue_ShardingSpreadsLoad(t *testing.T) {
	t.Parallel()

	q := NewQueue(4, 8)

	// Enqueue many distinct IDs; they should land in different shards
	// often enough that no single shard holds them all.
	for i := 0; i < 32; i++ {
		q.Enqueue("svc-" + itoa(i))
	}

	nonEmpty := 0
	for _, s := range q.shards {
		s.mu.Lock()
		if len(s.pending) > 0 {
			nonEmpty++
		}
		s.mu.Unlock()
	}
	if nonEmpty < 2 {
		t.Fatalf("expected work spread across multiple shards, got %d non-empty", nonEmpty)
	}
}

// --- small test helpers -------------------------------------------------

func findCall(log []dockerCall, kind dockerCallKind) *dockerCall {
	for i := range log {
		if log[i].Kind == kind {
			return &log[i]
		}
	}
	return nil
}

func sawDockerCall(log []dockerCall, kind dockerCallKind, serviceID string) bool {
	for _, c := range log {
		if c.Kind != kind {
			continue
		}
		if serviceID != "" && c.ServiceID != serviceID {
			continue
		}
		return true
	}
	return false
}

func countDockerCalls(log []dockerCall, kind dockerCallKind) int {
	n := 0
	for _, c := range log {
		if c.Kind == kind {
			n++
		}
	}
	return n
}

func sawCall(log []fakeCall, kind fakeCallKind, keyID, nodeID string) bool {
	for _, c := range log {
		if c.Kind != kind {
			continue
		}
		if keyID != "" && c.KeyID != keyID {
			continue
		}
		if nodeID != "" && c.NodeID != nodeID {
			continue
		}
		return true
	}
	return false
}

func countCalls(log []fakeCall, kind fakeCallKind) int {
	n := 0
	for _, c := range log {
		if c.Kind == kind {
			n++
		}
	}
	return n
}

func itoa(i int) string {
	// Local helper to avoid importing strconv in this test file just
	// for one call.
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	neg := i < 0
	if neg {
		i = -i
	}
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
