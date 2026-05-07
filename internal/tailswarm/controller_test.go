package tailswarm

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeController is an in-memory Controller used by reconciler tests in
// later steps. It records every call (so tests can assert ordering and
// counts), supports per-method error injection, and is safe for
// concurrent use.
type fakeController struct {
	mu sync.Mutex

	keys  map[string]Key  // ID → minted key
	nodes map[string]Node // ID → registered node

	// keySeq and nodeSeq generate deterministic IDs so test assertions
	// don't have to deal with random output.
	keySeq  atomic.Uint64
	nodeSeq atomic.Uint64

	// errCreate / errExpire / errDelete / errList override the next
	// call to the corresponding method when set. Cleared after firing
	// so tests can inject a transient failure without affecting later
	// calls.
	errCreate error
	errExpire error
	errDelete error
	errList   error

	calls []fakeCall
}

type fakeCallKind int

const (
	callCreate fakeCallKind = iota
	callExpire
	callDelete
	callList
)

type fakeCall struct {
	Kind fakeCallKind
	// One of the following is set depending on Kind.
	Req    KeyRequest // callCreate
	KeyID  string     // callExpire
	NodeID string     // callDelete
	User   string     // callList
}

func newFakeController() *fakeController {
	return &fakeController{
		keys:  map[string]Key{},
		nodes: map[string]Node{},
	}
}

// addNode lets tests pre-seed nodes so ListNodes / DeleteNode have
// something to return without going through CreateEphemeralKey.
func (f *fakeController) addNode(n Node) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if n.ID == "" {
		n.ID = "node-" + strconv.FormatUint(f.nodeSeq.Add(1), 10)
	}
	f.nodes[n.ID] = n
}

func (f *fakeController) callLog() []fakeCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]fakeCall, len(f.calls))
	copy(out, f.calls)
	return out
}

func (f *fakeController) CreateEphemeralKey(_ context.Context, req KeyRequest) (Key, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, fakeCall{Kind: callCreate, Req: req})

	if err := f.errCreate; err != nil {
		f.errCreate = nil
		return Key{}, err
	}

	id := "key-" + strconv.FormatUint(f.keySeq.Add(1), 10)
	k := Key{
		ID:        id,
		Secret:    "secret-" + id,
		ExpiresAt: time.Now().Add(req.Expiration),
	}
	f.keys[id] = k
	return k, nil
}

func (f *fakeController) ExpireKey(_ context.Context, keyID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, fakeCall{Kind: callExpire, KeyID: keyID})

	if err := f.errExpire; err != nil {
		f.errExpire = nil
		return err
	}

	if _, ok := f.keys[keyID]; !ok {
		return fmt.Errorf("fakeController: unknown keyID %q", keyID)
	}
	delete(f.keys, keyID)
	return nil
}

func (f *fakeController) DeleteNode(_ context.Context, nodeID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, fakeCall{Kind: callDelete, NodeID: nodeID})

	if err := f.errDelete; err != nil {
		f.errDelete = nil
		return err
	}

	if _, ok := f.nodes[nodeID]; !ok {
		return fmt.Errorf("fakeController: unknown nodeID %q", nodeID)
	}
	delete(f.nodes, nodeID)
	return nil
}

func (f *fakeController) ListNodes(_ context.Context, user string) ([]Node, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, fakeCall{Kind: callList, User: user})

	if err := f.errList; err != nil {
		f.errList = nil
		return nil, err
	}

	out := make([]Node, 0, len(f.nodes))
	for _, n := range f.nodes {
		if user != "" && n.User != user {
			continue
		}
		out = append(out, n)
	}
	return out, nil
}

// Compile-time check that the fake satisfies the interface so a
// signature change breaks the test file before it breaks the reconciler
// suite.
var _ Controller = (*fakeController)(nil)

func TestFakeController_CreateAndExpire(t *testing.T) {
	t.Parallel()

	f := newFakeController()
	ctx := context.Background()

	k, err := f.CreateEphemeralKey(ctx, KeyRequest{
		User:       "swarm",
		Tags:       []string{"tag:swarm-billing"},
		Ephemeral:  true,
		Reusable:   false,
		Expiration: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("CreateEphemeralKey: %v", err)
	}
	if k.ID == "" || k.Secret == "" {
		t.Fatalf("expected non-empty Key, got %+v", k)
	}
	if !k.ExpiresAt.After(time.Now()) {
		t.Fatalf("ExpiresAt should be in the future, got %v", k.ExpiresAt)
	}

	if err := f.ExpireKey(ctx, k.ID); err != nil {
		t.Fatalf("ExpireKey: %v", err)
	}

	// Second expire of the same key should fail (already gone).
	if err := f.ExpireKey(ctx, k.ID); err == nil {
		t.Fatalf("ExpireKey of already-expired key: want error, got nil")
	}
}

func TestFakeController_ListNodesAndDelete(t *testing.T) {
	t.Parallel()

	f := newFakeController()
	ctx := context.Background()

	f.addNode(Node{ID: "n-1", Hostname: "billing-api", User: "swarm", Tags: []string{"tag:swarm-billing"}})
	f.addNode(Node{ID: "n-2", Hostname: "other", User: "other-user"})

	got, err := f.ListNodes(ctx, "swarm")
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if len(got) != 1 || got[0].ID != "n-1" {
		t.Fatalf("ListNodes(swarm) = %+v, want only n-1", got)
	}

	all, err := f.ListNodes(ctx, "")
	if err != nil {
		t.Fatalf("ListNodes(\"\"): %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("ListNodes(\"\") len = %d, want 2", len(all))
	}

	if err := f.DeleteNode(ctx, "n-1"); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	if err := f.DeleteNode(ctx, "n-1"); err == nil {
		t.Fatalf("DeleteNode of already-deleted node: want error, got nil")
	}
}

func TestFakeController_ErrorInjection(t *testing.T) {
	t.Parallel()

	f := newFakeController()
	ctx := context.Background()

	boom := errors.New("boom")

	f.errCreate = boom
	if _, err := f.CreateEphemeralKey(ctx, KeyRequest{User: "swarm", Expiration: time.Minute}); !errors.Is(err, boom) {
		t.Fatalf("CreateEphemeralKey err = %v, want %v", err, boom)
	}
	// Injection clears after firing.
	if _, err := f.CreateEphemeralKey(ctx, KeyRequest{User: "swarm", Expiration: time.Minute}); err != nil {
		t.Fatalf("second CreateEphemeralKey: %v (injection should be one-shot)", err)
	}

	f.errList = boom
	if _, err := f.ListNodes(ctx, "swarm"); !errors.Is(err, boom) {
		t.Fatalf("ListNodes err = %v, want %v", err, boom)
	}

	f.addNode(Node{ID: "n-x", User: "swarm"})
	f.errDelete = boom
	if err := f.DeleteNode(ctx, "n-x"); !errors.Is(err, boom) {
		t.Fatalf("DeleteNode err = %v, want %v", err, boom)
	}
	// Node still present because delete errored before mutation.
	if err := f.DeleteNode(ctx, "n-x"); err != nil {
		t.Fatalf("retry DeleteNode: %v", err)
	}
}

func TestFakeController_RecordsCalls(t *testing.T) {
	t.Parallel()

	f := newFakeController()
	ctx := context.Background()

	k, err := f.CreateEphemeralKey(ctx, KeyRequest{User: "swarm", Expiration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.ListNodes(ctx, "swarm"); err != nil {
		t.Fatal(err)
	}
	if err := f.ExpireKey(ctx, k.ID); err != nil {
		t.Fatal(err)
	}

	log := f.callLog()
	if len(log) != 3 {
		t.Fatalf("call log length = %d, want 3", len(log))
	}
	if log[0].Kind != callCreate || log[0].Req.User != "swarm" {
		t.Fatalf("call[0] = %+v, want callCreate for swarm", log[0])
	}
	if log[1].Kind != callList || log[1].User != "swarm" {
		t.Fatalf("call[1] = %+v, want callList for swarm", log[1])
	}
	if log[2].Kind != callExpire || log[2].KeyID != k.ID {
		t.Fatalf("call[2] = %+v, want callExpire for %q", log[2], k.ID)
	}
}

func TestFakeController_ConcurrentSafe(t *testing.T) {
	t.Parallel()

	f := newFakeController()
	ctx := context.Background()

	const n = 50
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			k, err := f.CreateEphemeralKey(ctx, KeyRequest{User: "swarm", Expiration: time.Minute})
			if err != nil {
				t.Errorf("CreateEphemeralKey: %v", err)
				return
			}
			if err := f.ExpireKey(ctx, k.ID); err != nil {
				t.Errorf("ExpireKey: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := len(f.callLog()); got != 2*n {
		t.Fatalf("call log length = %d, want %d", got, 2*n)
	}
}
