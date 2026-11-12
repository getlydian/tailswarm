package tailswarm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestHeadscaleCreateEphemeralKey(t *testing.T) {
	var lookups, creates int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v1/user"):
			lookups++
			_ = json.NewEncoder(w).Encode(map[string]any{
				"users": []map[string]string{{"id": "42", "name": "swarm"}},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/preauthkey":
			creates++
			body, _ := io.ReadAll(r.Body)
			var got map[string]any
			_ = json.Unmarshal(body, &got)
			if got["user"] != "42" {
				t.Errorf("user id: %v", got["user"])
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"preAuthKey": map[string]any{
					"id":         "k-1",
					"key":        "secret",
					"expiration": time.Now().Add(time.Minute).Format(time.RFC3339Nano),
				},
			})
		default:
			http.Error(w, "unexpected", http.StatusBadRequest)
		}
	}))
	defer srv.Close()

	h := &Headscale{BaseURL: srv.URL, APIKey: "x"}
	ctx := context.Background()
	key, err := h.CreateEphemeralKey(ctx, KeyRequest{
		User: "swarm", Tags: []string{"tag:a"}, Ephemeral: true, Expiration: time.Minute,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if key.ID != "k-1" || key.Secret != "secret" {
		t.Errorf("key: %+v", key)
	}
	// Cached on second call.
	if _, err := h.CreateEphemeralKey(ctx, KeyRequest{User: "swarm", Expiration: time.Minute}); err != nil {
		t.Fatal(err)
	}
	if lookups != 1 {
		t.Errorf("lookups: got %d want 1 (cached)", lookups)
	}
	if creates != 2 {
		t.Errorf("creates: got %d want 2", creates)
	}
}

func TestHeadscaleExpireKey(t *testing.T) {
	var got map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &got)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	h := &Headscale{BaseURL: srv.URL, APIKey: "x"}
	if err := h.ExpireKey(context.Background(), "k-1"); err != nil {
		t.Fatal(err)
	}
	if got["id"] != "k-1" {
		t.Fatalf("body: %+v", got)
	}
}

// A gateway that died leaves its ephemeral node behind, offline, until
// Headscale reaps it; the replacement registers under the same name. Any
// online node by that name counts.
func TestHeadscaleNodeOnline(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/node" {
			http.Error(w, "unexpected", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"nodes": []map[string]any{
				{"name": "gw-a", "givenName": "gw-a", "online": false},
				{"name": "gw-a", "givenName": "gw-a-x1y2", "online": true},
				{"name": "gw-b", "givenName": "gw-b", "online": false},
			},
		})
	}))
	defer srv.Close()

	h := &Headscale{BaseURL: srv.URL, APIKey: "x"}
	for hostname, want := range map[string]bool{"gw-a": true, "gw-b": false, "gw-c": false} {
		got, err := h.NodeOnline(context.Background(), hostname)
		if err != nil {
			t.Fatalf("%s: %v", hostname, err)
		}
		if got != want {
			t.Errorf("%s online: got %v want %v", hostname, got, want)
		}
	}
}

func TestHeadscaleErrorClassification(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	h := &Headscale{BaseURL: srv.URL, APIKey: "x"}
	err := h.ExpireKey(context.Background(), "k-1")
	var hErr *HeadscaleError
	if !errors.As(err, &hErr) {
		t.Fatalf("err type: %T", err)
	}
	if !hErr.IsServerError() || hErr.IsClientError() {
		t.Fatalf("classification wrong: %+v", hErr)
	}
}

// resolveUserID is called from every reconcile worker at once, so its
// cache must be safe under concurrency. Unguarded, this trips Go's
// "concurrent map writes" fatal error — which kills the process rather
// than failing the reconcile. Run under -race to catch the read side too.
func TestHeadscaleResolveUserIDConcurrent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/user" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"users":[{"id":"7","name":"swarm@example.com"}]}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	h := &Headscale{BaseURL: srv.URL, APIKey: "k"}

	const workers = 16
	var wg sync.WaitGroup
	wg.Add(workers)
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			id, err := h.resolveUserID(context.Background(), "swarm@example.com")
			if err != nil {
				errs <- err
				return
			}
			if id != "7" {
				errs <- fmt.Errorf("id = %q, want 7", id)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("resolveUserID: %v", err)
	}
}
