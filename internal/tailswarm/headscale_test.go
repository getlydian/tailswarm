package tailswarm

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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
