package tailswarm

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// recordedRequest is what the test server captures so assertions can
// look at headers, methods, paths, queries, and request bodies after
// the fact.
type recordedRequest struct {
	Method string
	Path   string
	Query  string
	Auth   string
	Body   string
}

// fakeHeadscale is a small router over httptest.Server that returns
// canned JSON for each endpoint and records every request. Each handler
// is a func — set to nil to make the path 404, or override to inject
// errors.
type fakeHeadscale struct {
	server *httptest.Server
	calls  []recordedRequest

	// Per-route handlers. The router dispatches by (method, pathPrefix).
	createKey  http.HandlerFunc
	expireKey  http.HandlerFunc
	listNodes  http.HandlerFunc
	deleteNode http.HandlerFunc
	lookupUser http.HandlerFunc

	// requireAuth, when true, makes every handler 401 without a Bearer
	// header (in addition to the always-recorded auth header).
	requireAuth bool
	apiKey      string
}

func newFakeHeadscale(t *testing.T) *fakeHeadscale {
	t.Helper()
	f := &fakeHeadscale{apiKey: "test-token"}
	f.server = httptest.NewServer(http.HandlerFunc(f.route))
	t.Cleanup(f.server.Close)
	// Default user-lookup handler: a single user "swarm" with id 7.
	f.lookupUser = func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Query().Get("name")
		if name == "swarm" {
			writeJSON(w, http.StatusOK, map[string]any{
				"users": []map[string]any{{"id": "7", "name": "swarm"}},
			})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"users": []map[string]any{}})
	}
	return f
}

func (f *fakeHeadscale) route(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	_ = r.Body.Close()
	f.calls = append(f.calls, recordedRequest{
		Method: r.Method,
		Path:   r.URL.Path,
		Query:  r.URL.RawQuery,
		Auth:   r.Header.Get("Authorization"),
		Body:   string(body),
	})
	// Restore body for downstream handlers that want to decode it.
	r.Body = io.NopCloser(strings.NewReader(string(body)))

	if f.requireAuth && r.Header.Get("Authorization") != "Bearer "+f.apiKey {
		http.Error(w, `{"message":"unauthorized"}`, http.StatusUnauthorized)
		return
	}

	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/api/v1/preauthkey":
		dispatch(w, r, f.createKey)
	case r.Method == http.MethodPost && r.URL.Path == "/api/v1/preauthkey/expire":
		dispatch(w, r, f.expireKey)
	case r.Method == http.MethodGet && r.URL.Path == "/api/v1/node":
		dispatch(w, r, f.listNodes)
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/v1/node/"):
		dispatch(w, r, f.deleteNode)
	case r.Method == http.MethodGet && r.URL.Path == "/api/v1/user":
		dispatch(w, r, f.lookupUser)
	default:
		http.NotFound(w, r)
	}
}

func dispatch(w http.ResponseWriter, r *http.Request, h http.HandlerFunc) {
	if h == nil {
		http.NotFound(w, r)
		return
	}
	h(w, r)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func newClient(f *fakeHeadscale) *Headscale {
	return &Headscale{
		BaseURL: f.server.URL,
		APIKey:  f.apiKey,
		HTTP:    f.server.Client(),
	}
}

func TestHeadscale_CreateEphemeralKey_RoundTrip(t *testing.T) {
	t.Parallel()

	f := newFakeHeadscale(t)
	expiry := time.Now().Add(5 * time.Minute).UTC().Truncate(time.Second)
	f.createKey = func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			User       string   `json:"user"`
			Reusable   bool     `json:"reusable"`
			Ephemeral  bool     `json:"ephemeral"`
			Expiration string   `json:"expiration"`
			ACLTags    []string `json:"aclTags"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode create body: %v", err)
		}
		if req.User != "7" {
			t.Errorf("create body user = %q, want %q (resolved id)", req.User, "7")
		}
		if !req.Ephemeral || req.Reusable {
			t.Errorf("create body flags: ephemeral=%v reusable=%v, want true/false", req.Ephemeral, req.Reusable)
		}
		if len(req.ACLTags) != 1 || req.ACLTags[0] != "tag:swarm-billing" {
			t.Errorf("create body aclTags = %v, want [tag:swarm-billing]", req.ACLTags)
		}
		if req.Expiration == "" {
			t.Errorf("create body expiration is empty")
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"preAuthKey": map[string]any{
				"id":         "42",
				"key":        "abcdef",
				"expiration": expiry.Format(time.RFC3339Nano),
				"reusable":   false,
				"ephemeral":  true,
				"aclTags":    req.ACLTags,
			},
		})
	}

	c := newClient(f)
	got, err := c.CreateEphemeralKey(context.Background(), KeyRequest{
		User:       "swarm",
		Tags:       []string{"tag:swarm-billing"},
		Ephemeral:  true,
		Reusable:   false,
		Expiration: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("CreateEphemeralKey: %v", err)
	}
	if got.ID != "42" || got.Secret != "abcdef" {
		t.Fatalf("Key = %+v, want id=42 secret=abcdef", got)
	}
	if !got.ExpiresAt.Equal(expiry) {
		t.Fatalf("ExpiresAt = %v, want %v", got.ExpiresAt, expiry)
	}

	// Two calls were made: user lookup, then create.
	if len(f.calls) != 2 {
		t.Fatalf("call count = %d, want 2 (user lookup + create)", len(f.calls))
	}
	if f.calls[0].Path != "/api/v1/user" || f.calls[0].Query != "name=swarm" {
		t.Fatalf("first call = %+v, want GET /api/v1/user?name=swarm", f.calls[0])
	}
	if f.calls[1].Path != "/api/v1/preauthkey" || f.calls[1].Method != http.MethodPost {
		t.Fatalf("second call = %+v, want POST /api/v1/preauthkey", f.calls[1])
	}
}

func TestHeadscale_AuthHeaderOnEveryRequest(t *testing.T) {
	t.Parallel()

	f := newFakeHeadscale(t)
	f.createKey = func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"preAuthKey": map[string]any{"id": "1", "key": "k"},
		})
	}
	f.expireKey = func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{})
	}
	f.listNodes = func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"nodes": []any{}})
	}
	f.deleteNode = func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{})
	}

	c := newClient(f)
	ctx := context.Background()

	if _, err := c.CreateEphemeralKey(ctx, KeyRequest{User: "swarm", Expiration: time.Minute}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := c.ExpireKey(ctx, "1"); err != nil {
		t.Fatalf("expire: %v", err)
	}
	if _, err := c.ListNodes(ctx, "swarm"); err != nil {
		t.Fatalf("list: %v", err)
	}
	if err := c.DeleteNode(ctx, "node-1"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	if len(f.calls) == 0 {
		t.Fatal("no recorded calls")
	}
	for i, c := range f.calls {
		if c.Auth != "Bearer "+f.apiKey {
			t.Errorf("call[%d] %s %s: Authorization = %q, want Bearer %s",
				i, c.Method, c.Path, c.Auth, f.apiKey)
		}
	}
}

func TestHeadscale_ExpireKey(t *testing.T) {
	t.Parallel()

	f := newFakeHeadscale(t)
	var captured string
	f.expireKey = func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID string `json:"id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		captured = req.ID
		writeJSON(w, http.StatusOK, map[string]any{})
	}

	c := newClient(f)
	if err := c.ExpireKey(context.Background(), "key-99"); err != nil {
		t.Fatalf("ExpireKey: %v", err)
	}
	if captured != "key-99" {
		t.Fatalf("expire body id = %q, want key-99", captured)
	}
}

func TestHeadscale_ListNodes(t *testing.T) {
	t.Parallel()

	f := newFakeHeadscale(t)
	f.listNodes = func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("user") != "swarm" {
			t.Errorf("list nodes user query = %q, want swarm", r.URL.Query().Get("user"))
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"nodes": []map[string]any{
				{
					"id":        "11",
					"name":      "billing-api",
					"givenName": "billing-api",
					"tags":      []string{"tag:swarm-billing"},
					"user":      map[string]any{"id": "7", "name": "swarm"},
				},
				{
					"id":   "12",
					"name": "other",
					"user": map[string]any{"id": "7", "name": "swarm"},
				},
			},
		})
	}

	c := newClient(f)
	got, err := c.ListNodes(context.Background(), "swarm")
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("ListNodes len = %d, want 2", len(got))
	}
	if got[0].ID != "11" || got[0].Hostname != "billing-api" || got[0].User != "swarm" {
		t.Fatalf("node[0] = %+v, want id=11 hostname=billing-api user=swarm", got[0])
	}
	if len(got[0].Tags) != 1 || got[0].Tags[0] != "tag:swarm-billing" {
		t.Fatalf("node[0].Tags = %v, want [tag:swarm-billing]", got[0].Tags)
	}
}

func TestHeadscale_DeleteNode(t *testing.T) {
	t.Parallel()

	f := newFakeHeadscale(t)
	f.deleteNode = func(w http.ResponseWriter, r *http.Request) {
		want := "/api/v1/node/node-99"
		if r.URL.Path != want {
			t.Errorf("delete path = %q, want %q", r.URL.Path, want)
		}
		writeJSON(w, http.StatusOK, map[string]any{})
	}

	c := newClient(f)
	if err := c.DeleteNode(context.Background(), "node-99"); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
}

func TestHeadscale_StatusErrorTyping(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		status     int
		wantClient bool
		wantServer bool
	}{
		{"401 unauthorized", http.StatusUnauthorized, true, false},
		{"404 not found", http.StatusNotFound, true, false},
		{"408 timeout retryable", http.StatusRequestTimeout, false, true},
		{"429 rate limit retryable", http.StatusTooManyRequests, false, true},
		{"500 server error", http.StatusInternalServerError, false, true},
		{"503 unavailable", http.StatusServiceUnavailable, false, true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFakeHeadscale(t)
			f.expireKey = func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, `{"message":"nope"}`, tc.status)
			}

			c := newClient(f)
			err := c.ExpireKey(context.Background(), "k-1")
			if err == nil {
				t.Fatalf("ExpireKey: want error, got nil")
			}
			var hs *HeadscaleError
			if !errors.As(err, &hs) {
				t.Fatalf("err = %v, want *HeadscaleError", err)
			}
			if hs.Status != tc.status {
				t.Fatalf("Status = %d, want %d", hs.Status, tc.status)
			}
			if hs.IsClientError() != tc.wantClient {
				t.Fatalf("IsClientError = %v, want %v", hs.IsClientError(), tc.wantClient)
			}
			if hs.IsServerError() != tc.wantServer {
				t.Fatalf("IsServerError = %v, want %v", hs.IsServerError(), tc.wantServer)
			}
		})
	}
}

func TestHeadscale_AuthFailureSurfaced(t *testing.T) {
	t.Parallel()

	f := newFakeHeadscale(t)
	f.requireAuth = true
	f.expireKey = func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{})
	}

	c := newClient(f)
	c.APIKey = "wrong-token"

	err := c.ExpireKey(context.Background(), "k-1")
	if err == nil {
		t.Fatal("want auth error, got nil")
	}
	var hs *HeadscaleError
	if !errors.As(err, &hs) || hs.Status != http.StatusUnauthorized {
		t.Fatalf("err = %v, want HeadscaleError with status 401", err)
	}
}

func TestHeadscale_APIKeyNotInError(t *testing.T) {
	t.Parallel()

	f := newFakeHeadscale(t)
	f.expireKey = func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"message":"boom"}`, http.StatusInternalServerError)
	}

	c := newClient(f)
	c.APIKey = "super-secret-token-must-not-leak"

	err := c.ExpireKey(context.Background(), "k-1")
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if strings.Contains(err.Error(), c.APIKey) {
		t.Fatalf("error message leaks API key: %q", err.Error())
	}
}

func TestHeadscale_UserIDCached(t *testing.T) {
	t.Parallel()

	f := newFakeHeadscale(t)
	var lookups atomic.Int32
	f.lookupUser = func(w http.ResponseWriter, r *http.Request) {
		lookups.Add(1)
		writeJSON(w, http.StatusOK, map[string]any{
			"users": []map[string]any{{"id": "7", "name": "swarm"}},
		})
	}
	f.createKey = func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"preAuthKey": map[string]any{"id": "1", "key": "k"},
		})
	}

	c := newClient(f)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := c.CreateEphemeralKey(ctx, KeyRequest{User: "swarm", Expiration: time.Minute}); err != nil {
			t.Fatalf("create #%d: %v", i, err)
		}
	}
	if got := lookups.Load(); got != 1 {
		t.Fatalf("user lookups = %d, want 1 (cached)", got)
	}
}

func TestHeadscale_UnknownUserIsClientError(t *testing.T) {
	t.Parallel()

	f := newFakeHeadscale(t)
	// lookupUser default returns empty for anything except "swarm".

	c := newClient(f)
	_, err := c.CreateEphemeralKey(context.Background(), KeyRequest{User: "ghost", Expiration: time.Minute})
	if err == nil {
		t.Fatal("want error for unknown user, got nil")
	}
	if !strings.Contains(err.Error(), `"ghost"`) {
		t.Fatalf("error should name the missing user: %v", err)
	}
}

func TestHeadscale_BaseURLTrailingSlashTolerated(t *testing.T) {
	t.Parallel()

	f := newFakeHeadscale(t)
	f.deleteNode = func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/node/n-1" {
			t.Errorf("path = %q, want /api/v1/node/n-1", r.URL.Path)
		}
		writeJSON(w, http.StatusOK, map[string]any{})
	}

	c := newClient(f)
	c.BaseURL = f.server.URL + "/"

	if err := c.DeleteNode(context.Background(), "n-1"); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
}
