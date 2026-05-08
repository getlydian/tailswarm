package tailswarm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Headscale is a Controller backed by a Headscale REST API. Endpoints
// follow the v1 surface documented at /swagger; field names match the
// generated proto/swagger schema (camelCase JSON, see DESIGN.md §3
// "Headscale's pre-auth key API").
//
// Headscale's CreatePreAuthKey takes a numeric user ID, but tailswarm
// configures a user *name* (DESIGN.md §9.2) — Headscale users are
// human-meaningful names, not IDs. The client therefore resolves the
// configured name to an ID on first use and caches the result; the
// mapping is stable for the lifetime of a Headscale user.
type Headscale struct {
	// BaseURL is the Headscale root, e.g. "https://headscale.internal".
	// A trailing slash is tolerated.
	BaseURL string

	// APIKey is a bearer token loaded from a Docker secret or env var.
	// It is never logged.
	APIKey string

	// HTTP is the client used for every request. Nil falls back to a
	// reasonable default with a 30s timeout.
	HTTP *http.Client

	// userIDCache memoises name → numeric ID lookups. Headscale does not
	// renumber users, so a single resolution per process lifetime is
	// enough.
	userIDCache map[string]string
}

// Compile-time check that Headscale satisfies Controller.
var _ Controller = (*Headscale)(nil)

// HeadscaleError is the typed error wrapping non-2xx responses. It
// preserves the HTTP status so the reconciler's backoff path can
// distinguish 4xx (configuration / auth) from 5xx (transient) without
// string-matching.
type HeadscaleError struct {
	Status int
	Op     string // "create preauthkey", "expire preauthkey", ...
	Body   string // truncated response body excerpt for debuggability
}

func (e *HeadscaleError) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("headscale: %s: status %d", e.Op, e.Status)
	}
	return fmt.Sprintf("headscale: %s: status %d: %s", e.Op, e.Status, e.Body)
}

// IsClientError reports whether the response was a 4xx (excluding 408
// and 429, which are transient). Useful for the reconciler to short-
// circuit retries on configuration errors.
func (e *HeadscaleError) IsClientError() bool {
	if e.Status == http.StatusRequestTimeout || e.Status == http.StatusTooManyRequests {
		return false
	}
	return e.Status >= 400 && e.Status < 500
}

// IsServerError reports whether the response was a 5xx (or 408/429),
// i.e. retryable.
func (e *HeadscaleError) IsServerError() bool {
	if e.Status == http.StatusRequestTimeout || e.Status == http.StatusTooManyRequests {
		return true
	}
	return e.Status >= 500 && e.Status < 600
}

func (h *Headscale) httpClient() *http.Client {
	if h.HTTP != nil {
		return h.HTTP
	}
	return &http.Client{Timeout: 30 * time.Second}
}

func (h *Headscale) baseURL() string {
	return strings.TrimRight(h.BaseURL, "/")
}

// CreateEphemeralKey mints a preauth key on Headscale. The configured
// user name is resolved to a numeric ID on first use and cached. Tags
// are passed through verbatim — the caller (reconciler) is responsible
// for deriving safe tag names per DESIGN.md §8.
func (h *Headscale) CreateEphemeralKey(ctx context.Context, req KeyRequest) (Key, error) {
	if req.User == "" {
		return Key{}, errors.New("headscale: KeyRequest.User is empty")
	}

	userID, err := h.resolveUserID(ctx, req.User)
	if err != nil {
		return Key{}, err
	}

	expiration := time.Now().Add(req.Expiration).UTC()

	body := map[string]any{
		"user":       userID,
		"reusable":   req.Reusable,
		"ephemeral":  req.Ephemeral,
		"expiration": expiration.Format(time.RFC3339Nano),
		"aclTags":    nilIfEmpty(req.Tags),
	}

	var resp struct {
		PreAuthKey struct {
			ID         string    `json:"id"`
			Key        string    `json:"key"`
			Expiration time.Time `json:"expiration"`
		} `json:"preAuthKey"`
	}
	if err := h.do(ctx, http.MethodPost, "/api/v1/preauthkey", body, &resp, "create preauthkey"); err != nil {
		return Key{}, err
	}
	if resp.PreAuthKey.Key == "" {
		return Key{}, errors.New("headscale: create preauthkey: empty key in response")
	}

	expiresAt := resp.PreAuthKey.Expiration
	if expiresAt.IsZero() {
		expiresAt = expiration
	}
	return Key{
		ID:        resp.PreAuthKey.ID,
		Secret:    resp.PreAuthKey.Key,
		ExpiresAt: expiresAt,
	}, nil
}

// ExpireKey marks a previously-minted preauth key as expired. Headscale's
// expire endpoint is idempotent: a 200 means "expired now or earlier".
//
// keyID must be the value returned by CreateEphemeralKey (Key.ID), not
// the secret string.
func (h *Headscale) ExpireKey(ctx context.Context, keyID string) error {
	if keyID == "" {
		return errors.New("headscale: keyID is empty")
	}
	body := map[string]string{"id": keyID}
	return h.do(ctx, http.MethodPost, "/api/v1/preauthkey/expire", body, nil, "expire preauthkey")
}

// DeleteNode removes a registered node from Headscale. Used both during
// per-service teardown and during the orphan sweep in Reconciler.Resync.
func (h *Headscale) DeleteNode(ctx context.Context, nodeID string) error {
	if nodeID == "" {
		return errors.New("headscale: nodeID is empty")
	}
	path := "/api/v1/node/" + url.PathEscape(nodeID)
	return h.do(ctx, http.MethodDelete, path, nil, nil, "delete node")
}

// ListNodes returns every node owned by user, suitable for orphan
// detection. The user filter is forwarded to Headscale as a query
// parameter; an empty user lists every node visible to the API key.
func (h *Headscale) ListNodes(ctx context.Context, user string) ([]Node, error) {
	path := "/api/v1/node"
	if user != "" {
		path += "?user=" + url.QueryEscape(user)
	}

	var resp struct {
		Nodes []struct {
			ID        string   `json:"id"`
			Name      string   `json:"name"`
			GivenName string   `json:"givenName"`
			Tags      []string `json:"tags"`
			User      struct {
				Name string `json:"name"`
			} `json:"user"`
		} `json:"nodes"`
	}
	if err := h.do(ctx, http.MethodGet, path, nil, &resp, "list nodes"); err != nil {
		return nil, err
	}

	out := make([]Node, 0, len(resp.Nodes))
	for _, n := range resp.Nodes {
		hostname := n.GivenName
		if hostname == "" {
			hostname = n.Name
		}
		out = append(out, Node{
			ID:       n.ID,
			Hostname: hostname,
			User:     n.User.Name,
			Tags:     n.Tags,
		})
	}
	return out, nil
}

// resolveUserID looks up the numeric ID for a Headscale user by name,
// caching the result. CreatePreAuthKey requires the numeric ID even
// though every other tailswarm-facing API uses the name.
func (h *Headscale) resolveUserID(ctx context.Context, name string) (string, error) {
	if id, ok := h.userIDCache[name]; ok {
		return id, nil
	}

	path := "/api/v1/user?name=" + url.QueryEscape(name)
	var resp struct {
		Users []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"users"`
	}
	if err := h.do(ctx, http.MethodGet, path, nil, &resp, "lookup user"); err != nil {
		return "", err
	}
	for _, u := range resp.Users {
		if u.Name == name && u.ID != "" {
			if h.userIDCache == nil {
				h.userIDCache = map[string]string{}
			}
			h.userIDCache[name] = u.ID
			return u.ID, nil
		}
	}
	return "", fmt.Errorf("headscale: user %q not found", name)
}

// do is the shared request helper. It marshals body (when non-nil),
// adds the bearer header, decodes a 2xx JSON response into out (when
// non-nil), and translates non-2xx responses into *HeadscaleError.
func (h *Headscale) do(ctx context.Context, method, path string, body, out any, op string) error {
	var reqBody io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("headscale: %s: marshal request: %w", op, err)
		}
		reqBody = bytes.NewReader(buf)
	}

	req, err := http.NewRequestWithContext(ctx, method, h.baseURL()+path, reqBody)
	if err != nil {
		return fmt.Errorf("headscale: %s: build request: %w", op, err)
	}
	req.Header.Set("Authorization", "Bearer "+h.APIKey)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := h.httpClient().Do(req)
	if err != nil {
		return fmt.Errorf("headscale: %s: %w", op, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		excerpt, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return &HeadscaleError{
			Status: resp.StatusCode,
			Op:     op,
			Body:   strings.TrimSpace(string(excerpt)),
		}
	}

	if out == nil {
		// Drain so the connection can be reused.
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("headscale: %s: decode response: %w", op, err)
	}
	return nil
}

func nilIfEmpty(s []string) []string {
	if len(s) == 0 {
		return nil
	}
	return s
}
