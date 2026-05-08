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
// generated proto/swagger schema (camelCase JSON).
//
// Headscale's CreatePreAuthKey takes a numeric user ID, but tailswarm
// configures a user *name* — Headscale users are human-meaningful names,
// not IDs. The client therefore resolves the configured name to an ID on
// first use and caches the result.
type Headscale struct {
	BaseURL string
	APIKey  string
	HTTP    *http.Client

	userIDCache map[string]string
}

var _ Controller = (*Headscale)(nil)

type HeadscaleError struct {
	Status int
	Op     string
	Body   string
}

func (e *HeadscaleError) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("headscale: %s: status %d", e.Op, e.Status)
	}
	return fmt.Sprintf("headscale: %s: status %d: %s", e.Op, e.Status, e.Body)
}

func (e *HeadscaleError) IsClientError() bool {
	if e.Status == http.StatusRequestTimeout || e.Status == http.StatusTooManyRequests {
		return false
	}
	return e.Status >= 400 && e.Status < 500
}

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

func (h *Headscale) ExpireKey(ctx context.Context, keyID string) error {
	if keyID == "" {
		return errors.New("headscale: keyID is empty")
	}
	body := map[string]string{"id": keyID}
	return h.do(ctx, http.MethodPost, "/api/v1/preauthkey/expire", body, nil, "expire preauthkey")
}

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
