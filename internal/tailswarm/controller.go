package tailswarm

import (
	"context"
	"time"
)

// KeyRequest is the input to Controller.CreateEphemeralKey. Mirrors the
// fields Headscale's preauthkey endpoint actually consumes, but is kept
// abstract so a SaaS Tailscale Controller can satisfy it later without
// churning callers.
type KeyRequest struct {
	User       string
	Tags       []string
	Ephemeral  bool
	Reusable   bool
	Expiration time.Duration
}

// Key is what a Controller hands back after minting. The Secret is the
// preauth key string the tsnet server exchanges for an identity; the ID
// is what callers pass to ExpireKey for cleanup.
type Key struct {
	ID        string
	Secret    string
	ExpiresAt time.Time
}

// Controller is the control-plane abstraction. With tsnet there is no
// per-service container that needs orphan-sweep cleanup: ephemeral keys
// produce ephemeral nodes that Headscale removes when the tsnet server
// disconnects. The interface therefore exposes key minting and expiration,
// plus NodeOnline so an egress gateway that can no longer register with
// the key in its spec can be told apart from one that is up.
type Controller interface {
	CreateEphemeralKey(ctx context.Context, req KeyRequest) (Key, error)
	ExpireKey(ctx context.Context, keyID string) error
	// NodeOnline reports whether any node registered under hostname is
	// currently connected to the tailnet.
	NodeOnline(ctx context.Context, hostname string) (bool, error)
}
