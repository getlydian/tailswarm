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
	// User is the Headscale user (or equivalent tenant) that owns the
	// minted key. Comes from tailswarm config, not from per-service
	// labels.
	User string

	// Tags is the ACL tag set the joining node will assert. Headscale
	// requires these be allow-listed in the ACL policy for the user.
	Tags []string

	// Ephemeral asks the controller for a key whose node is removed
	// automatically when it logs out / disconnects. tailswarm always
	// sets this true for sidecars.
	Ephemeral bool

	// Reusable allows the same key to authenticate multiple nodes.
	// tailswarm always sets this false: one key per sidecar, per
	// reconcile.
	Reusable bool

	// Expiration is how long the key is valid for. tailswarm uses a
	// short window (minutes) since the sidecar consumes the key
	// immediately on first start.
	Expiration time.Duration
}

// Key is what a Controller hands back after minting. The Secret is the
// preauth key string the sidecar exchanges for an identity; the ID is
// what callers pass to ExpireKey for cleanup.
type Key struct {
	ID        string
	Secret    string
	ExpiresAt time.Time
}

// Node is a registered machine on the tailnet. Only the fields tailswarm
// needs for orphan cleanup are modeled.
type Node struct {
	ID       string
	Hostname string
	User     string
	Tags     []string
}

// Controller is the control-plane abstraction from DESIGN.md §5.2. The
// Headscale implementation is the only one shipped today; the interface
// exists so the reconciler can be developed and tested against fakes,
// and so a SaaS Tailscale equivalent can drop in later.
type Controller interface {
	CreateEphemeralKey(ctx context.Context, req KeyRequest) (Key, error)
	ExpireKey(ctx context.Context, keyID string) error
	DeleteNode(ctx context.Context, nodeID string) error
	ListNodes(ctx context.Context, user string) ([]Node, error)
}
