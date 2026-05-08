# tailswarm — Design Document

**Status:** Draft v0.1
**Date:** 2026-05-07
**Owner:** Kristen Gilden

## 1. Summary

tailswarm is a Go daemon that watches a Docker Swarm cluster for services
that opt in to the tailnet via deploy labels and, for each opted-in service,
provisions a dedicated [tailscale](https://tailscale.com/) sidecar service
attached to the same overlay network. tailswarm mints an ephemeral pre-auth
key from a [Headscale](https://github.com/juanfont/headscale) controller
for each sidecar and reconciles sidecar state to match the desired set of
labeled services. When a service is removed or its labels are stripped,
tailswarm tears the sidecar down and expires the corresponding key.

The design is modeled on Traefik's Swarm provider (single daemon on a
manager node, label-driven, no per-node agent) and on the spirit of
[tcmuxer](https://github.com/getlydian/tcmuxer) (label discovery + a
periodic reconcile loop, with event-driven nudges).

## 2. Goals and non-goals

### Goals

- Zero-touch tailnet onboarding for Swarm services: add a label, get a
  sidecar.
- Single deployment unit. One tailswarm service constrained to a manager
  node, the same way Traefik is typically run.
- Headscale-first. v1 targets Headscale only (REST `/api/v1` with Bearer
  auth).
- Ephemeral identity. Every sidecar uses an ephemeral, single-use key and
  registers with an ACL tag derived from the service name (overridable via
  label) so Headscale ACLs can target services rather than node IDs.
- Convergent reconciliation. The world can drift (manual `docker service
  rm`, Headscale node expiry, daemon restart) and tailswarm should pull it
  back to the desired state without operator intervention.

### Non-goals (v1)

- Tailscale SaaS support. The control-plane interface is abstracted but
  only a Headscale implementation ships.
- Non-Swarm orchestrators (plain Docker, Kubernetes, Nomad).
- Userspace SOCKS/HTTP proxy mode for the sidecar. v1 assumes the kernel
  module / `/dev/net/tun` is available on worker nodes.
- Multi-tailnet routing (one tailswarm = one Headscale instance).
- Transparent egress via shared network namespace
  (`network_mode: container:` / `service:`) is not on the table — Swarm
  doesn't support those modes for services. Reachability is via
  attaching the sidecar to the target's overlay and using
  Tailscale's subnet/proxy features.
- Per-task sidecars. Every service gets exactly one sidecar regardless
  of replica count; per-replica tailnet identity is out of scope.

## 3. Background and prior art

- **Traefik Swarm provider** runs a single replica pinned to a manager
  node, reads services through the Docker API, and acts on
  `traefik.*` deploy labels. tailswarm copies this topology and label
  shape.
- **tcmuxer** is a sister project that discovers Swarm services via a
  `tcmuxer.url` label and reconciles every 30s. tailswarm reuses the same
  discovery + reconcile loop pattern.
- **Tailscale's Docker sidecar pattern** (`tailscale/tailscale` image with
  `TS_AUTHKEY`, `TS_HOSTNAME`, `TS_EXTRA_ARGS`, `TS_STATE_DIR`) is the
  building block we orchestrate.
- **Headscale's pre-auth key API** exposes
  `POST /api/v1/preauthkey` with fields for `user`, `reusable`,
  `ephemeral`, `expiration`, `aclTags`, authenticated via
  `Authorization: Bearer <api-key>`.

## 4. User-facing model

### 4.1 Opting a service in

A service opts in by setting deploy labels:

```yaml
deploy:
  labels:
    tailswarm.enable: "true"
    tailswarm.network: "app-overlay"          # which network to attach the sidecar to
    tailswarm.hostname: "billing-api"          # optional, defaults to <stack>-<service>
    tailswarm.tag: "tag:billing"               # optional, override derived ACL tag
    tailswarm.advertise-routes: "10.0.5.0/24"  # optional, passed through to tailscale up
```

Only `tailswarm.enable=true` is required. `tailswarm.network` is required
when the service is attached to more than one overlay network (same
disambiguation reason Traefik has `traefik.docker.network`); if there is
exactly one user-defined overlay it is auto-selected.

### 4.2 What tailswarm does in response

For each opted-in service, tailswarm ensures a companion service exists:

- Name: `tailswarm_<service-id-prefix>_<service-name>` (deterministic and
  collision-free across stacks).
- Image: `tailscale/tailscale:<pinned-version>` (configurable).
- Network: only the overlay named by `tailswarm.network`.
- Placement: replicated, 1 replica, no node constraint.
- Env: `TS_AUTHKEY` (ephemeral, freshly minted), `TS_HOSTNAME`,
  `TS_EXTRA_ARGS` (built from the labels), `TS_STATE_DIR=/var/lib/tailscale`,
  `TS_USERSPACE=false`.
- Capabilities: `NET_ADMIN`, `SYS_MODULE`; `/dev/net/tun` device mapped in.
- Owner labels: `tailswarm.managed=true`, `tailswarm.target-service=<id>`,
  `tailswarm.target-version=<service-spec-version>` so the sidecar is
  unambiguously matched back to its target.

### 4.3 Lifecycle

| Event                                     | tailswarm action                                                           |
| ----------------------------------------- | -------------------------------------------------------------------------- |
| Service created with `tailswarm.enable`   | Mint key → create sidecar service                                          |
| Labels change (hostname, tag, network)    | Mint new key → update sidecar in place (`UpdateService`); old key expired  |
| Service removed                           | Remove sidecar → expire key → delete Headscale node                        |
| Sidecar service missing/unhealthy         | Reconcile recreates it with a fresh key                                    |
| Headscale node still present after teardown | Best-effort delete on next reconcile                                       |
| tailswarm restart                         | Resync from labels + sidecar inventory; orphans cleaned                    |

## 5. Architecture

```
                ┌──────────────────────────────────────────────────┐
                │                tailswarm (1 replica)              │
                │                                                   │
   docker.sock ─┤  Swarm watcher  ─┐                                │
   (manager)    │  (events + poll) │                                │
                │                  ▼                                │
                │            Reconciler  ──► Sidecar planner        │
                │                  │           │                    │
                │                  ▼           ▼                    │
                │            State store   Docker client            │
                │                  ▲           │                    │
                │  Headscale ◄─────┘           ▼                    │
                │  client          (services: create/update/rm)     │
                └──────────────────────────────────────────────────┘
                          │
                          ▼
                 Headscale REST API
                 (preauthkey + node)
```

### 5.1 Components

- **Swarm watcher**: subscribes to `/events` filtered to
  `type=service` and runs a periodic full list (default 30s) as a safety
  net. Both feed a single channel of `Reconcile(serviceID)` requests.
- **Reconciler**: serializes work per service ID. For each tick it
  computes the desired sidecar spec from the target's labels, diffs
  against the live sidecar, and applies create/update/remove operations.
- **Sidecar planner**: pure function `(target spec, config) → sidecar
  spec`. Easy to unit-test, no I/O.
- **Headscale client**: thin wrapper over the REST API. Methods:
  `CreatePreAuthKey(user, tags, ephemeral=true, reusable=false,
  expiration=<short>)`, `ExpirePreAuthKey(key)`, `ListNodes(user)`,
  `DeleteNode(id)`. Uses a single bearer token loaded from a Docker
  secret or env var.
- **State store**: in-memory map `serviceID → {sidecarID, lastSpecHash,
  preAuthKeyID, headscaleNodeID, lastReconcileAt}`. Authoritative source
  is always the Docker + Headscale state — the in-memory store is a
  cache that is rebuilt on startup by listing
  `tailswarm.managed=true` services and Headscale nodes for the
  configured user.

### 5.2 Control-plane interface

```go
type Controller interface {
    CreateEphemeralKey(ctx context.Context, req KeyRequest) (Key, error)
    ExpireKey(ctx context.Context, keyID string) error
    DeleteNode(ctx context.Context, nodeID string) error
    ListNodes(ctx context.Context, user string) ([]Node, error)
}
```

The Headscale implementation is the only one shipped; the interface
exists so the SaaS Tailscale equivalent can drop in later without
churning the reconciler.

### 5.3 Concurrency

- One reconcile worker per `serviceID` (sharded by hash). This keeps the
  Headscale API call rate bounded and avoids two reconciles fighting
  over the same sidecar.
- A global rate limiter on Headscale calls (default 5 RPS, configurable).
- The Docker event stream is the fast path; the periodic full-list is
  the slow path. Both enqueue into the same per-service queue and dedupe.

## 6. Configuration

tailswarm itself is configured via env vars and/or a YAML file, with env
winning. Minimal config:

```yaml
headscale:
  url: https://headscale.internal
  api_key_file: /run/secrets/headscale_api_key
  user: swarm

sidecar:
  image: tailscale/tailscale:v1.78
  state_volume: tailswarm_state    # named volume per sidecar (auto-created)

reconcile:
  full_resync_interval: 30s
  rate_limit_rps: 5

label_namespace: tailswarm          # in case you want to run two instances
```

`label_namespace` lets two tailswarm instances coexist (e.g. staging vs.
prod tailnets) by reading e.g. `tailswarm-stage.enable=true`.

## 7. Failure modes and recovery

| Failure                                | Behavior                                                                  |
| -------------------------------------- | ------------------------------------------------------------------------- |
| Headscale unreachable                  | Reconcile errors recorded, retried with backoff; existing sidecars untouched |
| Docker API unreachable                 | Process exits non-zero; Swarm restarts it (it's just another service)     |
| Sidecar fails to come up               | Sidecar's own restart policy handles it; tailswarm only intervenes on spec drift |
| Key minted but sidecar create fails    | Key is expired in the rollback path; never leaked                         |
| tailswarm crashes mid-reconcile        | On startup, lists managed sidecars + Headscale nodes; orphans (sidecar without target, node without sidecar) cleaned |
| Node-side `/dev/net/tun` missing       | Sidecar fails fast; surfaced via the target service's healthcheck if configured |

## 8. Security

- Headscale API token is mounted as a Docker secret; never logged.
- The minted ephemeral keys have a short expiry (default 5 min — they
  only need to live long enough for the sidecar to come up and register).
- ACL tags are namespaced (`tag:swarm-<service>`) so a compromised
  sidecar can't impersonate a tag it wasn't granted; the `tailswarm.tag`
  label only narrows or remaps within an allowlist (`allowed_tag_prefixes`
  in tailswarm config).
- tailswarm needs Docker API access from a manager node. Mounting
  `docker.sock` directly works but gives the daemon root-equivalent
  control over every node. **Recommended deployment** is to put a
  [`tecnativa/docker-socket-proxy`](https://github.com/Tecnativa/docker-socket-proxy)
  in front of it, constrained to a manager node, with only the API
  sections tailswarm actually uses enabled:

  ```yaml
  # docker-socket-proxy environment
  SERVICES: 1     # list/inspect/create/update/remove the sidecar services
  NETWORKS: 1     # resolve tailswarm.network → network ID
  TASKS: 1        # optional; needed only if per-task mode (§9.2) is enabled
  EVENTS: 1       # fast-path service change notifications (default-on)
  POST: 1         # required — without it create/update/delete all return 403
  # everything else stays at default-denied: CONTAINERS, EXEC, IMAGES,
  # VOLUMES, SECRETS, AUTH, SWARM, NODES, BUILD, CONFIGS, PLUGINS, …
  ```

  tailswarm then connects via `DOCKER_HOST=tcp://docker-proxy:2375` over
  an internal overlay and never sees the raw socket. A compromised
  tailswarm can still manipulate Swarm services (that's its job) but
  cannot `docker exec` into containers, read secrets, pull arbitrary
  images, or alter swarm membership.

  Caveat: the proxy is API-section-granular, not resource-granular —
  `SERVICES=1, POST=1` permits operating on *any* service, not only those
  labeled `tailswarm.managed=true`. Treat the proxy endpoint as
  sensitive and keep it on an internal overlay.

## 9. Decisions and remaining questions

### 9.1 Decided

- **One sidecar per service**, regardless of replica count. Per-replica
  tailnet identity isn't a use case we have; in-overlay app replicas
  reach the sidecar via Swarm DNS and present a single tailnet identity
  to the rest of the tailnet.
- **No key rotation.** Pre-auth keys are single-use at registration
  time; the resulting node identity persists for the sidecar's
  lifetime, then is reaped when the sidecar is torn down.
- **No transparent-egress mode.** Swarm services can't use
  `network_mode: container:` / `service:`, so the question is moot.
  Reachability is overlay-attachment + Tailscale's proxy/subnet
  features.

### 9.2 Headscale users

A Headscale "user" (formerly "namespace") is a logical owner of nodes,
not an operator login — operators authenticate to Headscale separately
(OIDC, CLI) and that's outside tailswarm's concern. Every registered
node must belong to *some* user, and the admin-scoped API token tells
Headscale which one when minting a pre-auth key.

tailswarm uses a **single dedicated user** (configured as
`headscale.user`, e.g. `swarm`) that owns every sidecar. ACL scoping
is done by **tag**, not by user — that's what `tailswarm.tag` / the
derived `tag:swarm-<service>` is for. The user must already exist in
Headscale; tailswarm fails loud at startup if it doesn't, and will not
create users on the fly.

## 10. Milestones

1. **M1 — skeleton:** Docker client, Swarm event watcher, label parser,
   in-memory state, no Headscale yet (sidecars come up with a static
   key for dev).
2. **M2 — Headscale integration:** ephemeral key minting, node cleanup,
   bearer-auth client.
3. **M3 — reconciliation hardening:** orphan cleanup, rate limiting,
   label-change diffing, restart safety.
4. **M4 — packaging:** Dockerfile, example `docker-compose.yml` for the
   tailswarm service itself, sample stack with a labeled app.
5. **M5 — docs + first internal deploy.**
