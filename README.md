# tailswarm

tailswarm is a Go daemon that watches a Docker Swarm cluster for services
opted into a tailnet via deploy labels, brings up an in-process
[`tsnet`](https://pkg.go.dev/tailscale.com/tsnet) server for each one,
and TCP-forwards tailnet connections to the real service over a shared
overlay network. One manager-pinned daemon, no sidecar containers, no
per-node agent, no `NET_ADMIN`, no `/dev/net/tun`.

## How it works

```
tailswarm-overlay
  ├── tailswarm           (one tsnet.Server per opted-in service)
  ├── stack-a_redis       (joins by label)
  └── stack-b_mysql       (joins by label)
```

All managed services join a single shared overlay (`tailswarm-overlay`
by default). tailswarm itself is statically attached to that overlay at
deploy time, so it can resolve every managed service's Swarm DNS name
(`<stack>_<service>`) and dial the TCP ports declared on each service's
`EndpointSpec`. Services remain isolated from each other at the overlay
level — they only see each other over the tailnet.

That is the **inbound** path: a service is *reached* from the tailnet.
tailswarm also supports **egress** — a service that *dials out* to
another tailnet member by its real MagicDNS name. For each egressing
service tailswarm deploys a companion gateway per target (its own image in
`gateway` mode) that listens on the overlay and forwards out the tailnet. See
[Egress (outbound) labels](#egress-outbound-labels).

## Quickstart

```sh
# 1. Headscale API token, mounted as a Docker secret.
printf '%s' "$HEADSCALE_API_KEY" \
  | docker secret create tailswarm_headscale_api_key -

# 2. Daemon config (edit headscale.url + headscale.user first).
cat > tailswarm.yml <<'YAML'
headscale:
  url: https://headscale.internal
  api_key_file: /run/secrets/tailswarm_headscale_api_key
  user: swarm
network: tailswarm-overlay
tsnet:
  state_dir: /var/lib/tailswarm
YAML
docker config create tailswarm_config tailswarm.yml

# 3. The shared overlay every managed service will join.
docker network create -d overlay --attachable tailswarm-overlay

# 4. The internal overlay between tailswarm and the socket proxy.
docker network create -d overlay --attachable tailswarm-internal

# 5. A volume so tsnet identities survive restarts.
docker volume create tailswarm-tsnet-state

# 6. Deploy.
docker stack deploy -c examples/tailswarm-stack.yml tailswarm
```

To opt a service in, add the label and join the shared overlay — see
[`examples/sample-app.yml`](examples/sample-app.yml):

```yaml
services:
  api:
    image: nginxdemos/hello:plain-text
    networks:
      - tailswarm-overlay
    ports:
      - target: 80
    deploy:
      labels:
        tailswarm.enable: "true"
```

tailswarm picks up the new service from the Docker event stream, mints
an ephemeral pre-auth key, brings up an in-process tsnet server with the
declared TCP ports, and forwards every tailnet connection to
`<stack>_<service>:<port>` on the shared overlay.

## Configuration

Configuration is YAML plus environment variables. Env wins.

| YAML key                          | Env override                              | Default             | Description                                                                       |
| --------------------------------- | ----------------------------------------- | ------------------- | --------------------------------------------------------------------------------- |
| `headscale.url`                   | `TAILSWARM_HEADSCALE_URL`                 | *(required)*        | Headscale base URL (e.g. `https://headscale.internal`). Also used as the tsnet `ControlURL`. |
| `headscale.api_key_file`          | `TAILSWARM_HEADSCALE_API_KEY_FILE`        | —                   | Path to a file containing the bearer token (typically a Docker secret mount).     |
| —                                 | `TAILSWARM_HEADSCALE_API_KEY`             | —                   | Inline bearer token. Use the file form in production.                             |
| `headscale.user`                  | `TAILSWARM_HEADSCALE_USER`                | *(required)*        | Headscale user that owns every minted node. Must already exist.                   |
| `headscale.key_expiration`        | `TAILSWARM_HEADSCALE_KEY_EXPIRATION`      | `5m`                | Lifetime of minted pre-auth keys; only needs to outlive tsnet registration.       |
| `network`                         | `TAILSWARM_NETWORK`                       | `tailswarm-overlay` | Shared overlay tailswarm and managed services join.                               |
| `tsnet.state_dir`                 | `TAILSWARM_TSNET_STATE_DIR`               | `/var/lib/tailswarm`| Directory each tsnet server persists its node identity under (one subdir per hostname). |
| `reconcile.full_resync_interval`  | `TAILSWARM_RECONCILE_FULL_RESYNC_INTERVAL`| `30s`               | Period of the safety-net full list, on top of event-driven reconciles.            |
| `reconcile.rate_limit_rps`        | `TAILSWARM_RECONCILE_RATE_LIMIT_RPS`      | `5`                 | Global cap on Headscale API calls per second.                                     |
| `label_namespace`                 | `TAILSWARM_LABEL_NAMESPACE`               | `tailswarm`         | Label prefix. Set to e.g. `tailswarm-stage` to run two daemons against one Swarm. |
| `allowed_tags`                    | `TAILSWARM_ALLOWED_TAGS`                  | —                   | Comma-separated list of whole-tag glob patterns a service is allowed to request. `*` matches any run of characters; patterns are anchored at both ends. |

The daemon also reads `TAILSWARM_CONFIG` for the path to the YAML file
and `DOCKER_HOST` for the Docker endpoint (set to the socket proxy in
the example stack).

## Deploy labels

| Label                | Required | Default                  | Description                                                                                          |
| -------------------- | -------- | ------------------------ | ---------------------------------------------------------------------------------------------------- |
| `tailswarm.enable`   | yes      | —                        | Must be `"true"` for tailswarm to bring up a tsnet server.                                           |
| `tailswarm.hostname` | no       | `<stack>-<service>`      | Tailnet hostname.                                                                                    |
| `tailswarm.tag`      | no       | `tag:swarm-<service>`    | ACL tag override. Must match one of the daemon's `allowed_tags` patterns.                            |
| `tailswarm.network`  | no       | shared overlay           | Override the shared overlay for the edge case where a service can't join `tailswarm-overlay`.        |

Ports are sourced automatically from the service's `EndpointSpec.Ports`
(TCP only). If `label_namespace` is changed, replace the `tailswarm.`
prefix accordingly (e.g. `tailswarm-stage.enable`).

### Egress (outbound) labels

The labels above make a service *reachable* from the tailnet (inbound).
Egress is the mirror image: it lets a service *dial out* to another
tailnet member by its real MagicDNS name. A service may use ingress
labels, egress labels, or both — they are independent.

| Label                       | Required | Default                       | Description                                                                                                                  |
| --------------------------- | -------- | ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `tailswarm.egress.enable`   | yes      | —                             | Must be `"true"` to opt the service into outbound proxying.                                                                  |
| `tailswarm.egress.targets`  | yes      | —                             | Comma-separated `host:port` list of MagicDNS targets to reach. No local-port suffix — the app dials the real remote name.    |
| `tailswarm.egress.tag`      | no       | `tag:swarm-<service>`         | Tailnet identity outbound connections originate as (the ACL *source*). Must match one of the daemon's `allowed_tags` patterns. |
| `tailswarm.egress.hostname` | no       | `<stack>-<service>-egress`    | Base tailnet hostname; each per-target gateway appends its target host (`<base>-<target-host>`).                              |
| `tailswarm.egress.network`  | no       | shared overlay                | Override the overlay the gateway joins.                                                                                      |

The operator writes **only** these labels on the app. For each egressing
service tailswarm derives and manages one companion **gateway** service
**per target** (`tsgw-<egress-hostname>-<target-host>`) running its own
image in `gateway` mode: each joins the overlay carrying its target host as
a network alias, so Docker's overlay DNS resolves the real remote name to
that gateway, which forwards the connection out the tailnet under the app's
`egress.tag`. One gateway per target is required because Docker assigns a
single VIP per service per network — multiple same-port targets on one
shared gateway would collide on bind and be indistinguishable at L4. The
app's config names the real remote host and port — no synthesized ports,
nothing to read back. Adding a target is a one-line edit to
`tailswarm.egress.targets`; tailswarm spins up another gateway for it and
leaves the existing ones running.

The gateway image needs no configuration: tailswarm runs the same image
the daemon itself runs, discovered at startup from the daemon's own Swarm
service. Egress requires `POST` enabled on the socket proxy (see
[Security](#security)).

See [`examples/egress-app.yml`](examples/egress-app.yml) for a worked
example.

## Lifecycle

| Event                                      | tailswarm action                                                          |
| ------------------------------------------ | ------------------------------------------------------------------------- |
| Service created with `tailswarm.enable`    | Mint key → start tsnet server → open per-port listeners.                  |
| Labels or ports change                     | Mint new key → start replacement tsnet server → close old one, expire old key. |
| Service removed or disabled                | Close tsnet server → expire key. Headscale auto-removes the ephemeral node. |
| Service created with `tailswarm.egress.enable` | Mint key → create the gateway service (its own image, `gateway` mode) with the target hosts as overlay aliases under `egress.tag`. |
| Egress targets or tag change               | Mint new key → update the gateway in place → expire the old key. A matching desired-spec hash is a no-op. |
| Egress disabled or service removed         | Remove the gateway service → expire its key. |
| tailswarm restarts                         | tsnet servers restart from persisted state in `tsnet.state_dir`; no new Headscale key needed if node identity is intact. Egress gateways outlive the daemon and are re-adopted by their marker label rather than recreated. |

## Security

tailswarm needs Docker API access from a manager node. The recommended
deployment puts a
[`tecnativa/docker-socket-proxy`](https://github.com/Tecnativa/docker-socket-proxy)
in front of it with only the API sections tailswarm uses enabled
(`SERVICES`, `NETWORKS`, `TASKS`, `EVENTS`). Ingress is read-only: it
never creates, updates, or removes a Docker service, so a pure-ingress
deployment runs with `POST: 0`. Egress reintroduces write access —
gateway create/update/remove — so it requires `POST`, scoped to the
`SERVICES` section. tailswarm still needs no `CONTAINERS`, `EXEC`,
`SECRETS`, etc. It connects via `DOCKER_HOST=tcp://docker-proxy:2375`
over an internal overlay.

tsnet runs in userspace — the tailswarm container *and* every egress
gateway need no `NET_ADMIN`, no `/dev/net/tun`, and no privileged flags.
Pre-auth keys are minted with a short expiry (default 5 minutes — long
enough for tsnet to register, no longer) and are never logged. ACL tags
are namespaced as `tag:swarm-<service>` by default; the `tailswarm.tag`
and `tailswarm.egress.tag` labels can only narrow within `allowed_tags`.

Egress makes a `tag:svc-*` a tailnet *source*, not just a destination: a
gateway can dial any MagicDNS name its ACL permits, so the Headscale ACL
is the sole containment boundary for outbound traffic. Per-app gateways
keep the tailnet source identity equal to the app's own `egress.tag`
rather than collapsing all egress to one shared tag.

## Failure modes

| Failure                              | Behaviour                                                                                  |
| ------------------------------------ | ------------------------------------------------------------------------------------------ |
| Headscale unreachable                | Reconcile errors logged and retried with backoff; existing tsnet servers keep running.     |
| Docker API unreachable               | Process exits non-zero; Swarm restarts it. tsnet state is durable.                         |
| Target service unreachable on dial   | Individual connection fails; the listener stays up and retries on the next dial.           |
| Key minted but tsnet startup fails   | Key is expired in the rollback path; never leaked.                                         |
| tailswarm crashes mid-reconcile      | On restart, tsnet identities reload from disk; ephemeral nodes that disconnected mid-flight are auto-cleaned by Headscale. |

## Building and testing

```sh
go test -race ./...
go build ./cmd/tailswarm
docker build -t tailswarm:dev .
```

CI runs `go vet`, `go test -race`, `golangci-lint`, and `go build` on
every push (see [`.github/workflows/ci.yml`](.github/workflows/ci.yml));
multi-arch images are published to `ghcr.io/getlydian/tailswarm` from
`main` and tags (see
[`.github/workflows/release.yml`](.github/workflows/release.yml)).

## License

MIT — see [LICENSE](LICENSE).
