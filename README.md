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
| `allowed_tag_prefixes`            | `TAILSWARM_ALLOWED_TAG_PREFIXES`          | —                   | Comma-separated list of ACL-tag prefixes a service is allowed to request.         |

The daemon also reads `TAILSWARM_CONFIG` for the path to the YAML file
and `DOCKER_HOST` for the Docker endpoint (set to the socket proxy in
the example stack).

## Deploy labels

| Label                | Required | Default                  | Description                                                                                          |
| -------------------- | -------- | ------------------------ | ---------------------------------------------------------------------------------------------------- |
| `tailswarm.enable`   | yes      | —                        | Must be `"true"` for tailswarm to bring up a tsnet server.                                           |
| `tailswarm.hostname` | no       | `<stack>-<service>`      | Tailnet hostname.                                                                                    |
| `tailswarm.tag`      | no       | `tag:swarm-<service>`    | ACL tag override. Must start with one of the daemon's `allowed_tag_prefixes`.                        |
| `tailswarm.network`  | no       | shared overlay           | Override the shared overlay for the edge case where a service can't join `tailswarm-overlay`.        |

Ports are sourced automatically from the service's `EndpointSpec.Ports`
(TCP only). If `label_namespace` is changed, replace the `tailswarm.`
prefix accordingly (e.g. `tailswarm-stage.enable`).

## Lifecycle

| Event                                      | tailswarm action                                                          |
| ------------------------------------------ | ------------------------------------------------------------------------- |
| Service created with `tailswarm.enable`    | Mint key → start tsnet server → open per-port listeners.                  |
| Labels or ports change                     | Mint new key → start replacement tsnet server → close old one, expire old key. |
| Service removed or disabled                | Close tsnet server → expire key. Headscale auto-removes the ephemeral node. |
| tailswarm restarts                         | tsnet servers restart from persisted state in `tsnet.state_dir`; no new Headscale key needed if node identity is intact. |

## Security

tailswarm needs Docker API access from a manager node. The recommended
deployment puts a
[`tecnativa/docker-socket-proxy`](https://github.com/Tecnativa/docker-socket-proxy)
in front of it with only the API sections tailswarm uses enabled
(`SERVICES`, `NETWORKS`, `TASKS`, `EVENTS`); the tsnet design no longer
needs `POST`, since the daemon never creates, updates, or removes any
Docker service. tailswarm connects via `DOCKER_HOST=tcp://docker-proxy:2375`
over an internal overlay.

tsnet runs in userspace — the tailswarm container needs no `NET_ADMIN`,
no `/dev/net/tun`, and no privileged flags. Pre-auth keys are minted
with a short expiry (default 5 minutes — long enough for tsnet to
register, no longer) and are never logged. ACL tags are namespaced as
`tag:swarm-<service>` by default; the `tailswarm.tag` label can only
narrow within `allowed_tag_prefixes`.

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
