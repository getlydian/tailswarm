# tailswarm

tailswarm is a Go daemon that watches a Docker Swarm cluster for services
opted into a tailnet via deploy labels, provisions a dedicated
[`tailscale/tailscale`](https://hub.docker.com/r/tailscale/tailscale) sidecar
for each one, and reconciles the world against a self-hosted
[Headscale](https://github.com/juanfont/headscale) controller. One
manager-pinned daemon, no per-node agent, no operator action beyond
labelling a service.

See [DESIGN.md](DESIGN.md) for the full design rationale.

## Quickstart

Deploy tailswarm itself on a Swarm manager. The example stack runs
tailswarm behind a [`tecnativa/docker-socket-proxy`](https://github.com/Tecnativa/docker-socket-proxy)
so the daemon never sees the raw socket.

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
sidecar:
  image: tailscale/tailscale:v1.78
YAML
docker config create tailswarm_config tailswarm.yml

# 3. Internal overlay between tailswarm and the socket proxy.
docker network create -d overlay --attachable tailswarm-internal

# 4. Deploy.
docker stack deploy -c examples/tailswarm-stack.yml tailswarm
```

To opt a service in, add deploy labels — see
[`examples/sample-app.yml`](examples/sample-app.yml) for a worked example:

```yaml
deploy:
  labels:
    tailswarm.enable: "true"
    tailswarm.network: "app-overlay"
```

tailswarm picks up the new service from the Docker event stream, mints an
ephemeral pre-auth key, and creates a sidecar attached to `app-overlay`.

## Configuration

Configuration is YAML plus environment variables. Env wins. For each YAML
key the table lists the matching env override.

| YAML key                          | Env override                              | Default            | Description                                                                       |
| --------------------------------- | ----------------------------------------- | ------------------ | --------------------------------------------------------------------------------- |
| `headscale.url`                   | `TAILSWARM_HEADSCALE_URL`                 | *(required)*       | Headscale base URL (e.g. `https://headscale.internal`).                           |
| `headscale.api_key_file`          | `TAILSWARM_HEADSCALE_API_KEY_FILE`        | —                  | Path to a file containing the bearer token (typically a Docker secret mount).     |
| —                                 | `TAILSWARM_HEADSCALE_API_KEY`             | —                  | Inline bearer token. Use the file form in production.                             |
| `headscale.user`                  | `TAILSWARM_HEADSCALE_USER`                | *(required)*       | Headscale user that owns every sidecar node. Must already exist.                  |
| `headscale.key_expiration`        | `TAILSWARM_HEADSCALE_KEY_EXPIRATION`      | `5m`               | Lifetime of minted pre-auth keys; only needs to outlive sidecar startup.          |
| `sidecar.image`                   | `TAILSWARM_SIDECAR_IMAGE`                 | *(required)*       | Pinned Tailscale image, e.g. `tailscale/tailscale:v1.78`.                         |
| `reconcile.full_resync_interval`  | `TAILSWARM_RECONCILE_FULL_RESYNC_INTERVAL`| `30s`              | Period of the safety-net full list, on top of event-driven reconciles.            |
| `reconcile.rate_limit_rps`        | `TAILSWARM_RECONCILE_RATE_LIMIT_RPS`      | `5`                | Global cap on Headscale API calls per second.                                     |
| `label_namespace`                 | `TAILSWARM_LABEL_NAMESPACE`               | `tailswarm`        | Label prefix. Set to e.g. `tailswarm-stage` to run two daemons against one Swarm. |
| `allowed_tag_prefixes`            | `TAILSWARM_ALLOWED_TAG_PREFIXES`          | —                  | Comma-separated list of ACL-tag prefixes a service is allowed to request.         |

The daemon also reads `TAILSWARM_CONFIG` for the path to the YAML file and
`DOCKER_HOST` for the Docker endpoint (set to the socket proxy in the
example stack).

## Deploy labels

| Label                          | Required | Default                  | Description                                                                                            |
| ------------------------------ | -------- | ------------------------ | ------------------------------------------------------------------------------------------------------ |
| `tailswarm.enable`             | yes      | —                        | Must be `"true"` for tailswarm to attach a sidecar.                                                    |
| `tailswarm.network`            | when >1  | sole user overlay        | Which overlay the sidecar joins. Required when the target is on more than one user overlay.           |
| `tailswarm.hostname`           | no       | `<stack>-<service>`      | Tailnet hostname for the sidecar.                                                                      |
| `tailswarm.tag`                | no       | `tag:swarm-<service>`    | ACL tag override. Must start with one of the daemon's `allowed_tag_prefixes`.                          |
| `tailswarm.advertise-routes`   | no       | —                        | Comma-separated CIDRs passed through to `tailscale up --advertise-routes`. Headscale must approve them.|

If `label_namespace` is changed, replace the `tailswarm.` prefix
accordingly (e.g. `tailswarm-stage.enable`).

## Lifecycle

| Event                                      | tailswarm action                                                            |
| ------------------------------------------ | --------------------------------------------------------------------------- |
| Service created with `tailswarm.enable`    | Mint key → create sidecar service.                                          |
| Labels change (hostname, tag, network)     | Mint new key → update sidecar in place; old key expired.                    |
| Service removed                            | Remove sidecar → expire key → delete Headscale node.                        |
| Sidecar service missing/unhealthy          | Reconcile recreates it with a fresh key.                                    |
| Headscale node still present after teardown| Best-effort delete on next reconcile.                                       |
| tailswarm restart                          | Resync from labels + sidecar inventory; orphans cleaned on both sides.      |

## Security

tailswarm needs Docker API access from a manager node. Mounting
`docker.sock` directly works but gives the daemon root-equivalent
control of every node. The recommended deployment puts a
[`tecnativa/docker-socket-proxy`](https://github.com/Tecnativa/docker-socket-proxy)
in front of it with only the API sections tailswarm uses enabled
(`SERVICES`, `NETWORKS`, `TASKS`, `EVENTS`, `POST`); everything else stays
default-denied. tailswarm then connects via `DOCKER_HOST=tcp://docker-proxy:2375`
over an internal overlay. A compromised tailswarm can still manipulate
Swarm services (that's its job) but cannot exec into containers, read
secrets, or alter swarm membership. The wired-up version is in
[`examples/tailswarm-stack.yml`](examples/tailswarm-stack.yml); see
[DESIGN.md §8](DESIGN.md#8-security) for the longer discussion, including
the proxy's API-section-vs-resource granularity caveat.

Pre-auth keys are minted with a short expiry (default 5 minutes — long
enough for the sidecar to register, no longer) and are never logged. ACL
tags are namespaced as `tag:swarm-<service>` by default; the
`tailswarm.tag` label can only narrow within `allowed_tag_prefixes`.

## Failure modes

| Failure                              | Behaviour                                                                                  |
| ------------------------------------ | ------------------------------------------------------------------------------------------ |
| Headscale unreachable                | Reconcile errors logged and retried with backoff; existing sidecars left alone.            |
| Docker API unreachable               | Process exits non-zero; Swarm restarts it.                                                 |
| Sidecar fails to come up             | Sidecar's own restart policy handles it; tailswarm only intervenes on spec drift.          |
| Key minted but sidecar create fails  | Key is expired in the rollback path; never leaked.                                         |
| tailswarm crashes mid-reconcile      | On startup, Headscale nodes and managed sidecars are listed; orphans on either side cleaned.|

See [DESIGN.md §7](DESIGN.md#7-failure-modes-and-recovery) for the
exhaustive table.

## Building and testing

```sh
go test -race ./...
go build ./cmd/tailswarm
docker build -t tailswarm:dev .
```

CI runs `go vet`, `go test -race`, `golangci-lint`, and `go build` on
every push (see [`.github/workflows/ci.yml`](.github/workflows/ci.yml));
multi-arch images are published to
`ghcr.io/getlydian/tailswarm` from `main` and tags
(see [`.github/workflows/release.yml`](.github/workflows/release.yml)).

## License

MIT — see [LICENSE](LICENSE).
