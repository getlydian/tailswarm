# tailswarm — tsnet proxy mode

**Status:** Draft  **Date:** 2026-05-08  **Owner:** Kristen Gilden

## 1. Summary

Replace the `tailscale/tailscale` sidecar service model with an in-process
`tsnet.Server` per opted-in service. tailswarm itself acts as the TCP
forwarder: it listens on the tailnet via tsnet and proxies connections to the
real service over a shared overlay network. No sidecar containers, no
`NET_ADMIN`, no TUN device.

## 2. Shared overlay model

Rather than dynamically joining each service's native overlay, all
tailnet-exposed services join a single dedicated overlay (`tailswarm-overlay`
by default, configurable). tailswarm is statically attached to this overlay at
deploy time.

```
tailswarm-overlay
  ├── tailswarm          (tsnet servers, one per opted-in service)
  ├── stack-a_redis      (opts in via label)
  └── stack-b_mysql      (opts in via label)
```

Services reach each other only via the tailnet — they are isolated at the
overlay level since each service's primary overlay remains separate.
`tailswarm.network` is still accepted for the edge case where a service cannot
join the shared overlay, but the shared overlay is the default and recommended
path.

## 3. Label changes

| Label | Change |
|---|---|
| `tailswarm.enable` | unchanged |
| `tailswarm.network` | optional; defaults to `tailswarm-overlay` |
| `tailswarm.hostname` | unchanged |
| `tailswarm.tag` | unchanged |
| `tailswarm.advertise-routes` | dropped for now; subnet routing is out of scope |

Ports are sourced from the service's `EndpointSpec.Ports` (TargetPort +
Protocol). No new label needed.

## 4. Architecture changes

### 4.1 What is removed

- Sidecar planner (no sidecar services created or managed).
- `tailscale/tailscale` image config.
- Headscale pre-auth key minting (tsnet handles auth directly via
  `TS_AUTHKEY` / `tsnet.Server.AuthKey`; one key per tsnet server, minted
  the same way as before but passed to tsnet instead of a sidecar env var).

### 4.2 What is added

**`proxy` type** — owns one `tsnet.Server` and its listeners:

```go
type proxy struct {
    srv      *tsnet.Server
    cancel   context.CancelFunc
}

func startProxy(ctx context.Context, cfg proxyConfig) (*proxy, error) {
    srv := &tsnet.Server{
        Hostname: cfg.hostname,
        AuthKey:  cfg.authKey,
        Dir:      filepath.Join(stateDir, cfg.hostname),
    }
    ctx, cancel := context.WithCancel(ctx)
    for _, p := range cfg.ports {
        ln, err := srv.Listen("tcp", fmt.Sprintf(":%d", p.Target))
        if err != nil { cancel(); return nil, err }
        go forwardTCP(ctx, ln, fmt.Sprintf("%s:%d", cfg.target, p.Target))
    }
    return &proxy{srv: srv, cancel: cancel}, nil
}
```

**`forwardTCP`** — standard bidirectional `io.Copy` loop, context-aware.

**State store** gains `proxy *proxy` alongside existing fields. On service
removal: `proxy.cancel()` → `proxy.srv.Close()`.

### 4.3 Port discovery

```go
svc, _, _ := dockerClient.ServiceInspectWithRaw(ctx, id, types.ServiceInspectOptions{})
for _, ep := range svc.Spec.EndpointSpec.Ports {
    if ep.Protocol == "tcp" {
        // ep.TargetPort is the port to forward to on the overlay DNS name
    }
}
```

The overlay DNS target is the service's Swarm DNS name (`<stack>_<service>`),
resolved inside tailswarm's network namespace since tailswarm is on the same
overlay.

## 5. Capabilities

tsnet operates in userspace — no `/dev/net/tun`, no `NET_ADMIN`. tailswarm's
own service spec loses both. The socket proxy permissions are unchanged.

## 6. State directory

Each tsnet server persists its node identity under
`<state_dir>/<hostname>/`. This should be a named volume so identities
survive tailswarm restarts without re-registering on Headscale.

```yaml
# tailswarm service spec
volumes:
  - tailswarm-tsnet-state:/var/lib/tailswarm
```

## 7. Failure modes

| Failure | Behaviour |
|---|---|
| tsnet fails to register | `startProxy` returns error; reconciler retries with backoff |
| Target service unreachable on dial | Individual connection fails; listener stays up and retries on next connection |
| tailswarm restarts | tsnet servers restart from persisted state; no new Headscale key needed if node identity is intact |
| Service removed mid-connection | `cancel()` closes context; in-flight copies drain then exit |

## 8. Milestones

1. **M1** — `proxy` type + `forwardTCP`, unit-tested with a loopback tsnet server.
2. **M2** — reconciler wired to `startProxy`/`cancel` instead of sidecar create/remove.
3. **M3** — port discovery from `EndpointSpec`, overlay DNS target derivation.
4. **M4** — state volume, Headscale key minting plumbed into tsnet AuthKey.
5. **M5** — remove sidecar planner, update example stacks, internal deploy.
