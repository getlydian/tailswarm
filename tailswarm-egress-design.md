# tailswarm — egress (outbound) proxy mode

**Status:** Draft for review  **Date:** 2026-06-04  **Owner:** Kristen Gilden

## 1. Summary

Today tailswarm is **inbound-only**: for each opted-in service it runs an
in-process `tsnet.Server` that *listens* on the tailnet and forwards
incoming connections down to the service over the shared overlay
(`tailnet listener → overlay`, see `proxy.go`). A service can be *reached*
by other tailnet members but cannot *originate* connections to them.

Egress mode adds the mirror image: a service can **dial out** to other
tailnet members by MagicDNS name. tailswarm runs a `tsnet.Server` for the
egressing service (giving it a tailnet identity/tag), listens on the
**overlay** side, and forwards each connection **out over the tailnet**
via `tsnet.Server.Dial` (`overlay listener → tsnet.Dial`).

Motivating use case: the Thanos query in `lydian-hcloud/ops` must dial the
StoreAPI (`:10901`) of Thanos sidecars/stores in other Swarms
(`lydian/prd`, `lydian/stg`, `kulmin/ops`) to show real-time (< 2h) metrics
fleet-wide. Object-storage federation alone leaves a ~2h blind spot on
remote envs — unacceptable for a monitoring system. Egress closes it.

## 2. The existing inbound path (for contrast)

```
remote tailnet client ──► tsnet listener (:Port) ──forwardTCP──► <stack>_<svc>:Port  (overlay)
```

`forwardTCP(conn, target)` is already direction-agnostic — it pumps bytes
between an accepted `conn` and a dialed `target`. Egress reuses it
verbatim; only the *listener* and the *dialer* swap sides.

## 3. The egress path

```
local app ──► overlay listener ──forwardTCP──► tsnet.Dial(magicdns-target:Port)  (tailnet)
```

- The egress proxy still owns a `tsnet.Server` (so it has a tailnet
  identity = its tag, which ACLs match as the *source* of outbound
  connections).
- Instead of `srv.Listen` (tailnet side), it `net.Listen`s on the overlay
  side, reachable by the app.
- Instead of a plain `net.Dialer`, it forwards via `srv.Dial(ctx, "tcp",
  "<magicdns-host>:<port>")`, which resolves MagicDNS and routes over the
  tailnet.

`tsnet.Server` exposes `Dial(ctx, network, addr) (net.Conn, error)` — this
is the outbound primitive we don't currently use.

## 4. The addressing question (resolved)

How does the app name the remote target it wants to reach? The app and
the egress path share the overlay, so the app dials *something on the
overlay* and tailswarm forwards it to *something on the tailnet*. We
explored several models (see §4.1 for the rejected ones); the decision is
the **alias gateway**:

### 4.0 Decision — per-app alias gateway (alias model, 4B-ii)

For each service carrying `tailswarm.egress.*` labels, tailswarm
**creates one companion gateway service per egress target** on the swarm,
each attached to the shared overlay and carrying that target's MagicDNS name
as an overlay **network alias**. Docker's built-in overlay DNS then resolves
the real name to the right gateway, so the app dials the *real remote name*
and is transparently intercepted and forwarded out the tailnet.

> **Granularity: one gateway per target, not per app.** A single gateway
> holding every target as an alias does not work when an app has two or more
> targets on the *same port* (the motivating Thanos case: five StoreAPI
> targets all on `:10901`). Docker assigns **one VIP per service per
> network** regardless of alias count, so all aliases collapse to one VIP and
> one container IP — the per-target listeners both collide on `bind(:10901)`
> *and* become indistinguishable at L4 (the gateway can't tell which alias
> the client dialed). Giving each target its own gateway service gives each
> its own VIP/task IP, so the listeners never collide and the destination IP
> encodes target identity. All of an app's gateways share the app's single
> `egress.tag`, preserving source identity. (See moby PR #52236, issue
> #33770.)

```
app dials  db-mysql:3306
            └─ Docker overlay DNS resolves "db-mysql" to tsgw-<app>
               (the gateway holds that name as a network alias)
            └─ gateway forwards via srv.Dial("db-mysql:3306")
               over the tailnet to the real node
```

The app's config is **identical to the no-mesh ideal** — e.g.
`PMA_HOST=db-mysql, PMA_PORT=3306`, or `--endpoint=thanos-sidecar-prd-lydian:10901`.
No synthesized ports, no writeback, no second-pass deploy. Adding a
target is a one-line edit to `egress.targets`; tailswarm reconciles the
gateway's alias set from the app's labels. Service churn is handled by
tailswarm, not by humans editing endpoint addresses.

The gateway runs **tailswarm's own image** in a new `gateway` subcommand,
reusing the tsnet + `forwardTCP` code. Its tsnet identity is the app's
`egress.tag`, preserving per-app ACL source identity (see §8).

### 4.1 Rejected alternatives

- **4A — port-map.** App declares `host:remote@local` targets; tailswarm
  opens one overlay listener per target on a (synthesized or
  deterministic) local port; the app dials `tailswarm:<local-port>`.
  Works and needs no managed services, but the app's config is coupled to
  tailswarm's port allocation — endpoints become `tailswarm:21001`
  instead of the readable real name, and either the operator hand-picks
  cluster-unique ports or has to discover auto-assigned ones via a
  log/label writeback. Rejected: the manual, churn-prone endpoint
  bookkeeping is exactly the pain this feature should remove.
- **4B-i — app-side alias.** The app declares the target names as aliases
  on the shared overlay. Doesn't work: aliases resolve to the service
  that declares them, so the app resolves the name to *itself*.
- **4B-iii — tailswarm self-aliases.** tailswarm's own service spec lists
  the union of all egress target names as aliases. Requires tailswarm to
  mutate (and restart) its own spec on every target-set change. Awkward
  and risky; also forces a single tailnet identity for all egress.
- **Single shared swarm-wide gateway.** One gateway for the whole swarm
  rather than one per app. Fewer managed services, but (a) blurs ACL
  source identity — all egress shares one tag — and (b) hits a same-port
  aliasing trap across apps (two targets on `:3306` both resolving to one
  gateway IP, indistinguishable to a raw-TCP listener). Per-app gateways
  fix (a). They do **not** fix (b) on their own — a single app with two
  same-port targets hits the same trap — which is why the granularity is
  ultimately **one gateway per target** (see §4.0).
- **Transparent router (capture by destination IP via SO_ORIGINAL_DST /
  TUN).** Nicest UX — real names and ports, zero gateway management — but
  reintroduces `NET_ADMIN` / `/dev/net/tun`, the privileged networking
  the tsnet redesign deliberately removed. Rejected on those grounds.

## 5. What this costs, and what stays pure

The accepted cost is that **egress reintroduces Docker write access** —
`ServiceCreate` / `ServiceUpdate` / `ServiceRemove`, i.e. `POST: 1` on the
socket proxy (currently `POST: 0`) — and a managed-service lifecycle with
the same create/rotate/teardown discipline the sidecar design once had.

Crucially this is **localized to egress**. Ingress remains pure in-process
tsnet proxies with no Docker-side artifact ("the proxy *is* the artifact",
`state.go`). Only the egress gateway is a managed Docker service.

Two footguns the implementation must handle:

- The gateway must **not** carry `tailswarm.enable`, or the watcher's
  slow-path full list (which filters on `<namespace>.enable=true`) would
  reconcile it as an ingress target. Gateways carry a distinct internal
  marker label and are skipped by the egress-app reconcile.
- The egress-app reconcile must detect "my gateway already exists and
  matches desired" and not recreate it — same hash/diff discipline as the
  existing proxy lifecycle.

## 5.1 End-user experience

Two independent swarms on one tailnet. Swarm A exposes mysql (ingress,
already supported); Swarm B's phpmyadmin dials it (egress, this feature).

**Swarm A — mysql exposes itself (unchanged ingress):**

```yaml
services:
  mysql:
    image: mysql:8
    networks: [tailswarm-overlay]
    ports:
      - target: 3306
    deploy:
      labels:
        tailswarm.enable: "true"
        tailswarm.hostname: "db-mysql"        # its MagicDNS name
        tailswarm.tag: "tag:svc-mysql-db"
```

`db-mysql:3306` is now reachable from anywhere on the tailnet (ACLs
permitting).

**Swarm B — phpmyadmin dials out (egress):**

```yaml
services:
  phpmyadmin:
    image: phpmyadmin:latest
    networks: [tailswarm-overlay]
    environment:
      PMA_HOST: db-mysql        # the REAL remote name
      PMA_PORT: "3306"          # the REAL remote port
    deploy:
      labels:
        tailswarm.egress.enable: "true"
        tailswarm.egress.tag: "tag:svc-phpmyadmin-admin"
        tailswarm.egress.targets: "db-mysql:3306"
```

That is the entire operator surface. tailswarm sees the labels and creates
one gateway per target (here a single `tsgw-phpmyadmin-egress-db-mysql`: its
own image in gateway mode, identity `tag:svc-phpmyadmin-admin`, overlay alias
`db-mysql`), and forwards. The runtime path:

```
phpmyadmin ─connect db-mysql:3306─► overlay DNS → tsgw-…-db-mysql
                                    └─ srv.Dial("db-mysql:3306") over tailnet
                                    └─ Swarm A's ingress proxy → db_mysql:3306
```

Adding another database is a one-line edit:
`tailswarm.egress.targets: "db-mysql:3306, analytics-mysql:3306"`.
tailswarm then runs a second gateway for `analytics-mysql`, leaving the
existing one untouched. No ports to assign, nothing to read back, no
app-config churn.

## 6. Labels

| Label | Meaning |
|---|---|
| `tailswarm.egress.enable` | opt this service into egress |
| `tailswarm.egress.tag` | tailnet identity the outbound connections originate as (ACL source). Validated against `allowed_tags` like the ingress tag. |
| `tailswarm.egress.targets` | comma-separated `host:port` list of MagicDNS targets to reach (no local-port suffix — the app dials the real name) |
| `tailswarm.egress.network` | optional; overlay the gateway joins (defaults to shared overlay) |

A service may use ingress labels, egress labels, or both. They are
independent; a service that only egresses needs no `EndpointSpec.Ports`.

The operator writes **only** these labels on the app. tailswarm derives
and manages the gateway service (its image, auth key, overlay attachment,
and aliases) entirely from them. The gateway itself carries an internal
marker label (e.g. `tailswarm.gateway.for=<app-service-id>`) and never
`tailswarm.enable`.

## 7. Code shape

The egress data plane runs **inside the gateway container** (tailswarm's
own image, `gateway` mode); the egress *control* plane runs in the main
daemon and manages the gateway's Docker lifecycle.

**Gateway mode (data plane), in the gateway container:**

- A `gateway` subcommand on the tailswarm binary. Each gateway fronts one
  target: it starts one `tsnet.Server` (identity = the app's `egress.tag`,
  hostname per-target so sibling gateways' tsnet state dirs don't collide),
  does a single `net.Listen` on the overlay, and forwards each accepted conn
  via a dialer backed by `srv.Dial(ctx, "tcp", "<host>:<port>")`. (The data
  plane still accepts a comma-separated target list and opens one listener
  each; with one target per gateway there is exactly one listener.)
- **`forwardTCP` refactor:** today it dials a literal `target` string with
  a `net.Dialer` (`proxy.go`). Generalize it to take a
  `dial func(ctx) (net.Conn, error)`. Ingress passes a `net.Dialer`-backed
  func to the overlay target; egress passes a `srv.Dial`-backed func to the
  MagicDNS target. The byte-pump body is unchanged.
- Each gateway holds its single target name as an overlay **alias** (set on
  its service spec by the control plane), so Docker DNS resolves the real
  name to that gateway. One gateway (hence one VIP/IP) per target is what
  avoids the same-port ambiguity — a single shared gateway with multiple
  same-port aliases collapses to one IP and cannot demux them (see §4.0).

**Control plane (gateway lifecycle), in the main daemon:**

- **`Target`** gains `EgressTargets []EgressTarget{Host string; Port uint32}`
  and an `Egress bool` (no `LocalPort` — the app dials the real name).
- **`Labels.Parse`** parses the egress label set; `tagAllowed` reused for
  `egress.tag`.
- **`DockerClient`** gains write methods (`CreateService` /
  `UpdateService` / `RemoveService`) — used *only* on the egress path.
  Ingress keeps using the read-only surface.
- **Reconciler:** an egressing service drives a **set** of gateways (one per
  target) through a create/remove lifecycle keyed on each target's
  `host:port` addr — desired-vs-tracked diff per pass: added targets mint a
  key + create a gateway; removed targets are deleted and their key expired;
  surviving targets are left running untouched (no key churn on unrelated
  targets). The Store entry holds `Gateways []GatewayRef` plus a single
  `GatewayHash` over the whole desired target set (fast no-op path). After a
  daemon restart the still-running gateways are re-adopted by marker label
  (matched back to their target via `TAILSWARM_GATEWAY_TARGETS`) rather than
  recreated. Gateways are skipped by `Reconcile` via their marker label so
  they are never themselves treated as egress apps.
- **Config:** no new global config required. The gateway image is not a
  knob — at startup the daemon discovers its own image by matching its
  container hostname against the Swarm task list and reading that task's
  service's `ContainerSpec.Image`, then deploys gateways with that exact
  (digest-pinned) image. This needs the `TASKS` API section in addition to
  `SERVICES`. Discovery failure is non-fatal: a pure-ingress deployment
  never needs it, and an egressing service surfaces the misconfiguration
  as a reconcile error.

## 8. Security / ACL implications

This is the consequential part. Today every `tag:svc-*` is purely a
*destination* in the Headscale ACL — backends never initiate. Egress
introduces `tag:svc-*` as a **source**.

- The asymmetric-trust invariant ("no `tag:svc-*-kulmin` source names a
  `*-lydian` dst") must now be checked for **egress sources too**, not just
  tool→backend. For the metrics case the only egress source is
  `tag:svc-thanos-query-ops-lydian` dialing lydian targets + kulmin's
  query — that's lydian→kulmin, the allowed direction. Safe.
- `allowed_tags` must gate `egress.tag` exactly as it gates the ingress
  tag, so a Swarm can't mint an egress identity it doesn't own. (Same
  mechanism, already present.)
- An egress gateway is a real tailnet client that can dial **any** MagicDNS
  name its ACL permits. The ACL is therefore the sole containment boundary
  for outbound — worth calling out in the admin-mesh doc.
- **Per-app gateways preserve source identity.** Because each egressing app
  gets its own gateway minted under that app's `egress.tag`, the tailnet
  source of an outbound connection is the app, not a shared aggregate. A
  single swarm-wide gateway would have collapsed all egress to one tag and
  defeated source-based ACLs — one reason it was rejected (§4.1).
- **Docker write surface.** Egress requires `POST` back on the socket proxy.
  Scope it to the `SERVICES` section only; tailswarm still needs no
  `CONTAINERS`, `EXEC`, `SECRETS`, etc. The gateway containers themselves
  remain unprivileged userspace tsnet — no `NET_ADMIN`, no `/dev/net/tun`.

## 9. Milestones

1. **M1** — `forwardTCP` refactor to a dial func + `gateway` subcommand
   (overlay-listen → `srv.Dial`), unit-tested with a loopback tsnet pair
   (one ingress, one egress).
2. **M2** — egress label parsing (`host:port`), `tagAllowed` reuse;
   `DockerClient` write methods + marker-label skip.
3. **M3** ✅ — reconciler wiring: gateway create/rotate/teardown lifecycle,
   Store gains `GatewayServiceID` (+ `GatewayHash`/`GatewayKeyID`), hashing
   includes egress targets + gateway image. Gateways are adopted by marker
   label after a daemon restart rather than recreated.
4. **M4** ✅ — docs + example stack (an abstract worker dialing a remote
   database; one swarm exposing, one egressing). README gains egress
   labels and egress lifecycle/security notes; `examples/egress-app.yml`
   is the worked example; the `gateway` subcommand entrypoint
   (`RunGateway`) is wired in `main.go`; the gateway image is
   auto-discovered from the daemon's own service (no config knob), and
   gateway services are named `tsgw-<hostname>`.
5. **M5** — release image (gateway mode ships in the same image); bump
   pinned digest in ops `stacks/`.

## 10. Decision log

- [x] Addressing model: **alias gateway (4B-ii)**. App dials the real
      MagicDNS name; tailswarm manages companion gateway(s) carrying the
      target names as overlay aliases. (2026-06-04)
- [x] Gateway granularity: **one gateway per target**, not per app
      (supersedes the original per-app gateway plan). A single per-app
      gateway with multiple same-port targets collapses to one Docker VIP /
      one container IP — the per-target listeners collide on `bind` and are
      indistinguishable at L4. Per-target services each get their own VIP/IP;
      all share the app's `egress.tag`. The Store tracks `Gateways
      []GatewayRef`; the reconciler diffs the target set per pass, leaving
      surviving gateways untouched. Surfaced by the Thanos query crash (five
      `:10901` targets → `bind: address already in use`). (2026-06-04)
- [x] Local ports: **none** — the alias model dials the real name, so the
      `@local-port` / auto-assign question is moot. (2026-06-04)
- [x] Gateway image: **tailswarm's own image** in a `gateway` subcommand,
      not a separate image. **Auto-discovered** from the daemon's own Swarm
      service at startup (no config knob) so gateways always match the
      running daemon's digest and can't drift. (2026-06-04)
- [x] An egressing-only service may also be an ingress destination in the
      same labels block — yes, independent. (2026-06-04)
- [x] Gateway placement constraints / restart policy defaults: **none —
      rely on Swarm defaults** (1 replica, restart-on-failure). The gateway
      is a stateless forwarder; tsnet identity is keyed on hostname+state
      dir, so a reschedule re-registers cleanly. Add constraints later only
      if a deployment needs the gateway pinned near its app. (2026-06-04)
- [x] Debounce strategy for alias updates under rapid target churn:
      **none beyond the existing reconcile machinery**. The per-serviceID
      sharded queue dedupes pending work, and reconcileEgress re-reads the
      live spec each pass and no-ops on `gatewayHash` match, so a burst of
      target edits collapses to a single update converging on the latest
      desired set. (2026-06-04)
