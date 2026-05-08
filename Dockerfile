FROM golang:1.26.2-alpine AS build
WORKDIR /src

ARG VERSION=dev

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux \
    go build \
        -trimpath \
        -ldflags="-s -w -X main.version=${VERSION}" \
        -o /out/tailswarm \
        ./cmd/tailswarm

# Pre-create the tsnet state dir owned by uid/gid 65532 (distroless
# `nonroot`). When a fresh Docker volume is mounted at /var/lib/tailswarm
# at runtime, Docker copies the mount point's ownership onto the empty
# volume — without this, the volume comes up root:root and the nonroot
# process can't `mkdir` per-tsnet subdirs inside it.
RUN mkdir -p /out/var/lib/tailswarm && chown -R 65532:65532 /out/var/lib/tailswarm

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/tailswarm /tailswarm
COPY --from=build --chown=65532:65532 /out/var/lib/tailswarm /var/lib/tailswarm
USER nonroot:nonroot
ENTRYPOINT ["/tailswarm"]
