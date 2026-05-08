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

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/tailswarm /tailswarm
USER nonroot:nonroot
ENTRYPOINT ["/tailswarm"]
