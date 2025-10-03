FROM --platform=$BUILDPLATFORM golang:1.31-bookworm AS build

WORKDIR /src

ARG TARGETOS TARGETARCH VERSION=dev

# Keep build lean; no native deps required for parquet/dstore
ENV CGO_ENABLED=0

# Cache modules
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Debug tree in CI
RUN ls -la /src && ls -la /src/cmd || true && ls -la /src/cmd/substreams-sink-parquet || true && find /src -maxdepth 3 -name '*.go' -print || true

# Build
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags "-s -w -X main.version=$VERSION" -o /src/substreams-sink-parquet ./cmd/substreams-sink-parquet

FROM ubuntu:24.04

RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get -y install \
    ca-certificates curl jq && \
    rm -rf /var/cache/apt /var/lib/apt/lists/*

COPY --from=build /src/substreams-sink-parquet /src/substreams-sink-parquet

ENV PATH=/src:$PATH

ENTRYPOINT ["/src/substreams-sink-parquet"]


