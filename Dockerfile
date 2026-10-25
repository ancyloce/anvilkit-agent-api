# anvilkit-agent-api: built from this repository alone (the build context is the
# repository root; nothing from the parent checkout is read). The generated
# contract module is an ordinary versioned dependency resolved through GOPROXY:
# pass --build-arg GOPROXY=... (and GONOSUMDB=... for a module that is not in the
# public checksum database yet) to build against a private or local module proxy.
FROM golang:1.27.3-alpine@sha256:b8c9b82affbbb82a020bbcf998dad2ba9516944ac7e883524642a481a9505e9c AS build
ARG GOPROXY=https://proxy.golang.org,direct
ARG GONOSUMDB=
ENV GOWORK=off GOFLAGS=-mod=readonly CGO_ENABLED=0 GOPROXY=$GOPROXY GONOSUMDB=$GONOSUMDB
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN go build -trimpath -ldflags="-s -w" -o /out/anvilkit-agent-api ./cmd/anvilkit-agent-api

# Runtime: the binary, the reviewed secret-free configuration file and a
# non-root user. The listener, the Control address, the auth mode and the
# principals file are supplied through the allowlisted ANVILKIT_API_*
# environment overrides; the file itself may be replaced by mounting one at
# the path named by ANVILKIT_API_CONFIG.
FROM alpine:3.24@sha256:294b683cb724975bec92580e1e685676bd4b50bda910ddb8c51d4cabeaec77e6
COPY --from=build /out/anvilkit-agent-api /usr/local/bin/anvilkit-agent-api
COPY config.yaml /etc/anvilkit/anvilkit-agent-api/config.yaml
ENV ANVILKIT_API_CONFIG=/etc/anvilkit/anvilkit-agent-api/config.yaml
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/anvilkit-agent-api"]
