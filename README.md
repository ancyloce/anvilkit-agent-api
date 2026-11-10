# anvilkit-agent-api

The public REST/SSE entry of the AnvilKit Agent platform: authentication, OpenAPI validation, command mapping, authorized queries and the operation event stream. It holds no business database and no Temporal, Pagix or provider client; every business decision belongs to the owning service — Control, or Knowledge and MCP for their endpoints — reached over gRPC. The architecture that owns this service is the parent repository `anvilkit-services` (`docs/architecture/`: the service catalog, `contracts.md` for the public endpoints and SSE); the parent mounts this repository as the submodule `services/agent/api`.

This repository holds the replacement implementation (Gin, oapi-codegen strict server over the generated contract, kin-openapi validation, gin-contrib/sse frames, go-oidc bearer verification, OpenTelemetry traces and Prometheus metrics, Fx lifecycle, koanf configuration). The previous implementation that lived here (Connect RPC, `ReadDSN` read model) is outside the build closure of the replacement and is not a starting point; its last worktree is preserved by the parent's cleanup record, not here.

## Layout

| Path | Content |
| --- | --- |
| `cmd/anvilkit-agent-api` | `main`: `fx.New(bootstrap.Module()).Run()` |
| `internal/bootstrap` | Fx assembly: configuration snapshot, logger, the bearer verifier (OIDC, or the DEVELOPMENT_ONLY fixture), the workload identities, the Control client, the Knowledge and MCP gateways, tracer, metrics, readiness, HTTP server bound inside `OnStart` |
| `internal/config`, `config.yaml` | One typed, validated configuration snapshot (defaults < the reviewed file < the allowlisted `ANVILKIT_API_*` overrides; unknown keys, ranges and cross-field rules reject the candidate) |
| `internal/transport/http` | Gin engine, generated strict handlers, strict JSON body limit, kin-openapi request validation through its own adapter (refusals carry a fixed message and `details.field`, never the request or the schema; RB-S13, DEC-046), SSE single-writer stream with bounded buffer, error envelope |
| `internal/application` | Principal, the verifier, the Control port and the Knowledge and MCP gateways the transport calls |
| `internal/adapters/control` | gRPC client of `anvilkit.control.v1` (OperationService, PreparationService, ArtifactService, PreviewService, ReleaseService, RecoveryService) |
| `internal/adapters/knowledge`, `internal/adapters/mcp` | gRPC gateways to the Knowledge owner (SourceService, SnapshotService, RetrievalService, MemoryService) and the MCP owner (CatalogService, GrantService, CallService) under the principal's scope and command identity |
| `internal/adapters/oidcauth` | The OpenID Connect verifier (`auth.mode: oidc`): bearer JWTs checked against the issuer's discovered JWKS, audience, algorithm allowlist and clock skew, claims mapped to tenant, project, actor and roles |
| `internal/adapters/fixtureauth` | DEVELOPMENT_ONLY bearer-token principals file (`auth.mode: fixture`), admitted only with `development.enabled` |
| `internal/transport/identity` | The workload identity of the gRPC clients: rotating mTLS credentials from mounted certificate files, verifying each server by its reviewed name |
| `internal/requestid` | The request identifier carried inward as gRPC metadata |
| `deploy/chart` | The service's Helm chart (Deployment, Service, ServiceAccount, ConfigMap, PodDisruptionBudget, the cert-manager workload `Certificate`, an optional HorizontalPodAutoscaler) |
| `Dockerfile`, `.dockerignore` | Image build from this repository root alone |
| `.github/workflows/ci.yml` | Build, vet, test, transport race check, image build, chart lint and render, and the refusal of a tag-only image outside development |

Dependency direction: `cmd -> bootstrap(Fx) -> transport -> application`; the generated contract (`github.com/ancyloce/anvilkit-agent-contracts/go`: `agentapi`, `anvilkit/control/v1`, `anvilkit/knowledge/v1`, `anvilkit/mcp/v1`) is an ordinary versioned module requirement, resolved through GOPROXY. There is no replace directive, no workspace file and nothing read from a sibling checkout.

## Configuration

The service reads one reviewed, secret-free file (`config.yaml`, path from `ANVILKIT_API_CONFIG`, default `./config.yaml`) with the sections `development` (the top-level DEVELOPMENT_ONLY guard), `identity` (the workload certificate's reload interval), `telemetry`, `http` (listen, read header timeout, body limit, shutdown), `control`, `knowledge` and `mcp` (address, timeout and the mTLS `server_name` of each owner), `auth` (`oidc` with issuer, audience, algorithms, clock skew and claim paths, or the DEVELOPMENT_ONLY `fixture`), `sse` (heartbeat, frame buffer, slow-consumer grace, write timeout, stream caps, reconnect interval), `artifacts` (the transfer window) and `preparations` (`internal/config/config.go`, `Config`). The only environment overrides are `ANVILKIT_API_LISTEN`, `ANVILKIT_API_CONTROL_ADDRESS`, `ANVILKIT_API_AUTH_MODE`, `ANVILKIT_API_PRINCIPALS_FILE`, `ANVILKIT_API_AUTH_OIDC_{ISSUER,AUDIENCE,CA_FILE}`, `ANVILKIT_API_KNOWLEDGE_ADDRESS`, `ANVILKIT_API_MCP_ADDRESS`, `ANVILKIT_API_TELEMETRY_{OTLP_ENDPOINT,METRICS_LISTEN}` and `ANVILKIT_API_IDENTITY_{CERT,KEY,CA}_FILE` (`envOverrides` in the same file); any other `ANVILKIT_API_*` variable stops the process. Without a Knowledge or MCP address those owners' endpoints answer `DEPENDENCY_UNAVAILABLE`. `/healthz` answers while the process runs, `/readyz` while Control reports healthy (cached) and until shutdown begins; `/metrics` is served only on `telemetry.metrics_listen`, never on the public listener.

## Build and verify from this repository alone

```sh
export GOWORK=off GOFLAGS=-mod=readonly
go build ./... && go vet ./... && go test -count=1 ./... && go test -race -count=1 ./internal/transport/...
docker build -t anvilkit-agent-api:dev .          # --build-arg GOPROXY=... GONOSUMDB=... only for a private module proxy
helm lint deploy/chart --set control.address=control:9101 --set auth.principalsSecret.name=principals --set identity.trustDomain=ci.example \
  --set identity.certificate.issuerRef.name=ci-issuer --set auth.oidc.issuer=https://issuer.ci.example \
  --set image.digest=sha256:0000000000000000000000000000000000000000000000000000000000000001
```

The contract module is an ordinary published dependency: the version in the generated line below is a tag of the `anvilkit-agent-contracts` repository, served by `proxy.golang.org` and verified against the checksum database (`go.sum`; the zip includes that repository's root `LICENSE`, as Go requires for a module in a subdirectory). No replace directive, workspace or local proxy is involved; a newer contract version is adopted by changing the `require` line.

<!-- generated:contracts -->
Contracts: this build requires `github.com/ancyloce/anvilkit-agent-contracts/go v0.1.18` (`go.mod`; tag `go/v0.1.18`).
<!-- /generated:contracts -->

## Deploy

`deploy/chart` carries only what this service needs. Required values: `control.address` (the Control gRPC endpoint); under `auth.mode: oidc` (the default) `auth.oidc.issuer` (`http://` only with `development.enabled`); in the DEVELOPMENT_ONLY fixture mode, admitted only with `development.enabled`, `auth.principalsSecret.name` (an existing Secret holding the principals file; the chart never creates secrets); under `identity.mode: mtls` (the default) `identity.trustDomain` outside development and `identity.certificate.issuerRef.name` (or `identity.certificate.create: false` with `identity.secretName`); with `telemetry.otlpEndpoint`, `telemetry.otlpTls.caSecret.name` unless its mode is `development`. `knowledge.address` and `mcp.address` are optional. `image.digest` pins the exact image and is required outside development (a tag renders only with `development.enabled`); `config` renders the reviewed file into a ConfigMap; `resources`, probes, the security context and a PodDisruptionBudget are declared. Environment values (addresses, replica counts, digests, secret names) and the pinned deployment combination belong to the deploying repository (`anvilkit-services`, `deploy/dev` for the development foundation), never to this chart. Runtime qualification (the parent's G gates) is not claimed by any check here.

## License

MIT, see `LICENSE`.
