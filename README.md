# anvilkit-agent-api

The public REST/SSE entry of the AnvilKit Agent platform: authentication, OpenAPI validation, command mapping, authorized queries and the operation event stream. It holds no business database and no Temporal, Pagix or provider client; every business decision is Control's, reached over gRPC. The architecture that owns this service is the parent repository `anvilkit-services` (`docs/architecture/`: the service catalog, `contracts.md` for the public endpoints and SSE, `delivery.md` P05 for what the LocalCheck chain proves); the parent mounts this repository as the submodule `services/agent/api`.

This repository holds the replacement implementation (Gin, oapi-codegen strict server over the generated contract, kin-openapi validation, gin-contrib/sse frames, Fx lifecycle, koanf configuration). The previous implementation that lived here (Connect RPC, `ReadDSN` read model) is outside the build closure of the replacement and is not a starting point; its last worktree is preserved by the parent's cleanup record, not here.

## Layout

| Path | Content |
| --- | --- |
| `cmd/anvilkit-agent-api` | `main`: `fx.New(bootstrap.Module()).Run()` |
| `internal/bootstrap` | Fx assembly: configuration snapshot, logger, fixture verifier, Control client, HTTP server bound inside `OnStart` |
| `internal/config`, `config.yaml` | One typed, validated configuration snapshot (defaults < the reviewed file < the allowlisted `ANVILKIT_API_*` overrides; unknown keys, ranges and cross-field rules reject the candidate) |
| `internal/transport/http` | Gin engine, generated strict handlers, strict JSON body limit, SSE single-writer stream with bounded buffer, error envelope |
| `internal/application` | Principal and the Control port the transport calls |
| `internal/adapters/control` | gRPC client of `anvilkit.control.v1` (OperationService) |
| `internal/adapters/fixtureauth` | DEVELOPMENT_ONLY bearer-token principals file (`auth.mode: fixture`) until ENV-07 supplies the IdP protocol |
| `deploy/chart` | The service's Helm chart (Deployment, Service, ServiceAccount, ConfigMap, PodDisruptionBudget) |
| `Dockerfile`, `.dockerignore` | Image build from this repository root alone |
| `.github/workflows/ci.yml` | Build, vet, test, transport race check, image build, chart lint and render |

Dependency direction: `cmd -> bootstrap(Fx) -> transport -> application`; the generated contract (`github.com/ancyloce/anvilkit-agent-contracts/go`: `agentapi`, `anvilkit/control/v1`) is an ordinary versioned module requirement, resolved through GOPROXY. There is no replace directive, no workspace file and nothing read from a sibling checkout.

## Configuration

The service reads one reviewed, secret-free file (`config.yaml`, path from `ANVILKIT_API_CONFIG`, default `./config.yaml`) with the sections `http` (listen, read header timeout, body limit, shutdown), `control.address`, `auth.mode` and `sse` (heartbeat, frame buffer, slow-consumer grace, write timeout). The only environment overrides are `ANVILKIT_API_LISTEN`, `ANVILKIT_API_CONTROL_ADDRESS`, `ANVILKIT_API_AUTH_MODE` and `ANVILKIT_API_PRINCIPALS_FILE`; any other `ANVILKIT_API_*` variable stops the process. `/healthz` answers while the process runs, `/readyz` while new work can be admitted.

## Build and verify from this repository alone

```sh
export GOWORK=off GOFLAGS=-mod=readonly
go build ./... && go vet ./... && go test -count=1 ./... && go test -race -count=1 ./internal/transport/...
docker build -t anvilkit-agent-api:dev .          # --build-arg GOPROXY=... GONOSUMDB=... only for a private module proxy
helm lint deploy/chart --set control.address=control:9101 --set auth.principalsSecret.name=principals
```

The contract module is an ordinary published dependency: `github.com/ancyloce/anvilkit-agent-contracts/go v0.1.1` is the tag `go/v0.1.1` (commit `4025ff6`) of the `anvilkit-agent-contracts` repository, served by `proxy.golang.org` and verified against the checksum database (`go.sum`: `h1:w9BVVTxDkahXVUSwFrnDM1ZR+hbMGTFe/JkhATbGQhY=`; the zip includes that repository's root `LICENSE`, as Go requires for a module in a subdirectory). No replace directive, workspace or local proxy is involved; a newer contract version is adopted by changing the `require` line.

## Deploy

`deploy/chart` carries only what this service needs. Required values: `control.address` (the Control gRPC endpoint) and, in the DEVELOPMENT_ONLY fixture mode, `auth.principalsSecret.name` (an existing Secret holding the principals file; the chart never creates secrets). `image.digest` pins the exact image; `config` renders the reviewed file into a ConfigMap; `resources`, probes, the security context and a PodDisruptionBudget are declared. Environment values (addresses, replica counts, digests, secret names) and the pinned deployment combination belong to the deploying repository (`anvilkit-services`, `deploy/dev` for the development foundation), never to this chart. Runtime qualification (the parent's G gates) is not claimed by any check here.

## License

MIT, see `LICENSE`.
