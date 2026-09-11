# anvilkit-agent-api

Public API for submitting AnvilKit operations, accessing authorized state and
results, and subscribing to live events.

This service is the Go public boundary of the AnvilKit Agent platform. It
validates browser and developer input, derives authenticated context, forwards a
permitted command to `anvilkit-agent-control` and serves the committed
projection view. It holds **no** Agent authority-write role, Temporal client,
Pagix connection, provider credential or npm credential; every business decision
belongs to Control.

## Current scope

Definition validation, the authorized operation read surface, the reserved
cancellation lane and local-check intake are implemented:

| Route | Action | Success |
| --- | --- | --- |
| `POST /v1/definitions/validations` | `definition.validate` | `200` validation report |
| `GET /v1/operations/{operationId}` | `operation.read` | `200` `OperationView` |
| `GET /v1/operations/{operationId}/snapshot` | `operation.read` | `200` `OperationSnapshotV1` |
| `GET /v1/operations/{operationId}/events` | `operation.read` | `200` SSE stream or JSON replay page |
| `POST /v1/operations/{operationId}/cancel` | `operation.cancel` | `202` `ControlCommandResult` |
| `POST /v1/local-checks` | `local-check.create` | `202` `OperationAccepted` (`200` on replay) |

`POST /v1/local-checks` exists **only** in the controlled local profile. In
every other deployment it is not registered at all, so it answers exactly like
any undeclared path; no body, header or credential can reach it.

The validation route returns Control's report for an existing component
definition. It creates no operation, activates nothing, takes no source lease
and makes no commercial reservation. A report is a read, not a permission.

The read routes serve committed PostgreSQL projections and durable events under
current Control disclosure evidence. They mutate nothing, and a browser
disconnecting from a subscription never cancels the work it was watching.

The private listener serves `GET /healthz` (liveness) and `GET /readyz`
(readiness). They are never reachable from the public listener.

Business command submission (generation, preview, release, re-certification),
operator hold and resume, definition changes and the bounded multi-operation
view are later tasks and are absent here. Hold, resume and definition changes
are outside the fixed local profile by design, so a local-check has no route to
reach them. Any other path or method answers `404 NOT_FOUND` rather than
describing the surface.

## Layout

```
cmd/anvilkit-agent-api/     executable entry point
internal/config/            environment configuration and contract limits
internal/identity/          trusted caller resolution and action checks
internal/httpapi/           HTTP edge, strict decoding, disclosure, SSE
internal/readmodel/         committed projections and events, restricted role
internal/logging/           log records for the parent telemetry contract
internal/contracts/         generated bindings and retained parent contracts
```

## Contracts

The parent repository (`anvilkit-services`) owns the contract sources. This
clone retains only what it consumes:

- `internal/contracts/definitionvalidationv1/` — Go and Connect bindings
  generated from `contracts/proto/definition-validation-v1.proto`.
- `internal/contracts/controlv1/` and `internal/contracts/valuesv1/` — bindings
  generated from `contracts/proto/control-v1.proto` and its `common-v1.proto`
  import. Only `ControlService.GetDisclosureAuthorization`, `AdmitOperation` and
  `Cancel` are consumed: the generated client carries Control's whole private
  surface, and the boundary binds it to those methods through narrow interfaces,
  so no generic Control passthrough exists.
- `internal/contracts/generated.json` — the source digests and the exact
  generator versions the retained bindings were produced with.
- `internal/contracts/retained/` — the parent schemas, fixtures and migration
  SQL this service is held to, each recorded in `generated.json` with the digest
  it was extracted at. `contracts.VerifiedInputs()` checks every digest before
  a test or migration consumes the bytes, so an edited copy fails loudly rather
  than quietly weakening what it is supposed to hold this service to.
- `internal/contracts/fixtures/` — the `validateDefinition` request and response
  examples from `contracts/openapi/agent-api-v1.examples.json`, with
  `fixtures.json` recording the source file and its digest.

Nothing here is hand-maintained. Regenerate from the parent checkout:

```sh
python3 tools/generate-proto-bindings.py --consumer anvilkit-agent-api
python3 tools/generate-proto-bindings.py --consumer anvilkit-agent-api --check
```

The `--check` form fails if the retained output does not reproduce. The proto
files carry no `option go_package`; the parent maps them onto this module's
import paths through `protoc` `M` options recorded in
`tools/proto-bindings.json`, so the same bytes serve every consumer.

The public JSON surface follows
`urn:anvilkit:definition-validation-interface:v1`; the private call is
`DefinitionValidation.ValidateDefinition` over Protobuf/HTTP-2 with Connect-Go.
ProtoJSON is never the public JSON authority.

## Configuration

Every value is read from the environment at startup. The architecture supplies
no ports or endpoints, so the addresses are required and have no default.

| Variable | Required | Default | Meaning |
| --- | --- | --- | --- |
| `ANVILKIT_API_PUBLIC_LISTEN` | yes | — | Public listener address |
| `ANVILKIT_API_PRIVATE_LISTEN` | yes | — | Health listener address; must differ from the public one |
| `ANVILKIT_API_CONTROL_ENDPOINT` | yes | — | Control base URL; `https` uses mTLS/gRPC, `http` permits h2c/gRPC only on a numeric loopback address |
| `ANVILKIT_API_CONTROL_CA` | for HTTPS | — | Separate API-to-Control CA certificate; server name/IP must match the endpoint |
| `ANVILKIT_API_CONTROL_CERT` | for HTTPS | — | Client certificate with the exact `anvilkit-agent-api` DNS SAN |
| `ANVILKIT_API_CONTROL_KEY` | for HTTPS | — | Matching private key, loaded once at startup |
| `ANVILKIT_API_CONTROL_VALIDATION_TOKEN` | yes | — | Existing Control development credential, 32-256 characters without whitespace; sent only to `ValidateDefinition` |
| `ANVILKIT_API_READ_DSN` | yes | — | Agent database as the restricted `anvilkit_api_ro` role; pool sizing travels in the DSN (`pool_max_conns`, …) |
| `ANVILKIT_API_CONTROL_TIMEOUT_SECONDS` | no | `10` | Bound on one Control call, 1 to 120 |
| `ANVILKIT_API_IDENTITY_PROFILE` | no | — | Controlled identity profile path; **unset denies every protected route** |
| `ANVILKIT_API_PROFILE` | no | `standard` | Serving profile. `controlled-local` additionally registers `POST /v1/local-checks`; any other value refuses to start |
| `ANVILKIT_API_ENVIRONMENT` | no | `local` | `environment` on every log record |
| `ANVILKIT_API_SERVICE_VERSION` | no | `0.0.0-development` | `service.version` on every log record |
| `ANVILKIT_API_SERVICE_INSTANCE_ID` | no | hostname | `service.instance.id` on every log record |

Body ceilings, the issue ceiling, the drain window and every `sse.*`,
`authorization.*` and `clock.*` bound are **not** configurable: they mirror
`contracts/profiles/pilot-limits-v1.json` as constants so a deployment cannot
widen a contract limit.

### Controlled identity profile

The Agent OpenAPI declares a bearer SSO scheme but records the role-to-action
mapping as an *activation input*: no issuer, verification key or production role
mapping exists yet. Until one does, this service resolves callers through a
controlled profile file that binds each accepted credential to a fixed actor,
tenant and action set.

```json
{
  "identities": [
    {
      "token": "<a locally generated credential of at least 24 characters>",
      "actorId": "developer-1",
      "tenantId": "tenant-1",
      "grantedActions": ["definition.validate"]
    }
  ]
}
```

`local-check.create` and `operation.cancel` are granted the same way. The
controlled local developer of the fixed local-check profile is an ordinary
profile entry; no identity is special-cased in code.

Rules the loader enforces, all of which reject the whole file rather than a
single entry — a partly applied authorization mapping is worse than none:

- Unknown members, repeated credentials, repeated actions and credentials under
  24 characters are refused.
- `actorId` and `tenantId` must be `urn:anvilkit:values:v1#/$defs/id` values.
- Presenting a credential never confers permission by itself: a resolved actor
  without the route's action receives `403 PERMISSION_DENIED`.
- An unset or empty profile authorizes nothing. The process still starts, logs a
  warning and denies every protected route with `401 UNAUTHENTICATED`.

Credentials are matched by SHA-256 digest and compared in constant time. This
file is a controlled local input; it is never a production credential store and
real SSO claims are never accepted through it.

For the approved two-tenant disclosure fixture, use the same `token`, `actorId`
and `tenantId` in this API profile and Control's `control-03-fixture` identities.
Use Control's 32-256-character token bounds and grant `operation.read` in the API
profile. The resolved read actor retains that configured fixture credential for
`GetDisclosureAuthorization`; an unvalidated public header is never forwarded.
Control independently verifies the credential's actor/tenant, the API mTLS
identity, the destination method, operation binding and current membership.
The validation credential remains separate and authorizes no disclosure call.
No production SSO token exchange or new profile format is introduced.

## Running locally

```sh
export ANVILKIT_API_PUBLIC_LISTEN=127.0.0.1:8080
export ANVILKIT_API_PRIVATE_LISTEN=127.0.0.1:8081
export ANVILKIT_API_CONTROL_ENDPOINT=https://127.0.0.1:9090
export ANVILKIT_API_CONTROL_CA=/path/to/api-control/ca.crt
export ANVILKIT_API_CONTROL_CERT=/path/to/api-control/anvilkit-agent-api.crt
export ANVILKIT_API_CONTROL_KEY=/path/to/api-control/anvilkit-agent-api.key
# Set ANVILKIT_API_CONTROL_VALIDATION_TOKEN to Control's development credential
# using the local secret environment; it is separate from the public TOKEN.
export ANVILKIT_API_READ_DSN='postgres://api-login@127.0.0.1:5432/anvilkit?pool_max_conns=8'
export ANVILKIT_API_IDENTITY_PROFILE=/path/to/identities.json

go run ./cmd/anvilkit-agent-api
```

```sh
curl -sS -X POST http://127.0.0.1:8080/v1/definitions/validations \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $TOKEN" \
  --data @definition-validation-request.json
```

A running `anvilkit-agent-control` is required for a report; without it the
route answers `503 DEPENDENCY_UNAVAILABLE`.

Both private clients use Protobuf gRPC over HTTP/2. TLS verifies the server and
presents this service's certificate; Control additionally authorizes the exact
API DNS SAN and the existing method-scoped credential. The validation token is
never copied from a browser header or attached to disclosure/command calls.
Missing TLS inputs refuse startup, and private clients do not follow redirects.
Loopback h2c remains available for the existing controlled transport tests, with
all three TLS inputs unset. This controlled mapping does not qualify production
SSO or delegated tenant identity.

API-01 acceptance is reproducible from the parent with
`python3 tools/run-verification.py --only api-validation`. It copies both service
trees outside the parent, builds and race-tests them, then serves the actual API
HTTP handler against a real Control executable using fresh test certificates.
The retained generation/release reports, semantic rejection without a digest,
strict input boundaries, untrusted public headers, private credential and
certificate rejection, and log redaction are checked. This validation proof
needs neither PostgreSQL nor Temporal. The normal API executable also serves
API-02 reads, so its configured read database remains a startup requirement.
Real disclosure/command integration has its own task acceptance; the validation
credential does not authorize those methods.

```sh
curl -sS -N http://127.0.0.1:8080/v1/operations/op-1/events \
  -H 'Accept: text/event-stream' \
  -H "Authorization: Bearer $TOKEN"
```

## Authorized reads

### Database role and migrations

The read pool connects as `anvilkit_api_ro`, which the contract defines as a
`NOLOGIN NOBYPASSRLS` group role: a deployment grants it to the login or
workload identity the service authenticates with, and that mapping is a
deployment reference, never a committed value. The role holds `SELECT` on
`agent_control.operations`, `step_executions` and `operation_events` and nothing
else.

Migrations are run separately from startup, by the migrator role, in the order
the files themselves record — `roles-v1.sql`, then `activation-v1.sql`, then
`control-v1.sql`. This service never migrates.

Every read runs inside a read-only transaction that binds the request's tenant
with `set_config('anvilkit.tenant_id', …, true)`. Because the setting is
transaction-local it is discarded when the transaction ends, so a pooled
connection cannot carry one request's tenant into the next. Row-level security
does the scoping; an operation of another tenant is reported as absent, exactly
as one that does not exist, so no cross-tenant existence is disclosed.

### Disclosure

Control owns the whole business-authorization decision. Before any projection,
event or snapshot page is read, the boundary calls
`GetDisclosureAuthorization(operationId, actorContext)` and receives four facts
and a deadline — decision, bound resource scope, `freshUntil` and evidence
revision — and nothing else. This service keeps **no** cache of its own, holds
no business-API credential and opens no connection to one.

- A negative decision denies with `403 PERMISSION_DENIED`.
- Evidence that cannot be established — unreachable Control, an undecided
  decision, a scope bound to another resource, or a grant that arrives already
  inside the two-second clock margin of its expiry — fails closed with `503
  DEPENDENCY_UNAVAILABLE`. It is never reported as the caller's denial.
- There is no stale-while-revalidate path. A live subscription renews its grant
  on the bounded schedule and closes as soon as a renewal fails or is negative.

Readiness is deliberately not authorization: a ready replica still denies every
read whose evidence has expired, and Control's reachability is not a readiness
input.

### Subscriptions and reconnection

`GET /v1/operations/{operationId}/events` serves an SSE stream when `Accept` is
`text/event-stream`, and a JSON `EventPage` otherwise. Both read the same
committed events; the durable stream key is `(operationId, eventSeq)` and
sequences cross as decimal strings compared numerically.

1. The cursor is validated before anything is streamed. A cursor of another
   operation is `409 REVISION_CONFLICT`; one that does not decode is `400`.
2. Committed events after the cursor replay in order, at most 200 per batch.
   Each frame is `event: <registered type>`, `id: <opaque cursor>`,
   `data: <one operation-event-payloads:v1 object>`.
3. A cursor below the retention floor cannot be served without skipping a
   sequence, which is never allowed. The server sends one non-durable
   `event: snapshot-required` frame carrying `{operationId, coveredSeqHint}` and
   closes; the client fetches `/snapshot`, finishes every page under the same
   `coveredSeq` within 60 seconds, then reconnects with the cursor form of that
   sequence.
4. A `:hb` comment line every 15 seconds. It carries no `id`, so it advances no
   durable cursor.
5. Close reasons are `authorization_expired`, `slow_consumer`, `drain` and
   `retention_restart`. Only `drain` has a wire effect: a randomized 1–5 second
   SSE `retry:` hint, so the connections one replica sheds do not return
   together.

Subscribers of one operation share a single catch-up read every second, woken
early by a PostgreSQL `NOTIFY` hint on `anvilkit_operation_event`. A browser
costs a bounded buffer and a writer, never a database connection or a polling
loop. A lost, duplicated or unparsable notification costs at most a second of
latency and never an event, so the hint is never a source of truth.

Each subscriber holds at most 256 events and 1 MiB. A full backlog does not drop
an event to make room: the subscriber's position simply stops advancing, the
events stay committed, and it resumes exactly where it stopped. One blocked
subscriber is left out of the shared read window so it cannot pin the position
and starve the others. A subscriber that has been unable to drain for 30 seconds
is disconnected and recovers through replay; disconnecting it never cancels the
operation.

Cursors are opaque to a client but carry no secret and need no shared key: they
locate a position and never authorize disclosure, which is checked separately on
every read. Any replica therefore decodes a cursor any other replica issued,
which is what lets a drained connection resume elsewhere.

## Commands

### Local-check intake

`POST /v1/local-checks` submits one of two retained fixtures for the fixed local
verification profile. It is local integration verification: it validates no
component, executes no authored definition and establishes no production
readiness.

```sh
export ANVILKIT_API_PROFILE=controlled-local

curl -sS -i -X POST http://127.0.0.1:8080/v1/local-checks \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $TOKEN" \
  --data '{"commandId":"cmd-local-1","fixtureId":"plain-v1"}'
```

The body carries exactly `commandId` and `fixtureId`, at most 1,024 UTF-8 bytes,
and `fixtureId` is exactly `plain-v1` or `newline-v1`. No text, source, path or
URL is accepted, and a `tenantId`, `actorId` or `operationId` member is an
undeclared member like any other: the caller's identity comes from the
credential and reaches Control as authenticated context, never from the body.

Acceptance requires Control's durable intake confirmation. Only then does the
route answer `202` with `Location: /v1/operations/{operationId}` and a body
carrying `operationId`, `operationRevision`, `acceptedAt`, `requestDigest`,
`queueExpiresAt` and `existing`. A repeated command with the same fixture
answers `200` with the original identity, revision and digest and claims no new
`Location`; a repeated command with a *different* fixture is `409
IDEMPOTENCY_CONFLICT`. Control computes the digest and owns the idempotency
decision; this service reports it.

Anything short of a contract-shaped confirmation — an unreachable Control, a
timeout, a transport loss, or an acceptance missing its identity or digest — is
`503 DEPENDENCY_UNAVAILABLE` and never an acknowledgement. That answer does not
mean no operation exists: the same command, retried, is the only thing that
resolves it.

The controlled environment admits one unresolved local-check at a time. While
that slot is occupied a new command is `429 OVERLOADED` with reason
`queue_full`, and `retryAfterMs`. The single verified result reaches the client
through the read surface as `localCheckResult` on the operation view, the
snapshot and the `operation.lifecycle` event; the fixture bytes themselves are
never disclosed, only their length and digest.

### The reserved cancellation lane

`POST /v1/operations/{operationId}/cancel` forwards the existing cancellation
contract. It stays available while local capacity is occupied, because nothing
on this path consults an intake bound; a duplicate command coalesces onto the
tracked one and still answers `202`.

```sh
curl -sS -X POST http://127.0.0.1:8080/v1/operations/op-1/cancel \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $TOKEN" \
  --data '{"commandId":"cmd-cancel-1","expectedOperationRevision":"3","reasonCode":"CANCEL_REQUESTED"}'
```

Cancellation fences new dispatch and recalls nothing already issued. This
service establishes no fence of its own and infers no terminal outcome: the
`202` body is Control's tracked state, and the current state comes from `GET`.

## Build and test

The Go 1.27 toolchain is the selected target. From a clone of this repository
alone, with no parent checkout and no workspace:

```sh
GOWORK=off go build -mod=readonly ./...
GOWORK=off go test -mod=readonly ./...
GOWORK=off go build -mod=readonly -o anvilkit-agent-api ./cmd/anvilkit-agent-api
```

Unit tests require no PostgreSQL, Temporal, provider account or network. They
cover the trusted identity and action checks, the strict decoding rules, the
mapping of Control's report and failures, a real Connect round trip over HTTP/2
against an in-process Control stand-in, and — for the read surface — disclosure
outcomes, cursor rules, snapshot handshake bounds, subscriber buffer bounds and
live SSE subscriptions over a real listener. Response bodies are validated
against the retained public schemas rather than against the tests' own
expectations.

### Integration tests

The checks that need a real database are behind the `integration` build tag, so
an ordinary `go test ./...` never requires one:

```sh
docker run -d --name anvilkit-api-it -e POSTGRES_PASSWORD=… -e POSTGRES_DB=anvilkit \
  -p 55432:5432 postgres:18.6-alpine

ANVILKIT_API_TEST_ADMIN_DSN='postgres://postgres:…@127.0.0.1:55432/anvilkit?sslmode=disable' \
  GOWORK=off go test -tags integration -count=1 -timeout 20m ./...
```

The suite applies the parent's own migration files to a disposable database,
seeds explicitly synthetic records, and covers tenant scoping under row-level
security, gapless replay across pages, the retention floor and its snapshot
handshake, paginated snapshots pinned to one `coveredSeq`, the notification
hint, interleaved commits with a reconnect, delivery with no notification at
all, resumption on a second replica after a restart, agreement between the
status, snapshot and event surfaces, and the slow-consumer disconnection. The
last of these waits out the contract's thirty-second bound by design.

It also covers the local-check storage contract against the real schema: the
restricted role reading `localCheckResult` through its own tenant policy, a
second tenant seeing neither the operation nor its result, the conditional
constraints that stop a business kind borrowing the local funding state or
omitting its activation, the single unresolved local-check slot, and the
refusal to overwrite an accepted result.

Disclosure is answered by an in-test stand-in for the four facts, because
CONTROL-03 does not exist yet. A run without `ANVILKIT_API_TEST_ADMIN_DSN`
reports SKIP: that is an unexecuted check, never evidence that the behaviour
holds.

## Request handling

The boundary rejects, before any decoding and before any Control call:

- bodies over the route's ceiling — 65,536 UTF-8 bytes for definition
  validation, 4,096 for a control-lane command, 1,024 for a local-check — empty
  bodies and bodies that are not valid UTF-8;
- a `Content-Type` other than `application/json`;
- repeated JSON member names at **any** depth, including inside the opaque
  definition;
- explicit `null` anywhere — optional members are omitted, never nulled;
- members the request contract does not declare, and missing required members;
- a `descriptorDigest`, `runtimeProfileRef` or `policyRefs` entry outside its
  contract shape, and `policyRefs` that is empty, repeated or longer than 16.

The definition itself is forwarded as its exact submitted bytes. This service
checks only that it is an object declaring `schemaVersion: 1`; every semantic
rule, canonicalization and digest belongs to Control, so there is one semantic
authority rather than two that can drift.

### Failure mapping

Every 4xx and 5xx response is `urn:anvilkit:error-envelope:v1`, and every
response carries a server-assigned `X-Request-Id`. A client-supplied request
identifier is never adopted.

| Cause | Status | Code |
| --- | --- | --- |
| Bounds, shape or strictness violation | 400 | `INVALID_ARGUMENT` |
| Definition schema version not transported | 400 | `UNSUPPORTED_SCHEMA` |
| No accepted platform identity | 401 | `UNAUTHENTICATED` |
| Trusted actor without `definition.validate` | 403 | `PERMISSION_DENIED` |
| Undeclared path or method | 404 | `NOT_FOUND` |
| Control reports an absent reference | 404 | `NOT_FOUND` |
| Control reports an incompatible retained contract or profile | 409 | `PROFILE_QUALIFICATION_FAILED` |
| Control unreachable, timed out, or its report breaks the contract | 503 | `DEPENDENCY_UNAVAILABLE` |
| Trusted actor without `operation.read` | 403 | `PERMISSION_DENIED` |
| Disclosure evidence is negative | 403 | `PERMISSION_DENIED` |
| Disclosure evidence cannot be established, or arrives expired | 503 | `DEPENDENCY_UNAVAILABLE` |
| Operation absent within the caller's tenant scope | 404 | `NOT_FOUND` |
| Reconnect or steps cursor belonging to another operation | 409 | `REVISION_CONFLICT` |
| Snapshot window passed, or the projection advanced mid-handshake | 409 | `RESTART_REQUIRED` |
| Projection read role unavailable | 503 | `DEPENDENCY_UNAVAILABLE` |
| Trusted actor without `local-check.create` or `operation.cancel` | 403 | `PERMISSION_DENIED` |
| Same `commandId` already accepted for a different fixture | 409 | `IDEMPOTENCY_CONFLICT` |
| The single unresolved local-check slot is occupied | 429 | `OVERLOADED` (`reason: queue_full`) |
| A cancellation conflicts with the operation's current state | 409 | `ABORTED` |
| Control refuses a command on a precondition | 409 | `CHANGE_BLOCKED` |
| Durable intake unconfirmed, unknown or outside its contract | 503 | `DEPENDENCY_UNAVAILABLE` |

An invalid *definition* is not a failure: it is `200` with `valid: false`,
issues and no `definitionDigest`.

### Known gaps

- **The private transport carries a family, not a code.** Control signals
  failures by family through the RPC status code and defines no error-detail
  message, so a family with more than one registered code cannot be resolved
  from the status alone. Two consequences:
  - A cancellation conflict answers `409` with the family name `ABORTED` as the
    envelope's open-string code, rather than guessing between
    `REVISION_CONFLICT`, `IDEMPOTENCY_CONFLICT` and `OPERATION_TERMINAL`. The
    code is routed by family and an unrecognized code inside a known family is
    the family default, so a client behaves correctly while this service states
    only what Control told it.
  - `RESOURCE_EXHAUSTED` on the validation route degrades to `503
    DEPENDENCY_UNAVAILABLE`, because the `overloadReason` the envelope requires
    cannot be recovered. The local-check route is the exception: its profile has
    exactly one capacity bound, so the reason is `queue_full` by construction
    rather than by guess, and its ABORTED outcome is likewise unambiguous
    because the route creates an operation and carries no expected revision.

  Closing this in general needs a private error-detail mapping agreed with
  Control; it is a machine-contract addition beyond the approved CD-01..CD-05
  slice, so it is recorded rather than taken here.
- **Ingress rate limiting is absent.** `ingress.requestsPerActorPerSecond`,
  `ingress.requestsPerConnectionPerSecond` and the `controlLane.*` reservations
  are edge flood guards this stage does not implement. The command routes exist
  now, so this gap is live: the reserved lane is honoured in the sense that
  cancellation consults no business quota, but no connection or request
  reservation is enforced at the edge.
- **`reconciliation` cannot be populated.** `urn:anvilkit:operation-view:v1`
  requires the object when `cleanupState` is `reconciling`, but the effect
  identities it names live in `agent_control.effects`, which the API read role
  cannot select and which the `operations` projection does not carry. No
  committed record available to this service can supply it, so the view omits
  it. Closing this needs a parent decision — reconciliation columns on the
  operations projection, or a read source for them — not a value invented here.
- **A paginated snapshot restarts when its operation advances.** Steps are
  pinned by `covered_seq`, but the operation row cannot be read as of a past
  sequence, so a later page whose projection has moved on answers `409
  RESTART_REQUIRED` rather than mixing two snapshots. Pages hold 200 steps and
  the window is 60 seconds, so this is rare; it is a deliberate choice of
  correctness over completing a stale handshake.
- **`sse.connectionsPerReplica` is not enforced.** The 2,000-connection ceiling
  is a pilot limit, but `OVERLOADED` requires an `overloadReason` and the closed
  enum has no member for connection exhaustion. Rather than invent one, this
  stage implements the bounds CD-03 lists and leaves the ceiling to a parent
  decision on the reason value.
- An unrecognized backend credential is a deployment fault and is reported as
  `503`. Explicit command-scope denial uses the approved `403` mapping; disclosure
  grants retain their separate decision/error handling.

### API-02 real service acceptance

From the parent checkout, run
`python3 tools/run-verification.py --only api-disclosure`. The driver builds
standalone API and Control copies and runs their unit/race checks, then starts
both canonical binaries with mTLS and separate runtime logins against disposable
PostgreSQL 18.6 pinned by digest. Its controlled profile and evidence files
follow Control's retained schema. Only its own container and synthetic rows are
changed; it neither uses nor restarts the persistent Temporal handoff.

The real process checks cover both tenant identities, spoofed-header rejection,
pooled read isolation, schema-valid status/snapshot/event responses, 200-item
pages, the original 60-second snapshot window, actual API process loss and
reconnect, interleaved commits with and without NOTIFY, retention recovery,
heartbeats, revoked/incomplete/unavailable evidence and Control process loss.
A real database lock holds a snapshot read past its grant deadline; the API
returns a failure without writing protected data. The existing PostgreSQL
integration suite then exercises read-role isolation and blocked subscribers.

Control and API now agree, through the real RPC, that `boundResourceScope` is
the exact requested operation ID and that an allow reply includes its evidence
revision. The API's `anvilkit_operation_event` listener remains an optional wake
hint; Control need not emit it for the one-second shared catch-up to work.
Both the hint path and absent hints are tested. Colons in legal operation IDs
survive reconnect cursors; unsigned cursors beyond PostgreSQL's signed range
cannot wrap around and replay earlier events. Pruning every event preserves
the committed position and advances the retention floor, so older cursors
still require a snapshot before resuming.

Protected reads recheck expiry after database I/O. Replay and live writes are
bounded by the earlier of the current disclosure expiry (minus the two-second
clock margin) and the slow-consumer deadline. A blocked writer may therefore
close with `authorization_expired` before the 30-second slow-consumer bound.
Subscriber byte accounting includes the complete next event before enqueueing,
so a full buffer never exceeds 1 MiB or advances past an unbuffered event.

This establishes the selected CD-03 controlled local acceptance. The broader
reconciliation projection and deployment-capacity/error mappings listed above,
real SSO/Pagix authorization and production qualification remain separate work.

## Observability

Records follow `contracts/telemetry/log-record-v1.schema.json` on stdout as JSON:
one `rpc.server.completed` per executed request, one `rpc.client.completed` per
executed Control call attempt, one `sse.subscribed` and one `sse.closed` per
connection, one `sse.snapshot` per served snapshot page, `health.transition` when
the read role's reachability changes, and the service lifecycle events. Records
carry identifiers and outcomes only — never a request body, definition source,
credential, header or dependency error text. Logs are diagnostic and authorize
nothing.

A presented cursor never enters a log field. Connection records carry the
**decoded** `attributes.fromSeq` and `attributes.lastSentSeq`, which is what
makes reconnection continuity visible: a reconnecting subscriber's `fromSeq`
equals the previous connection's `lastSentSeq`. A cursor that does not decode is
recorded only as an irreversible fingerprint, so two records for the same bad
cursor still correlate without the cursor being recoverable from them.
Individual events and heartbeats are never logged.

A caller's W3C `traceparent` is classified, never adopted. Every request starts a
new trace: a well-formed context is kept only as `trace.source: linked` with its
`link.traceId`, an absent one is `new`, and a malformed one is `rejected`. Trace
input confers no identity and cannot name a server-side trace. The
`unauthenticated` caller peer is reserved for a request that resolved to no
trusted actor, and such a record carries no `actorId` or `tenantId`.

No telemetry infrastructure is added at this stage: records go to stdout and
collection is a deployment concern.

## API-03 real local command acceptance

The parent `workflow-recovery` step now completes API-03 together with
CONTROL-04/05 and WORKFLOW-02. Both retained fixtures traverse the actual API,
Control and Worker, with original-identity replay/concurrency, occupied-capacity
cancel/replay, tenant isolation and durable GET/snapshot/SSE agreement.

The resolved fixture credential is forwarded on create/read/cancel calls;
Control independently verifies actor, tenant, method, current membership and
separate action permissions. An explicit command-scope denial maps to HTTP 403;
an unrecognized backend credential or unavailable dependency remains HTTP 503.
Cancellation's family-only ABORTED mapping is unchanged. Once cancellation is
terminal, the API omits the outstanding `cancelRequested` flag, while Control
retains its durable fence. Database outage health records include the required
safe error type/code.

Run the parent verification entry point with `--only workflow-recovery` and
`ANVILKIT_WORKFLOW_TEST_ENVIRONMENT` pointing to the retained environment handoff.
This controlled acceptance does not add business authorization or close the
separate ingress, deployment-capacity and reconciliation gaps listed above.
