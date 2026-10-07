# Configuration

Every setting is an environment variable, read once at start-up. A missing required setting or a
value that doesn't parse stops the service with every problem listed; nothing falls back quietly.
`.env.example` has the local defaults.

| Variable | Default | Meaning |
| --- | --- | --- |
| `DATABASE_DSN` | required | Postgres connection for the service. |
| `MIGRATE_DSN` | `DATABASE_DSN` | A direct connection for migrations, when `DATABASE_DSN` goes through a transaction-pooling proxy. |
| `SAGA_DATABASE_DSN` | `DATABASE_DSN` | The database for the embedded saga engine's tables. Every replica must share it. |
| `MIGRATIONS_DIR` | `migrations` | Where the SQL migrations are. The image sets `/migrations`. |
| `RABBITMQ_URL` | required | The broker audit events and approval notices are published to. |
| `IDENTITY_GRPC_ADDR` | `identity:9090` | steward-identity: user access and roles, and the health check the outage reconciler watches. |
| `CORE_GRPC_ADDR` | `core:9090` | steward-core: a category's default workflow and owners, the policy for the read filter, and version status. |
| `GRPC_PORT` | `9092` | The gRPC listen port. |
| `PROBE_PORT` | `8080` | Plain HTTP for `/livez` and `/readyz`. |
| `STAGE_SLA_HOURS` | `72` | A stage's SLA when it sets no `sla_days`. |
| `STAGE_REMINDER_HOURS` | `48` | When the stage's reminder falls. |
| `GRPC_TLS_CERT_FILE`, `GRPC_TLS_KEY_FILE`, `GRPC_TLS_CLIENT_CA_FILE` | empty | mTLS: the service certificate and key, and the CA peers chain to. Used both to serve and to dial core and identity. All three or none. Transport only: it grants no caller any trust. |
| `WORKLOAD_OIDC_ISSUER` | required | The cluster's service-account token issuer, an `https` URL that must equal the token's `iss`. |
| `WORKLOAD_OIDC_JWKS_URL` | discovered | The issuer's JWKS, when it isn't at the `jwks_uri` of `<issuer>/.well-known/openid-configuration`. `https` only. |
| `WORKLOAD_OIDC_CA_FILE` | system roots | Extra PEM CA trusted for the discovery and JWKS fetch (the cluster CA). |
| `WORKLOAD_OIDC_BEARER_FILE` | empty | A token sent on the discovery and JWKS fetch, re-read on every fetch (the pod's API token, not the `steward` caller token). |
| `WORKLOAD_AUDIENCE` | `steward` | The audience a caller's token must carry. |
| `WORKLOAD_ALLOWED_SERVICEACCOUNTS` | required | Comma list of `<namespace>/<serviceaccount>` that may call workflow at all: `steward/steward-gateway` and `steward/steward-identity`. |
| `WORKLOAD_AUTH` | enabled | `disabled` turns caller authentication off, for local runs only. Nothing else turns it off, and it can't be combined with `WORKLOAD_OIDC_ISSUER`. |
| `WORKLOAD_TOKEN_FILE` | `/var/run/secrets/steward/token` while authentication is on | Workflow's own projected token (audience `steward`), sent to core and identity on every call and re-read each time. An unreadable file stops the boot. With `WORKLOAD_AUTH=disabled` it is sent only when set, since core or identity may still enforce. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | The OTLP collector for traces and metrics. |
| `LOG_LEVEL`, `LOG_FORMAT` | go-log's defaults | `trace` to `error`; `console` locally, `json` in every cluster. |

## Service-to-service authentication

Every call to workflow carries the calling service's projected Kubernetes service-account token,
with audience `steward`, as `authorization: Bearer <token>`. Workflow verifies it against the
issuer's JWKS and maps the service account `<namespace>/steward-<name>` to the caller `<name>`. The
service account must be in `WORKLOAD_ALLOWED_SERVICEACCOUNTS`, and the caller must be listed for the
method in workflow's allow-list (`internal/grpcsvc/callers.go`):

| Caller | Methods | Access |
| --- | --- | --- |
| `gateway` | every method except `ReassignUserWorkflowItems` | on behalf of the signed-in user |
| `identity` | `ReassignUserWorkflowItems` (account merge), `ListPendingTasks` (the delete's approval check and preview) | on behalf of the admin running them |

- **Refusals:** a missing or rejected token (wrong issuer or audience, expired, or a service account
  outside the allow-list) is `Unauthenticated`, a caller the method doesn't list is
  `PermissionDenied`, and a verifier that hasn't loaded a key set yet answers `Unavailable`. Every
  refusal is logged and audited as `rpc.denied`, with the caller (or `unauthenticated`) as the actor.
- **Open methods:** `grpc.health.v1` and server reflection need no token.
- **Off switch:** `WORKLOAD_AUTH=disabled` is the only way to run without authentication, for local
  runs. Workflow logs a warning at start-up and every 5 minutes, and readiness reports
  `workloadauth` degraded. With neither `WORKLOAD_OIDC_ISSUER` nor `WORKLOAD_AUTH=disabled`,
  workflow doesn't start.
- **Outbound:** workflow sends its own token (`WORKLOAD_TOKEN_FILE`) on every call to core and
  identity. Both list it as acting as itself.

## Act-as and forwarded actors

During act-as, a decision is made as the target, and the history and audit events name the admin at
the keyboard with the target in `impersonated_user_id`. Workflow learns both from the actor the
gateway forwards (go-grpc-actor) and believes it only from a verified caller with on-behalf access
to the method. With authentication disabled, no forwarded actor is believed. Workflow still forwards
the actor on its calls to core and identity, but they take workflow's calls as workflow's own; core
records the user named in the request's `actor_user_id`.

## Migrations

The schema is one baseline, `migrations/0001_baseline.up.sql`, applied with go-postgres at start-up
under the `workflow_schema_migrations` table. The saga engine migrates its own tables. Installs of
the original service come over through `steward-migrate`; see [data.md](data.md).
