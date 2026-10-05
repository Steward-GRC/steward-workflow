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
| `GRPC_TLS_CERT_FILE`, `GRPC_TLS_KEY_FILE`, `GRPC_TLS_CLIENT_CA_FILE` | empty | mTLS: the service certificate and key, and the CA peers chain to. Used both to serve and to dial core and identity. All three or none. |
| `WORKFLOW_TRUSTED_CALLERS` | empty | Comma-separated SPIFFE IDs whose forwarded actor (go-grpc-actor) is believed, such as the gateway's and identity's. Needs mTLS. Empty ignores every forwarded actor. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | The OTLP collector for traces and metrics. |
| `LOG_LEVEL`, `LOG_FORMAT` | go-log's defaults | `trace` to `error`; `console` locally, `json` in every cluster. |

## Act-as and trusted callers

During act-as, a decision is made as the target, and the history and audit events name the admin at
the keyboard with the target in `impersonated_user_id`. Workflow learns both from the actor the
gateway forwards and believes it only from a caller whose verified client certificate carries a
SPIFFE ID in `WORKFLOW_TRUSTED_CALLERS`. It forwards the actor on every call to core and identity, so
core records the same admin.

## Migrations

The schema is one baseline, `migrations/0001_baseline.up.sql`, applied with go-postgres at start-up
under the `workflow_schema_migrations` table. The saga engine migrates its own tables. Installs of
the original service come over through `steward-migrate`; see [data.md](data.md).
