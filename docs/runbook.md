# Runbook

## Health

- **Liveness**: `GET /livez` on `PROBE_PORT`, or the gRPC health check for the `liveness`
  service. It reports the process only, never a dependency, so an outage never restarts the pods.
- **Readiness**: `GET /readyz`, or the gRPC health check for `""` or `readiness`. Postgres and
  RabbitMQ are required: while either is down, readiness is `NOT_SERVING` (HTTP 503) and recovers on
  its own. Identity and core are optional and show as `degraded`. Checks are short-timeout pings,
  cached for five seconds.
- While service-to-service authentication is on, `jwks` (the issuer's key set) is required too:
  without it no caller can be verified, so every call needing a token is refused with `Unavailable`
  and readiness is `NOT_SERVING`. A good fetch keeps it up for a minute; a failure is retried on the
  next probe. With `WORKLOAD_AUTH=disabled`, `workloadauth` is reported, always degraded.
- The `/readyz` body lists each dependency's state, whether it's required, the last error class,
  the time of the check and its version.
- Every health answer carries the build and dependency headers: `steward-version`,
  `steward-commit`, `steward-dep-postgres` (the server version) and `steward-depstate-<name>` for
  `postgres`, `rabbitmq`, `identity`, `core` and `jwks` (or `workloadauth`). Read them with grpcurl:

  ```sh
  grpcurl -v -plaintext localhost:9092 grpc.health.v1.Health/Check
  ```

The image is stamped at build time with `--build-arg VERSION=<tag> --build-arg COMMIT=<sha>`; an
unstamped build reports `dev`.

## Caller refusals

| Symptom | Look at |
| --- | --- |
| Workflow won't start: `WORKLOAD_OIDC_ISSUER is not set` | Set the `WORKLOAD_OIDC_*` block, or `WORKLOAD_AUTH=disabled` for a local run. |
| Workflow won't start: `read WORKLOAD_TOKEN_FILE` | Its projected token isn't mounted, or `WORKLOAD_TOKEN_FILE` points elsewhere. |
| `Unauthenticated: no workload token` | The caller sent no `authorization` metadata: check its `WORKLOAD_TOKEN_FILE` and the projected token mount (audience `steward`). |
| `Unauthenticated: workload token rejected` | The log line `caller token rejected` gives the reason: wrong `iss` or `aud`, expired, or a service account missing from `WORKLOAD_ALLOWED_SERVICEACCOUNTS`. |
| `PermissionDenied: caller not allowed on this method` | The caller is verified but workflow's allow-list doesn't list it for the method. The `rpc.denied` audit event names the caller and method. |
| `Unavailable: workload verifier unavailable` | No JWKS has loaded since start: `steward-depstate-jwks`, then the `JWKS refresh failed` log line (CA file, bearer file, issuer URL). A `status 401` there means the API server refused `WORKLOAD_OIDC_BEARER_FILE`: it must hold a token with the API server's own audience, not the `steward` caller token. |
| Act-as decisions name the target, not the admin | The call didn't come from a caller with on-behalf access: check the gateway's token and that `steward/steward-gateway` is in `WORKLOAD_ALLOWED_SERVICEACCOUNTS`. |
| Core or identity refuse workflow's calls | Workflow's `WORKLOAD_TOKEN_FILE` mount, and that the callee lists `steward/steward-workflow` in its `WORKLOAD_ALLOWED_SERVICEACCOUNTS`. |

## Identity outages

The outage reconciler checks identity every 30 seconds. After three failures in a row it pauses
every pending seat, keeping the time each had left, and marks the runs `paused_external_dep`; the
first good check resumes them with their deadlines moved on. Both moves are audited
(`workflow.run.paused`, `workflow.run.resumed`).

## The SLA timer

Every replica runs the saga timer, but only the one holding the engine's Postgres advisory lock
ticks. When it goes, the lock is released with its connection and another replica takes over.

## A run stuck in review

1. `GetStatus` for the version: which stage it's on and who holds its seats.
2. `GetAssignmentHistory` for that stage: what happened to each seat.
3. A stage with no seat logs `assign_stage: empty approver pool` or `no eligible approver and no
   owner backstop`: nobody could read the policy, and the category has no owner. Reassign a seat
   (`SwapAssignee`) or withdraw and resubmit after fixing the definition.
4. A resubmit cancels the version's earlier saga runs and keeps their records as `aborted`.
