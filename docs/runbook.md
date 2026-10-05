# Runbook

## Health

- **Liveness**: `GET /livez` on `PROBE_PORT`, or the gRPC health check for the `liveness`
  service. It reports the process only, never a dependency, so an outage never restarts the pods.
- **Readiness**: `GET /readyz`, or the gRPC health check for `""` or `readiness`. Postgres and
  RabbitMQ are required: while either is down, readiness is `NOT_SERVING` (HTTP 503) and recovers on
  its own. Identity and core are optional and show as `degraded`. Checks are short-timeout pings,
  cached for five seconds.
- The `/readyz` body lists each dependency's state, whether it's required, the last error class,
  the time of the check and its version.
- Every health answer carries the build and dependency headers: `steward-version`,
  `steward-commit`, `steward-dep-postgres` (the server version) and `steward-depstate-<name>` for
  `postgres`, `rabbitmq`, `identity` and `core`. Read them with grpcurl:

  ```sh
  grpcurl -v -plaintext localhost:9092 grpc.health.v1.Health/Check
  ```

The image is stamped at build time with `--build-arg VERSION=<tag> --build-arg COMMIT=<sha>`; an
unstamped build reports `dev`.

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
