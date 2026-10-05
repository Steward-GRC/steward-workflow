# Data

Workflow keeps its state in its own Postgres database. The schema is one baseline,
[`migrations/0001_baseline.up.sql`](../migrations/0001_baseline.up.sql), recorded in the
`workflow_schema_migrations` table. The embedded saga engine keeps its own tables in the same
database, with its own migrations, so the two version sequences never share a table.

| Table | Holds |
| --- | --- |
| `workflow_defs` | Definitions in the builder model (`stages_json`), their live version, author and archive time |
| `workflow_def_versions` | Earlier versions still pinned by a run; removed once no active run uses one |
| `policy_workflow_overrides` | A policy's own workflow; a NULL definition means no approval |
| `approval_runs` | Every run of a policy version, finished ones kept; at most one active (`in_review` or `scheduled`) per version |
| `approval_assignments` | Seats: one per version, stage, person and group (`group_id` empty for an individual seat) |
| `assignment_history` | Append-only log of every seat; `actor_user_id` is who really acted, `impersonated_user_id` who they acted as |
| `workflow_run_state` | Per-version run state, paused while identity is unreachable |

## Act-as

During act-as, a decision is made as the target: the seat is the target's. The history row names
the real admin in `actor_user_id` and keeps the target in `impersonated_user_id`. A run's
`submitted_by` stays the effective submitter (the approval notices go to them and an account merge
moves it); the admin is `submitted_by_actor_id`.

## Coming from the original service

The baseline equals the original v1.2.0 chain with these changes, which `steward-migrate` applies to
an old database:

```sql
ALTER TABLE approval_runs RENAME COLUMN home_group_id TO home_category_id;
ALTER TABLE approval_runs DROP COLUMN stage_units_json;
ALTER TABLE workflow_defs ADD COLUMN author_user_id text NOT NULL DEFAULT '';
ALTER TABLE approval_assignments ADD COLUMN group_id text NOT NULL DEFAULT '';
ALTER TABLE approval_assignments DROP CONSTRAINT approval_assignments_unique_active;
ALTER TABLE approval_assignments ADD CONSTRAINT approval_assignments_unique_seat
    UNIQUE (policy_version_id, stage_index, user_id, group_id);
DROP TABLE schema_marker;
```

Inside `stages_json` (in `workflow_defs` and `workflow_def_versions`) each stage's
`approvers_by_group` key becomes `approvers_by_category`; `approver_groups`, which no definition
could set through the API, is dropped.

In the original, a member of an approve-as-group unit had one row per stage, counted for both their
individual vote and the group's. An old in-flight run's unit members keep that one individual seat
after the change; `steward-migrate` adds their group seats from the pinned definition.
