# API

The workflow service serves `steward.workflow.v1.WorkflowService` over gRPC. The protos are in
[`proto/steward/workflow/v1`](../proto/steward/workflow/v1) and the generated Go stubs in
`gen/go/steward/workflow/v1`. Coded refusals are listed in [error-codes.md](error-codes.md).

## Seats

A run gives every approver of the stage it is on a seat. There are two kinds:

- an **individual seat** is one person's own vote;
- a **group seat** is a member's vote inside an approve-as-group unit (`GroupUnit`). The unit
  decides by its own quorum of yes votes over its members' group seats (`internal_quorum`:
  `one`, `majority` or `all`). The stage quorum does not count groups; see below.

A person who is an individual approver and a member of a group on the same stage holds both seats
and votes each separately. An individual approval never counts toward a group's quorum.

A stage can name veto people in `veto_user_ids`. There is no workflow-wide switch: a non-empty list
applies by itself. Each person must be one of the stage's explicit approvers (`approver_ids`,
`approvers_by_category` or a group unit's `member_user_ids`), or the save is refused with
`InvalidArgument`. At submit a named person holds the veto, on every seat they hold, only when they
are in the stage's approver pool after the access filter; a named person who is not seated holds
none and nothing is raised. Owners who join only to make up a quorum, site admin and root never
hold one. Any one holder rejecting ends the run at once and `GetStatus` names them in `vetoed_by`.
The old workflow-wide switch is gone: `SetWorkflowVetoEnabled`, `WorkflowDef.veto_enabled` and the
stage fields `veto_approver_ids` and `veto_group_ids` no longer exist, and their numbers and names
are reserved in the proto.

Apart from that, a rejection is a vote, not a veto. A rejection on an individual seat takes that seat's approval out of
reach, and the stage ends rejected only when the approvals still possible can no longer reach the
stage quorum (`any`, `majority`, `all` or n-of): a majority of three survives one rejection, a
majority of two or a stage with a single seat does not. The stage's other seats keep their tasks
until it ends. A rejection on a group seat rejects the stage only once the group can no longer
reach its own quorum. On a stage that mixes individual seats and groups, the individual seats are
scored against the stage quorum on their own, and every group unit must also be met on its own
quorum, as the mock server does. The stage ends rejected when the individual seats can no longer
reach the stage quorum or when any group can no longer reach its own. A met group never stands in
for an individual seat, and a met quorum of individual seats never stands in for a group.

The `group_id` fields on `SignalRequest`, `Decision`, `SwapAssigneeRequest`,
`GetStageEligiblePoolRequest` and `StageAssignee` name the seat; empty means the individual seat.
`Signal` with an empty `group_id` from someone who holds no individual seat uses their only
pending group seat.

## Calls

| RPC | What it does |
| --- | --- |
| `ResolveWorkflow` | The workflow a policy would be submitted under: its override, else its home category's default. Empty: not submittable. |
| `Submit` | Starts an approval run for a policy version, on the definition version current at submit. Optional `change_type` and `change_type_reason` pass the author's pick to core with the in_review write (see Change type below). |
| `Signal` | Approve, reject (both need a comment that is not blank), request changes, withdraw or retire. Applies to the version's current run whatever `run_id` says; a run that has ended refuses it with `FailedPrecondition`. Request changes keeps the caller's seat open and changes no status; the comment is kept in the seat's history. Withdraw returns the version to draft in core. |
| `GetStatus` | The current run's status, stage names, seats and group tallies. |
| `ListPendingTasks` | The stages awaiting the approver. |
| `ListUpcomingTasks` | Active runs where the approver sits on a stage not started yet. |
| `ListDecidedApprovals` | The runs still in review where the user decided a seat of their own: one entry per run, newest decision first, ids only. |
| `SwapAssignee` | Reassigns a pending seat, as an admin, the seat holder or the workflow's author. |
| `BulkDecide` | Approves or rejects several of the caller's seats; the decisions share a `bulk_batch_id`. |
| `GetAssignmentHistory` | One stage's append-only log, with the real actor during act-as. |
| `GetStageEligiblePool` | The people `SwapAssignee` accepts for a seat without the out-of-eligibility acknowledgement. |
| `ReassignUserWorkflowItems` | Moves a merged account's pending and paused seats and active runs to the account it was merged into. |
| `ListWorkflowDefs`, `GetWorkflowDef`, `CreateWorkflowDef`, `UpdateWorkflowDef`, `ArchiveWorkflowDef` | Workflow definitions. Every update bumps the version; running approvals stay on theirs. |
| `DeleteWorkflowDef` | Removes a definition and all its kept versions in one transaction. Refused (6013, with `run_count` and `override_count`) while any approval run ever pinned it, finished runs included, or a policy override uses it; archive it instead. Unknown id is 6014. A category default lives in core, so the caller checks that first. Gateway only. |

## Definitions

A `WorkflowDef` is an ordered list of `WorkflowStage`s. A stage's approvers are, in order:

1. `approvers_by_category`: the individual approvers for the policy's home category, else the
   nearest ancestor category with an entry;
2. `approver_ids`: the individual approvers when no category entry applies;
3. `group_units`: the groups that approve as a whole, each with its members.

`quorum` is the stage's own: `one`, `majority` (more than half) or `all`, counted over its
individual seats only; every group unit must be met on its own quorum as well. A stage left without any approver can be saved as a draft
but can't be submitted (6003). `sla_days` is a number of whole working days, counted on the site work calendar
(working weekdays, holidays and time zone, set in core) that Submit reads and pins on the run: the
stage is due at the end of the Nth working day after the day it is entered, so a later edit to the
calendar never moves a run in flight. Zero or unset uses the service-wide SLA. Submit returns
`Unavailable` when core cannot be reached to read the calendar; if core refuses the read or has no
calendar, the run is submitted without a pin and counts calendar days; `reject_on_sla_breach` rejects instead of escalating when it passes.

`remind_every_days` and `escalate_after_days` are whole working days, 0 to 365, counted past the due
date, and 0 means off. Either one needs `sla_days`, and neither can be set on a stage with
`reject_on_sla_breach` (it rejects at the due date, so the stage is never overdue): a save that does
either is refused with `InvalidArgument`. A time limit never approves.

A sweeper (see the runbook) sends the notices while the run is in review and the stage is still
undecided by its quorum. The due instant stored on the seat is the end of the due day.

- **Reminders:** `workflow.approval_reminder` to each approver still pending, numbered `n` = 1, 2, ...
  Reminder 1 falls at the due instant and reminder k+1 falls `remind_every_days` working days after
  the day of reminder k. If the sweeper was down, only the newest elapsed number goes out. The
  consumer de-duplicates on `<task_id>:r<n>`.
- **Escalation:** `workflow.stage_escalated` to each category owner (the owners of the policy's home
  category and its ancestors, not root), once per run and stage, `escalate_after_days` working days
  after the due day. It carries how many people are still undecided.
- **Calendar:** the run's pinned site calendar; calendar days when the run has no pin, as for the due
  date.
- It only notifies. It never approves, rejects, reassigns or adds a seat. Decided, paused, superseded
  and withdrawn seats and stages that reject on breach are skipped.
- **Audit:** `workflow.stage_reminder.sent` and `workflow.stage_escalated`, ids only.

## Change type

Core classifies each version as Major, Minor or Fix. `Submit` passes the author's pick (`change_type`,
with `change_type_reason`) to core; unset leaves core's suggestion standing. A pick at or above the
suggestion applies at once. A pick below it needs a reason, and core holds it as a requested lowering
until workflow records agreement: the run keeps the requested type, and when the first stage's quorum
is met (by `Signal` or `BulkDecide`) workflow calls core's `RecordChangeTypeAgreement` once, naming the
users whose approvals made the quorum. Later stages do not take part. A refusal from core (for example a
pick below the suggestion with no reason) comes back to the caller with core's code and message; a
version whose lowering is no longer pending in core does not block the approver. The audit events are
`workflow.change_type.requested` (at submit) and `workflow.change_type.agreed`.

## Events

Workflow publishes to RabbitMQ:

| Exchange | Routing key | Body | When |
| --- | --- | --- | --- |
| `audit` | `audit.audit` | `steward.audit.v1.AuditEvent`, protobuf | Every change: submit, decisions, signals, reassignments, definitions, merges, outages |
| `jobs` | `workflow.approval_requested` | JSON | A seat is created; one per approver, de-duplicated by obligations on `task_id` and approver |
| `jobs` | `workflow.started` | JSON | A run starts; to the submitter |
| `jobs` | `workflow.denied` | JSON | A stage is rejected; to the submitter |

During act-as, an audit event's `actor_user_id` is the real admin and the account acted as is in
the `impersonated_user_id` attribute.

## Calling other services

Every call workflow serves is authenticated by the caller's workload token and checked against a
per-method allow-list; see [configuration.md](configuration.md#service-to-service-authentication).
The gateway and identity are its callers: identity re-points a merged account's seats
(`ReassignUserWorkflowItems`) and reads an account's pending seats before a delete
(`ListPendingTasks`).

Workflow calls two services through protos pinned in `proto-refs.env`, never their Go modules, and
sends its own workload token on each call:

- **steward-core** (`CategoryService.GetCategory`, `GetEffectiveRuleset`,
  `PolicyService.GetPolicyVersion`, `GetPolicy`, `SetVersionStatus`): a category's default workflow,
  its rules and owners with its ancestors' in one call, the policy the approve decision is made for,
  and the version's status after approval or withdrawal.
- **steward-identity** (`IdentityReadService.GetUser`, `ListAllUsers`, `grpc.health.v1`): a user's
  access for the approve decision and the read check (the catalog's `policy.read`) and for an admin
  reassignment (`workflow.manage`), the accounts an approver pool is drawn from, and the health the
  outage reconciler watches.

### Who may approve

A designated approver (`approver_ids`, the category list in `approvers_by_category`, or a group's
members) keeps a seat only when the category's rules allow them Approve. The rules are go-authz's
`ordered` model: per top-level category, ordered rows with a cell per action (Read, Author, Approve,
Manage, Ack), the first non-blank cell for an action wins, and a child's rows come before its
parent's. Owners always approve and a deny does not remove them. Manage gives Read and Author, never
Approve. Root, the account flagged as root in identity, always has Read, Author and Manage: a deny
saved against it is reported but never applied. Approve and Ack are not global for root; they follow
the rows, and so does every action for a site admin who is not root. The approver pool of a category
is its owners (and its ancestors'), then everyone the rules allow Approve; root is in it only when a
rule allows it. When too few designated approvers are left for the quorum, the owners join as seats.
The permission catalog is `internal/authz/catalog.json`, a copy of the
canonical one; `task catalog:refresh COMMIT=<sha>` re-fetches it.

It publishes steward-audit's `AuditEvent`. Bump a pin, run `scripts/proto-generate.sh` and commit
`gen/` together.
