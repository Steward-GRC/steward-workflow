# API

The workflow service serves `steward.workflow.v1.WorkflowService` over gRPC. The protos are in
[`proto/steward/workflow/v1`](../proto/steward/workflow/v1) and the generated Go stubs in
`gen/go/steward/workflow/v1`. Coded refusals are listed in [error-codes.md](error-codes.md).

## Seats

A run gives every approver of the stage it is on a seat. There are two kinds:

- an **individual seat** is one person's own vote;
- a **group seat** is a member's vote inside an approve-as-group unit (`GroupUnit`). The unit
  decides by its own quorum of yes votes over its members' group seats (`internal_quorum`:
  `one`, `majority` or `all`) and then counts as one vote in the stage's quorum.

A person who is an individual approver and a member of a group on the same stage holds both seats
and votes each separately. An individual approval never counts toward a group's quorum. A rejection
on an individual seat rejects the stage. A rejection on a group seat rejects the stage only once the
group can no longer reach its quorum.

The `group_id` fields on `SignalRequest`, `Decision`, `SwapAssigneeRequest`,
`GetStageEligiblePoolRequest` and `StageAssignee` name the seat; empty means the individual seat.
`Signal` with an empty `group_id` from someone who holds no individual seat uses their only
pending group seat.

## Calls

| RPC | What it does |
| --- | --- |
| `ResolveWorkflow` | The workflow a policy would be submitted under: its override, else its home category's default. Empty: not submittable. |
| `Submit` | Starts an approval run for a policy version, on the definition version current at submit. |
| `Signal` | Approve, reject (both need a comment), request changes, withdraw or retire. Applies to the version's current run whatever `run_id` says. |
| `GetStatus` | The current run's status, stage names, seats and group tallies. |
| `ListPendingTasks` | The stages awaiting the approver. |
| `ListUpcomingTasks` | Active runs where the approver sits on a stage not started yet. |
| `SwapAssignee` | Reassigns a pending seat, as an admin, the seat holder or the workflow's author. |
| `BulkDecide` | Approves or rejects several of the caller's seats; the decisions share a `bulk_batch_id`. |
| `GetAssignmentHistory` | One stage's append-only log, with the real actor during act-as. |
| `GetStageEligiblePool` | The people `SwapAssignee` accepts for a seat without the out-of-eligibility acknowledgement. |
| `ReassignUserWorkflowItems` | Moves a merged account's pending and paused seats and active runs to the account it was merged into. |
| `ListWorkflowDefs`, `GetWorkflowDef`, `CreateWorkflowDef`, `UpdateWorkflowDef`, `ArchiveWorkflowDef` | Workflow definitions. Every update bumps the version; running approvals stay on theirs. |

## Definitions

A `WorkflowDef` is an ordered list of `WorkflowStage`s. A stage's approvers are, in order:

1. `approvers_by_category`: the individual approvers for the policy's home category, else the
   nearest ancestor category with an entry;
2. `approver_ids`: the individual approvers when no category entry applies;
3. `group_units`: the groups that approve as a whole, each with its members.

`quorum` is the stage's own: `one`, `majority` (more than half) or `all`, counted over its
individual seats plus one vote per group. A stage left without any approver can be saved as a draft
but can't be submitted (6003). `sla_days` sets the stage's deadline (zero uses the service-wide
SLA); `reject_on_sla_breach` rejects instead of escalating when it passes.
