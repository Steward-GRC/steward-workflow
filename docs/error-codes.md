# Error codes

Every coded gRPC error from the workflow service carries an `ErrorInfo` with the symbol as
its reason, the domain `workflow` and the code in `codeNum`. Only user-safe messages reach
the caller; every other code is sent as `Code N: Internal Error`.

| Code | Symbol | Area | Cause | User-safe |
| --- | --- | --- | --- | --- |
| 6000 | `INTERNAL` | workflow | an uncoded failure inside the workflow service | no |
| 6002 | `SWAP_ASSIGNEE_FORBIDDEN` | reassign | the caller doesn't hold the initiator role it claimed: not an admin, not the seat holder, or not the workflow's author | yes |
| 6003 | `STAGE_NOT_STAFFED` | submit | a stage has no approver once its individual approvers and group members are resolved, so a run would never finish | yes |
| 6004 | `SWAP_REASON_REQUIRED` | reassign | the reassignment has no reason; every reassignment is audited with one | yes |
| 6005 | `SWAP_USERS_REQUIRED` | reassign | the reassignment names no current or no new approver | yes |
| 6006 | `SWAP_SAME_USER` | reassign | the new approver is the current one | yes |
| 6007 | `SWAP_INITIATOR_ROLE_REQUIRED` | reassign | the request didn't set initiator_role; the gateway always sets it, so this is a client bug | no |
| 6008 | `SWAP_ASSIGNMENT_NOT_FOUND` | reassign | the person being replaced holds no seat on the stage, usually because the run moved on since the page loaded | yes |
| 6009 | `SWAP_ASSIGNMENT_NOT_PENDING` | reassign | the seat has already been acted on; only a pending seat can be reassigned | yes |
| 6010 | `SWAP_ASSIGNEE_NOT_ELIGIBLE` | reassign | the new approver isn't in the seat's pool and the request isn't an acknowledged admin reassignment | yes |
| 6011 | `STAGE_INDEX_OUT_OF_RANGE` | stage | the stage index is below zero or past the run's stage count, usually from a page loaded before the definition changed | yes |
| 6012 | `APPROVAL_RUN_NOT_FOUND` | stage | the policy version has no approval run, usually because it was never submitted | yes |
