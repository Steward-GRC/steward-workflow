// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc

import (
	"context"

	log "github.com/Bugs5382/go-log"

	workflowv1 "github.com/Steward-GRC/steward-workflow/gen/go/steward/workflow/v1"
	"github.com/Steward-GRC/steward-workflow/internal/audit"
	"github.com/Steward-GRC/steward-workflow/internal/workloadauth"
)

// CallerGateway is the gateway's caller name, from its service account
// steward-gateway.
const CallerGateway = "gateway"

// CallerIdentity is identity's caller name, from its service account
// steward-identity.
const CallerIdentity = "identity"

// gatewayMethods are the methods the gateway calls, each on behalf of the
// signed-in user (and, during act-as, the admin behind them).
var gatewayMethods = []string{
	workflowv1.WorkflowService_Submit_FullMethodName,
	workflowv1.WorkflowService_Signal_FullMethodName,
	workflowv1.WorkflowService_GetStatus_FullMethodName,
	workflowv1.WorkflowService_ListPendingTasks_FullMethodName,
	workflowv1.WorkflowService_ListUpcomingTasks_FullMethodName,
	workflowv1.WorkflowService_SwapAssignee_FullMethodName,
	workflowv1.WorkflowService_BulkDecide_FullMethodName,
	workflowv1.WorkflowService_GetAssignmentHistory_FullMethodName,
	workflowv1.WorkflowService_GetStageEligiblePool_FullMethodName,
	workflowv1.WorkflowService_ListWorkflowDefs_FullMethodName,
	workflowv1.WorkflowService_GetWorkflowDef_FullMethodName,
	workflowv1.WorkflowService_CreateWorkflowDef_FullMethodName,
	workflowv1.WorkflowService_UpdateWorkflowDef_FullMethodName,
	workflowv1.WorkflowService_ArchiveWorkflowDef_FullMethodName,
	workflowv1.WorkflowService_ResolveWorkflow_FullMethodName,
}

// CallerPolicy is workflow's per-method allow-list. The gateway calls every
// method but the merge reassign, passing the user's actor. Identity calls
// ReassignUserWorkflowItems for an account merge and ListPendingTasks for the
// delete's approval check, passing the admin who runs them. Anything else is
// refused.
func CallerPolicy() workloadauth.Policy {
	p := workloadauth.Policy{}
	for _, m := range gatewayMethods {
		p[m] = map[string]workloadauth.Access{CallerGateway: workloadauth.OnBehalf}
	}
	p[workflowv1.WorkflowService_ReassignUserWorkflowItems_FullMethodName] = map[string]workloadauth.Access{CallerIdentity: workloadauth.OnBehalf}
	p[workflowv1.WorkflowService_ListPendingTasks_FullMethodName][CallerIdentity] = workloadauth.OnBehalf
	return p
}

type auditEmitter interface {
	Emit(ctx context.Context, ev audit.Event) error
}

// AuditDenial records a call the workload-auth interceptor refused, as
// rpc.denied in the audit tier. The actor is the authenticated caller (or
// "unauthenticated"), never a user the call claimed.
func AuditDenial(emitter auditEmitter, lg log.Logger) workloadauth.DenyHook {
	return func(ctx context.Context, d workloadauth.Denial) {
		caller := d.Caller.Name
		if caller == "" {
			caller = "unauthenticated"
		}
		err := emitter.Emit(ctx, audit.Event{
			Tier: audit.TierAudit, Action: "rpc.denied", ActorUserID: "service:" + caller, Subject: d.Method,
			Attributes: map[string]string{
				"method": d.Method, "caller": d.Caller.Name, "service_account": d.Caller.ServiceAccount,
				"code": d.Code.String(), "reason": d.Reason,
			},
		})
		if err != nil {
			lg.Ctx(ctx).Error(err, "audit of a refused call failed", log.F("method", d.Method), log.F("caller", caller))
		}
	}
}
