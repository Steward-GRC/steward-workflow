// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc

import (
	"context"
	"testing"

	log "github.com/Bugs5382/go-log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	workflowv1 "github.com/Steward-GRC/steward-workflow/gen/go/steward/workflow/v1"
	"github.com/Steward-GRC/steward-workflow/internal/audit"
	"github.com/Steward-GRC/steward-workflow/internal/workloadauth"
)

func allWorkflowMethods() []string {
	sd := workflowv1.WorkflowService_ServiceDesc
	out := make([]string, 0, len(sd.Methods))
	for _, md := range sd.Methods {
		out = append(out, "/"+sd.ServiceName+"/"+md.MethodName)
	}
	return out
}

// callerMethods is every method caller is listed on, with its access.
func callerMethods(caller string) map[string]workloadauth.Access {
	p := CallerPolicy()
	got := map[string]workloadauth.Access{}
	for _, m := range allWorkflowMethods() {
		if a, ok := p.Lookup(m, caller); ok {
			got[m] = a
		}
	}
	return got
}

func TestCallerPolicyGatewayCallsEveryMethodButTheMergeReassignOnBehalf(t *testing.T) {
	want := map[string]workloadauth.Access{}
	for _, m := range allWorkflowMethods() {
		if m != workflowv1.WorkflowService_ReassignUserWorkflowItems_FullMethodName {
			want[m] = workloadauth.OnBehalf
		}
	}
	require.Equal(t, want, callerMethods(CallerGateway))
}

func TestCallerPolicyLetsIdentityReassignAndReadPendingTasksOnBehalf(t *testing.T) {
	require.Equal(t, map[string]workloadauth.Access{
		workflowv1.WorkflowService_ReassignUserWorkflowItems_FullMethodName: workloadauth.OnBehalf,
		workflowv1.WorkflowService_ListPendingTasks_FullMethodName:          workloadauth.OnBehalf,
	}, callerMethods(CallerIdentity), "identity's account merge and delete check call these, passing the admin")
}

func TestCallerPolicyListsNoOtherCaller(t *testing.T) {
	p := CallerPolicy()
	served := map[string]bool{}
	for _, m := range allWorkflowMethods() {
		served[m] = true
	}
	for m, callers := range p {
		require.True(t, served[m], "the policy lists %s, which workflow doesn't serve", m)
		for c := range callers {
			require.Contains(t, []string{CallerGateway, CallerIdentity}, c, "%s lists caller %q", m, c)
		}
	}
	for _, c := range []string{"core", "obligations", "reporting", "collab", "delivery", "ai"} {
		require.Empty(t, callerMethods(c), "%s may call nothing on workflow", c)
	}
}

type recordingEmitter struct{ evs []audit.Event }

func (r *recordingEmitter) Emit(_ context.Context, ev audit.Event) error {
	r.evs = append(r.evs, ev)
	return nil
}

func TestAuditDenialRecordsTheCallerNotAClaimedUser(t *testing.T) {
	rec := &recordingEmitter{}
	hook := AuditDenial(rec, log.Nop())
	hook(context.Background(), workloadauth.Denial{
		Method: workflowv1.WorkflowService_Signal_FullMethodName, Code: codes.PermissionDenied, Reason: workloadauth.ReasonMethodNotAllowed,
		Caller: workloadauth.Caller{Name: "reporting", ServiceAccount: "steward/steward-reporting"},
	})
	hook(context.Background(), workloadauth.Denial{Method: "/m", Code: codes.Unauthenticated, Reason: workloadauth.ReasonNoToken})
	require.Len(t, rec.evs, 2)
	require.Equal(t, audit.TierAudit, rec.evs[0].Tier)
	require.Equal(t, "rpc.denied", rec.evs[0].Action)
	require.Equal(t, "service:reporting", rec.evs[0].ActorUserID)
	require.Equal(t, workflowv1.WorkflowService_Signal_FullMethodName, rec.evs[0].Subject)
	require.Equal(t, map[string]string{
		"method": workflowv1.WorkflowService_Signal_FullMethodName, "caller": "reporting", "service_account": "steward/steward-reporting",
		"code": "PermissionDenied", "reason": workloadauth.ReasonMethodNotAllowed,
	}, rec.evs[0].Attributes)
	require.Equal(t, "service:unauthenticated", rec.evs[1].ActorUserID)
}
