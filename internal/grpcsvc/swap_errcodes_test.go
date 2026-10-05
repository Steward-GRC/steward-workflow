// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc

import (
	"context"
	"testing"

	"github.com/Bugs5382/go-apperr/apperrgrpc"
	workflowv1 "github.com/Steward-GRC/steward-workflow/gen/go/steward/workflow/v1"
	"github.com/Steward-GRC/steward-workflow/internal/errcodes"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSwap_RefusalsAreCoded(t *testing.T) {
	valid := func() *workflowv1.SwapAssigneeRequest {
		return &workflowv1.SwapAssigneeRequest{
			PolicyVersionId: "pv-swap", StageIndex: 0, CurrentUserId: "u1", NewUserId: "u2",
			Reason: "PTO", InitiatorRole: workflowv1.InitiatorRole_INITIATOR_ROLE_ADMIN,
		}
	}
	cases := []struct {
		name   string
		mutate func(*workflowv1.SwapAssigneeRequest, *stubAssignments)
		entry  codeEntry
		grpc   codes.Code
		md     map[string]string
	}{
		{
			name:   "missing reason",
			mutate: func(r *workflowv1.SwapAssigneeRequest, _ *stubAssignments) { r.Reason = "" },
			entry:  entry(errcodes.CodeSwapReasonRequired), grpc: codes.InvalidArgument,
		},
		{
			name:   "missing new user",
			mutate: func(r *workflowv1.SwapAssigneeRequest, _ *stubAssignments) { r.NewUserId = "" },
			entry:  entry(errcodes.CodeSwapUsersRequired), grpc: codes.InvalidArgument,
		},
		{
			name:   "same user",
			mutate: func(r *workflowv1.SwapAssigneeRequest, _ *stubAssignments) { r.NewUserId = "u1" },
			entry:  entry(errcodes.CodeSwapSameUser), grpc: codes.InvalidArgument,
		},
		{
			name: "initiator role unset",
			mutate: func(r *workflowv1.SwapAssigneeRequest, _ *stubAssignments) {
				r.InitiatorRole = workflowv1.InitiatorRole_INITIATOR_ROLE_UNSPECIFIED
			},
			entry: entry(errcodes.CodeSwapInitiatorRoleRequired), grpc: codes.InvalidArgument,
		},
		{
			name:   "not in eligible pool",
			mutate: func(r *workflowv1.SwapAssigneeRequest, _ *stubAssignments) { r.NewUserId = "outsider" },
			entry:  entry(errcodes.CodeSwapAssigneeNotEligible), grpc: codes.FailedPrecondition,
			md: map[string]string{"stage": "0", "stage_name": "s", "new_user_id": "outsider"},
		},
		{
			name:   "no assignment for current user",
			mutate: func(_ *workflowv1.SwapAssigneeRequest, as *stubAssignments) { as.getErr = pgx.ErrNoRows },
			entry:  entry(errcodes.CodeSwapAssignmentNotFound), grpc: codes.NotFound,
			md: map[string]string{"stage": "0", "current_user_id": "u1"},
		},
		{
			name: "assignment not pending",
			mutate: func(_ *workflowv1.SwapAssigneeRequest, as *stubAssignments) {
				k := keyOf("pv-swap", 0, "u1")
				row := as.rows[k]
				row.State = "approved"
				as.rows[k] = row
			},
			entry: entry(errcodes.CodeSwapAssignmentNotPending), grpc: codes.FailedPrecondition,
			md: map[string]string{"stage": "0", "current_user_id": "u1", "state": "approved"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, as := setupServerForSwap(t)
			req := valid()
			tc.mutate(req, as)

			_, err := srv.SwapAssignee(context.Background(), req)
			st, ok := status.FromError(err)
			if !ok {
				t.Fatalf("expected a gRPC status, got %v", err)
			}
			if st.Code() != tc.grpc {
				t.Fatalf("grpc code: got %v want %v", st.Code(), tc.grpc)
			}
			info, ok := apperrgrpc.FromStatus(st)
			if !ok {
				t.Fatalf("status %q carries no ErrorInfo", st.Message())
			}
			if info.Symbol != tc.entry.Symbol || info.Code != tc.entry.Code || info.Domain != "workflow" {
				t.Fatalf("ErrorInfo: got %+v want %s/%d", info, tc.entry.Symbol, tc.entry.Code)
			}
			for k, v := range tc.md {
				if info.Metadata[k] != v {
					t.Fatalf("metadata %q: got %q want %q", k, info.Metadata[k], v)
				}
			}
		})
	}
}
