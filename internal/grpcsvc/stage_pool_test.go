// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc

import (
	"context"
	"slices"
	"strconv"
	"testing"

	"github.com/Bugs5382/go-apperr/apperrgrpc"
	workflowv1 "github.com/Steward-GRC/steward-workflow/gen/go/steward/workflow/v1"
	"github.com/Steward-GRC/steward-workflow/internal/errcodes"
	"github.com/Steward-GRC/steward-workflow/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type codeEntry struct {
	Code   int
	Symbol string
}

func entry(code int) codeEntry {
	e, _ := errcodes.Registry().Describe(code)
	return codeEntry{Code: code, Symbol: e.Symbol}
}

func requireCoded(t *testing.T, err error, grpc codes.Code, entry codeEntry, md map[string]string) {
	t.Helper()
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected a gRPC status, got %v", err)
	}
	if st.Code() != grpc {
		t.Fatalf("grpc code: got %v want %v (%q)", st.Code(), grpc, st.Message())
	}
	info, ok := apperrgrpc.FromStatus(st)
	if !ok {
		t.Fatalf("status %q carries no ErrorInfo", st.Message())
	}
	if info.Symbol != entry.Symbol || info.Code != entry.Code || info.Domain != "workflow" {
		t.Fatalf("ErrorInfo: got %+v want %s/%d", info, entry.Symbol, entry.Code)
	}
	for k, v := range md {
		if info.Metadata[k] != v {
			t.Fatalf("metadata %q: got %q want %q", k, info.Metadata[k], v)
		}
	}
}

func setupServerForPool(t *testing.T) *WorkflowServer {
	t.Helper()
	srv := newTestServer()
	srv.runStore.(*stubRunStore).tracked = &store.ApprovalRun{
		PolicyVersionID: "pv-pool", WorkflowDefID: "def-1", RunID: "r-pool", Status: "in_review",
	}
	return srv
}

func TestGetStageEligiblePool_ReturnsStagePool(t *testing.T) {
	srv := setupServerForPool(t)
	resp, err := srv.GetStageEligiblePool(context.Background(), &workflowv1.GetStageEligiblePoolRequest{
		PolicyVersionId: "pv-pool", StageIndex: 0,
	})
	if err != nil {
		t.Fatalf("GetStageEligiblePool: %v", err)
	}
	if resp.GetStageIndex() != 0 || resp.GetStageName() != "s" {
		t.Fatalf("stage: got %d/%q want 0/%q", resp.GetStageIndex(), resp.GetStageName(), "s")
	}
	if want := []string{"u1", "u2", "u3"}; !slices.Equal(resp.GetEligibleUserIds(), want) {
		t.Fatalf("pool: got %v want %v", resp.GetEligibleUserIds(), want)
	}
}

func TestGetStageEligiblePool_MatchesSwapEligibility(t *testing.T) {
	srv, _ := setupServerForSwap(t)
	resp, err := srv.GetStageEligiblePool(context.Background(), &workflowv1.GetStageEligiblePoolRequest{
		PolicyVersionId: "pv-swap", StageIndex: 0,
	})
	if err != nil {
		t.Fatalf("GetStageEligiblePool: %v", err)
	}
	if !slices.Contains(resp.GetEligibleUserIds(), "u2") || slices.Contains(resp.GetEligibleUserIds(), "outsider") {
		t.Fatalf("pool %v disagrees with SwapAssignee eligibility (u2 in, outsider out)", resp.GetEligibleUserIds())
	}
}

func TestGetStageEligiblePool_StageOutOfRange(t *testing.T) {
	for _, idx := range []int32{1, -1} {
		srv := setupServerForPool(t)
		_, err := srv.GetStageEligiblePool(context.Background(), &workflowv1.GetStageEligiblePoolRequest{
			PolicyVersionId: "pv-pool", StageIndex: idx,
		})
		requireCoded(t, err, codes.InvalidArgument, entry(errcodes.CodeStageIndexOutOfRange), map[string]string{
			"stage": strconv.Itoa(int(idx)), "stage_count": "1",
		})
	}
}

func TestGetStageEligiblePool_NoRun(t *testing.T) {
	srv := setupServerForPool(t)
	srv.runStore.(*stubRunStore).notFoundPV = "pv-missing"
	_, err := srv.GetStageEligiblePool(context.Background(), &workflowv1.GetStageEligiblePoolRequest{
		PolicyVersionId: "pv-missing", StageIndex: 0,
	})
	requireCoded(t, err, codes.NotFound, entry(errcodes.CodeApprovalRunNotFound), map[string]string{
		"policy_version_id": "pv-missing",
	})
}

func TestGetStageEligiblePool_NoInServiceAuthzLikeOtherReads(t *testing.T) {
	srv := setupServerForPool(t)
	srv.actorExtractor = func(_ context.Context) (ActorClaims, bool) { return ActorClaims{}, false }

	if _, err := srv.GetStatus(context.Background(), &workflowv1.GetStatusRequest{PolicyVersionId: "pv-pool"}); err != nil {
		t.Fatalf("GetStatus without an actor: %v", err)
	}
	if _, err := srv.GetStageEligiblePool(context.Background(), &workflowv1.GetStageEligiblePoolRequest{
		PolicyVersionId: "pv-pool", StageIndex: 0,
	}); err != nil {
		t.Fatalf("GetStageEligiblePool without an actor: %v", err)
	}
}

func TestSwap_StageAndRunRefusalsAreCoded(t *testing.T) {
	t.Run("stage index out of range", func(t *testing.T) {
		srv, _ := setupServerForSwap(t)
		_, err := srv.SwapAssignee(context.Background(), &workflowv1.SwapAssigneeRequest{
			PolicyVersionId: "pv-swap", StageIndex: 3, CurrentUserId: "u1", NewUserId: "u2",
			Reason: "PTO", InitiatorRole: workflowv1.InitiatorRole_INITIATOR_ROLE_ADMIN,
		})
		requireCoded(t, err, codes.InvalidArgument, entry(errcodes.CodeStageIndexOutOfRange), map[string]string{
			"stage": "3", "stage_count": "1",
		})
	})
	t.Run("no run, admin path", func(t *testing.T) {
		srv, _ := setupServerForSwap(t)
		srv.runStore.(*stubRunStore).notFoundPV = "pv-missing"
		_, err := srv.SwapAssignee(context.Background(), &workflowv1.SwapAssigneeRequest{
			PolicyVersionId: "pv-missing", StageIndex: 0, CurrentUserId: "u1", NewUserId: "u2",
			Reason: "PTO", InitiatorRole: workflowv1.InitiatorRole_INITIATOR_ROLE_ADMIN,
		})
		requireCoded(t, err, codes.NotFound, entry(errcodes.CodeApprovalRunNotFound), map[string]string{
			"policy_version_id": "pv-missing",
		})
	})
	t.Run("no run, workflow author path", func(t *testing.T) {
		srv, _ := setupServerForSwap(t)
		srv.runStore.(*stubRunStore).notFoundPV = "pv-missing"
		_, err := srv.SwapAssignee(context.Background(), &workflowv1.SwapAssigneeRequest{
			PolicyVersionId: "pv-missing", StageIndex: 0, CurrentUserId: "u1", NewUserId: "u2",
			Reason: "PTO", InitiatorRole: workflowv1.InitiatorRole_INITIATOR_ROLE_WORKFLOW_AUTHOR,
		})
		requireCoded(t, err, codes.NotFound, entry(errcodes.CodeApprovalRunNotFound), map[string]string{
			"policy_version_id": "pv-missing",
		})
	})
}
