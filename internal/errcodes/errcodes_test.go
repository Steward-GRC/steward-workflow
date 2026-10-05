// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package errcodes_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/Bugs5382/go-apperr/apperrgrpc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Steward-GRC/steward-workflow/internal/errcodes"
)

func TestSwapAssigneeForbiddenRoundTrip(t *testing.T) {
	st := status.Convert(errcodes.New(context.Background(), errcodes.CodeSwapAssigneeForbidden, "role", "admin"))
	require.Equal(t, codes.PermissionDenied, st.Code())
	require.Equal(t, "You don't have permission to reassign this approval.", st.Message())

	info, ok := apperrgrpc.FromStatus(st)
	require.True(t, ok, "status must carry ErrorInfo")
	require.Equal(t, "SWAP_ASSIGNEE_FORBIDDEN", info.Symbol)
	require.Equal(t, 6002, info.Code)
	require.Equal(t, "workflow", info.Domain)
	require.Equal(t, "admin", info.Metadata["role"])
}

func TestStageNotStaffedInterpolatesName(t *testing.T) {
	st := status.Convert(errcodes.New(context.Background(), errcodes.CodeStageNotStaffed, "stage", "1", "stage_name", "Legal Review"))
	require.Equal(t, codes.FailedPrecondition, st.Code())
	require.Contains(t, st.Message(), "Legal Review")
	require.NotContains(t, st.Message(), "{stage_name}")

	info, ok := apperrgrpc.FromStatus(st)
	require.True(t, ok, "status must carry ErrorInfo")
	require.Equal(t, "STAGE_NOT_STAFFED", info.Symbol)
	require.Equal(t, 6003, info.Code)
	require.Equal(t, "workflow", info.Domain)
}

func TestRefusalCodesKeepTheirStatusAndSymbol(t *testing.T) {
	cases := []struct {
		code   int
		symbol string
		grpc   codes.Code
	}{
		{errcodes.CodeSwapReasonRequired, "SWAP_REASON_REQUIRED", codes.InvalidArgument},
		{errcodes.CodeSwapUsersRequired, "SWAP_USERS_REQUIRED", codes.InvalidArgument},
		{errcodes.CodeSwapSameUser, "SWAP_SAME_USER", codes.InvalidArgument},
		{errcodes.CodeSwapInitiatorRoleRequired, "SWAP_INITIATOR_ROLE_REQUIRED", codes.InvalidArgument},
		{errcodes.CodeSwapAssignmentNotFound, "SWAP_ASSIGNMENT_NOT_FOUND", codes.NotFound},
		{errcodes.CodeSwapAssignmentNotPending, "SWAP_ASSIGNMENT_NOT_PENDING", codes.FailedPrecondition},
		{errcodes.CodeSwapAssigneeNotEligible, "SWAP_ASSIGNEE_NOT_ELIGIBLE", codes.FailedPrecondition},
		{errcodes.CodeStageIndexOutOfRange, "STAGE_INDEX_OUT_OF_RANGE", codes.InvalidArgument},
		{errcodes.CodeApprovalRunNotFound, "APPROVAL_RUN_NOT_FOUND", codes.NotFound},
	}
	for _, c := range cases {
		st := status.Convert(errcodes.New(context.Background(), c.code))
		require.Equal(t, c.grpc, st.Code(), c.symbol)
		info, ok := apperrgrpc.FromStatus(st)
		require.True(t, ok, c.symbol)
		require.Equal(t, c.symbol, info.Symbol)
		require.Equal(t, c.code, info.Code)
	}
}

func TestSwapAssignmentNotPendingInterpolatesState(t *testing.T) {
	st := status.Convert(errcodes.New(context.Background(), errcodes.CodeSwapAssignmentNotPending,
		"stage", "0", "current_user_id", "user-alice", "state", "approved"))
	require.Equal(t, "This approval is already approved, so it can't be reassigned.", st.Message())
	info, _ := apperrgrpc.FromStatus(st)
	require.Equal(t, "user-alice", info.Metadata["current_user_id"])
}

func TestSwapInitiatorRoleRequiredIsNotUserSafe(t *testing.T) {
	st := status.Convert(errcodes.New(context.Background(), errcodes.CodeSwapInitiatorRoleRequired))
	require.Equal(t, "Code 6007: Internal Error", st.Message(), "a client bug's detail stays off the wire")
}

func TestUncodedErrorsFallBackToInternal(t *testing.T) {
	info, _ := apperrgrpc.FromError(errcodes.Error(context.Background(), errors.New("boom")))
	require.Equal(t, errcodes.CodeInternal, info.Code)
	require.Equal(t, "INTERNAL", info.Symbol)
}

func TestRegistryBandAndDomain(t *testing.T) {
	require.NotEmpty(t, errcodes.Entries())
	for _, e := range errcodes.Entries() {
		require.Equal(t, 6, e.Code/1000, "code %d must be in band 6", e.Code)
		_, ok := errcodes.Registry().Describe(e.Code)
		require.True(t, ok)
	}
}

// docs/error-codes.md is generated from the registry; refresh it with
// UPDATE_DOCS=1 go test ./internal/errcodes.
func TestErrorCodesDocIsCurrent(t *testing.T) {
	const path = "../../docs/error-codes.md"
	want := errcodes.Doc()
	if os.Getenv("UPDATE_DOCS") == "1" {
		require.NoError(t, os.WriteFile(path, []byte(want), 0o600))
	}
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, want, string(got), "docs/error-codes.md is stale; run UPDATE_DOCS=1 go test ./internal/errcodes")
}
