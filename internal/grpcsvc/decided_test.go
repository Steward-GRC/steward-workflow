// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	workflowv1 "github.com/Steward-GRC/steward-workflow/gen/go/steward/workflow/v1"
	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/store"
)

// ListDecidedApprovals (steward-workflow#52, gateway#68): the handler turns the
// store's decided seats into ids, the stage the run is on and the stage count.

func threeStageDef() builder.WorkflowDef {
	st := func(n string) builder.Stage {
		return builder.Stage{Name: n, Quorum: builder.QuorumAny, ApproverIDs: []string{"user-carol"}}
	}
	return builder.WorkflowDef{ID: "def-3", Name: "Three", Version: 1, Stages: []builder.Stage{st("a"), st("b"), st("c")}}
}

func decidedServer(decided ...store.DecidedSeat) (*WorkflowServer, *stubAssignments) {
	srv := newTestServer()
	srv.defStore = &stubWorkflowDefStore{wd: threeStageDef()}
	as := srv.assignments.(*stubAssignments)
	as.decidedUser = "user-carol"
	as.decided = decided
	return srv, as
}

func decidedList(t *testing.T, srv *WorkflowServer, user string) []*workflowv1.DecidedApproval {
	t.Helper()
	resp, err := srv.ListDecidedApprovals(context.Background(), &workflowv1.ListDecidedApprovalsRequest{UserId: user})
	require.NoError(t, err)
	return resp.GetApprovals()
}

func TestListDecidedApprovals_MapsTheDecidedSeatToIdsStageAndDecision(t *testing.T) {
	at := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)
	srv, as := decidedServer(store.DecidedSeat{
		PolicyVersionID: "pv-1", RunID: "run-1", WorkflowDefID: "def-3", WorkflowVersion: 1,
		Decision: "approved", DecidedAt: at,
	})
	as.entries = map[string][]int{"run-1": {0, 1}}

	got := decidedList(t, srv, "user-carol")
	require.Len(t, got, 1)
	require.Equal(t, "pv-1", got[0].GetPolicyVersionId())
	require.Equal(t, "run-1", got[0].GetRunId())
	require.EqualValues(t, 1, got[0].GetCurrentStageIdx(), "the highest stage entered, as GetStatus reports it")
	require.EqualValues(t, 3, got[0].GetStageCount())
	require.Equal(t, "approved", got[0].GetDecision())
	require.True(t, at.Equal(got[0].GetDecidedAt().AsTime()))
}

func TestListDecidedApprovals_KeepsTheStoresNewestFirstOrder(t *testing.T) {
	now := time.Now().UTC()
	srv, _ := decidedServer(
		store.DecidedSeat{PolicyVersionID: "pv-new", RunID: "run-new", WorkflowDefID: "def-3", WorkflowVersion: 1, Decision: "rejected", DecidedAt: now},
		store.DecidedSeat{PolicyVersionID: "pv-old", RunID: "run-old", WorkflowDefID: "def-3", WorkflowVersion: 1, Decision: "approved", DecidedAt: now.Add(-time.Hour)},
	)
	got := decidedList(t, srv, "user-carol")
	require.Len(t, got, 2)
	require.Equal(t, "run-new", got[0].GetRunId())
	require.Equal(t, "rejected", got[0].GetDecision())
	require.Equal(t, "run-old", got[1].GetRunId())
}

func TestListDecidedApprovals_ClampsTheStageToTheLastStage(t *testing.T) {
	srv, as := decidedServer(store.DecidedSeat{PolicyVersionID: "pv-1", RunID: "run-1", WorkflowDefID: "def-3", WorkflowVersion: 1, Decision: "approved", DecidedAt: time.Now()})
	as.entries = map[string][]int{"run-1": {0, 1, 2, 3}}
	got := decidedList(t, srv, "user-carol")
	require.Len(t, got, 1)
	require.EqualValues(t, 2, got[0].GetCurrentStageIdx())
}

func TestListDecidedApprovals_ALegacyRunFallsBackToItsPendingSeat(t *testing.T) {
	srv, as := decidedServer(store.DecidedSeat{PolicyVersionID: "pv-1", RunID: "run-1", WorkflowDefID: "def-3", WorkflowVersion: 1, Decision: "approved", DecidedAt: time.Now()})
	require.NoError(t, as.setSeat("pv-1", 1, "user-dave", "pending"))
	got := decidedList(t, srv, "user-carol")
	require.Len(t, got, 1)
	require.EqualValues(t, 1, got[0].GetCurrentStageIdx(), "no entries recorded: the lowest stage with a pending seat")
}

func TestListDecidedApprovals_AnUnreadableDefinitionLeavesTheStageCountZero(t *testing.T) {
	srv, as := decidedServer(store.DecidedSeat{PolicyVersionID: "pv-1", RunID: "run-1", WorkflowDefID: "def-3", WorkflowVersion: 1, Decision: "approved", DecidedAt: time.Now()})
	srv.defStore = &stubWorkflowDefStore{err: errors.New("gone")}
	as.entries = map[string][]int{"run-1": {0}}
	got := decidedList(t, srv, "user-carol")
	require.Len(t, got, 1)
	require.EqualValues(t, 0, got[0].GetStageCount())
	require.EqualValues(t, 0, got[0].GetCurrentStageIdx())
}

func TestListDecidedApprovals_AUserWithNoDecisionsGetsAnEmptyList(t *testing.T) {
	srv, _ := decidedServer(store.DecidedSeat{PolicyVersionID: "pv-1", RunID: "run-1", WorkflowDefID: "def-3", WorkflowVersion: 1, Decision: "approved", DecidedAt: time.Now()})
	require.Empty(t, decidedList(t, srv, "user-dave"))
}

func TestListDecidedApprovals_ABlankUserIsRefused(t *testing.T) {
	srv, _ := decidedServer()
	_, err := srv.ListDecidedApprovals(context.Background(), &workflowv1.ListDecidedApprovalsRequest{})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestListDecidedApprovals_AStoreFailureIsInternal(t *testing.T) {
	srv, as := decidedServer()
	as.decidedErr = errors.New("db down")
	_, err := srv.ListDecidedApprovals(context.Background(), &workflowv1.ListDecidedApprovalsRequest{UserId: "user-carol"})
	require.Equal(t, codes.Internal, status.Code(err))
}
