// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	workflowv1 "github.com/Steward-GRC/steward-workflow/gen/go/steward/workflow/v1"
	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/fixture"
	"github.com/Steward-GRC/steward-workflow/internal/store"
)

// GetStatus lists seats only for a stage the run has entered, and the
// definition's people for one it has not (steward-workflow#50). The mock's
// workflowStatus keeps current_stage_idx on the last stage it reached and never
// past the last stage; a stage nobody could be seated on is shown with no
// seats, not with the definition's approvers.

const entriesPV = "pv-entries"

// entriesServer builds a two-stage run. Stage 0 has Carol and a group unit of
// Erin; stage 1 has Dave. No seats are seeded: each test sets what was entered.
func entriesServer(t *testing.T, entered ...int) *WorkflowServer {
	t.Helper()
	srv := newTestServer()
	srv.defStore = &stubWorkflowDefStore{wd: builder.WorkflowDef{
		ID: "def-entries", Name: fixture.FinanceSignOff, Version: 1,
		Stages: []builder.Stage{
			{Name: "Owners", Quorum: builder.QuorumAll, ApproverIDs: []string{fixture.Carol},
				GroupUnits: []builder.GroupUnit{{GroupID: "group-a", InternalQuorum: builder.QuorumAny, Members: []string{fixture.Erin}}}},
			{Name: "Publish", Quorum: builder.QuorumAny, ApproverIDs: []string{fixture.Dave}},
		},
	}}
	srv.runStore.(*stubRunStore).tracked = &store.ApprovalRun{
		PolicyVersionID: entriesPV, RunID: "run-entries", WorkflowDefID: "def-entries", WorkflowVersion: 1,
		Status: "in_review", SubmittedBy: fixture.Bob,
	}
	if len(entered) > 0 {
		srv.assignments.(*stubAssignments).entries = map[string][]int{"run-entries": entered}
	}
	return srv
}

func entriesStatus(t *testing.T, srv *WorkflowServer) *workflowv1.GetStatusResponse {
	t.Helper()
	resp, err := srv.GetStatus(context.Background(), &workflowv1.GetStatusRequest{PolicyVersionId: entriesPV})
	require.NoError(t, err)
	return resp
}

func TestGetStatusEntries_EnteredStageWithNoSeatsShowsNoUnitsAndNoPeople(t *testing.T) {
	resp := entriesStatus(t, entriesServer(t, 0))
	require.Empty(t, resp.GetStageAssignees()[0].GetAssignees(), "entered, nobody seated: not the definition's approvers")
	require.Empty(t, resp.GetStageUnitProgress()[0].GetUnits(), "a unit with no seated member is left out")
}

func TestGetStatusEntries_NotYetEnteredStageShowsTheDefinition(t *testing.T) {
	resp := entriesStatus(t, entriesServer(t, 0))
	require.Len(t, resp.GetStageAssignees()[1].GetAssignees(), 1)
	require.Equal(t, fixture.Dave, resp.GetStageAssignees()[1].GetAssignees()[0].GetUserId())
}

func TestGetStatusEntries_NotYetEnteredStageShowsTheDefinitionsUnits(t *testing.T) {
	srv := entriesServer(t) // legacy shape is covered below; enter nothing but stage 1's sibling
	srv.assignments.(*stubAssignments).entries = map[string][]int{"run-entries": {1}}
	resp := entriesStatus(t, srv)
	require.Len(t, resp.GetStageUnitProgress()[0].GetUnits(), 1, "stage 0 was not entered here, so its unit shows from the definition")
	require.Len(t, resp.GetStageAssignees()[0].GetAssignees(), 2)
}

func TestGetStatusEntries_CurrentStageIsTheLastEnteredNotPastTheLastStage(t *testing.T) {
	// Stage 1 was entered with no one to seat: nothing is pending, yet the run
	// waits on stage 1, not on a stage 2 that does not exist.
	srv := entriesServer(t, 0, 1)
	require.NoError(t, srv.assignments.(*stubAssignments).setSeat(entriesPV, 0, fixture.Carol, "approved"))
	require.Equal(t, int32(1), entriesStatus(t, srv).GetCurrentStageIdx())
}

func TestGetStatusEntries_CurrentStageIsTheEnteredStageWhenOnlyTheFirstIsEntered(t *testing.T) {
	require.Equal(t, int32(0), entriesStatus(t, entriesServer(t, 0)).GetCurrentStageIdx())
}

// A run from before the entries table has no rows in it: GetStatus derives
// entered from the seats, as it did.
func TestGetStatusEntries_RunWithoutEntriesBehavesAsBefore(t *testing.T) {
	srv := entriesServer(t)
	require.NoError(t, srv.assignments.(*stubAssignments).setSeat(entriesPV, 0, fixture.Carol, "pending"))
	resp := entriesStatus(t, srv)
	require.Equal(t, int32(0), resp.GetCurrentStageIdx(), "the pending seat's stage")
	require.Len(t, resp.GetStageAssignees()[0].GetAssignees(), 1, "stage 0 has a seat, so it is entered: seats only")
	require.Len(t, resp.GetStageAssignees()[1].GetAssignees(), 1, "stage 1 has none, so the definition shows")

	srv = entriesServer(t)
	require.Equal(t, int32(2), entriesStatus(t, srv).GetCurrentStageIdx(), "legacy: nothing pending is past the last stage")
}

// stage_progress.required for a stage not yet entered uses the pool the
// submit's category lineage resolves, as the assigner will.
func TestGetStatusEntries_UnenteredStageRequiredFollowsTheRunsCategories(t *testing.T) {
	srv := entriesServer(t, 0)
	wd := &srv.defStore.(*stubWorkflowDefStore).wd
	wd.Stages[1].Quorum = builder.QuorumAll
	wd.Stages[1].ApproversByCategory = map[string][]string{"cat-home": {fixture.Carol, fixture.Dave, fixture.Erin}}
	srv.runStore.(*stubRunStore).tracked.AncestorCategoryIDs = []string{"cat-home", "cat-root"}
	require.Equal(t, int32(3), entriesStatus(t, srv).GetStageProgress()[1].GetRequired())

	srv.runStore.(*stubRunStore).tracked.AncestorCategoryIDs = nil
	require.Equal(t, int32(1), entriesStatus(t, srv).GetStageProgress()[1].GetRequired(), "no lineage on the run: the default approvers")
}
