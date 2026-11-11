// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	workflowv1 "github.com/Steward-GRC/steward-workflow/gen/go/steward/workflow/v1"
	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/fixture"
	"github.com/Steward-GRC/steward-workflow/internal/store"
)

// GetStatus tells who submitted the run and when, and per stage its due date
// and rule with the approvals it has and needs (steward-workflow#25). The mock
// server's n-of-m stage test (nofm.test.ts) fixes the rule names: one,
// majority, all, and nofm with the N it needs.

const fieldsPV = "pv-fields"

var fieldsSubmitted = time.Date(2026, 3, 2, 9, 30, 0, 0, time.UTC)

// fieldsServer builds a three-stage run: stage 0 is "all" over three approvers
// (entered, Carol and Dave approved), stage 1 is 2-of-3 (entered, nothing yet),
// and stage 2 is "any" and not yet entered.
func fieldsServer(t *testing.T) (*WorkflowServer, time.Time, time.Time) {
	t.Helper()
	srv := newTestServer()
	srv.defStore = &stubWorkflowDefStore{wd: builder.WorkflowDef{
		ID: "def-fields", Name: fixture.FinanceSignOff, Version: 1,
		Stages: []builder.Stage{
			{Name: "Owners", Quorum: builder.QuorumAll, ApproverIDs: []string{fixture.Carol, fixture.Dave, fixture.Erin}},
			{Name: "Review", Quorum: builder.QuorumNofM, QuorumN: 2, ApproverIDs: []string{fixture.Carol, fixture.Dave, fixture.Erin}},
			{Name: "Publish", Quorum: builder.QuorumAny, ApproverIDs: []string{fixture.Carol, fixture.Dave}},
		},
	}}
	srv.runStore.(*stubRunStore).tracked = &store.ApprovalRun{
		PolicyVersionID: fieldsPV, RunID: "run-fields", WorkflowDefID: "def-fields", WorkflowVersion: 1,
		Status: "in_review", SubmittedBy: fixture.Bob, StartedAt: fieldsSubmitted,
	}
	as := srv.assignments.(*stubAssignments)
	due0 := fieldsSubmitted.Add(72 * time.Hour)
	due1 := fieldsSubmitted.Add(120 * time.Hour)
	seed := func(stage int, user, state string, due time.Time) {
		_, err := as.CreateAssignment(context.Background(), store.AssignmentRow{
			PolicyVersionID: fieldsPV, StageIndex: stage, UserID: user, State: state,
			SLADeadlineAt: due, ReminderAt: due,
		}, fixture.Bob)
		require.NoError(t, err)
		row := as.rows[seatKey(fieldsPV, stage, user, "")]
		row.State = state
		as.rows[seatKey(fieldsPV, stage, user, "")] = row
	}
	seed(0, fixture.Carol, "approved", due0)
	seed(0, fixture.Dave, "approved", due0)
	seed(0, fixture.Erin, "pending", due0)
	seed(1, fixture.Carol, "pending", due1)
	seed(1, fixture.Dave, "pending", due1)
	seed(1, fixture.Erin, "pending", due1)
	return srv, due0, due1
}

func fieldsStatus(t *testing.T, srv *WorkflowServer) *workflowv1.GetStatusResponse {
	t.Helper()
	resp, err := srv.GetStatus(context.Background(), &workflowv1.GetStatusRequest{PolicyVersionId: fieldsPV})
	require.NoError(t, err)
	return resp
}

func TestGetStatusFields_SendsWhoSubmittedAndWhen(t *testing.T) {
	srv, _, _ := fieldsServer(t)
	resp := fieldsStatus(t, srv)
	require.Equal(t, fixture.Bob, resp.GetSubmittedBy())
	require.True(t, fieldsSubmitted.Equal(resp.GetSubmittedAt().AsTime()), "submitted_at is the run's start")
}

func TestGetStatusFields_EachStageCarriesItsRuleAndApprovals(t *testing.T) {
	srv, _, _ := fieldsServer(t)
	prog := fieldsStatus(t, srv).GetStageProgress()
	require.Len(t, prog, 3, "index-aligned with stage_names, including the stage not yet entered")

	require.Equal(t, "all", prog[0].GetQuorum())
	require.Equal(t, int32(2), prog[0].GetApprovals())
	require.Equal(t, int32(3), prog[0].GetRequired())

	require.Equal(t, "nofm", prog[1].GetQuorum())
	require.Equal(t, int32(2), prog[1].GetQuorumN())
	require.Equal(t, int32(0), prog[1].GetApprovals())
	require.Equal(t, int32(2), prog[1].GetRequired(), "2 of 3 needs two, not all three")
}

func TestGetStatusFields_StageNotYetEnteredHasNoDueDateButHasItsRule(t *testing.T) {
	srv, _, _ := fieldsServer(t)
	p := fieldsStatus(t, srv).GetStageProgress()[2]
	require.Nil(t, p.GetDueAt(), "no seats yet, so no due date")
	require.Equal(t, "one", p.GetQuorum())
	require.Equal(t, int32(0), p.GetApprovals())
	require.Equal(t, int32(1), p.GetRequired())
}

func TestGetStatusFields_EnteredStagesCarryTheirDueDate(t *testing.T) {
	srv, due0, due1 := fieldsServer(t)
	prog := fieldsStatus(t, srv).GetStageProgress()
	require.True(t, due0.Equal(prog[0].GetDueAt().AsTime()))
	require.True(t, due1.Equal(prog[1].GetDueAt().AsTime()))
}

func TestGetStatusFields_MajorityNeedsMoreThanHalfOfTheSeats(t *testing.T) {
	srv, _, _ := fieldsServer(t)
	srv.defStore.(*stubWorkflowDefStore).wd.Stages[0].Quorum = builder.QuorumMajority
	p := fieldsStatus(t, srv).GetStageProgress()[0]
	require.Equal(t, "majority", p.GetQuorum())
	require.Equal(t, int32(2), p.GetRequired())
}

// Mock submit-seats.test.ts "leaves a unit with no reader out of the run": a
// group unit none of whose members got a seat is not in the status groups.
func TestGetStatusFields_UnitWithNoSeatedMemberIsLeftOut(t *testing.T) {
	srv, _, _ := fieldsServer(t)
	srv.defStore.(*stubWorkflowDefStore).wd.Stages[0].GroupUnits = []builder.GroupUnit{
		{GroupID: "group-gone", InternalQuorum: builder.QuorumAny, Members: []string{fixture.Erin}},
	}
	units := fieldsStatus(t, srv).GetStageUnitProgress()[0].GetUnits()
	require.Empty(t, units, "stage 0 is entered and nobody holds a seat for group-gone")
}
