// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package store_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/store"
)

// The inbox's "In progress" list (steward-workflow#52, gateway#68): the
// user's decided seats on runs still in review. Mock: the seats with a
// decidedAt on a run whose status is in_review.

type decidedFixture struct {
	runs *store.AssignmentStore
	seat *store.Assignments
	def  string
}

func newDecidedFixture(t *testing.T) decidedFixture {
	t.Helper()
	db := newTestDB(t)
	def, err := store.NewWorkflowDefStore(db).Create(context.Background(), builder.WorkflowDef{
		Name:   "Decided WF",
		Stages: []builder.Stage{{Name: "s", Quorum: builder.QuorumAny, ApproverIDs: []string{"user-carol"}}},
	})
	require.NoError(t, err)
	return decidedFixture{runs: store.NewAssignmentStore(db), seat: store.NewAssignments(db), def: def}
}

// run tracks an in_review run of the version.
func (f decidedFixture) run(t *testing.T, pv, runID string) {
	t.Helper()
	require.NoError(t, f.runs.TrackRun(context.Background(), store.ApprovalRun{
		PolicyVersionID: pv, RunID: runID, WorkflowDefID: f.def, WorkflowVersion: 1,
	}))
}

// decide seats the user on the version and decides the seat.
func (f decidedFixture) decide(t *testing.T, pv, user, group, decision string) {
	t.Helper()
	row := seat(t, f.seat, pv, user, group)
	require.NoError(t, f.seat.DecideAssignment(context.Background(), store.DecideAssignmentInput{
		AssignmentID: row.ID, Decision: decision, Comment: "reviewed", ActorUserID: user,
	}))
}

func TestDecidedSeats_AnOpenRunWithADecidedSeatIsListed(t *testing.T) {
	f := newDecidedFixture(t)
	f.run(t, "pv-open", "run-open")
	f.decide(t, "pv-open", "user-carol", "", "approved")

	got, err := f.seat.ListDecidedSeats(context.Background(), "user-carol")
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "pv-open", got[0].PolicyVersionID)
	require.Equal(t, "run-open", got[0].RunID)
	require.Equal(t, f.def, got[0].WorkflowDefID)
	require.Equal(t, 1, got[0].WorkflowVersion)
	require.Equal(t, "approved", got[0].Decision)
	require.False(t, got[0].DecidedAt.IsZero())
}

func TestDecidedSeats_ARejectionIsADecisionToo(t *testing.T) {
	f := newDecidedFixture(t)
	f.run(t, "pv-rej", "run-rej")
	f.decide(t, "pv-rej", "user-carol", "", "rejected")

	got, err := f.seat.ListDecidedSeats(context.Background(), "user-carol")
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "rejected", got[0].Decision)
}

func TestDecidedSeats_AResolvedRunDropsOut(t *testing.T) {
	for _, status := range []string{"published", "rejected", "withdrawn"} {
		t.Run(status, func(t *testing.T) {
			f := newDecidedFixture(t)
			f.run(t, "pv-done", "run-done")
			f.decide(t, "pv-done", "user-carol", "", "approved")
			require.NoError(t, f.runs.UpdateRunStatus(context.Background(), "pv-done", status))

			got, err := f.seat.ListDecidedSeats(context.Background(), "user-carol")
			require.NoError(t, err)
			require.Empty(t, got)
		})
	}
}

func TestDecidedSeats_AnotherUsersDecisionIsNotListed(t *testing.T) {
	f := newDecidedFixture(t)
	f.run(t, "pv-other", "run-other")
	f.decide(t, "pv-other", "user-dave", "", "approved")

	got, err := f.seat.ListDecidedSeats(context.Background(), "user-carol")
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestDecidedSeats_ARequestForChangesIsNotADecision(t *testing.T) {
	f := newDecidedFixture(t)
	f.run(t, "pv-rc", "run-rc")
	row := seat(t, f.seat, "pv-rc", "user-carol", "")
	require.NoError(t, f.seat.NoteChangesRequested(context.Background(), row.ID, "user-carol", "Please fix."))

	got, err := f.seat.ListDecidedSeats(context.Background(), "user-carol")
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestDecidedSeats_AStillPendingSeatIsNotListed(t *testing.T) {
	f := newDecidedFixture(t)
	f.run(t, "pv-pend", "run-pend")
	seat(t, f.seat, "pv-pend", "user-carol", "")

	got, err := f.seat.ListDecidedSeats(context.Background(), "user-carol")
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestDecidedSeats_TwoSeatsOnOneRunGiveOneEntryWithTheNewestDecision(t *testing.T) {
	f := newDecidedFixture(t)
	f.run(t, "pv-two", "run-two")
	f.decide(t, "pv-two", "user-carol", "", "approved")
	f.decide(t, "pv-two", "user-carol", "finance", "rejected")

	got, err := f.seat.ListDecidedSeats(context.Background(), "user-carol")
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "rejected", got[0].Decision, "the group seat was decided last")
}

func TestDecidedSeats_RunsComeNewestDecisionFirst(t *testing.T) {
	f := newDecidedFixture(t)
	for _, n := range []string{"a", "b", "c"} {
		f.run(t, "pv-"+n, "run-"+n)
		f.decide(t, "pv-"+n, "user-carol", "", "approved")
	}

	got, err := f.seat.ListDecidedSeats(context.Background(), "user-carol")
	require.NoError(t, err)
	require.Len(t, got, 3)
	require.Equal(t, []string{"run-c", "run-b", "run-a"}, []string{got[0].RunID, got[1].RunID, got[2].RunID})
}

func TestDecidedSeats_ADecisionFromBeforeTheCurrentRunStartedIsNotListed(t *testing.T) {
	f := newDecidedFixture(t)
	f.run(t, "pv-old", "run-old")
	f.decide(t, "pv-old", "user-carol", "", "approved")
	_, err := f.runs.AbortActiveRuns(context.Background(), "pv-old")
	require.NoError(t, err)
	f.run(t, "pv-old", "run-new")

	got, err := f.seat.ListDecidedSeats(context.Background(), "user-carol")
	require.NoError(t, err)
	require.Empty(t, got, "the seat belongs to the aborted run")
}
