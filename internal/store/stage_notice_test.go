// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Steward-GRC/steward-workflow/internal/store"
)

// overdueSeat creates a pending seat that fell due at due, on stage 0.
func overdueSeat(t *testing.T, a *store.Assignments, pv, user string, due time.Time) store.AssignmentRow {
	t.Helper()
	row, err := a.CreateAssignment(context.Background(), store.AssignmentRow{
		PolicyVersionID: pv, StageIndex: 0, UserID: user,
		SLADeadlineAt: due, ReminderAt: due,
	}, "user-author")
	require.NoError(t, err)
	return row
}

func trackRun(t *testing.T, rs *store.AssignmentStore, defID, pv, runID, status string) {
	t.Helper()
	require.NoError(t, rs.TrackRun(context.Background(), store.ApprovalRun{
		PolicyVersionID: pv, RunID: runID, WorkflowDefID: defID, WorkflowVersion: 1,
		Status: status, PolicyID: "pol-" + pv, SubmittedBy: "user-author",
	}))
}

// The sweeper reads only pending seats that are past due on a run still in
// review; a decided, paused, superseded or withdrawn seat, a seat not yet due
// and a seat of a finished run never come back (steward-workflow#76).
func TestListOverdueSeats_OnlyLivePendingSeatsPastDue(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	a, rs := store.NewAssignments(db), store.NewAssignmentStore(db)
	def := seedDef(t, db)
	now := time.Date(2026, 3, 4, 10, 0, 0, 0, time.UTC)
	past := now.Add(-time.Hour)

	trackRun(t, rs, def, "pv-live", "run-live", "in_review")
	pending := overdueSeat(t, a, "pv-live", "u-pending", past)
	overdueSeat(t, a, "pv-live", "u-notdue", now.Add(time.Hour))
	for user, state := range map[string]string{
		"u-approved": "approved", "u-paused": "paused", "u-superseded": "superseded", "u-withdrawn": "withdrawn",
	} {
		seat := overdueSeat(t, a, "pv-live", user, past)
		setState(t, db, seat.ID, state)
	}

	trackRun(t, rs, def, "pv-done", "run-done", "approved")
	overdueSeat(t, a, "pv-done", "u-finished-run", past)
	overdueSeat(t, a, "pv-norun", "u-no-run", past)

	got, err := a.ListOverdueSeats(ctx, now)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, pending.ID, got[0].ID)
	require.Equal(t, "run-live", got[0].RunID)
	require.Equal(t, def, got[0].WorkflowDefID)
	require.Equal(t, 1, got[0].WorkflowVersion)
	require.Equal(t, "pol-pv-live", got[0].PolicyID)
	require.Equal(t, "user-author", got[0].SubmittedBy)
	require.Zero(t, got[0].RemindersSent)
}

// A reminder number is recorded once and only forward: the same number, an
// older one, or a seat that was decided in the meantime records nothing.
func TestRecordReminderSent_ForwardOnlyAndOnlyWhilePending(t *testing.T) {
	ctx := context.Background()
	a := store.NewAssignments(newTestDB(t))
	due := time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)
	seat := overdueSeat(t, a, "pv-r", "u1", due)
	next := due.Add(72 * time.Hour)

	ok, err := a.RecordReminderSent(ctx, seat.ID, 2, next)
	require.NoError(t, err)
	require.True(t, ok)
	got, err := a.GetAssignment(ctx, "pv-r", 0, "u1", "")
	require.NoError(t, err)
	require.Equal(t, 2, got.RemindersSent)
	require.True(t, got.ReminderAt.Equal(next), "reminder_at moves to when the next reminder is due")

	for _, n := range []int{2, 1} {
		ok, err = a.RecordReminderSent(ctx, seat.ID, n, next.Add(time.Hour))
		require.NoError(t, err)
		require.False(t, ok, "reminder %d is not newer than 2", n)
	}
	got, err = a.GetAssignment(ctx, "pv-r", 0, "u1", "")
	require.NoError(t, err)
	require.Equal(t, 2, got.RemindersSent)
	require.True(t, got.ReminderAt.Equal(next))

	require.NoError(t, a.DecideAssignment(ctx, store.DecideAssignmentInput{
		AssignmentID: seat.ID, Decision: "approved", Comment: "ok", ActorUserID: "u1",
	}))
	ok, err = a.RecordReminderSent(ctx, seat.ID, 3, next)
	require.NoError(t, err)
	require.False(t, ok, "a decided seat is not counted")
}

// A seat created again (a stage entered again) starts its reminders over.
func TestCreateAssignment_ResetsRemindersSent(t *testing.T) {
	ctx := context.Background()
	a := store.NewAssignments(newTestDB(t))
	due := time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)
	seat := overdueSeat(t, a, "pv-reset", "u1", due)
	ok, err := a.RecordReminderSent(ctx, seat.ID, 4, due.Add(time.Hour))
	require.NoError(t, err)
	require.True(t, ok)

	overdueSeat(t, a, "pv-reset", "u1", due.Add(24*time.Hour))
	got, err := a.GetAssignment(ctx, "pv-reset", 0, "u1", "")
	require.NoError(t, err)
	require.Zero(t, got.RemindersSent)
}

// A stage escalates once per run: the second record reports it was already
// there, another stage and another run are separate.
func TestStageEscalation_RecordedOncePerRunAndStage(t *testing.T) {
	ctx := context.Background()
	a := store.NewAssignments(newTestDB(t))
	at := time.Date(2026, 3, 9, 0, 0, 0, 0, time.UTC)

	done, err := a.StageEscalated(ctx, "run-1", 0)
	require.NoError(t, err)
	require.False(t, done)

	first, err := a.RecordStageEscalation(ctx, "run-1", "pv-1", 0, at)
	require.NoError(t, err)
	require.True(t, first)
	again, err := a.RecordStageEscalation(ctx, "run-1", "pv-1", 0, at.Add(time.Hour))
	require.NoError(t, err)
	require.False(t, again, "the stage is recorded once")

	done, err = a.StageEscalated(ctx, "run-1", 0)
	require.NoError(t, err)
	require.True(t, done)
	for _, c := range []struct {
		run   string
		stage int
	}{{"run-1", 1}, {"run-2", 0}} {
		done, err = a.StageEscalated(ctx, c.run, c.stage)
		require.NoError(t, err)
		require.False(t, done, "%s stage %d is its own", c.run, c.stage)
	}
}
