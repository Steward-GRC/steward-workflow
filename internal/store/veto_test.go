// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/store"
)

// Veto storage (steward-workflow#26). Mock: veto.test.ts "opting a workflow in".

// Mock: "is off by default and set by a template admin" (the store half).
func TestWorkflowDefStore_VetoEnabledIsOffByDefaultAndSet(t *testing.T) {
	s := store.NewWorkflowDefStore(newTestDB(t))
	ctx := context.Background()
	id, err := s.Create(ctx, builder.WorkflowDef{
		Name:   "Veto WF",
		Stages: []builder.Stage{{Name: "s", Quorum: builder.QuorumAny, ApproverIDs: []string{"u1"}}},
	})
	require.NoError(t, err)

	got, err := s.Get(ctx, id)
	require.NoError(t, err)
	require.False(t, got.VetoEnabled)

	set, err := s.SetVetoEnabled(ctx, id, true)
	require.NoError(t, err)
	require.True(t, set.VetoEnabled)

	listed, err := s.List(ctx)
	require.NoError(t, err)
	require.True(t, listed[0].VetoEnabled)

	// A new version keeps the opt-in; editing stages does not switch it off.
	updated, err := s.Update(ctx, id, builder.WorkflowDef{Name: "Veto WF", Stages: got.Stages})
	require.NoError(t, err)
	require.True(t, updated.VetoEnabled)
}

// Mock: "marks users and group seats on a stage" (the store half): the marks
// ride the stage JSON.
func TestWorkflowDefStore_StageVetoMarksRoundTrip(t *testing.T) {
	s := store.NewWorkflowDefStore(newTestDB(t))
	ctx := context.Background()
	id, err := s.Create(ctx, builder.WorkflowDef{
		Name: "Marks",
		Stages: []builder.Stage{{
			Name: "s", Quorum: builder.QuorumAny, ApproverIDs: []string{"u1"},
			VetoApproverIDs: []string{"u1"}, VetoGroupIDs: []string{"g1"},
		}},
	})
	require.NoError(t, err)
	got, err := s.Get(ctx, id)
	require.NoError(t, err)
	require.Equal(t, []string{"u1"}, got.Stages[0].VetoApproverIDs)
	require.Equal(t, []string{"g1"}, got.Stages[0].VetoGroupIDs)
}

// A stage's veto people ride the stage JSON (steward-workflow#64).
func TestWorkflowDefStore_StageVetoPeopleRoundTrip(t *testing.T) {
	s := store.NewWorkflowDefStore(newTestDB(t))
	ctx := context.Background()
	id, err := s.Create(ctx, builder.WorkflowDef{
		Name: "People",
		Stages: []builder.Stage{
			{Name: "s", Quorum: builder.QuorumAny, ApproverIDs: []string{"u1", "u2"}, VetoUserIDs: []string{"u1", "u2"}},
			{Name: "t", Quorum: builder.QuorumAny, ApproverIDs: []string{"u1"}},
		},
	})
	require.NoError(t, err)
	got, err := s.Get(ctx, id)
	require.NoError(t, err)
	require.Equal(t, []string{"u1", "u2"}, got.Stages[0].VetoUserIDs)
	require.Empty(t, got.Stages[1].VetoUserIDs)
	require.False(t, got.VetoEnabled, "the people apply with the old switch off")
}

// Mock: "carries the marks into a run only when the workflow has opted in"
// (the seat half): the mark is stored on the seat and survives a reset.
func TestSeats_VetoMarkIsStoredOnTheSeat(t *testing.T) {
	a := store.NewAssignments(newTestDB(t))
	ctx := context.Background()
	now := time.Now().UTC()
	for _, veto := range []bool{true, false} {
		_, err := a.CreateAssignment(ctx, store.AssignmentRow{
			PolicyVersionID: "pv-v1", UserID: "user-dave", Veto: veto,
			SLADeadlineAt: now.Add(72 * time.Hour), ReminderAt: now.Add(48 * time.Hour),
		}, "user-bob")
		require.NoError(t, err)
		got, err := a.GetAssignment(ctx, "pv-v1", 0, "user-dave", "")
		require.NoError(t, err)
		require.Equal(t, veto, got.Veto)
	}
}

// The mark belongs to the seat, so a swap hands it to the new holder.
func TestSeats_SwapKeepsTheVetoMark(t *testing.T) {
	a := store.NewAssignments(newTestDB(t))
	ctx := context.Background()
	now := time.Now().UTC()
	old, err := a.CreateAssignment(ctx, store.AssignmentRow{
		PolicyVersionID: "pv-v2", UserID: "user-dave", Veto: true,
		SLADeadlineAt: now.Add(72 * time.Hour), ReminderAt: now.Add(48 * time.Hour),
	}, "user-bob")
	require.NoError(t, err)
	swapped, err := a.Swap(ctx, store.SwapInput{
		OldAssignmentID: old.ID, NewUserID: "user-erin", ActorUserID: "user-grace", ActorRole: "admin", Reason: "PTO",
		NewSLADeadlineAt: now.Add(72 * time.Hour), NewReminderAt: now.Add(48 * time.Hour),
	})
	require.NoError(t, err)
	require.True(t, swapped.Veto)
	got, err := a.GetAssignment(ctx, "pv-v2", 0, "user-erin", "")
	require.NoError(t, err)
	require.True(t, got.Veto)
}
