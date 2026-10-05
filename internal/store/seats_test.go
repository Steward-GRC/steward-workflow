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

// A person who is an individual approver and a member of a group on the same
// stage holds two seats and decides each separately.

func seat(t *testing.T, a *store.Assignments, pv, user, group string) store.AssignmentRow {
	t.Helper()
	now := time.Now().UTC()
	row, err := a.CreateAssignment(context.Background(), store.AssignmentRow{
		PolicyVersionID: pv, StageIndex: 0, UserID: user, GroupID: group,
		SLADeadlineAt: now.Add(72 * time.Hour), ReminderAt: now.Add(48 * time.Hour),
	}, "user-bob")
	require.NoError(t, err)
	return row
}

func TestSeats_IndividualAndGroupSeatsAreSeparate(t *testing.T) {
	a := store.NewAssignments(newTestDB(t))
	ctx := context.Background()
	individual := seat(t, a, "pv-d1", "user-carol", "")
	group := seat(t, a, "pv-d1", "user-carol", "grp-finance")
	require.NotEqual(t, individual.ID, group.ID)

	require.NoError(t, a.DecideAssignment(ctx, store.DecideAssignmentInput{
		AssignmentID: individual.ID, Decision: "approved", Comment: "fine by me", ActorUserID: "user-carol",
	}))

	gotIndividual, err := a.GetAssignment(ctx, "pv-d1", 0, "user-carol", "")
	require.NoError(t, err)
	require.Equal(t, "approved", gotIndividual.State)
	gotGroup, err := a.GetAssignment(ctx, "pv-d1", 0, "user-carol", "grp-finance")
	require.NoError(t, err)
	require.Equal(t, "pending", gotGroup.State, "an individual approval never decides the group seat")
	require.Equal(t, "grp-finance", gotGroup.GroupID)

	rows, err := a.ListStageAssignments(ctx, "pv-d1", 0)
	require.NoError(t, err)
	require.Len(t, rows, 2)
}

func TestSeats_PendingSeatsListsTheIndividualSeatFirst(t *testing.T) {
	a := store.NewAssignments(newTestDB(t))
	seat(t, a, "pv-d1", "user-carol", "grp-finance")
	seat(t, a, "pv-d1", "user-carol", "")
	seat(t, a, "pv-d1", "user-dave", "")

	got, err := a.PendingSeats(context.Background(), "pv-d1", "user-carol")
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Empty(t, got[0].GroupID)
	require.Equal(t, "grp-finance", got[1].GroupID)
}

func TestSeats_SwapKeepsTheGroup(t *testing.T) {
	a := store.NewAssignments(newTestDB(t))
	old := seat(t, a, "pv-d1", "user-carol", "grp-finance")
	now := time.Now().UTC()

	got, err := a.Swap(context.Background(), store.SwapInput{
		OldAssignmentID: old.ID, NewUserID: "user-erin",
		NewSLADeadlineAt: now.Add(time.Hour), NewReminderAt: now.Add(time.Hour),
		ActorUserID: "user-alice", ActorRole: "admin", Reason: "on leave",
	})
	require.NoError(t, err)
	require.Equal(t, "grp-finance", got.GroupID)

	moved, err := a.GetAssignment(context.Background(), "pv-d1", 0, "user-erin", "grp-finance")
	require.NoError(t, err)
	require.Equal(t, "pending", moved.State)
}

func TestSeats_MergeKeepsAGroupSeatBesideTheTargetsIndividualSeat(t *testing.T) {
	a := store.NewAssignments(newTestDB(t))
	seat(t, a, "pv-d1", "user-target", "")
	seat(t, a, "pv-d1", "user-source", "grp-finance")

	res, err := a.ReassignUserItems(context.Background(), "user-source", "user-target", "user-alice", false)
	require.NoError(t, err)
	require.Equal(t, 1, res.AssignmentsReassigned, "a different seat is moved, not deduplicated")
	require.Zero(t, res.AssignmentsDeduped)

	got, err := a.GetAssignment(context.Background(), "pv-d1", 0, "user-target", "grp-finance")
	require.NoError(t, err)
	require.Equal(t, "pending", got.State)
}
