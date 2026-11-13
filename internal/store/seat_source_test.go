// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package store_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Steward-GRC/steward-workflow/internal/store"
)

// The seat source column (steward-workflow#80, migration 0009).

func TestAssignments_SeatSourceIsStoredAndReadBack(t *testing.T) {
	a := store.NewAssignments(newTestDB(t))
	ctx := context.Background()
	now := time.Now().UTC()
	for user, source := range map[string]string{
		"u-approver": store.SeatSourceApprover,
		"u-owner":    store.SeatSourceCategoryOwner,
		"u-none":     "",
	} {
		_, err := a.CreateAssignment(ctx, store.AssignmentRow{
			PolicyVersionID: "pv-src", UserID: user, Source: source,
			SLADeadlineAt: now.Add(time.Hour), ReminderAt: now.Add(time.Hour),
		}, "system")
		require.NoError(t, err)
		got, err := a.GetAssignment(ctx, "pv-src", 0, user, "")
		require.NoError(t, err)
		require.Equal(t, source, got.Source, user)
	}
}

func TestAssignments_ReseatingResetsTheSource(t *testing.T) {
	a := store.NewAssignments(newTestDB(t))
	ctx := context.Background()
	now := time.Now().UTC()
	seat := store.AssignmentRow{PolicyVersionID: "pv-re", UserID: "u1", Source: store.SeatSourceApprover,
		SLADeadlineAt: now.Add(time.Hour), ReminderAt: now.Add(time.Hour)}
	_, err := a.CreateAssignment(ctx, seat, "system")
	require.NoError(t, err)
	// The stage is entered again and the person now joins as an owner.
	seat.Source = store.SeatSourceCategoryOwner
	_, err = a.CreateAssignment(ctx, seat, "system")
	require.NoError(t, err)
	got, err := a.GetAssignment(ctx, "pv-re", 0, "u1", "")
	require.NoError(t, err)
	require.Equal(t, store.SeatSourceCategoryOwner, got.Source)
}

func TestAssignments_ASwappedInSeatKeepsTheSource(t *testing.T) {
	a := store.NewAssignments(newTestDB(t))
	ctx := context.Background()
	now := time.Now().UTC()
	old, err := a.CreateAssignment(ctx, store.AssignmentRow{PolicyVersionID: "pv-sw", UserID: "u1",
		Source: store.SeatSourceCategoryOwner, SLADeadlineAt: now.Add(time.Hour), ReminderAt: now.Add(time.Hour)}, "system")
	require.NoError(t, err)
	got, err := a.Swap(ctx, store.SwapInput{OldAssignmentID: old.ID, NewUserID: "u2",
		NewSLADeadlineAt: now.Add(time.Hour), NewReminderAt: now.Add(time.Hour), ActorUserID: "admin", ActorRole: "admin", Reason: "away"})
	require.NoError(t, err)
	require.Equal(t, store.SeatSourceCategoryOwner, got.Source)
	read, err := a.GetAssignment(ctx, "pv-sw", 0, "u2", "")
	require.NoError(t, err)
	require.Equal(t, store.SeatSourceCategoryOwner, read.Source)
}

// schemaBefore0009 returns a migrations dir holding everything before 0009.
func schemaBefore0009(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	files, err := filepath.Glob("../../migrations/000[1-8]_*.sql")
	require.NoError(t, err)
	require.NotEmpty(t, files)
	for _, f := range files {
		b, err := os.ReadFile(f)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, filepath.Base(f)), b, 0o600))
	}
	return dir
}

func migration0009(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile("../../migrations/0009_seat_source." + name + ".sql")
	require.NoError(t, err)
	return string(b)
}

// A seat made before the migration reads as unspecified, and the column is
// there for new seats; down removes it again.
func TestMigration0009_SeatsFromBeforeReadUnspecified(t *testing.T) {
	ctx := context.Background()
	db := newTestDBIn(t, schemaBefore0009(t))
	q := db.Querier()
	_, err := q.Exec(ctx,
		`INSERT INTO approval_assignments (id, policy_version_id, stage_index, user_id, group_id, sla_deadline_at, reminder_at, state)
		 VALUES ('00000000-0000-0000-0000-000000000001', 'pv-old', 0, 'u1', '', now(), now(), 'pending')`)
	require.NoError(t, err)

	_, err = q.Exec(ctx, migration0009(t, "up"))
	require.NoError(t, err)

	got, err := store.NewAssignments(db).GetAssignment(ctx, "pv-old", 0, "u1", "")
	require.NoError(t, err)
	require.Equal(t, "", got.Source, "a seat from before the migration is unspecified")

	_, err = q.Exec(ctx, migration0009(t, "down"))
	require.NoError(t, err)
	var n int
	require.NoError(t, q.QueryRow(ctx,
		`SELECT count(*) FROM information_schema.columns WHERE table_name='approval_assignments' AND column_name='seat_source'`).Scan(&n))
	require.Zero(t, n)
}
