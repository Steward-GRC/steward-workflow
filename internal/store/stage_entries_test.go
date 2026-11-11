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

// Recording the entry of a stage is idempotent: the saga may enter a stage
// again on a retry or the request-changes loop, and the first entry time holds
// (steward-workflow#50).
func TestStageEntries_RecordIsIdempotent(t *testing.T) {
	ctx := context.Background()
	a := store.NewAssignments(newTestDB(t))
	first := time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC)

	require.NoError(t, a.RecordStageEntry(ctx, "run-e1", 0, first))
	require.NoError(t, a.RecordStageEntry(ctx, "run-e1", 0, first.Add(time.Hour)), "a second entry is not an error")
	require.NoError(t, a.RecordStageEntry(ctx, "run-e1", 2, first))

	got, err := a.ListStageEntries(ctx, "run-e1")
	require.NoError(t, err)
	require.Equal(t, []int{0, 2}, got, "one row per (run, stage), in stage order")

	none, err := a.ListStageEntries(ctx, "run-other")
	require.NoError(t, err)
	require.Empty(t, none, "a run with no entries lists none (a run from before the table)")
}

// The run keeps the category lineage it was submitted with, so a stage not yet
// entered can show its per-category pool.
func TestTrackRun_KeepsAncestorCategories(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	rs := store.NewAssignmentStore(db)
	def := seedDef(t, db)
	require.NoError(t, rs.TrackRun(ctx, store.ApprovalRun{
		PolicyVersionID: "pv-anc", RunID: "run-anc", WorkflowDefID: def, WorkflowVersion: 1,
		AncestorCategoryIDs: []string{"cat-home", "cat-root"},
	}))
	got, err := rs.GetRun(ctx, "pv-anc")
	require.NoError(t, err)
	require.Equal(t, []string{"cat-home", "cat-root"}, got.AncestorCategoryIDs)
}
