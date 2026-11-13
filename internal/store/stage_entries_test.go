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

// The run keeps the lowering the author asked for, and the agreement is
// claimed once: the first caller wins, a second gets false (steward-workflow#10).
func TestRun_KeepsTheRequestedChangeTypeAndClaimsTheAgreementOnce(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	rs := store.NewAssignmentStore(db)
	def := seedDef(t, db)
	require.NoError(t, rs.TrackRun(ctx, store.ApprovalRun{
		PolicyVersionID: "pv-ct", RunID: "run-ct", WorkflowDefID: def, WorkflowVersion: 1, RequestedChangeType: "minor",
	}))
	require.NoError(t, rs.TrackRun(ctx, store.ApprovalRun{
		PolicyVersionID: "pv-none", RunID: "run-none", WorkflowDefID: def, WorkflowVersion: 1,
	}))

	got, err := rs.GetRun(ctx, "pv-ct")
	require.NoError(t, err)
	require.Equal(t, "minor", got.RequestedChangeType)
	require.Nil(t, got.ChangeTypeAgreedAt)

	first, err := rs.MarkChangeTypeAgreed(ctx, "pv-ct")
	require.NoError(t, err)
	require.True(t, first)
	second, err := rs.MarkChangeTypeAgreed(ctx, "pv-ct")
	require.NoError(t, err)
	require.False(t, second, "the agreement is claimed once")

	got, err = rs.GetRun(ctx, "pv-ct")
	require.NoError(t, err)
	require.NotNil(t, got.ChangeTypeAgreedAt)

	none, err := rs.GetRun(ctx, "pv-none")
	require.NoError(t, err)
	require.Empty(t, none.RequestedChangeType, "a run with no pick has nothing pending")
	claimed, err := rs.MarkChangeTypeAgreed(ctx, "pv-none")
	require.NoError(t, err)
	require.False(t, claimed, "nothing pending, nothing to claim")
}
