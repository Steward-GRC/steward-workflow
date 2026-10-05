// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package store_test

import (
	"context"
	"testing"

	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/store"
)

func TestAssignmentStore_SetAndGetPolicyOverride(t *testing.T) {
	pool := newTestDB(t)
	ds := store.NewWorkflowDefStore(pool)
	as := store.NewAssignmentStore(pool)
	ctx := context.Background()

	realDefID, err := ds.Create(ctx, builder.WorkflowDef{
		Name: "Override Test WF",
		Stages: []builder.Stage{
			{Name: "s", Quorum: builder.QuorumAny, ApproverIDs: []string{"user-carol"}},
		},
	})
	if err != nil {
		t.Fatalf("Create workflow def fixture: %v", err)
	}

	if err := as.SetPolicyOverride(ctx, "policy-1", realDefID); err != nil {
		t.Fatalf("SetPolicyOverride: %v", err)
	}

	gotDefID, exists, err := as.GetPolicyOverride(ctx, "policy-1")
	if err != nil {
		t.Fatalf("GetPolicyOverride: %v", err)
	}
	if !exists {
		t.Fatal("expected exists=true")
	}
	if gotDefID != realDefID {
		t.Fatalf("GetPolicyOverride: want %q, got %q", realDefID, gotDefID)
	}
}

func TestAssignmentStore_GetPolicyOverride_NotFound(t *testing.T) {
	pool := newTestDB(t)
	as := store.NewAssignmentStore(pool)
	ctx := context.Background()

	defID, exists, err := as.GetPolicyOverride(ctx, "missing-policy")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exists {
		t.Fatal("expected exists=false for missing policy")
	}
	if defID != "" {
		t.Fatalf("expected empty defID, got %q", defID)
	}
}

func TestAssignmentStore_SetPolicyOverride_NoWorkflow(t *testing.T) {
	pool := newTestDB(t)
	as := store.NewAssignmentStore(pool)
	ctx := context.Background()

	// Empty workflow_def_id means "this policy explicitly requires no approval".
	if err := as.SetPolicyOverride(ctx, "policy-skip", ""); err != nil {
		t.Fatalf("SetPolicyOverride: %v", err)
	}

	defID, exists, err := as.GetPolicyOverride(ctx, "policy-skip")
	if err != nil {
		t.Fatalf("GetPolicyOverride: %v", err)
	}
	if !exists {
		t.Fatal("expected exists=true for explicit no-workflow override")
	}
	if defID != "" {
		t.Fatalf("expected empty defID for explicit no-workflow, got %q", defID)
	}
}

func TestAssignmentStore_TrackAndGetRun(t *testing.T) {
	pool := newTestDB(t)
	ds := store.NewWorkflowDefStore(pool)
	as := store.NewAssignmentStore(pool)
	ctx := context.Background()

	// Create a real WorkflowDef so the FK on approval_runs is satisfied.
	realDefID, err := ds.Create(ctx, builder.WorkflowDef{
		Name: "Track Run WF",
		Stages: []builder.Stage{
			{Name: "s", Quorum: builder.QuorumAny, ApproverIDs: []string{"user-carol"}},
		},
	})
	if err != nil {
		t.Fatalf("Create workflow def fixture: %v", err)
	}

	if err := as.TrackRun(ctx, store.ApprovalRun{
		PolicyVersionID: "pv-001",
		RunID:           "run-abc",
		WorkflowDefID:   realDefID,
		WorkflowVersion: 1,
		HomeCategoryID:  "cat-home-42",
	}); err != nil {
		t.Fatalf("TrackRun: %v", err)
	}

	got, err := as.GetRun(ctx, "pv-001")
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if got.RunID != "run-abc" {
		t.Fatalf("RunID: want \"run-abc\", got %q", got.RunID)
	}
	if got.Status != "in_review" {
		t.Fatalf("Status: want \"in_review\", got %q", got.Status)
	}
	// HomeCategoryID round-trips so decision/signal audit events can stamp group.id.
	if got.HomeCategoryID != "cat-home-42" {
		t.Fatalf("HomeCategoryID: want \"cat-home-42\", got %q", got.HomeCategoryID)
	}
	// GetRunByRunID also carries the home group.
	byRun, err := as.GetRunByRunID(ctx, "run-abc")
	if err != nil {
		t.Fatalf("GetRunByRunID: %v", err)
	}
	if byRun.HomeCategoryID != "cat-home-42" {
		t.Fatalf("GetRunByRunID HomeCategoryID: want \"cat-home-42\", got %q", byRun.HomeCategoryID)
	}

	// GetRun on non-existent version returns not-found error
	if _, err = as.GetRun(ctx, "pv-nope"); err == nil {
		t.Fatal("expected not-found error for missing policy version")
	}
}

// TestAssignmentStore_ReSubmitPreservesPriorRun verifies Part 1: AbortActiveRuns
// + a plain-INSERT TrackRun preserve the prior run as terminal 'aborted' and
// GetRun returns the latest ACTIVE run.
func TestAssignmentStore_ReSubmitPreservesPriorRun(t *testing.T) {
	pool := newTestDB(t)
	ds := store.NewWorkflowDefStore(pool)
	as := store.NewAssignmentStore(pool)
	ctx := context.Background()

	defID, err := ds.Create(ctx, builder.WorkflowDef{
		Name: "Preserve WF",
		Stages: []builder.Stage{
			{Name: "s", Quorum: builder.QuorumAny, ApproverIDs: []string{"user-carol"}},
		},
	})
	if err != nil {
		t.Fatalf("Create workflow def fixture: %v", err)
	}

	// First submission.
	if err := as.TrackRun(ctx, store.ApprovalRun{
		PolicyVersionID: "pv-pres", RunID: "run-1", WorkflowDefID: defID, WorkflowVersion: 1,
	}); err != nil {
		t.Fatalf("TrackRun #1: %v", err)
	}

	// Re-submit: abort the active run, then insert a fresh one.
	aborted, err := as.AbortActiveRuns(ctx, "pv-pres")
	if err != nil {
		t.Fatalf("AbortActiveRuns: %v", err)
	}
	if aborted != 1 {
		t.Fatalf("aborted = %d, want 1", aborted)
	}
	if err := as.TrackRun(ctx, store.ApprovalRun{
		PolicyVersionID: "pv-pres", RunID: "run-2", WorkflowDefID: defID, WorkflowVersion: 1,
	}); err != nil {
		t.Fatalf("TrackRun #2: %v", err)
	}

	// GetRun returns the latest ACTIVE run (run-2).
	got, err := as.GetRun(ctx, "pv-pres")
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if got.RunID != "run-2" || got.Status != "in_review" {
		t.Fatalf("GetRun = (%s,%s), want (run-2,in_review)", got.RunID, got.Status)
	}

	// The prior run is PRESERVED as terminal 'aborted'.
	var priorStatus string
	if err := pool.Querier().QueryRow(ctx, `SELECT status FROM approval_runs WHERE run_id='run-1'`).Scan(&priorStatus); err != nil {
		t.Fatalf("read prior run: %v", err)
	}
	if priorStatus != "aborted" {
		t.Fatalf("prior run status = %q, want aborted", priorStatus)
	}
}

// TestAssignmentStore_OneActiveRunPerVersion verifies the partial unique index:
// a second active run for a version without aborting the first is rejected.
func TestAssignmentStore_OneActiveRunPerVersion(t *testing.T) {
	pool := newTestDB(t)
	ds := store.NewWorkflowDefStore(pool)
	as := store.NewAssignmentStore(pool)
	ctx := context.Background()

	defID, err := ds.Create(ctx, builder.WorkflowDef{
		Name: "One Active WF",
		Stages: []builder.Stage{
			{Name: "s", Quorum: builder.QuorumAny, ApproverIDs: []string{"user-carol"}},
		},
	})
	if err != nil {
		t.Fatalf("Create workflow def fixture: %v", err)
	}

	if err := as.TrackRun(ctx, store.ApprovalRun{
		PolicyVersionID: "pv-active", RunID: "run-a", WorkflowDefID: defID, WorkflowVersion: 1,
	}); err != nil {
		t.Fatalf("TrackRun #1: %v", err)
	}
	// A second active run WITHOUT aborting the first must violate the partial
	// unique index (at most one active run per version).
	err = as.TrackRun(ctx, store.ApprovalRun{
		PolicyVersionID: "pv-active", RunID: "run-b", WorkflowDefID: defID, WorkflowVersion: 1,
	})
	if err == nil {
		t.Fatal("TrackRun #2 succeeded; want a unique-violation (two active runs for one version)")
	}
}

func TestAssignmentStore_UpdateRunStatus_Terminal(t *testing.T) {
	pool := newTestDB(t)
	ds := store.NewWorkflowDefStore(pool)
	as := store.NewAssignmentStore(pool)
	ctx := context.Background()

	realDefID, err := ds.Create(ctx, builder.WorkflowDef{
		Name: "Update Status WF",
		Stages: []builder.Stage{
			{Name: "s", Quorum: builder.QuorumAny, ApproverIDs: []string{"user-carol"}},
		},
	})
	if err != nil {
		t.Fatalf("Create workflow def fixture: %v", err)
	}

	if err := as.TrackRun(ctx, store.ApprovalRun{
		PolicyVersionID: "pv-002",
		RunID:           "run-xyz",
		WorkflowDefID:   realDefID,
		WorkflowVersion: 1,
	}); err != nil {
		t.Fatalf("TrackRun: %v", err)
	}

	if err := as.UpdateRunStatus(ctx, "pv-002", "approved"); err != nil {
		t.Fatalf("UpdateRunStatus: %v", err)
	}

	got, err := as.GetRun(ctx, "pv-002")
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if got.Status != "approved" {
		t.Fatalf("Status: want \"approved\", got %q", got.Status)
	}
	if got.ResolvedAt == nil {
		t.Fatal("expected ResolvedAt to be set after terminal status")
	}
}

func TestAssignmentStore_UpdateRunStatus_NonTerminal(t *testing.T) {
	pool := newTestDB(t)
	ds := store.NewWorkflowDefStore(pool)
	as := store.NewAssignmentStore(pool)
	ctx := context.Background()

	realDefID, err := ds.Create(ctx, builder.WorkflowDef{
		Name: "Non-Terminal Status WF",
		Stages: []builder.Stage{
			{Name: "s", Quorum: builder.QuorumAny, ApproverIDs: []string{"user-carol"}},
		},
	})
	if err != nil {
		t.Fatalf("Create workflow def fixture: %v", err)
	}

	if err := as.TrackRun(ctx, store.ApprovalRun{
		PolicyVersionID: "pv-003",
		RunID:           "run-nnn",
		WorkflowDefID:   realDefID,
		WorkflowVersion: 1,
	}); err != nil {
		t.Fatalf("TrackRun: %v", err)
	}

	if err := as.UpdateRunStatus(ctx, "pv-003", "escalated"); err != nil {
		t.Fatalf("UpdateRunStatus: %v", err)
	}

	got, err := as.GetRun(ctx, "pv-003")
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if got.Status != "escalated" {
		t.Fatalf("Status: want \"escalated\", got %q", got.Status)
	}
	if got.ResolvedAt != nil {
		t.Fatal("expected ResolvedAt to remain nil for non-terminal status")
	}
}
