// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package store_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/store"
)

// TestWorkflowDefStore_ApproversByCategoryRoundTrips verifies that a stage's
// per-category approver map survives Create and Get intact. The def is
// stored as JSON, so the map must round-trip through the stages_json column.
func TestWorkflowDefStore_ApproversByCategoryRoundTrips(t *testing.T) {
	pool := newTestDB(t)
	s := store.NewWorkflowDefStore(pool)
	ctx := context.Background()

	want := map[string][]string{
		"grpA": {"u1", "u2"},
		"grpB": {"u3"},
	}
	id, err := s.Create(ctx, builder.WorkflowDef{
		Name: "PerGroup Workflow",
		Stages: []builder.Stage{
			{
				Name:                "Stage 1",
				Quorum:              builder.QuorumAny,
				ApproverIDs:         []string{"u1", "u2", "u3"},
				ApproversByCategory: want,
			},
		},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := s.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got.Stages) != 1 {
		t.Fatalf("expected 1 stage, got %d", len(got.Stages))
	}
	if !reflect.DeepEqual(got.Stages[0].ApproversByCategory, want) {
		t.Fatalf("ApproversByCategory: got %+v, want %+v", got.Stages[0].ApproversByCategory, want)
	}
}

func TestWorkflowDefStore_CreateAndGet(t *testing.T) {
	pool := newTestDB(t)
	s := store.NewWorkflowDefStore(pool)
	ctx := context.Background()

	wd := builder.WorkflowDef{
		Name: "Test Workflow",
		Stages: []builder.Stage{
			{Name: "Stage 1", Quorum: builder.QuorumAny, ApproverIDs: []string{"u1"}},
		},
	}
	id, err := s.Create(ctx, wd)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if id == "" {
		t.Fatal("expected non-empty ID")
	}

	got, err := s.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Name != "Test Workflow" {
		t.Fatalf("Name: got %q", got.Name)
	}
	if got.Version != 1 {
		t.Fatalf("Version: got %d", got.Version)
	}
	if len(got.Stages) != 1 || got.Stages[0].Name != "Stage 1" {
		t.Fatalf("Stages: %+v", got.Stages)
	}
}

func TestWorkflowDefStore_Publish(t *testing.T) {
	pool := newTestDB(t)
	s := store.NewWorkflowDefStore(pool)
	ctx := context.Background()

	wd := builder.WorkflowDef{Name: "Pub Test", Stages: []builder.Stage{
		{Name: "s", Quorum: builder.QuorumAny, ApproverIDs: []string{"u1"}},
	}}
	id, err := s.Create(ctx, wd)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := s.Publish(ctx, id); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got, err := s.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.Published {
		t.Fatal("expected Published=true after Publish()")
	}
}

func TestWorkflowDefStore_UpdateBumpsAndKeepsOldVersion(t *testing.T) {
	pool := newTestDB(t)
	s := store.NewWorkflowDefStore(pool)
	ctx := context.Background()

	// Create at v1 with stage "S0".
	id, err := s.Create(ctx, builder.WorkflowDef{
		Name: "wf",
		Stages: []builder.Stage{
			{Name: "S0", ApproverIDs: []string{"u1"}, Quorum: builder.QuorumAny},
		},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Update to v2 with stage "S0b".
	if _, err := s.Update(ctx, id, builder.WorkflowDef{
		Name: "wf",
		Stages: []builder.Stage{
			{Name: "S0b", ApproverIDs: []string{"u2"}, Quorum: builder.QuorumAny},
		},
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	// Current row should be v2 with the new stage name.
	cur, err := s.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get after update: %v", err)
	}
	if cur.Version != 2 {
		t.Fatalf("expected version 2, got %d", cur.Version)
	}
	if len(cur.Stages) == 0 || cur.Stages[0].Name != "S0b" {
		t.Fatalf("current stage not updated: %+v", cur)
	}

	// v1 must still be retrievable via GetVersion.
	old, err := s.GetVersion(ctx, id, 1)
	if err != nil {
		t.Fatalf("GetVersion v1: %v", err)
	}
	if len(old.Stages) == 0 || old.Stages[0].Name != "S0" {
		t.Fatalf("v1 not retained: %+v", old)
	}
}

func TestWorkflowDefStore_GetVersionCurrentReadsLiveRow(t *testing.T) {
	pool := newTestDB(t)
	s := store.NewWorkflowDefStore(pool)
	ctx := context.Background()

	id, err := s.Create(ctx, builder.WorkflowDef{
		Name: "wf",
		Stages: []builder.Stage{
			{Name: "S0", ApproverIDs: []string{"u1"}, Quorum: builder.QuorumAny},
		},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// GetVersion(id, 1) on a fresh v1 def should read the live row.
	got, err := s.GetVersion(ctx, id, 1)
	if err != nil {
		t.Fatalf("GetVersion v1 (live row): %v", err)
	}
	if got.Version != 1 || got.Stages[0].Name != "S0" {
		t.Fatalf("unexpected: %+v", got)
	}
}

func TestWorkflowDefStore_List(t *testing.T) {
	pool := newTestDB(t)
	s := store.NewWorkflowDefStore(pool)
	ctx := context.Background()

	// Create two defs.
	id1, err := s.Create(ctx, builder.WorkflowDef{
		Name: "Alpha",
		Stages: []builder.Stage{
			{Name: "s", Quorum: builder.QuorumAny, ApproverIDs: []string{"u1"}},
		},
	})
	if err != nil {
		t.Fatalf("Create alpha: %v", err)
	}
	_, err = s.Create(ctx, builder.WorkflowDef{
		Name: "Beta",
		Stages: []builder.Stage{
			{Name: "s", Quorum: builder.QuorumAny, ApproverIDs: []string{"u1"}},
		},
	})
	if err != nil {
		t.Fatalf("Create beta: %v", err)
	}

	defs, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(defs) < 2 {
		t.Fatalf("expected at least 2 defs, got %d", len(defs))
	}

	// Archive id1; it should vanish from List.
	if err := s.Archive(ctx, id1); err != nil {
		t.Fatalf("Archive: %v", err)
	}
	defs2, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List after archive: %v", err)
	}
	for _, d := range defs2 {
		if d.ID == id1 {
			t.Fatal("archived def should not appear in List")
		}
	}
}

func TestWorkflowDefStore_ArchiveSetsArchivedAt(t *testing.T) {
	pool := newTestDB(t)
	s := store.NewWorkflowDefStore(pool)
	ctx := context.Background()

	id, err := s.Create(ctx, builder.WorkflowDef{
		Name: "ToArchive",
		Stages: []builder.Stage{
			{Name: "s", Quorum: builder.QuorumAny, ApproverIDs: []string{"u1"}},
		},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := s.Archive(ctx, id); err != nil {
		t.Fatalf("Archive: %v", err)
	}

	// Confirm it's gone from List.
	defs, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, d := range defs {
		if d.ID == id {
			t.Fatal("archived def must not appear in List")
		}
	}

	// Get still works (soft-archive, not delete).
	got, err := s.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get archived def: %v", err)
	}
	if got.ID != id {
		t.Fatalf("expected id %q, got %q", id, got.ID)
	}
}

func TestWorkflowDefStore_DescriptionPersistedThroughVersioning(t *testing.T) {
	pool := newTestDB(t)
	s := store.NewWorkflowDefStore(pool)
	ctx := context.Background()

	// Create with Description "desc-A".
	id, err := s.Create(ctx, builder.WorkflowDef{
		Name:        "wf-desc",
		Description: "desc-A",
		Stages: []builder.Stage{
			{Name: "S0", ApproverIDs: []string{"u1"}, Quorum: builder.QuorumAny},
		},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Get must return Description "desc-A".
	got, err := s.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get after Create: %v", err)
	}
	if got.Description != "desc-A" {
		t.Fatalf("Description after Create: got %q, want %q", got.Description, "desc-A")
	}

	// Update to Description "desc-B" → new live row must return "desc-B".
	if _, err := s.Update(ctx, id, builder.WorkflowDef{
		Name:        "wf-desc",
		Description: "desc-B",
		Stages: []builder.Stage{
			{Name: "S0b", ApproverIDs: []string{"u2"}, Quorum: builder.QuorumAny},
		},
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	live, err := s.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get after Update: %v", err)
	}
	if live.Description != "desc-B" {
		t.Fatalf("Description after Update (live): got %q, want %q", live.Description, "desc-B")
	}

	// GetVersion(id, 1) must return the original "desc-A" (copy-on-write preserved it).
	old, err := s.GetVersion(ctx, id, 1)
	if err != nil {
		t.Fatalf("GetVersion v1: %v", err)
	}
	if old.Description != "desc-A" {
		t.Fatalf("Description v1 (version copy): got %q, want %q", old.Description, "desc-A")
	}
}

func TestWorkflowDefStore_GCVersion(t *testing.T) {
	pool := newTestDB(t)
	s := store.NewWorkflowDefStore(pool)
	ctx := context.Background()

	id, err := s.Create(ctx, builder.WorkflowDef{
		Name: "wf",
		Stages: []builder.Stage{
			{Name: "S0", ApproverIDs: []string{"u1"}, Quorum: builder.QuorumAny},
		},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Bump to v2 so v1 is in workflow_def_versions.
	if _, err := s.Update(ctx, id, builder.WorkflowDef{
		Name: "wf",
		Stages: []builder.Stage{
			{Name: "S0b", ApproverIDs: []string{"u2"}, Quorum: builder.QuorumAny},
		},
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	// v1 must be reachable before GC.
	if _, err := s.GetVersion(ctx, id, 1); err != nil {
		t.Fatalf("GetVersion v1 pre-GC: %v", err)
	}

	// GC v1.
	if err := s.GCVersion(ctx, id, 1); err != nil {
		t.Fatalf("GCVersion: %v", err)
	}

	// v1 must no longer be reachable (returns an error).
	if _, err := s.GetVersion(ctx, id, 1); err == nil {
		t.Fatal("expected error after GCVersion, got nil")
	}
}

func TestWorkflowDefStore_AuthorFollowsTheLastEdit(t *testing.T) {
	s := store.NewWorkflowDefStore(newTestDB(t))
	ctx := context.Background()
	stages := []builder.Stage{{Name: "Review", Quorum: builder.QuorumAny, ApproverIDs: []string{"user-carol"}}}

	id, err := s.Create(ctx, builder.WorkflowDef{Name: "Standard 2-stage", Stages: stages, AuthorUserID: "user-frank"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	got, err := s.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.AuthorUserID != "user-frank" {
		t.Fatalf("author after create = %q, want user-frank", got.AuthorUserID)
	}

	updated, err := s.Update(ctx, id, builder.WorkflowDef{Name: "Standard 2-stage", Stages: stages, AuthorUserID: "user-alice"})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updated.AuthorUserID != "user-alice" {
		t.Fatalf("author after update = %q, want user-alice", updated.AuthorUserID)
	}
}
