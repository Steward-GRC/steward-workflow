// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package store_test

import (
	"context"
	"errors"
	"testing"

	pg "github.com/Bugs5382/go-postgres"

	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/store"
)

func newDeleteTestDef(t *testing.T, s *store.WorkflowDefStore, name string, extraVersions int) string {
	t.Helper()
	ctx := context.Background()
	wd := builder.WorkflowDef{Name: name, Stages: []builder.Stage{{Name: "S1", Quorum: builder.QuorumAny, ApproverIDs: []string{"u1"}}}}
	id, err := s.Create(ctx, wd)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	for i := 0; i < extraVersions; i++ {
		if _, err := s.Update(ctx, id, wd); err != nil {
			t.Fatalf("Update: %v", err)
		}
	}
	return id
}

func countRows(t *testing.T, db *pg.DB, query string, args ...any) int {
	t.Helper()
	var n int
	if err := db.Querier().QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", query, err)
	}
	return n
}

func TestWorkflowDefStore_DeleteRemovesUnpinnedDefAndVersions(t *testing.T) {
	db := newTestDB(t)
	s := store.NewWorkflowDefStore(db)
	ctx := context.Background()

	id := newDeleteTestDef(t, s, "Unused", 2)
	other := newDeleteTestDef(t, s, "Other", 1)
	if n := countRows(t, db, `SELECT COUNT(*) FROM workflow_def_versions WHERE def_id = $1`, id); n != 2 {
		t.Fatalf("setup: want 2 kept versions, got %d", n)
	}

	if err := s.Delete(ctx, id); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if n := countRows(t, db, `SELECT COUNT(*) FROM workflow_defs WHERE id = $1`, id); n != 0 {
		t.Fatalf("definition still present")
	}
	if n := countRows(t, db, `SELECT COUNT(*) FROM workflow_def_versions WHERE def_id = $1`, id); n != 0 {
		t.Fatalf("kept versions still present: %d", n)
	}
	// Another definition and its versions are untouched.
	if n := countRows(t, db, `SELECT COUNT(*) FROM workflow_def_versions WHERE def_id = $1`, other); n != 1 {
		t.Fatalf("other definition's versions changed: %d", n)
	}
	if _, err := s.Get(ctx, other); err != nil {
		t.Fatalf("other definition gone: %v", err)
	}
}

func TestWorkflowDefStore_DeleteRemovesAnArchivedDef(t *testing.T) {
	db := newTestDB(t)
	s := store.NewWorkflowDefStore(db)
	ctx := context.Background()

	id := newDeleteTestDef(t, s, "Archived", 0)
	if err := s.Archive(ctx, id); err != nil {
		t.Fatalf("Archive: %v", err)
	}
	if err := s.Delete(ctx, id); err != nil {
		t.Fatalf("Delete of an archived definition: %v", err)
	}
}

func TestWorkflowDefStore_DeleteRefusesWhileAnyRunPinnedIt(t *testing.T) {
	for _, status := range []string{"in_review", "approved", "rejected", "withdrawn"} {
		t.Run(status, func(t *testing.T) {
			db := newTestDB(t)
			s := store.NewWorkflowDefStore(db)
			ctx := context.Background()

			id := newDeleteTestDef(t, s, "Pinned", 1)
			for i, ver := range []int{1, 2} {
				if _, err := db.Querier().Exec(ctx,
					`INSERT INTO approval_runs (policy_version_id, run_id, workflow_def_id, workflow_version, status)
					 VALUES ($1, $2, $3, $4, $5)`,
					"pv-"+string(rune('a'+i)), "run-"+string(rune('a'+i)), id, ver, status,
				); err != nil {
					t.Fatalf("insert run: %v", err)
				}
			}

			err := s.Delete(ctx, id)
			var inUse *store.DefInUseError
			if !errors.As(err, &inUse) {
				t.Fatalf("want DefInUseError, got %v", err)
			}
			if inUse.Runs != 2 || inUse.Overrides != 0 {
				t.Fatalf("counts: got runs=%d overrides=%d, want 2 and 0", inUse.Runs, inUse.Overrides)
			}
			if n := countRows(t, db, `SELECT COUNT(*) FROM workflow_defs WHERE id = $1`, id); n != 1 {
				t.Fatalf("definition removed despite the refusal")
			}
			if n := countRows(t, db, `SELECT COUNT(*) FROM workflow_def_versions WHERE def_id = $1`, id); n != 1 {
				t.Fatalf("versions removed despite the refusal: %d", n)
			}
		})
	}
}

func TestWorkflowDefStore_DeleteRefusesWhileAPolicyOverrideUsesIt(t *testing.T) {
	db := newTestDB(t)
	s := store.NewWorkflowDefStore(db)
	ctx := context.Background()

	id := newDeleteTestDef(t, s, "Override target", 0)
	if _, err := db.Querier().Exec(ctx,
		`INSERT INTO policy_workflow_overrides (policy_id, workflow_def_id) VALUES ('pol-1', $1), ('pol-2', $1)`, id,
	); err != nil {
		t.Fatalf("insert overrides: %v", err)
	}

	err := s.Delete(ctx, id)
	var inUse *store.DefInUseError
	if !errors.As(err, &inUse) {
		t.Fatalf("want DefInUseError, got %v", err)
	}
	if inUse.Runs != 0 || inUse.Overrides != 2 {
		t.Fatalf("counts: got runs=%d overrides=%d, want 0 and 2", inUse.Runs, inUse.Overrides)
	}
	if n := countRows(t, db, `SELECT COUNT(*) FROM policy_workflow_overrides WHERE workflow_def_id = $1`, id); n != 2 {
		t.Fatalf("overrides were nulled: %d left", n)
	}
}

func TestWorkflowDefStore_DeleteUnknownIDIsNotFound(t *testing.T) {
	db := newTestDB(t)
	s := store.NewWorkflowDefStore(db)

	err := s.Delete(context.Background(), "00000000-0000-0000-0000-000000000000")
	if !errors.Is(err, store.ErrWorkflowDefNotFound) {
		t.Fatalf("want ErrWorkflowDefNotFound, got %v", err)
	}
}
