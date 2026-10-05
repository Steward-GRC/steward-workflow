// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package store_test

import (
	"context"
	"testing"
	"time"

	pg "github.com/Bugs5382/go-postgres"

	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/store"
)

// seedDef creates a minimal workflow def and returns its id, so approval_runs
// (workflow_def_id FK) can be seeded. Version defaults to 1.
func seedDef(t *testing.T, pool *pg.DB) string {
	t.Helper()
	id, err := store.NewWorkflowDefStore(pool).Create(context.Background(), builder.WorkflowDef{
		Name:   "merge-test-wf",
		Stages: []builder.Stage{{Name: "S1", Quorum: builder.QuorumAny, ApproverIDs: []string{"x"}}},
	})
	if err != nil {
		t.Fatalf("create def: %v", err)
	}
	return id
}

// setState forces an assignment row to a terminal/other state so tests can seed
// historical rows the reassignment must leave untouched.
func setState(t *testing.T, pool *pg.DB, id, state string) {
	t.Helper()
	if _, err := pool.Querier().Exec(context.Background(),
		`UPDATE approval_assignments SET state=$2 WHERE id=$1`, id, state); err != nil {
		t.Fatalf("setState %s=%s: %v", id, state, err)
	}
}

func historyEvents(t *testing.T, a *store.Assignments, pv string, stage int) []store.HistoryEntry {
	t.Helper()
	h, err := a.ListHistory(context.Background(), pv, stage)
	if err != nil {
		t.Fatalf("ListHistory: %v", err)
	}
	return h
}

func TestReassignUserItems_PendingReassignedInPlace(t *testing.T) {
	pool := newTestDB(t)
	a := store.NewAssignments(pool)
	ctx := context.Background()

	row := assignmentSeed(t, a, "pv-1", 0, "src")

	res, err := a.ReassignUserItems(ctx, "src", "tgt", "admin", false)
	if err != nil {
		t.Fatalf("ReassignUserItems: %v", err)
	}
	if res.AssignmentsReassigned != 1 || res.AssignmentsDeduped != 0 || res.Total() != 1 {
		t.Fatalf("counts = %+v, want 1 reassigned / 0 deduped / total 1", res)
	}

	// Seat is now the target's, same id/timers, still pending.
	got, err := a.GetAssignment(ctx, "pv-1", 0, "tgt", "")
	if err != nil {
		t.Fatalf("GetAssignment(tgt): %v", err)
	}
	if got.ID != row.ID {
		t.Fatalf("in-place re-point must keep assignment id: got %s want %s", got.ID, row.ID)
	}
	if got.State != "pending" {
		t.Fatalf("state = %q, want pending", got.State)
	}
	// Same instant, allowing for Postgres timestamptz microsecond truncation.
	if d := got.SLADeadlineAt.Sub(row.SLADeadlineAt); d < -time.Microsecond || d > time.Microsecond {
		t.Fatalf("SLA deadline must be preserved: got %v want %v (delta %v)", got.SLADeadlineAt, row.SLADeadlineAt, d)
	}
	// Source no longer holds the seat.
	if _, err := a.GetAssignment(ctx, "pv-1", 0, "src", ""); err == nil {
		t.Fatal("source should no longer hold the seat")
	}
	// A 'reassigned' history row records source->target + actor.
	h := historyEvents(t, a, "pv-1", 0)
	var found bool
	for _, e := range h {
		if e.Event == "reassigned" {
			found = true
			if e.PreviousUserID != "src" || e.NewUserID != "tgt" || e.ActorUserID != "admin" {
				t.Fatalf("reassigned history wrong: %+v", e)
			}
		}
	}
	if !found {
		t.Fatalf("expected a 'reassigned' history event, got %+v", h)
	}
}

func TestReassignUserItems_PausedAlsoMoves(t *testing.T) {
	pool := newTestDB(t)
	a := store.NewAssignments(pool)
	ctx := context.Background()

	row := assignmentSeed(t, a, "pv-2", 0, "src")
	setState(t, pool, row.ID, "paused")

	res, err := a.ReassignUserItems(ctx, "src", "tgt", "admin", false)
	if err != nil {
		t.Fatalf("ReassignUserItems: %v", err)
	}
	if res.AssignmentsReassigned != 1 {
		t.Fatalf("paused seat should move: %+v", res)
	}
	got, err := a.GetAssignment(ctx, "pv-2", 0, "tgt", "")
	if err != nil {
		t.Fatalf("GetAssignment(tgt): %v", err)
	}
	if got.State != "paused" {
		t.Fatalf("paused state must be preserved on re-point: %q", got.State)
	}
}

func TestReassignUserItems_TerminalUntouched(t *testing.T) {
	pool := newTestDB(t)
	a := store.NewAssignments(pool)
	ctx := context.Background()

	// One terminal seat per state the merge must NOT touch.
	terminal := map[string]string{
		"approved":    "pv-a",
		"rejected":    "pv-b",
		"withdrawn":   "pv-c",
		"swapped_out": "pv-d",
		"superseded":  "pv-e",
	}
	for state, pv := range terminal {
		r := assignmentSeed(t, a, pv, 0, "src")
		setState(t, pool, r.ID, state)
	}

	res, err := a.ReassignUserItems(ctx, "src", "tgt", "admin", false)
	if err != nil {
		t.Fatalf("ReassignUserItems: %v", err)
	}
	if res.Total() != 0 {
		t.Fatalf("terminal seats must not move, got %+v", res)
	}
	for state, pv := range terminal {
		got, err := a.GetAssignment(ctx, pv, 0, "src", "")
		if err != nil {
			t.Fatalf("terminal seat %s must survive on source: %v", state, err)
		}
		if got.UserID != "src" || got.State != state {
			t.Fatalf("terminal seat mutated: %+v", got)
		}
	}
}

func TestReassignUserItems_DedupeWhenTargetAlreadyAssigned(t *testing.T) {
	pool := newTestDB(t)
	a := store.NewAssignments(pool)
	ctx := context.Background()

	// Both source and target are pending approvers on the SAME (pv, stage).
	src := assignmentSeed(t, a, "pv-1", 0, "src")
	tgt := assignmentSeed(t, a, "pv-1", 0, "tgt")

	res, err := a.ReassignUserItems(ctx, "src", "tgt", "admin", false)
	if err != nil {
		t.Fatalf("ReassignUserItems: %v", err)
	}
	if res.AssignmentsReassigned != 0 || res.AssignmentsDeduped != 1 {
		t.Fatalf("expected dedupe, got %+v", res)
	}
	// Target's original seat is untouched and still pending.
	gotTgt, err := a.GetAssignment(ctx, "pv-1", 0, "tgt", "")
	if err != nil {
		t.Fatalf("target seat missing: %v", err)
	}
	if gotTgt.ID != tgt.ID || gotTgt.State != "pending" {
		t.Fatalf("target seat should be untouched: %+v", gotTgt)
	}
	// Source's seat is retired (swapped_out), not moved.
	gotSrc, err := a.GetAssignment(ctx, "pv-1", 0, "src", "")
	if err != nil {
		t.Fatalf("source seat should still exist (retired): %v", err)
	}
	if gotSrc.ID != src.ID || gotSrc.State != "swapped_out" {
		t.Fatalf("source seat should be swapped_out: %+v", gotSrc)
	}
}

func TestReassignUserItems_ActiveRunSubmitterMovesTerminalDoesNot(t *testing.T) {
	pool := newTestDB(t)
	a := store.NewAssignments(pool)
	rs := store.NewAssignmentStore(pool)
	ctx := context.Background()

	defID := seedDef(t, pool)
	if err := rs.TrackRun(ctx, store.ApprovalRun{
		PolicyVersionID: "pv-active", RunID: "run-1", WorkflowDefID: defID,
		WorkflowVersion: 1, Status: "in_review", SubmittedBy: "src",
	}); err != nil {
		t.Fatalf("track active run: %v", err)
	}
	if err := rs.TrackRun(ctx, store.ApprovalRun{
		PolicyVersionID: "pv-done", RunID: "run-2", WorkflowDefID: defID,
		WorkflowVersion: 1, Status: "approved", SubmittedBy: "src",
	}); err != nil {
		t.Fatalf("track terminal run: %v", err)
	}

	res, err := a.ReassignUserItems(ctx, "src", "tgt", "admin", false)
	if err != nil {
		t.Fatalf("ReassignUserItems: %v", err)
	}
	if res.RunsReassigned != 1 {
		t.Fatalf("only the active run's submitter should move, got %+v", res)
	}

	active, err := rs.GetRun(ctx, "pv-active")
	if err != nil {
		t.Fatalf("get active run: %v", err)
	}
	if active.SubmittedBy != "tgt" {
		t.Fatalf("active run submitter = %q, want tgt", active.SubmittedBy)
	}
	done, err := rs.GetRun(ctx, "pv-done")
	if err != nil {
		t.Fatalf("get terminal run: %v", err)
	}
	if done.SubmittedBy != "src" {
		t.Fatalf("terminal run submitter must be preserved, got %q", done.SubmittedBy)
	}
}

func TestReassignUserItems_Idempotent(t *testing.T) {
	pool := newTestDB(t)
	a := store.NewAssignments(pool)
	ctx := context.Background()

	assignmentSeed(t, a, "pv-1", 0, "src")
	assignmentSeed(t, a, "pv-1", 1, "src")

	first, err := a.ReassignUserItems(ctx, "src", "tgt", "admin", false)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	if first.AssignmentsReassigned != 2 {
		t.Fatalf("first run should move 2, got %+v", first)
	}
	histBefore := len(historyEvents(t, a, "pv-1", 0)) + len(historyEvents(t, a, "pv-1", 1))

	second, err := a.ReassignUserItems(ctx, "src", "tgt", "admin", false)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if second.Total() != 0 {
		t.Fatalf("re-run must be a no-op, got %+v", second)
	}
	histAfter := len(historyEvents(t, a, "pv-1", 0)) + len(historyEvents(t, a, "pv-1", 1))
	if histAfter != histBefore {
		t.Fatalf("re-run must not append history: before %d after %d", histBefore, histAfter)
	}
}

func TestReassignUserItems_DryRunCountsButDoesNotMutate(t *testing.T) {
	pool := newTestDB(t)
	a := store.NewAssignments(pool)
	rs := store.NewAssignmentStore(pool)
	ctx := context.Background()

	defID := seedDef(t, pool)
	assignmentSeed(t, a, "pv-1", 0, "src") // will move
	assignmentSeed(t, a, "pv-2", 0, "src") // will dedupe
	assignmentSeed(t, a, "pv-2", 0, "tgt") // target already here
	if err := rs.TrackRun(ctx, store.ApprovalRun{
		PolicyVersionID: "pv-3", RunID: "run-1", WorkflowDefID: defID,
		WorkflowVersion: 1, Status: "in_review", SubmittedBy: "src",
	}); err != nil {
		t.Fatalf("track run: %v", err)
	}

	res, err := a.ReassignUserItems(ctx, "src", "tgt", "admin", true)
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if res.AssignmentsReassigned != 1 || res.AssignmentsDeduped != 1 || res.RunsReassigned != 1 || res.Total() != 3 {
		t.Fatalf("dry-run counts wrong: %+v", res)
	}

	// Nothing changed: source still holds its seats and the run submitter.
	if _, err := a.GetAssignment(ctx, "pv-1", 0, "src", ""); err != nil {
		t.Fatalf("dry-run mutated pv-1: %v", err)
	}
	run, err := rs.GetRun(ctx, "pv-3")
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.SubmittedBy != "src" {
		t.Fatalf("dry-run must not move submitter, got %q", run.SubmittedBy)
	}
	for _, pv := range []string{"pv-1", "pv-2"} {
		if h := historyEvents(t, a, pv, 0); containsReassigned(h) {
			t.Fatalf("dry-run must not write history for %s", pv)
		}
	}
}

func TestReassignUserItems_Validation(t *testing.T) {
	pool := newTestDB(t)
	a := store.NewAssignments(pool)
	ctx := context.Background()
	if _, err := a.ReassignUserItems(ctx, "", "tgt", "admin", false); err == nil {
		t.Fatal("empty from must error")
	}
	if _, err := a.ReassignUserItems(ctx, "src", "src", "admin", false); err == nil {
		t.Fatal("from==to must error")
	}
}

func containsReassigned(h []store.HistoryEntry) bool {
	for _, e := range h {
		if e.Event == "reassigned" {
			return true
		}
	}
	return false
}
