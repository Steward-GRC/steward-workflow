// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/Steward-GRC/steward-workflow/internal/store"
)

// assignmentSeed creates a pending assignment, returning the row written.
func assignmentSeed(t *testing.T, a *store.Assignments, pv string, stage int, user string) store.AssignmentRow {
	t.Helper()
	now := time.Now().UTC()
	row, err := a.CreateAssignment(context.Background(), store.AssignmentRow{
		PolicyVersionID: pv,
		StageIndex:      stage,
		UserID:          user,
		SLADeadlineAt:   now.Add(72 * time.Hour),
		ReminderAt:      now.Add(48 * time.Hour),
	}, "system")
	if err != nil {
		t.Fatalf("CreateAssignment: %v", err)
	}
	return row
}

func TestAssignments_CreateAndGet(t *testing.T) {
	pool := newTestDB(t)
	a := store.NewAssignments(pool)
	row := assignmentSeed(t, a, "pv-1", 0, "u1")
	if row.ID == "" {
		t.Fatal("expected id")
	}
	got, err := a.GetAssignment(context.Background(), "pv-1", 0, "u1", "")
	if err != nil {
		t.Fatalf("GetAssignment: %v", err)
	}
	if got.State != "pending" {
		t.Fatalf("state %q", got.State)
	}
	hist, err := a.ListHistory(context.Background(), "pv-1", 0)
	if err != nil {
		t.Fatalf("ListHistory: %v", err)
	}
	if len(hist) != 1 || hist[0].Event != "created" {
		t.Fatalf("history: %+v", hist)
	}
}

func TestAssignments_DecideAndSupersede(t *testing.T) {
	pool := newTestDB(t)
	a := store.NewAssignments(pool)
	r1 := assignmentSeed(t, a, "pv-2", 0, "u1")
	r2 := assignmentSeed(t, a, "pv-2", 0, "u2")

	// u1 rejects.
	if err := a.DecideAssignment(context.Background(), store.DecideAssignmentInput{
		AssignmentID: r1.ID, Decision: "rejected", Comment: "no good", ActorUserID: "u1",
	}); err != nil {
		t.Fatalf("DecideAssignment: %v", err)
	}
	// supersede peers.
	affected, err := a.SupersedePendingPeers(context.Background(), "pv-2", 0, r1.ID, "u1")
	if err != nil {
		t.Fatalf("SupersedePendingPeers: %v", err)
	}
	if affected != 1 {
		t.Fatalf("expected 1 superseded, got %d", affected)
	}
	got2, err := a.GetAssignment(context.Background(), "pv-2", 0, "u2", "")
	if err != nil {
		t.Fatalf("GetAssignment u2: %v", err)
	}
	if got2.State != "superseded" {
		t.Fatalf("u2 state %q", got2.State)
	}
	// r2 had no second decide attempt.
	_ = r2

	// History includes 1 created + 1 rejected for u1, 1 created + 1 superseded(rejected) for u2.
	hist, err := a.ListHistory(context.Background(), "pv-2", 0)
	if err != nil {
		t.Fatalf("ListHistory: %v", err)
	}
	if len(hist) != 4 {
		t.Fatalf("expected 4 history entries, got %d: %+v", len(hist), hist)
	}
}

func TestAssignments_DecideRejectsConcurrent(t *testing.T) {
	pool := newTestDB(t)
	a := store.NewAssignments(pool)
	row := assignmentSeed(t, a, "pv-3", 0, "u1")

	if err := a.DecideAssignment(context.Background(), store.DecideAssignmentInput{
		AssignmentID: row.ID, Decision: "approved", Comment: "lgtm", ActorUserID: "u1",
	}); err != nil {
		t.Fatalf("first decide: %v", err)
	}
	if err := a.DecideAssignment(context.Background(), store.DecideAssignmentInput{
		AssignmentID: row.ID, Decision: "approved", Comment: "again", ActorUserID: "u1",
	}); err == nil {
		t.Fatal("expected second decide to fail")
	}
}

func TestAssignments_DecideRequiresComment(t *testing.T) {
	pool := newTestDB(t)
	a := store.NewAssignments(pool)
	row := assignmentSeed(t, a, "pv-4", 0, "u1")
	if err := a.DecideAssignment(context.Background(), store.DecideAssignmentInput{
		AssignmentID: row.ID, Decision: "approved", Comment: "", ActorUserID: "u1",
	}); err == nil {
		t.Fatal("expected error for empty comment")
	}
}

func TestAssignments_Swap(t *testing.T) {
	pool := newTestDB(t)
	a := store.NewAssignments(pool)
	row := assignmentSeed(t, a, "pv-5", 0, "u1")

	now := time.Now().UTC()
	newRow, err := a.Swap(context.Background(), store.SwapInput{
		OldAssignmentID:  row.ID,
		NewUserID:        "u2",
		NewSLADeadlineAt: now.Add(96 * time.Hour),
		NewReminderAt:    now.Add(72 * time.Hour),
		ActorUserID:      "admin1",
		ActorRole:        "admin",
		Reason:           "u1 on PTO",
	})
	if err != nil {
		t.Fatalf("Swap: %v", err)
	}
	if newRow.UserID != "u2" {
		t.Fatalf("new user id: %q", newRow.UserID)
	}
	old, err := a.GetAssignment(context.Background(), "pv-5", 0, "u1", "")
	if err != nil {
		t.Fatalf("GetAssignment u1: %v", err)
	}
	if old.State != "swapped_out" {
		t.Fatalf("old state %q", old.State)
	}
	hist, err := a.ListHistory(context.Background(), "pv-5", 0)
	if err != nil {
		t.Fatalf("ListHistory: %v", err)
	}
	// created + swapped_out + swapped_in = 3
	if len(hist) != 3 {
		t.Fatalf("expected 3 history entries, got %d: %+v", len(hist), hist)
	}
}

func TestAssignments_PauseAndResume(t *testing.T) {
	pool := newTestDB(t)
	a := store.NewAssignments(pool)
	row := assignmentSeed(t, a, "pv-6", 0, "u1")

	pauseAt := time.Now().UTC()
	ids, err := a.PauseAllPending(context.Background(), pauseAt, "reconciler", "identity_svc_unreachable")
	if err != nil {
		t.Fatalf("PauseAllPending: %v", err)
	}
	if len(ids) != 1 || ids[0] != row.ID {
		t.Fatalf("expected to pause %q, got %v", row.ID, ids)
	}
	paused, err := a.GetAssignment(context.Background(), "pv-6", 0, "u1", "")
	if err != nil {
		t.Fatalf("GetAssignment: %v", err)
	}
	if paused.State != "paused" {
		t.Fatalf("state %q", paused.State)
	}
	if paused.PausedRemainingSLASec == nil {
		t.Fatal("expected paused_remaining_sla_seconds set")
	}

	resumeAt := pauseAt.Add(5 * time.Minute)
	resumed, err := a.ResumeAllPaused(context.Background(), resumeAt, "reconciler")
	if err != nil {
		t.Fatalf("ResumeAllPaused: %v", err)
	}
	if len(resumed) != 1 || resumed[0] != row.ID {
		t.Fatalf("expected to resume %q, got %v", row.ID, resumed)
	}
	post, err := a.GetAssignment(context.Background(), "pv-6", 0, "u1", "")
	if err != nil {
		t.Fatalf("GetAssignment post-resume: %v", err)
	}
	if post.State != "pending" {
		t.Fatalf("state %q", post.State)
	}
	if post.PausedRemainingSLASec != nil {
		t.Fatal("expected paused_remaining_sla_seconds cleared")
	}
}

func TestAssignments_UpsertRunStateAndSetAll(t *testing.T) {
	pool := newTestDB(t)
	a := store.NewAssignments(pool)
	ctx := context.Background()

	if err := a.UpsertRunState(ctx, store.RunStateRow{PolicyVersionID: "pv-7", State: "running"}); err != nil {
		t.Fatalf("UpsertRunState: %v", err)
	}
	if err := a.SetAllRunsState(ctx, "paused_external_dep", "identity_svc_unreachable", time.Now().UTC()); err != nil {
		t.Fatalf("SetAllRunsState: %v", err)
	}
	n, err := a.CountPausedRuns(ctx)
	if err != nil {
		t.Fatalf("CountPausedRuns: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 paused run, got %d", n)
	}
	if err := a.SetAllRunsState(ctx, "running", "", time.Now().UTC()); err != nil {
		t.Fatalf("SetAllRunsState resume: %v", err)
	}
	n, _ = a.CountPausedRuns(ctx)
	if n != 0 {
		t.Fatalf("expected 0 paused runs after resume, got %d", n)
	}
}

func TestAssignments_TerminatePendingAssignments(t *testing.T) {
	pool := newTestDB(t)
	a := store.NewAssignments(pool)
	ctx := context.Background()

	// Two pending across stages + one already-approved that must be left alone.
	assignmentSeed(t, a, "pv-w", 0, "u1")
	assignmentSeed(t, a, "pv-w", 1, "u2")
	approved := assignmentSeed(t, a, "pv-w", 0, "u3")
	if err := a.DecideAssignment(ctx, store.DecideAssignmentInput{
		AssignmentID: approved.ID, Decision: "approved", Comment: "ok", ActorUserID: "u3",
	}); err != nil {
		t.Fatalf("DecideAssignment: %v", err)
	}

	n, err := a.TerminatePendingAssignments(ctx, "pv-w", "withdrawn", "owner", "policy_withdrawn")
	if err != nil {
		t.Fatalf("TerminatePendingAssignments: %v", err)
	}
	if n != 2 {
		t.Fatalf("terminated = %d, want 2", n)
	}

	// Both pending rows are now withdrawn; the approved row is untouched.
	for _, tc := range []struct {
		stage int
		user  string
		want  string
	}{
		{0, "u1", "withdrawn"},
		{1, "u2", "withdrawn"},
		{0, "u3", "approved"},
	} {
		got, err := a.GetAssignment(ctx, "pv-w", tc.stage, tc.user, "")
		if err != nil {
			t.Fatalf("GetAssignment %s: %v", tc.user, err)
		}
		if got.State != tc.want {
			t.Fatalf("%s state = %q, want %q", tc.user, got.State, tc.want)
		}
	}

	// A 'withdrawn' history event was written for each terminated row.
	hist, err := a.ListHistory(ctx, "pv-w", 0)
	if err != nil {
		t.Fatalf("ListHistory: %v", err)
	}
	var withdrawn int
	for _, h := range hist {
		if h.Event == "withdrawn" {
			withdrawn++
		}
	}
	if withdrawn != 1 {
		t.Fatalf("expected 1 withdrawn history event at stage 0, got %d (%+v)", withdrawn, hist)
	}
}

// TestAssignments_HasPendingAssignment encodes the inbox-filter primitive: it is
// true only for an exact (policy_version, stage, user) pending row, and false
// once that row leaves the pending state or for any non-matching tuple.
func TestAssignments_HasPendingAssignment(t *testing.T) {
	pool := newTestDB(t)
	a := store.NewAssignments(pool)
	ctx := context.Background()

	assignmentSeed(t, a, "pv-h", 0, "owner")
	other := assignmentSeed(t, a, "pv-h", 0, "decided")
	if err := a.DecideAssignment(ctx, store.DecideAssignmentInput{
		AssignmentID: other.ID, Decision: "approved", Comment: "ok", ActorUserID: "decided",
	}); err != nil {
		t.Fatalf("DecideAssignment: %v", err)
	}

	cases := []struct {
		name  string
		pv    string
		stage int
		user  string
		want  bool
	}{
		{"pending owner", "pv-h", 0, "owner", true},
		{"decided peer not pending", "pv-h", 0, "decided", false},
		{"wrong stage", "pv-h", 1, "owner", false},
		{"wrong user", "pv-h", 0, "stranger", false},
		{"wrong version", "pv-other", 0, "owner", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := a.HasPendingAssignment(ctx, tc.pv, tc.stage, tc.user)
			if err != nil {
				t.Fatalf("HasPendingAssignment: %v", err)
			}
			if got != tc.want {
				t.Fatalf("HasPendingAssignment(%s,%d,%s) = %v, want %v", tc.pv, tc.stage, tc.user, got, tc.want)
			}
		})
	}
}
