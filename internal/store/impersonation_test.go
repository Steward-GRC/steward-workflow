// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package store_test

import (
	"context"
	"testing"

	grpcactor "github.com/Bugs5382/go-grpc-actor"

	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/store"
)

// Store-side half of act-as attribution. The hop itself is covered by the
// server tests, which drive the real server options across a gRPC
// connection; these tests assert what the SQL persists once the acting admin
// is on the context.

const (
	impTarget = "user-carol" // the impersonated account
	impAdmin  = "user-alice" // the real site admin
)

// impersonatedCtx is the context go-grpc-actor's server interceptor builds
// for an act-as call: the subject is the target, with the real admin as the
// impersonator.
func impersonatedCtx() context.Context {
	return grpcactor.WithActor(context.Background(), grpcactor.Actor{Subject: impTarget, Impersonator: impAdmin})
}

func TestAttribution_OrdinaryRequestIsUnchanged(t *testing.T) {
	actor, impersonated := store.Attribution(context.Background(), impTarget)
	if actor != impTarget {
		t.Errorf("actor = %q, want %q", actor, impTarget)
	}
	if impersonated != nil {
		t.Errorf("impersonated = %q, want nil", *impersonated)
	}
	if admin, ok := store.ActingAdmin(context.Background()); ok {
		t.Errorf("ActingAdmin on a plain context returned %q; a background/system context must never look impersonated", admin)
	}
}

func TestAttribution_ImpersonationNamesTheAdmin(t *testing.T) {
	actor, impersonated := store.Attribution(impersonatedCtx(), impTarget)
	if actor != impAdmin {
		t.Errorf("actor = %q, want the acting admin %q", actor, impAdmin)
	}
	if impersonated == nil || *impersonated != impTarget {
		t.Errorf("impersonated = %v, want %q", impersonated, impTarget)
	}
}

// TestAttribution_BlankActorRecordsNoTarget: attributing to the admin is still
// right, but there is no effective actor to preserve, and writing "" would be
// indistinguishable from a real user id of "".
func TestAttribution_BlankActorRecordsNoTarget(t *testing.T) {
	actor, impersonated := store.Attribution(impersonatedCtx(), "")
	if actor != impAdmin {
		t.Errorf("actor = %q, want %q", actor, impAdmin)
	}
	if impersonated != nil {
		t.Errorf("impersonated = %q, want nil", *impersonated)
	}
}

// TestAssignments_ImpersonatedDecisionRecordsAdmin drives the real SQL: an
// approval made under impersonation must land in assignment_history with the
// ADMIN as actor_user_id and the target preserved in impersonated_user_id,
// while the assignment row itself stays the target's.
func TestAssignments_ImpersonatedDecisionRecordsAdmin(t *testing.T) {
	pool := newTestDB(t)
	a := store.NewAssignments(pool)
	row := assignmentSeed(t, a, "pv-imp", 0, impTarget)

	if err := a.DecideAssignment(impersonatedCtx(), store.DecideAssignmentInput{
		AssignmentID: row.ID,
		Decision:     "approved",
		Comment:      "approved while impersonating",
		ActorUserID:  impTarget, // effective subject, as the handler resolved it
	}); err != nil {
		t.Fatalf("DecideAssignment: %v", err)
	}

	hist, err := a.ListHistory(context.Background(), "pv-imp", 0)
	if err != nil {
		t.Fatalf("ListHistory: %v", err)
	}
	// [0] is the 'created' seed (not impersonated), [1] the decision.
	if len(hist) != 2 {
		t.Fatalf("expected 2 history entries, got %d: %+v", len(hist), hist)
	}
	if hist[0].ImpersonatedUserID != "" {
		t.Errorf("seed 'created' row impersonated_user_id = %q, want empty", hist[0].ImpersonatedUserID)
	}
	decision := hist[1]
	if decision.Event != "approved" {
		t.Fatalf("history[1].Event = %q, want approved", decision.Event)
	}
	if decision.ActorUserID != impAdmin {
		t.Errorf("actor_user_id = %q, want the acting admin %q — an approval made under "+
			"impersonation must not be recorded as the impersonated user",
			decision.ActorUserID, impAdmin)
	}
	if decision.ImpersonatedUserID != impTarget {
		t.Errorf("impersonated_user_id = %q, want %q", decision.ImpersonatedUserID, impTarget)
	}
	// The decision still belongs to the target's seat.
	got, err := a.GetAssignment(context.Background(), "pv-imp", 0, impTarget, "")
	if err != nil {
		t.Fatalf("GetAssignment: %v", err)
	}
	if got.State != "approved" {
		t.Errorf("assignment state = %q, want approved", got.State)
	}
}

// TestAssignments_OrdinaryDecisionRecordsNoImpersonation is the control: the
// same call without an impersonator must write NULL, not the actor again.
func TestAssignments_OrdinaryDecisionRecordsNoImpersonation(t *testing.T) {
	pool := newTestDB(t)
	a := store.NewAssignments(pool)
	row := assignmentSeed(t, a, "pv-plain", 0, impTarget)

	if err := a.DecideAssignment(context.Background(), store.DecideAssignmentInput{
		AssignmentID: row.ID, Decision: "approved", Comment: "ok", ActorUserID: impTarget,
	}); err != nil {
		t.Fatalf("DecideAssignment: %v", err)
	}
	hist, err := a.ListHistory(context.Background(), "pv-plain", 0)
	if err != nil {
		t.Fatalf("ListHistory: %v", err)
	}
	decision := hist[len(hist)-1]
	if decision.ActorUserID != impTarget {
		t.Errorf("actor_user_id = %q, want %q", decision.ActorUserID, impTarget)
	}
	if decision.ImpersonatedUserID != "" {
		t.Errorf("impersonated_user_id = %q, want empty for a non-impersonated decision", decision.ImpersonatedUserID)
	}
}

// TestAssignmentStore_ImpersonatedSubmitRecordsActor drives the approval_runs
// half: submitted_by keeps the TARGET (it addresses the workflow-started /
// workflow-denied emails and is what an account merge remaps) while
// submitted_by_actor_id records the real admin.
func TestAssignmentStore_ImpersonatedSubmitRecordsActor(t *testing.T) {
	pool := newTestDB(t)
	ds := store.NewWorkflowDefStore(pool)
	as := store.NewAssignmentStore(pool)

	defID, err := ds.Create(context.Background(), builder.WorkflowDef{
		Name: "Impersonation WF",
		Stages: []builder.Stage{
			{Name: "s", Quorum: builder.QuorumAny, ApproverIDs: []string{"user-carol"}},
		},
	})
	if err != nil {
		t.Fatalf("Create workflow def fixture: %v", err)
	}

	if err := as.TrackRun(impersonatedCtx(), store.ApprovalRun{
		PolicyVersionID: "pv-imp-run",
		RunID:           "run-imp",
		WorkflowDefID:   defID,
		WorkflowVersion: 1,
		SubmittedBy:     impTarget,
	}); err != nil {
		t.Fatalf("TrackRun: %v", err)
	}

	got, err := as.GetRun(context.Background(), "pv-imp-run")
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if got.SubmittedBy != impTarget {
		t.Errorf("submitted_by = %q, want the target %q — it addresses the run's "+
			"notifications and is the account-merge key, so it must stay the target",
			got.SubmittedBy, impTarget)
	}
	if got.SubmittedByActorID != impAdmin {
		t.Errorf("submitted_by_actor_id = %q, want the acting admin %q", got.SubmittedByActorID, impAdmin)
	}

	// Control: an ordinary submit records no acting admin.
	if err := as.TrackRun(context.Background(), store.ApprovalRun{
		PolicyVersionID: "pv-plain-run",
		RunID:           "run-plain",
		WorkflowDefID:   defID,
		WorkflowVersion: 1,
		SubmittedBy:     impTarget,
	}); err != nil {
		t.Fatalf("TrackRun (plain): %v", err)
	}
	plain, err := as.GetRun(context.Background(), "pv-plain-run")
	if err != nil {
		t.Fatalf("GetRun (plain): %v", err)
	}
	if plain.SubmittedByActorID != "" {
		t.Errorf("submitted_by_actor_id = %q, want empty for a non-impersonated submit", plain.SubmittedByActorID)
	}
}
