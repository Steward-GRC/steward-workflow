// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package saga

import (
	"context"
	"maps"
	"testing"
	"time"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/google/uuid"

	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/store"
)

// fakeSagaRun builds a domain.SagaRun with the given run id + inputs. The id is
// hashed into a deterministic UUID so GetRunByRunID sees a stable string.
func fakeSagaRun(runID string, inputs map[string]any) domain.SagaRun {
	return domain.SagaRun{
		ID:     uuid.NewSHA1(uuid.Nil, []byte(runID)),
		Inputs: inputs,
	}
}

// recordingCreator records every CreateAssignment call so the test can assert
// which (pvID, stage, user) rows were created and how many times.
type recordingCreator struct {
	calls []store.AssignmentRow
}

func (r *recordingCreator) CreateAssignment(_ context.Context, row store.AssignmentRow, _ string) (store.AssignmentRow, error) {
	r.calls = append(r.calls, row)
	return row, nil
}

func stageAssignerFor(pool []string, creator assignmentCreator) *StageAssigner {
	wd := builder.WorkflowDef{
		ID: "def-1", Version: 1,
		Stages: []builder.Stage{
			{Name: "s0", Quorum: builder.QuorumAll, ApproverIDs: []string{"a"}},
			{Name: "s1", Quorum: builder.QuorumAll, ApproverIDs: []string{"b"}, SLADays: 3},
		},
	}
	return NewStageAssigner(
		fakeDefs{wd: wd},
		creator,
		builder.CompileOptions{StageSLAHours: 72, StageReminderHours: 48},
	)
}

// baseInputs carries the run inputs every AssignStage call needs to resolve the
// policy version + pinned def (Submit puts these in the run inputs).
func baseInputs(extra map[string]any) map[string]any {
	m := map[string]any{
		"policy_version_id": "pv-1",
		"workflow_def_id":   "def-1",
		"workflow_version":  1,
		"submitted_by":      "boss",
	}
	maps.Copy(m, extra)
	return m
}

// TestStageAssigner_CreatesStageAssignments asserts that entering stage 1 with a
// pre-resolved pool of two users creates two approval_assignments rows for that
// stage.
func TestStageAssigner_CreatesStageAssignments(t *testing.T) {
	creator := &recordingCreator{}
	assigner := stageAssignerFor(nil, creator)

	run := fakeSagaRun("run-1", baseInputs(map[string]any{
		// JSON round-trips a []string as []any, so mirror that here.
		"approvers_s1": []any{"u1", "u2"},
	}))

	if err := assigner.AssignStage(context.Background(), run, 1); err != nil {
		t.Fatalf("AssignStage: %v", err)
	}

	if len(creator.calls) != 2 {
		t.Fatalf("want 2 assignment creations, got %d: %+v", len(creator.calls), creator.calls)
	}
	got := map[string]bool{}
	for _, c := range creator.calls {
		if c.PolicyVersionID != "pv-1" {
			t.Fatalf("pvID: want pv-1, got %q", c.PolicyVersionID)
		}
		if c.StageIndex != 1 {
			t.Fatalf("stage: want 1, got %d", c.StageIndex)
		}
		got[c.UserID] = true
		// stage 1 has SLADays=3, so reminder == deadline and deadline ~= now+72h.
		if !c.ReminderAt.Equal(c.SLADeadlineAt) {
			t.Fatalf("SLADays override: reminder should equal deadline, got %v vs %v", c.ReminderAt, c.SLADeadlineAt)
		}
		if d := c.SLADeadlineAt.Sub(c.AssignedAt); d < 71*time.Hour || d > 73*time.Hour {
			t.Fatalf("SLADays=3 → deadline ~72h from assign, got %v", d)
		}
	}
	if !got["u1"] || !got["u2"] {
		t.Fatalf("want assignments for u1 and u2, got %v", got)
	}
}

// TestStageAssigner_Idempotent asserts a re-run (pending loop / retry) creates
// rows again harmlessly — CreateAssignment is an UPSERT, so a second pass is a
// no-op at the store; the action must not error.
func TestStageAssigner_Idempotent(t *testing.T) {
	creator := &recordingCreator{}
	assigner := stageAssignerFor(nil, creator)
	run := fakeSagaRun("run-1", baseInputs(map[string]any{"approvers_s0": []any{"u1"}}))

	if err := assigner.AssignStage(context.Background(), run, 0); err != nil {
		t.Fatalf("first AssignStage: %v", err)
	}
	if err := assigner.AssignStage(context.Background(), run, 0); err != nil {
		t.Fatalf("second AssignStage (idempotent re-run): %v", err)
	}
	// Two passes → two CreateAssignment calls (each an UPSERT on the same key).
	if len(creator.calls) != 2 {
		t.Fatalf("want 2 create calls across 2 passes, got %d", len(creator.calls))
	}
}

// TestStageAssigner_EmptyPoolNoStall asserts an empty pool does not error (the
// run sits pending, which is diagnosable via the warn log) and creates no rows.
func TestStageAssigner_EmptyPoolNoStall(t *testing.T) {
	creator := &recordingCreator{}
	assigner := stageAssignerFor(nil, creator)
	run := fakeSagaRun("run-1", baseInputs(map[string]any{"approvers_s1": []any{}}))

	if err := assigner.AssignStage(context.Background(), run, 1); err != nil {
		t.Fatalf("empty pool must not error, got: %v", err)
	}
	if len(creator.calls) != 0 {
		t.Fatalf("empty pool → no creations, got %d", len(creator.calls))
	}
}

// recordingNotifier captures every ApprovalRequestedEvent emitted so a test can
// assert an approver was notified when their assignment row is created.
type recordingNotifier struct {
	events []ApprovalRequestedEvent
	err    error
}

func (n *recordingNotifier) NotifyApprovalRequested(_ context.Context, e ApprovalRequestedEvent) error {
	n.events = append(n.events, e)
	return n.err
}

// TestStageAssigner_EmitsApprovalRequested asserts that assigning stage 0 (the
// step the engine runs synchronously inside Submit's StartRun — i.e.
// "submit-for-approval") emits exactly one workflow.approval_requested event
// for the single stage-0 approver, carrying the ids obligations needs to
// resolve the recipient and dedup on (task_id, approver).
func TestStageAssigner_EmitsApprovalRequested(t *testing.T) {
	creator := &recordingCreator{}
	notifier := &recordingNotifier{}
	assigner := stageAssignerFor(nil, creator).WithApprovalNotifier(notifier)

	run := fakeSagaRun("run-1", baseInputs(map[string]any{
		"policy_id":    "pol-9",
		"approvers_s0": []any{"u1"},
	}))

	if err := assigner.AssignStage(context.Background(), run, 0); err != nil {
		t.Fatalf("AssignStage: %v", err)
	}

	if len(notifier.events) != 1 {
		t.Fatalf("want exactly 1 approval-requested event, got %d: %+v", len(notifier.events), notifier.events)
	}
	e := notifier.events[0]
	if e.EventType != "workflow.approval_requested" {
		t.Errorf("event_type: want workflow.approval_requested, got %q", e.EventType)
	}
	if e.ApproverUserID != "u1" {
		t.Errorf("approver_user_id: want u1, got %q", e.ApproverUserID)
	}
	if e.TaskID != "pv-1:0" {
		t.Errorf("task_id: want pv-1:0, got %q", e.TaskID)
	}
	if e.PolicyVersionID != "pv-1" || e.PolicyID != "pol-9" {
		t.Errorf("policy ids: want pv-1/pol-9, got %q/%q", e.PolicyVersionID, e.PolicyID)
	}
	if e.RequestedByUserID != "boss" {
		t.Errorf("requested_by_user_id: want boss, got %q", e.RequestedByUserID)
	}
	if e.StageIndex != 0 {
		t.Errorf("stage_index: want 0, got %d", e.StageIndex)
	}
}

// TestStageAssigner_EmitsPerApprover asserts one event per approver in the pool.
func TestStageAssigner_EmitsPerApprover(t *testing.T) {
	creator := &recordingCreator{}
	notifier := &recordingNotifier{}
	assigner := stageAssignerFor(nil, creator).WithApprovalNotifier(notifier)

	run := fakeSagaRun("run-1", baseInputs(map[string]any{"approvers_s1": []any{"u1", "u2"}}))
	if err := assigner.AssignStage(context.Background(), run, 1); err != nil {
		t.Fatalf("AssignStage: %v", err)
	}
	if len(notifier.events) != 2 {
		t.Fatalf("want 2 events (one per approver), got %d", len(notifier.events))
	}
	// stage 1 has SLADays=3, so the event carries a non-empty RFC3339 due_by.
	if notifier.events[0].DueBy == "" {
		t.Errorf("want a due_by for an SLA-bearing stage, got empty")
	}
}

// TestStageAssigner_EmitFailureDoesNotFailAssign asserts a publish error is
// swallowed: the assign step must still succeed (the run is authoritative; the
// notification is best-effort).
func TestStageAssigner_EmitFailureDoesNotFailAssign(t *testing.T) {
	creator := &recordingCreator{}
	notifier := &recordingNotifier{err: context.DeadlineExceeded}
	assigner := stageAssignerFor(nil, creator).WithApprovalNotifier(notifier)

	run := fakeSagaRun("run-1", baseInputs(map[string]any{"approvers_s0": []any{"u1"}}))
	if err := assigner.AssignStage(context.Background(), run, 0); err != nil {
		t.Fatalf("emit failure must not fail assign, got: %v", err)
	}
	if len(creator.calls) != 1 {
		t.Fatalf("assignment must still be created, got %d", len(creator.calls))
	}
}

// userIDs returns the UserID of each captured AssignmentRow in order.
func (r *recordingCreator) userIDs() []string {
	out := make([]string, len(r.calls))
	for i, c := range r.calls {
		out[i] = c.UserID
	}
	return out
}

// fakeFilter satisfies the assigner's approverFilter: it drops any candidate in
// `deny` and returns `owners` as the backstop pool.
type fakeFilter struct {
	deny   map[string]bool
	owners []string
}

func (f fakeFilter) Eligible(_ context.Context, _ string, candidates []string) ([]string, []string, error) {
	var elig []string
	for _, u := range candidates {
		if !f.deny[u] {
			elig = append(elig, u)
		}
	}
	return elig, f.owners, nil
}

func TestStageAssigner_SkipsDeniedApprover(t *testing.T) {
	// Stage pool [a,b], quorum all (equivalent to any for MeetsQuorum non-nofm); b is denied → only a is assigned (quorum still meetable).
	rec := &recordingCreator{}
	sa := stageAssignerFor(nil, rec).WithAccessFilter(fakeFilter{deny: map[string]bool{"b": true}, owners: []string{"owner"}})
	run := fakeSagaRun("run-filter-1", baseInputs(map[string]any{"approvers_s0": []string{"a", "b"}}))
	if err := sa.AssignStage(context.Background(), run, 0); err != nil {
		t.Fatal(err)
	}
	got := rec.userIDs()
	if len(got) != 1 || got[0] != "a" {
		t.Fatalf("assigned = %v, want [a] (b skipped)", got)
	}
}

func TestStageAssigner_OwnerBackstopWhenAllSkipped(t *testing.T) {
	// Both approvers denied, quorum all (equivalent to any for MeetsQuorum non-nofm) → owner backstops the stage.
	rec := &recordingCreator{}
	sa := stageAssignerFor(nil, rec).WithAccessFilter(fakeFilter{deny: map[string]bool{"a": true, "b": true}, owners: []string{"owner"}})
	run := fakeSagaRun("run-filter-2", baseInputs(map[string]any{"approvers_s0": []string{"a", "b"}}))
	if err := sa.AssignStage(context.Background(), run, 0); err != nil {
		t.Fatal(err)
	}
	got := rec.userIDs()
	if len(got) != 1 || got[0] != "owner" {
		t.Fatalf("assigned = %v, want [owner] (backstop)", got)
	}
}

// stageAssignerNofM builds a StageAssigner whose stage 0 uses QuorumNofM/N=2
// with the given pool, so we can exercise the nofm backstop path without
// touching stageAssignerFor (which hard-codes QuorumAll).
func stageAssignerNofM(pool []string, creator assignmentCreator) *StageAssigner {
	wd := builder.WorkflowDef{
		ID: "def-nofm", Version: 1,
		Stages: []builder.Stage{
			{Name: "s0", Quorum: builder.QuorumNofM, QuorumN: 2, ApproverIDs: pool},
		},
	}
	return NewStageAssigner(
		fakeDefs{wd: wd},
		creator,
		builder.CompileOptions{StageSLAHours: 72, StageReminderHours: 48},
	)
}

// TestStageAssigner_NofmBackstop: nofm N=2, pool [a,b,c], filter denies b+c
// → eligible=[a], MeetsQuorum("nofm",2,1)=false → backstop fires → [a,owner].
func TestStageAssigner_NofmBackstop(t *testing.T) {
	rec := &recordingCreator{}
	sa := stageAssignerNofM([]string{"a", "b", "c"}, rec).
		WithAccessFilter(fakeFilter{deny: map[string]bool{"b": true, "c": true}, owners: []string{"owner"}})
	run := fakeSagaRun("run-nofm-backstop", baseInputs(map[string]any{
		"workflow_def_id": "def-nofm",
		"approvers_s0":    []string{"a", "b", "c"},
	}))
	if err := sa.AssignStage(context.Background(), run, 0); err != nil {
		t.Fatal(err)
	}
	got := rec.userIDs()
	want := map[string]bool{"a": true, "owner": true}
	if len(got) != 2 {
		t.Fatalf("assigned = %v, want [a owner]", got)
	}
	for _, u := range got {
		if !want[u] {
			t.Fatalf("unexpected user %q in assigned %v", u, got)
		}
	}
}

// TestStageAssigner_OwnerDedup: nofm N=2, pool [a,b], filter denies b → eligible=[a],
// backstop fires, owners=[a,c] → union must not duplicate a → assigned=[a,c].
func TestStageAssigner_OwnerDedup(t *testing.T) {
	rec := &recordingCreator{}
	sa := stageAssignerNofM([]string{"a", "b"}, rec).
		WithAccessFilter(fakeFilter{deny: map[string]bool{"b": true}, owners: []string{"a", "c"}})
	run := fakeSagaRun("run-owner-dedup", baseInputs(map[string]any{
		"workflow_def_id": "def-nofm",
		"approvers_s0":    []string{"a", "b"},
	}))
	if err := sa.AssignStage(context.Background(), run, 0); err != nil {
		t.Fatal(err)
	}
	got := rec.userIDs()
	want := map[string]bool{"a": true, "c": true}
	if len(got) != 2 {
		t.Fatalf("assigned = %v, want [a c] (no duplicate a)", got)
	}
	for _, u := range got {
		if !want[u] {
			t.Fatalf("unexpected user %q in assigned %v", u, got)
		}
	}
}

// TestStageAssigner_NoEligibleNoOwners: pool [a], filter denies a, owners [] →
// after filter+backstop the pool is empty → no CreateAssignment calls, nil error.
func TestStageAssigner_NoEligibleNoOwners(t *testing.T) {
	rec := &recordingCreator{}
	sa := stageAssignerFor(nil, rec).
		WithAccessFilter(fakeFilter{deny: map[string]bool{"a": true}, owners: []string{}})
	run := fakeSagaRun("run-no-eligible", baseInputs(map[string]any{
		"approvers_s0": []string{"a"},
	}))
	if err := sa.AssignStage(context.Background(), run, 0); err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	if ids := rec.userIDs(); len(ids) != 0 {
		t.Fatalf("expected no assignments, got: %v", ids)
	}
}
