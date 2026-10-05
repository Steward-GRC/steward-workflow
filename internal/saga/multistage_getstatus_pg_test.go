// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package saga_test

// Real-Postgres regression test for: GetStatus's CurrentStageIdx
// must advance as a run moves through a THREE-stage approval, not just report
// whether each stage's approval_assignments rows exist (multistage_pg_test.go
// already covers that half). Before the fix (see internal/grpcsvc/workflow.go
// GetStatus), CurrentStageIdx was never set, so it defaulted to 0 and the
// /approvals stepper stayed pinned on "stage 1" even after later stages were
// entered and approved. This drives a real 3-stage run end-to-end through
// Submit → three Signal(APPROVE) calls, calling the actual GetStatus RPC after
// each transition and asserting CurrentStageIdx tracks the pending stage
// (0 → 1 → 2 → len(stages) once terminal).

import (
	"context"
	log "github.com/Bugs5382/go-log"
	"testing"
	"time"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	sagapg "github.com/Bugs5382/go-saga-orchestration/store/postgres"

	workflowv1 "github.com/Steward-GRC/steward-workflow/gen/go/steward/workflow/v1"
	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/grpcsvc"
	sagaclient "github.com/Steward-GRC/steward-workflow/internal/saga"
	"github.com/Steward-GRC/steward-workflow/internal/store"
)

// newThreeStageHarness wires the real stack over a THREE-stage ALL-quorum def
// with one distinct approver per stage (u0 → stage 0, u1 → stage 1, u2 → stage 2).
func newThreeStageHarness(t *testing.T) *lifecycleHarness {
	t.Helper()
	ctx := context.Background()
	dsn, pool := newSagaTestDB(t)

	sagaStore, err := sagapg.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open saga store: %v", err)
	}
	t.Cleanup(sagaStore.Close)

	defStore := store.NewWorkflowDefStore(pool)
	assignStore := store.NewAssignmentStore(pool)
	assignments := store.NewAssignments(pool)

	wd := builder.WorkflowDef{
		Name: "ThreeStage", Version: 1, Published: true,
		Stages: []builder.Stage{
			{Name: "S0", Quorum: builder.QuorumAll, ApproverIDs: []string{"u0"}},
			{Name: "S1", Quorum: builder.QuorumAll, ApproverIDs: []string{"u1"}},
			{Name: "S2", Quorum: builder.QuorumAll, ApproverIDs: []string{"u2"}},
		},
	}
	defID, err := defStore.Create(ctx, wd)
	if err != nil {
		t.Fatalf("create def: %v", err)
	}

	resolver := sagaclient.NewDecisionResolver(assignStore, defStore, assignments)
	assigner := sagaclient.NewStageAssigner(defStore, assignments, builder.CompileOptions{StageSLAHours: 72, StageReminderHours: 48})
	policy := &recordingPolicyClient{decision: "approve"}
	engine, err := sagaclient.Build(sagaStore, policy, resolver, assigner, log.Nop())
	if err != nil {
		t.Fatalf("build engine: %v", err)
	}
	sagaCli := sagaclient.NewEmbedded(engine)

	srv := grpcsvc.NewWorkflowServer(grpcsvc.WorkflowServerOpts{
		Resolver:    fixedResolver{defID: defID},
		DefStore:    defStore,
		RunStore:    assignStore,
		SagaClient:  sagaCli,
		AuditEmit:   noopAudit{},
		Assignments: assignments,
		CoreStatus:  &recordingCoreStatus{},
		CompileOpts: builder.CompileOptions{StageSLAHours: 72, StageReminderHours: 48},
	})

	return &lifecycleHarness{
		pool: pool, saga: sagaCli, srv: srv, defID: defID,
		assignments: assignments, policy: policy,
	}
}

// getStatus is a small helper around the WorkflowServer's GetStatus RPC.
func (h *lifecycleHarness) getStatus(t *testing.T, pv string) *workflowv1.GetStatusResponse {
	t.Helper()
	resp, err := h.srv.GetStatus(context.Background(), &workflowv1.GetStatusRequest{PolicyVersionId: pv})
	if err != nil {
		t.Fatalf("GetStatus(%s): %v", pv, err)
	}
	return resp
}

// approveStage signals APPROVE for the given stage's awaited task and actor.
func approveStage(t *testing.T, h *lifecycleHarness, runID, step, actor string) {
	t.Helper()
	taskID := h.awaitedTaskID(t, runID, step)
	if _, err := h.srv.Signal(context.Background(), &workflowv1.SignalRequest{
		RunId: runID, TaskId: taskID, Signal: workflowv1.SignalType_SIGNAL_TYPE_APPROVE,
		ActorUserId: actor, Comment: "ok",
	}); err != nil {
		t.Fatalf("approve %s as %s: %v", step, actor, err)
	}
}

// TestMultiStage_GetStatusCurrentStageIdxAdvances is the regression:
// it exercises a THREE-stage run end-to-end (not just two, so it also covers
// the middle-stage transition) and asserts the actual GetStatus RPC response —
// what the /approvals stepper renders from — advances CurrentStageIdx at every
// stage boundary instead of staying pinned on stage 0 ("stage 1" in the UI).
func TestMultiStage_GetStatusCurrentStageIdxAdvances(t *testing.T) {
	h := newThreeStageHarness(t)
	const pv = "pv-3stage"
	ctx := context.Background()

	if _, err := h.srv.Submit(ctx, &workflowv1.SubmitRequest{
		PolicyVersionId: pv, PolicyId: "pol-1", CategoryId: "g1",
		SubmittedBy: "submitter", AncestorCategoryIds: []string{"g1"},
	}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	runID := h.waitForPause(t, pv)

	// Awaiting stage 0: CurrentStageIdx must be 0, not defaulted past it.
	st := h.getStatus(t, pv)
	if len(st.GetStageNames()) != 3 {
		t.Fatalf("StageNames = %v, want 3 stages", st.GetStageNames())
	}
	if st.GetCurrentStageIdx() != 0 {
		t.Fatalf("after submit: CurrentStageIdx = %d, want 0 (awaiting stage 0)", st.GetCurrentStageIdx())
	}

	// Approve stage 0 → the run advances into stage 1. Before the fix, GetStatus
	// stayed hardcoded at 0 here — this is the exact "stuck on stage 1" bug.
	approveStage(t, h, runID, "s0_task_0", "u0")
	waitForStageAssignments(t, h, pv, 1, []string{"u1"})
	st = h.getStatus(t, pv)
	if st.GetCurrentStageIdx() != 1 {
		t.Fatalf("after stage-0 approve: CurrentStageIdx = %d, want 1 (awaiting stage 1)", st.GetCurrentStageIdx())
	}

	// Approve stage 1 → the run advances into stage 2 (the middle-stage
	// transition multistage_pg_test.go's 2-stage harness cannot exercise).
	approveStage(t, h, runID, "s1_task_0", "u1")
	waitForStageAssignments(t, h, pv, 2, []string{"u2"})
	st = h.getStatus(t, pv)
	if st.GetCurrentStageIdx() != 2 {
		t.Fatalf("after stage-1 approve: CurrentStageIdx = %d, want 2 (awaiting stage 2)", st.GetCurrentStageIdx())
	}

	// Approve stage 2 (final) → the run terminates; nothing pending, so
	// CurrentStageIdx must report len(stages) so the stepper marks all stages done.
	approveStage(t, h, runID, "s2_task_0", "u2")
	deadline := time.Now().Add(15 * time.Second)
	var final domain.RunState
	for time.Now().Before(deadline) {
		final = h.runState(t, runID)
		if final.IsTerminal() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if final != domain.RunStateSucceeded {
		t.Fatalf("after all stages approved: run state %s, want succeeded", final)
	}
	st = h.getStatus(t, pv)
	if st.GetCurrentStageIdx() != int32(len(st.GetStageNames())) {
		t.Fatalf("after final approve: CurrentStageIdx = %d, want %d (past the last stage)",
			st.GetCurrentStageIdx(), len(st.GetStageNames()))
	}
}
