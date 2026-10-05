// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package saga_test

// Real-Postgres INTEGRATION test for the multi-stage assignment fix.
//
// The bug: Submit created approval_assignments only for stage 0, so when the
// saga advanced past stage 0 it parked at stage 1's manual_approval task with NO
// assignment rows — the run sat in_review forever, invisible to stage-1
// approvers. The fix runs a policy.assign_stage action on ENTRY to every stage
// (s{i}_assign, chained before s{i}_task_0), which creates that stage's rows from
// the pre-resolved "approvers_s{i}" run input.
//
// This test wires the real stack (embedded engine + real StageAssigner + real
// DecisionResolver + real stores) with a TWO-stage def whose stages have distinct
// approvers, then asserts:
//   - after Submit, stage 0's rows exist and stage 1's do NOT yet;
//   - after approving stage 0, stage 1's rows now exist (the fix);
//   - approving stage 1 drives the run to succeeded.

import (
	"context"
	log "github.com/Bugs5382/go-log"
	"strings"
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

// newTwoStageHarness wires the real stack over a 2-stage ALL-quorum def with one
// distinct approver per stage (u0 → stage 0, u1 → stage 1).
func newTwoStageHarness(t *testing.T) *lifecycleHarness {
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
		Name: "TwoStage", Version: 1, Published: true,
		Stages: []builder.Stage{
			{Name: "S0", Quorum: builder.QuorumAll, ApproverIDs: []string{"u0"}},
			{Name: "S1", Quorum: builder.QuorumAll, ApproverIDs: []string{"u1"}},
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

// stageAssignmentUsers returns the user ids assigned at a stage (any state).
func stageAssignmentUsers(t *testing.T, h *lifecycleHarness, pv string, stage int) []string {
	t.Helper()
	rows, err := h.assignments.ListStageAssignments(context.Background(), pv, stage)
	if err != nil {
		t.Fatalf("ListStageAssignments(%s, %d): %v", pv, stage, err)
	}
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.UserID)
	}
	return out
}

func TestMultiStage_StageEntryCreatesAssignments(t *testing.T) {
	h := newTwoStageHarness(t)
	const pv = "pv-2stage"
	ctx := context.Background()

	// Submit → the saga runs s0_assign on Start, then parks at s0_task_0.
	if _, err := h.srv.Submit(ctx, &workflowv1.SubmitRequest{
		PolicyVersionId: pv, PolicyId: "pol-1", CategoryId: "g1",
		SubmittedBy: "submitter", AncestorCategoryIds: []string{"g1"},
	}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	runID := h.waitForPause(t, pv)

	// Stage 0 is assigned; stage 1 is NOT yet (the saga has not entered it).
	if got := stageAssignmentUsers(t, h, pv, 0); len(got) != 1 || got[0] != "u0" {
		t.Fatalf("stage 0 assignments: want [u0], got %v", got)
	}
	if got := stageAssignmentUsers(t, h, pv, 1); len(got) != 0 {
		t.Fatalf("stage 1 assignments must not exist before stage 0 is approved, got %v", got)
	}

	// Approve stage 0 (u0). The saga advances: s0 outcome_switch(approve) →
	// s1_assign → s1_task_0. s1_assign is the fix; before it, the run stalled here.
	taskID := h.awaitedTaskID(t, runID, "s0_task_0")
	if _, err := h.srv.Signal(ctx, &workflowv1.SignalRequest{
		RunId: runID, TaskId: taskID, Signal: workflowv1.SignalType_SIGNAL_TYPE_APPROVE,
		ActorUserId: "u0", Comment: "ok",
	}); err != nil {
		t.Fatalf("approve stage 0: %v", err)
	}

	// The run must re-pause at stage 1's task with stage-1 assignments created.
	waitForStageAssignments(t, h, pv, 1, []string{"u1"})

	// Sanity: the run is paused again (at stage 1), not terminal or failed.
	if st := h.runState(t, runID); st != domain.RunStatePaused {
		t.Fatalf("after stage-0 approve: run state %s, want paused at stage 1", st)
	}

	// Approve stage 1 (u1) → run succeeds (all stages approved). Read the task the
	// run is ACTUALLY awaiting (from awaited_signal, once paused) rather than the
	// newest unsubmitted task row: assignment rows are created by s1_assign BEFORE
	// the engine creates + pauses on s1_task_0, so a raw "newest task" read can win
	// the race and signal a task the run is not yet awaiting.
	task1 := h.awaitedTaskID(t, runID, "s1_task_0")
	if _, err := h.srv.Signal(ctx, &workflowv1.SignalRequest{
		RunId: runID, TaskId: task1, Signal: workflowv1.SignalType_SIGNAL_TYPE_APPROVE,
		ActorUserId: "u1", Comment: "ok",
	}); err != nil {
		t.Fatalf("approve stage 1: %v", err)
	}
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
		var curStep, lastErr, awaited string
		_ = h.pool.Querier().QueryRow(context.Background(),
			`SELECT current_step, COALESCE(last_error,''), COALESCE(awaited_signal,'') FROM runtime.saga_runs WHERE id=$1`, runID,
		).Scan(&curStep, &lastErr, &awaited)
		rows, _ := h.pool.Querier().Query(context.Background(),
			`SELECT step_id, COALESCE(submitted_at::text,'nil') FROM runtime.saga_user_tasks WHERE run_id=$1 ORDER BY id`, runID)
		tasks := ""
		for rows.Next() {
			var sid, sub string
			_ = rows.Scan(&sid, &sub)
			tasks += sid + "(" + sub + ") "
		}
		rows.Close()
		t.Fatalf("after both stages approved: run state %s, want succeeded; current_step=%q last_error=%q awaited_signal=%q statuses=%v tasks=[%s]",
			final, curStep, lastErr, awaited, h.policy.statuses, tasks)
	}
}

// awaitedTaskID waits until the run is paused on wantStep awaiting a user-task
// signal, then returns the task id the run is actually awaiting (parsed from
// awaited_signal = "user_task.<id>.submitted"). This is race-free: it reads the
// run's own committed awaited state, so it never signals a task the engine has
// not yet parked on.
func (h *lifecycleHarness) awaitedTaskID(t *testing.T, runID, wantStep string) string {
	t.Helper()
	const prefix, suffix = "user_task.", ".submitted"
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var state, curStep, awaited string
		err := h.pool.Querier().QueryRow(context.Background(),
			`SELECT state, COALESCE(current_step,''), COALESCE(awaited_signal,'')
			   FROM runtime.saga_runs WHERE id=$1`, runID,
		).Scan(&state, &curStep, &awaited)
		if err == nil && state == string(domain.RunStatePaused) && curStep == wantStep &&
			strings.HasPrefix(awaited, prefix) && strings.HasSuffix(awaited, suffix) {
			return strings.TrimSuffix(strings.TrimPrefix(awaited, prefix), suffix)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("run %s did not pause on %s with an awaited user-task signal", runID, wantStep)
	return ""
}

// waitForStageAssignments polls until the stage's assignment users match want.
func waitForStageAssignments(t *testing.T, h *lifecycleHarness, pv string, stage int, want []string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var got []string
	for time.Now().Before(deadline) {
		got = stageAssignmentUsers(t, h, pv, stage)
		if len(got) == len(want) {
			ok := true
			for i := range want {
				if got[i] != want[i] {
					ok = false
					break
				}
			}
			if ok {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("stage %d assignments: want %v, got %v (the multi-stage fix did not create stage-%d rows on entry)", stage, want, got, stage)
}
