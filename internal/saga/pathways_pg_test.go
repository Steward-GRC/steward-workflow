// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package saga_test

// Per-pathway INTEGRATION suite over REAL components.
//
// This is the test class that was MISSING when the live publish bug shipped: a
// no-op policy client (recordingPolicyClient) sank every SetStatus, so the saga
// reached a terminal state but the real worker.CorePolicyClient -> core
// SetVersionStatus seam was never exercised end to end. These tests wire the
// REAL stack exactly as cmd/server does:
//
//   - testcontainer Postgres (one DB, both the workflow schema and the saga
//     "runtime" schema migrated in, via newSagaTestDB);
//   - the real EmbeddedSagaClient over the sagapg store;
//   - the real WorkflowServer + stores (WorkflowDefStore / AssignmentStore /
//     Assignments) + the real DecisionResolver;
//   - the real worker.CorePolicyClient (Actor defaults to "system") over a
//     fakeCoreVersionClient that faithfully mirrors core's SetVersionStatus
//     actor contract (accepts a UUID / empty / system actor; rejects garbage).
//
// Each test drives ONE lifecycle pathway end to end against that stack.
//
// fakeCoreVersionClient + recordingAudit live in publish_seam_pg_test.go;
// fixedResolver / noopAudit / newSagaTestDB / the lifecycleHarness helpers live
// in lifecycle_pg_test.go. This file only adds the multi-approver real-core
// harness and the per-pathway tests.

import (
	"context"
	log "github.com/Bugs5382/go-log"
	"testing"
	"time"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	sagapg "github.com/Bugs5382/go-saga-orchestration/store/postgres"

	workflowv1 "github.com/Steward-GRC/steward-workflow/gen/go/steward/workflow/v1"
	corev1 "github.com/Steward-GRC/steward-workflow/gen/go/thirdparty/core/v1"
	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/grpcsvc"
	sagaclient "github.com/Steward-GRC/steward-workflow/internal/saga"
	"github.com/Steward-GRC/steward-workflow/internal/store"
	"github.com/Steward-GRC/steward-workflow/internal/worker"
)

// pathwayHarness is the real-core stack with a single-stage ANY-quorum def that
// carries TWO approvers (so the inbox-filter pathway has two distinct owners on
// one stage). It exposes the fake core + the run store so the per-pathway tests
// can assert the published seam and inspect approval_runs directly.
type pathwayHarness struct {
	*lifecycleHarness
	core     *fakeCoreVersionClient
	runStore *store.AssignmentStore
	defStore *store.WorkflowDefStore
}

func newPathwayHarness(t *testing.T) *pathwayHarness {
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
		Name: "Pathways", Version: 1, Published: true,
		Stages: []builder.Stage{{
			Name: "S0", Quorum: builder.QuorumAny,
			ApproverIDs: []string{"approverA", "approverB"},
		}},
	}
	defID, err := defStore.Create(ctx, wd)
	if err != nil {
		t.Fatalf("create def: %v", err)
	}

	resolver := sagaclient.NewDecisionResolver(assignStore, defStore, assignments)
	core := &fakeCoreVersionClient{}
	// REAL production client — Actor defaults to "system" (the live-bug trigger).
	policyClient := worker.NewCorePolicyClient(worker.CorePolicyClientOpts{
		Core:     core,
		Runs:     assignStore,
		Resolver: resolver,
		Audit:    &recordingAudit{},
	})
	assigner := sagaclient.NewStageAssigner(defStore, assignments, builder.CompileOptions{StageSLAHours: 72, StageReminderHours: 48})
	engine, err := sagaclient.Build(sagaStore, policyClient, resolver, assigner, log.Nop())
	if err != nil {
		t.Fatalf("build engine: %v", err)
	}
	sagaCli := sagaclient.NewEmbedded(engine)

	// A core-status setter so the withdraw pathway returns the version to draft
	// through the same fake core (mirrors cmd/server's coreStatusAdapter).
	srv := grpcsvc.NewWorkflowServer(grpcsvc.WorkflowServerOpts{
		Resolver:    fixedResolver{defID: defID},
		DefStore:    defStore,
		RunStore:    assignStore,
		SagaClient:  sagaCli,
		AuditEmit:   noopAudit{},
		Assignments: assignments,
		CoreStatus:  coreStatusSetter{core},
		CompileOpts: builder.CompileOptions{StageSLAHours: 72, StageReminderHours: 48},
	})

	h := &lifecycleHarness{
		pool: pool, saga: sagaCli, srv: srv, defID: defID, assignments: assignments,
	}
	return &pathwayHarness{lifecycleHarness: h, core: core, runStore: assignStore, defStore: defStore}
}

// coreStatusSetter adapts fakeCoreVersionClient to grpcsvc.CoreStatusSetter
// (the withdraw path's "return version to draft" seam), exercising the same
// actor contract the real core enforces.
type coreStatusSetter struct{ core *fakeCoreVersionClient }

func (c coreStatusSetter) SetVersionStatus(ctx context.Context, pvID, status, actor string) error {
	_, err := c.core.SetVersionStatus(ctx, &corev1.SetVersionStatusRequest{
		PolicyVersionId: pvID, Status: status, ActorUserId: actor,
	})
	return err
}

// waitTerminal blocks until the run reaches a terminal state (or fails the
// test). Returns the terminal state.
func (h *pathwayHarness) waitTerminal(t *testing.T, runID string) domain.RunState {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		st := h.runState(t, runID)
		if st.IsTerminal() {
			return st
		}
		time.Sleep(10 * time.Millisecond)
	}
	return h.runState(t, runID)
}

func (h *pathwayHarness) signal(t *testing.T, runID, taskID string, sig workflowv1.SignalType, actor, comment string) {
	t.Helper()
	_, err := h.srv.Signal(context.Background(), &workflowv1.SignalRequest{
		RunId: runID, TaskId: taskID, Signal: sig, ActorUserId: actor, Comment: comment,
	})
	if err != nil {
		t.Fatalf("signal %v by %s: %v", sig, actor, err)
	}
}

// --- per-pathway tests (real CorePolicyClient over a fake core) ---

// PATHWAY 1: submit -> approve -> core version PUBLISHED (+ publish seam).
// This is the regression that bit us: with the no-op client the run reached
// succeeded but the core version was never published. Asserts the real seam.
func TestPathway_SubmitApprovePublishes(t *testing.T) {
	h := newPathwayHarness(t)
	const pv = "pv-approve-publish"
	ctx := context.Background()

	if _, err := h.srv.Submit(ctx, &workflowv1.SubmitRequest{
		PolicyVersionId: pv, PolicyId: "pol-1", CategoryId: "g1",
		SubmittedBy: "submitter", AncestorCategoryIds: []string{"g1"},
	}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	runID := h.waitForPause(t, pv)
	taskID := h.pendingTaskID(t, runID)

	h.signal(t, runID, taskID, workflowv1.SignalType_SIGNAL_TYPE_APPROVE, "approverA", "ok")

	if st := h.waitTerminal(t, runID); st != domain.RunStateSucceeded {
		published, actor := h.core.wasPublished()
		t.Fatalf("run state = %s, want succeeded (published=%v actor=%q)", st, published, actor)
	}
	published, actor := h.core.wasPublished()
	if !published {
		t.Fatalf("core version was never published (lastActor=%q)", actor)
	}
	if actor == "" {
		t.Fatalf("publish was driven with an empty actor; want the system actor")
	}
}

// PATHWAY 2: submit -> reject -> run terminal, approval_runs rejected.
func TestPathway_SubmitRejectRejected(t *testing.T) {
	h := newPathwayHarness(t)
	const pv = "pv-reject"
	ctx := context.Background()

	if _, err := h.srv.Submit(ctx, &workflowv1.SubmitRequest{
		PolicyVersionId: pv, PolicyId: "pol-1", CategoryId: "g1",
		SubmittedBy: "submitter", AncestorCategoryIds: []string{"g1"},
	}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	runID := h.waitForPause(t, pv)
	taskID := h.pendingTaskID(t, runID)

	h.signal(t, runID, taskID, workflowv1.SignalType_SIGNAL_TYPE_REJECT, "approverA", "no")

	if st := h.runState(t, runID); st == domain.RunStatePaused {
		t.Fatalf("post-reject run still paused; want terminal")
	}
	if box := h.inbox(t, "approverA"); len(box) != 0 {
		t.Fatalf("post-reject inbox: want empty, got %v", box)
	}
	// The decision is recorded; the run is no longer active.
	if got := h.activeRunsForVersion(t, pv); got != 0 {
		t.Fatalf("post-reject active runs = %d, want 0", got)
	}
	// A rejected policy version is NOT published.
	if published, _ := h.core.wasPublished(); published {
		t.Fatalf("core version was published on a reject; want not published")
	}
}

// PATHWAY 3: submit -> withdraw -> version returned to draft, inbox cleared,
// run terminal, approval_runs withdrawn.
func TestPathway_SubmitWithdrawDraft(t *testing.T) {
	h := newPathwayHarness(t)
	const pv = "pv-withdraw"
	ctx := context.Background()

	if _, err := h.srv.Submit(ctx, &workflowv1.SubmitRequest{
		PolicyVersionId: pv, PolicyId: "pol-1", CategoryId: "g1",
		SubmittedBy: "submitter", AncestorCategoryIds: []string{"g1"},
	}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	runID := h.waitForPause(t, pv)
	if box := h.inbox(t, "approverA"); len(box) != 1 {
		t.Fatalf("pre-withdraw inbox: want 1, got %v", box)
	}

	// A real gateway passes a UUID actor; the fake core enforces that contract.
	const actor = "11111111-1111-1111-1111-111111111111"
	h.signal(t, runID, "", workflowv1.SignalType_SIGNAL_TYPE_WITHDRAW, actor, "")

	if box := h.inbox(t, "approverA"); len(box) != 0 {
		t.Fatalf("post-withdraw inbox: want empty, got %v", box)
	}
	if st := h.runState(t, runID); !st.IsTerminal() {
		t.Fatalf("post-withdraw run state = %s, want terminal", st)
	}
	run, err := h.runStore.GetRun(ctx, pv)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.Status != "withdrawn" {
		t.Fatalf("approval_runs status = %q, want withdrawn", run.Status)
	}
	// The withdraw path returned the core version to draft through the real seam.
	if gotStatus, gotActor := h.core.lastWrite(); gotStatus != "draft" || gotActor != actor {
		t.Fatalf("core last write = (%q, %q), want (draft, %q)", gotStatus, gotActor, actor)
	}
	// A withdrawn version is never published.
	if published, _ := h.core.wasPublished(); published {
		t.Fatalf("core version was published on a withdraw; want not published")
	}
}

// PATHWAY 4: re-submit idempotency + self-heal + PRIOR RUN PRESERVED as
// 'aborted' (Part 1). The version ends with exactly one active run, the prior
// active run's record is kept as a terminal 'aborted' row, and GetRun returns
// the fresh active run.
func TestPathway_ReSubmitPreservesPriorRun(t *testing.T) {
	h := newPathwayHarness(t)
	const pv = "pv-resubmit-preserve"
	ctx := context.Background()

	if _, err := h.srv.Submit(ctx, &workflowv1.SubmitRequest{
		PolicyVersionId: pv, PolicyId: "pol-1", CategoryId: "g1",
		SubmittedBy: "submitter", AncestorCategoryIds: []string{"g1"},
	}); err != nil {
		t.Fatalf("first Submit: %v", err)
	}
	h.waitForPause(t, pv)
	first, err := h.runStore.GetRun(ctx, pv)
	if err != nil {
		t.Fatalf("GetRun after first submit: %v", err)
	}
	if first.Status != "in_review" {
		t.Fatalf("first run status = %q, want in_review", first.Status)
	}

	// Re-submit: cancels the prior saga run AND aborts its approval_runs record.
	if _, err := h.srv.Submit(ctx, &workflowv1.SubmitRequest{
		PolicyVersionId: pv, PolicyId: "pol-1", CategoryId: "g1",
		SubmittedBy: "submitter", AncestorCategoryIds: []string{"g1"},
	}); err != nil {
		t.Fatalf("re-Submit: %v", err)
	}
	h.waitForPause(t, pv)

	// Exactly one active saga run (self-heal / idempotent).
	if got := h.activeRunsForVersion(t, pv); got != 1 {
		t.Fatalf("after re-submit: want 1 active run, got %d", got)
	}

	// approval_runs now has TWO rows for the version: the prior one preserved as
	// 'aborted', and the fresh one 'in_review'. GetRun returns the active one.
	current, err := h.runStore.GetRun(ctx, pv)
	if err != nil {
		t.Fatalf("GetRun after re-submit: %v", err)
	}
	if current.Status != "in_review" {
		t.Fatalf("current run status = %q, want in_review", current.Status)
	}
	if current.RunID == first.RunID {
		t.Fatalf("current run id == prior run id (%q); want a fresh run", current.RunID)
	}

	// The prior run record is PRESERVED (not overwritten) as terminal 'aborted'.
	var total, aborted int
	if err := h.pool.Querier().QueryRow(ctx,
		`SELECT count(*), count(*) FILTER (WHERE status='aborted')
		   FROM approval_runs WHERE policy_version_id=$1`, pv,
	).Scan(&total, &aborted); err != nil {
		t.Fatalf("count runs: %v", err)
	}
	if total != 2 {
		t.Fatalf("approval_runs rows for %s = %d, want 2 (prior preserved + fresh)", pv, total)
	}
	if aborted != 1 {
		t.Fatalf("aborted rows = %d, want 1 (the prior attempt)", aborted)
	}

	// The preserved prior row carries the ORIGINAL run id (its record was kept).
	var priorStatus string
	if err := h.pool.Querier().QueryRow(ctx,
		`SELECT status FROM approval_runs WHERE run_id=$1`, first.RunID,
	).Scan(&priorStatus); err != nil {
		t.Fatalf("read prior run %s: %v", first.RunID, err)
	}
	if priorStatus != "aborted" {
		t.Fatalf("prior run %s status = %q, want aborted", first.RunID, priorStatus)
	}
}

// PATHWAY 5: approver-filtered inbox — A sees only A's task. Two versions, one
// owned by each approver; each approver's inbox holds only their own.
func TestPathway_InboxFilteredByApprover(t *testing.T) {
	h := newPathwayHarness(t)
	ctx := context.Background()

	for _, pv := range []string{"pvA", "pvB"} {
		if _, err := h.srv.Submit(ctx, &workflowv1.SubmitRequest{
			PolicyVersionId: pv, PolicyId: "pol-1", CategoryId: "g1",
			SubmittedBy: "submitter", AncestorCategoryIds: []string{"g1"},
		}); err != nil {
			t.Fatalf("Submit(%s): %v", pv, err)
		}
		h.waitForPause(t, pv)
	}

	// Each version seeded approverA + approverB pending; supersede the peer so
	// each version has a single owner.
	mustExec(t, h.pool,
		`UPDATE approval_assignments SET state='superseded'
		   WHERE policy_version_id='pvA' AND user_id='approverB'`)
	mustExec(t, h.pool,
		`UPDATE approval_assignments SET state='superseded'
		   WHERE policy_version_id='pvB' AND user_id='approverA'`)

	if box := h.inbox(t, "approverA"); len(box) != 1 || box[0] != "pvA" {
		t.Fatalf("approverA inbox: want [pvA], got %v", box)
	}
	if box := h.inbox(t, "approverB"); len(box) != 1 || box[0] != "pvB" {
		t.Fatalf("approverB inbox: want [pvB], got %v", box)
	}
}
