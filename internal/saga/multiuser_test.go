// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package saga_test

import (
	"context"
	log "github.com/Bugs5382/go-log"
	"sync"
	"testing"
	"time"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
	"github.com/google/uuid"

	"github.com/Steward-GRC/steward-workflow/internal/builder"
	sagaclient "github.com/Steward-GRC/steward-workflow/internal/saga"
	"github.com/Steward-GRC/steward-workflow/internal/store"
)

// mutableAssignments is an in-memory approval_assignments fake whose rows the
// test flips to "approved" to simulate WorkflowServer.recordDecision running as
// each approver acts — in any order.
type mutableAssignments struct {
	mu   sync.Mutex
	rows map[string]string // userID -> state
}

func (m *mutableAssignments) approve(userID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rows[userID] = "approved"
}

func (m *mutableAssignments) ListStageAssignments(_ context.Context, _ string, _ int) ([]store.AssignmentRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]store.AssignmentRow, 0, len(m.rows))
	for uid, state := range m.rows {
		out = append(out, store.AssignmentRow{UserID: uid, State: state})
	}
	return out, nil
}

type fixedRun struct{ defID string }

func (f fixedRun) GetRunByRunID(_ context.Context, runID string) (store.ApprovalRun, error) {
	return store.ApprovalRun{RunID: runID, PolicyVersionID: "pv-1", WorkflowDefID: f.defID}, nil
}

type fixedDef struct{ wd builder.WorkflowDef }

func (f fixedDef) Get(_ context.Context, _ string) (builder.WorkflowDef, error) { return f.wd, nil }

func (f fixedDef) GetVersion(_ context.Context, _ string, _ int) (builder.WorkflowDef, error) {
	return f.wd, nil
}

// TestMultiUserQuorum_OutOfOrder proves the per-user quorum loop: a stage with a
// 2-of-3 quorum over a unified pool {u1,u2,u3} resolves correctly when approvals
// arrive OUT OF ORDER (u3 first, then u1; u2 never acts).
func TestMultiUserQuorum_OutOfOrder(t *testing.T) {
	// Stage 0: nofm, N=2, two groups whose members unify into one pool of 3 users.
	wd := builder.WorkflowDef{
		ID: "wf-mu", Name: "Multi-user", Version: 1, Published: true,
		Stages: []builder.Stage{{
			Name: "S0", Quorum: builder.QuorumNofM, QuorumN: 2,
			ApproverIDs: []string{"u1", "u2", "u3"},
		}},
	}
	def, err := builder.Compile(wd, builder.CompileOptions{StageSLAHours: 72})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	asg := &mutableAssignments{rows: map[string]string{"u1": "pending", "u2": "pending", "u3": "pending"}}
	resolver := sagaclient.NewDecisionResolver(fixedRun{defID: "wf-mu"}, fixedDef{wd: wd}, asg)

	policy := &recordingPolicyClient{} // set_status sink; ResolveDecision unused (resolver wired)
	st := memory.New()
	engine, err := sagaclient.Build(st, policy, resolver, nil, log.Nop())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	client := sagaclient.NewEmbedded(engine)
	ctx := context.Background()
	if err := client.PublishWorkflow(ctx, def); err != nil {
		t.Fatalf("publish: %v", err)
	}
	runIDStr, err := client.StartRun(ctx, def.ID, map[string]any{"policy_version_id": "pv-1"})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	rid := uuid.MustParse(runIDStr)

	// approve simulates: WorkflowServer.recordDecision flips the user's row, then
	// the saga is unpaused by submitting whatever task is currently pending.
	approve := func(userID string) {
		asg.approve(userID)
		task := waitForTask(t, engine.Store(), rid)
		if err := client.Signal(ctx, runIDStr, "approve", map[string]any{
			"task_id": task, "actor_user_id": userID, "comment": "ok",
		}); err != nil {
			t.Fatalf("signal %s: %v", userID, err)
		}
	}

	// u3 approves FIRST (only 1 of 2 → pending; the run loops back to a new task).
	approve("u3")
	if got := stateOf(t, engine, rid); got != domain.RunStatePaused {
		t.Fatalf("after u3 (1/2): want paused (pending loop), got %s", got)
	}

	// u1 approves SECOND (2 of 2 → quorum met → approve → published).
	approve("u1")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if stateOf(t, engine, rid) == domain.RunStateSucceeded {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := stateOf(t, engine, rid); got != domain.RunStateSucceeded {
		t.Fatalf("after u1 (2/2): want succeeded, got %s", got)
	}
	if len(policy.statuses) != 2 || policy.statuses[0] != "approved" || policy.statuses[1] != "published" {
		t.Fatalf("want set_status [approved published], got %v", policy.statuses)
	}
	t.Logf("out-of-order quorum met: u3 then u1 (u2 never acted) → %v", policy.statuses)
}

func waitForTask(t *testing.T, st interface {
	ListUserTasksByRun(context.Context, uuid.UUID) ([]domain.UserTask, error)
}, rid uuid.UUID) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		tasks, _ := st.ListUserTasksByRun(context.Background(), rid)
		for _, tk := range tasks {
			// Return a task that is still awaiting submission (no SubmittedAt).
			if tk.SubmittedAt == nil {
				return tk.ID.String()
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("no pending user task appeared")
	return ""
}

func stateOf(t *testing.T, engine interface {
	Get(context.Context, uuid.UUID) (domain.SagaRun, error)
}, rid uuid.UUID) domain.RunState {
	t.Helper()
	r, err := engine.Get(context.Background(), rid)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	return r.State
}
