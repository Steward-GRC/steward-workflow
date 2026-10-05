// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package saga_test

import (
	"context"
	"testing"
	"time"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/google/uuid"

	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/grpcsvc"
	sagaclient "github.com/Steward-GRC/steward-workflow/internal/saga"
)

// recordingPolicyClient implements worker.PolicyServiceClient, recording
// set_status calls and returning a canned decision from ResolveDecision.
type recordingPolicyClient struct {
	decision string
	statuses []string
}

func (c *recordingPolicyClient) SetStatus(_ context.Context, _, status string) error {
	c.statuses = append(c.statuses, status)
	return nil
}
func (c *recordingPolicyClient) Publish(_ context.Context, _ string) error { return nil }
func (c *recordingPolicyClient) ResolveDecision(_ context.Context, _ string, _ int) (string, error) {
	return c.decision, nil
}
func (c *recordingPolicyClient) EscalateStage(_ context.Context, _ string, _ int) error { return nil }

func compiledDef(t *testing.T) domain.WorkflowDefinition {
	t.Helper()
	def, err := builder.Compile(builder.WorkflowDef{
		ID: "wf-embed", Name: "Embedded", Version: 1, Published: true,
		Stages: []builder.Stage{{Name: "S0", Quorum: builder.QuorumAny, ApproverIDs: []string{"u-alice"}}},
	}, builder.CompileOptions{StageSLAHours: 72, StageReminderHours: 48})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return def
}

// driveEmbedded runs the full SagaClient path: PublishWorkflow → StartRun →
// (find the user task) → Signal with task_id → terminal. Returns recorded
// set_status statuses.
func driveEmbedded(t *testing.T, decision string) []string {
	t.Helper()
	policy := &recordingPolicyClient{decision: decision}
	engine, err := sagaclient.BuildInMemory(policy)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	client := sagaclient.NewEmbedded(engine)
	ctx := context.Background()

	def := compiledDef(t)
	if err := client.PublishWorkflow(ctx, def); err != nil {
		t.Fatalf("publish: %v", err)
	}
	runID, err := client.StartRun(ctx, def.ID, map[string]any{"policy_version_id": "pv-1"})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	// Wait for the manual_approval pause, then locate the task via the store.
	rid := uuid.MustParse(runID)
	var taskID string
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		tasks, _ := engine.Store().ListUserTasksByRun(ctx, rid)
		if len(tasks) > 0 {
			taskID = tasks[0].ID.String()
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if taskID == "" {
		t.Fatal("no user task created")
	}

	// This is exactly what WorkflowServer.Signal forwards.
	if err := client.Signal(ctx, runID, decision, map[string]any{
		"task_id":       taskID,
		"actor_user_id": "u-alice",
		"comment":       "decision recorded",
	}); err != nil {
		t.Fatalf("signal: %v", err)
	}

	// Synchronous action verb means the run reaches terminal within the Signal's
	// advance, but poll briefly to be safe.
	end := time.Now().Add(2 * time.Second)
	for time.Now().Before(end) {
		r, _ := engine.Get(ctx, rid)
		if r.State == domain.RunStateSucceeded {
			return policy.statuses
		}
		if r.State == domain.RunStateFailed {
			t.Fatalf("run failed; statuses so far=%v", policy.statuses)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("run did not reach terminal; statuses=%v", policy.statuses)
	return policy.statuses
}

func TestEmbeddedSagaClient_Approve(t *testing.T) {
	statuses := driveEmbedded(t, "approve")
	if len(statuses) != 2 || statuses[0] != "approved" || statuses[1] != "published" {
		t.Fatalf("approve: want set_status [approved published], got %v", statuses)
	}
}

func TestEmbeddedSagaClient_Reject(t *testing.T) {
	statuses := driveEmbedded(t, "reject")
	if len(statuses) != 1 || statuses[0] != "rejected" {
		t.Fatalf("reject: want set_status [rejected], got %v", statuses)
	}
}

func TestEmbeddedSagaClient_ListPendingTasks(t *testing.T) {
	policy := &recordingPolicyClient{decision: "approve"}
	engine, err := sagaclient.BuildInMemory(policy)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	client := sagaclient.NewEmbedded(engine)
	ctx := context.Background()
	def := compiledDef(t)
	_ = client.PublishWorkflow(ctx, def)
	if _, err := client.StartRun(ctx, def.ID, map[string]any{"policy_version_id": "pv-7"}); err != nil {
		t.Fatalf("start: %v", err)
	}
	// Give the run a moment to pause at the manual_approval.
	time.Sleep(50 * time.Millisecond)
	tasks, err := client.ListPendingTasks(ctx, "anyone")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("want 1 pending task, got %d", len(tasks))
	}
	if tasks[0].PolicyVersionID != "pv-7" || tasks[0].StageIndex != 0 {
		t.Fatalf("unexpected task info: %+v", tasks[0])
	}
}

// compile-time: ensure the embedded client satisfies the interface used by the server.
var _ grpcsvc.SagaClient = (*sagaclient.EmbeddedSagaClient)(nil)
