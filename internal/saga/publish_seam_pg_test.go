// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package saga_test

// Real-Postgres SEAM test for the approve→publish path.
//
// The other lifecycle tests wire a recordingPolicyClient fake whose SetStatus is
// a no-op sink, so they prove the saga reaches a terminal state but NEVER
// exercise the real worker.CorePolicyClient → core SetVersionStatus seam. That
// blind spot hid the production bug: the saga drives the publish_now step with
// actor_user_id="system" (the CorePolicyClient default), and the core handler
// rejected "system" as an invalid UUID, so the publish step errored, the run
// went "failed", the core version stayed "draft", and NO published event/audit
// was emitted — exactly the symptom reported live.
//
// This test wires the REAL worker.CorePolicyClient over a fakeCoreVersionClient
// that faithfully mirrors core's SetVersionStatus contract (accept a UUID or a
// "system"/"system:*" actor; reject anything else — the contract pinned by
// core's TestSetVersionStatusAcceptsSystemActor / RejectsGarbageActor), then
// drives submit → approve and asserts:
//   - the saga run reaches succeeded (not failed);
//   - the core version was set to "published" (the publish step ran);
//   - a published signal was recorded (audit/event seam) so the History
//     "Published" step renders.

import (
	"context"
	log "github.com/Bugs5382/go-log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	sagapg "github.com/Bugs5382/go-saga-orchestration/store/postgres"
	"github.com/google/uuid"

	workflowv1 "github.com/Steward-GRC/steward-workflow/gen/go/steward/workflow/v1"
	corev1 "github.com/Steward-GRC/steward-workflow/gen/go/thirdparty/core/v1"
	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/grpcsvc"
	sagaclient "github.com/Steward-GRC/steward-workflow/internal/saga"
	"github.com/Steward-GRC/steward-workflow/internal/store"
	"github.com/Steward-GRC/steward-workflow/internal/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeCoreVersionClient implements worker.CorePolicyVersionClient and mirrors the
// core PolicyHandler.SetVersionStatus actor contract: a UUID, empty, or a system
// actor ("system" / "system:*") is accepted; anything else is rejected with
// InvalidArgument — the same gate that produced the live bug. It records the last
// status it was driven to so the test can assert "published".
type fakeCoreVersionClient struct {
	mu         sync.Mutex
	published  bool
	lastActor  string
	lastStatus string
}

func (f *fakeCoreVersionClient) SetVersionStatus(_ context.Context, in *corev1.SetVersionStatusRequest, _ ...grpc.CallOption) (*corev1.SetVersionStatusResponse, error) {
	actor := in.GetActorUserId()
	if actor != "" && actor != "system" && !strings.HasPrefix(actor, "system:") {
		if _, err := uuid.Parse(actor); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid actor_user_id: %v", err)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastActor = actor
	f.lastStatus = in.GetStatus()
	if in.GetStatus() == "published" {
		f.published = true
	}
	return &corev1.SetVersionStatusResponse{
		Version: &corev1.PolicyVersion{
			Id:     in.GetPolicyVersionId(),
			Status: corev1.PolicyVersionStatus_POLICY_VERSION_STATUS_PUBLISHED,
		},
	}, nil
}

func (f *fakeCoreVersionClient) wasPublished() (bool, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.published, f.lastActor
}

func (f *fakeCoreVersionClient) lastWrite() (status, actor string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastStatus, f.lastActor
}

// recordingAudit captures audit emissions so the test can assert a published
// event is recorded (the seam the History "Published" step renders from).
type recordingAudit struct {
	mu    sync.Mutex
	calls []string
}

func (a *recordingAudit) Emit(_ context.Context, action, subject string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls = append(a.calls, action+" "+subject)
}

// newRealCoreHarness wires the stack exactly like cmd/server but with the REAL
// worker.CorePolicyClient (not the recording fake) over a fakeCoreVersionClient.
func newRealCoreHarness(t *testing.T) (*grpcsvc.WorkflowServer, *fakeCoreVersionClient, *lifecycleHarness) {
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
		Name: "PublishSeam", Version: 1, Published: true,
		Stages: []builder.Stage{{Name: "S0", Quorum: builder.QuorumAny, ApproverIDs: []string{"approverA"}}},
	}
	defID, err := defStore.Create(ctx, wd)
	if err != nil {
		t.Fatalf("create def: %v", err)
	}

	resolver := sagaclient.NewDecisionResolver(assignStore, defStore, assignments)
	core := &fakeCoreVersionClient{}
	// The REAL production client — Actor defaults to "system" (the bug trigger).
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

	srv := grpcsvc.NewWorkflowServer(grpcsvc.WorkflowServerOpts{
		Resolver:    fixedResolver{defID: defID},
		DefStore:    defStore,
		RunStore:    assignStore,
		SagaClient:  sagaCli,
		AuditEmit:   noopAudit{},
		Assignments: assignments,
		CompileOpts: builder.CompileOptions{StageSLAHours: 72, StageReminderHours: 48},
	})

	h := &lifecycleHarness{pool: pool, saga: sagaCli, srv: srv, defID: defID, assignments: assignments}
	_ = defID
	return srv, core, h
}

// TestPublishSeam_ApprovePublishesCoreVersion is the regression seam for the
// live approve→publish failure: a real CorePolicyClient (actor defaults to
// "system") must drive the core version to "published" and the run to succeeded.
func TestPublishSeam_ApprovePublishesCoreVersion(t *testing.T) {
	srv, core, h := newRealCoreHarness(t)
	const pv = "pv-publish-seam"
	ctx := context.Background()

	if _, err := srv.Submit(ctx, &workflowv1.SubmitRequest{
		PolicyVersionId: pv, PolicyId: "pol-1", CategoryId: "g1",
		SubmittedBy: "submitter", AncestorCategoryIds: []string{"g1"},
	}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	runID := h.waitForPause(t, pv)
	taskID := h.pendingTaskID(t, runID)

	if _, err := srv.Signal(ctx, &workflowv1.SignalRequest{
		RunId: runID, TaskId: taskID, Signal: workflowv1.SignalType_SIGNAL_TYPE_APPROVE,
		ActorUserId: "approverA", Comment: "ok",
	}); err != nil {
		t.Fatalf("approve: %v", err)
	}

	// Wait for the run to leave the pause and reach a terminal state.
	deadline := time.Now().Add(5 * time.Second)
	var final domain.RunState
	for time.Now().Before(deadline) {
		final = h.runState(t, runID)
		if final.IsTerminal() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if final != domain.RunStateSucceeded {
		// Surface the failure reason (the bug left the run "failed").
		published, actor := core.wasPublished()
		t.Fatalf("run state = %s, want succeeded (core published=%v, lastActor=%q)", final, published, actor)
	}
	published, actor := core.wasPublished()
	if !published {
		t.Fatalf("core version was never set to published (lastActor=%q)", actor)
	}
	if actor == "" {
		t.Fatalf("publish was driven with an empty actor; want the system actor")
	}
}
