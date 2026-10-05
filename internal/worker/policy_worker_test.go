// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package worker

import (
	"context"
	"errors"
	"testing"

	corev1 "github.com/Steward-GRC/steward-workflow/gen/go/thirdparty/core/v1"
	"google.golang.org/grpc"
)

type stubPolicyClient struct {
	setStatusCalls []setStatusCall
	publishCalls   []string // policy_version_ids
	escalateCalls  []int    // stage indexes
	decision       string   // canned ResolveDecision result
	decisionErr    error
}
type setStatusCall struct{ policyVersionID, status string }

func (s *stubPolicyClient) SetStatus(_ context.Context, policyVersionID, status string) error {
	s.setStatusCalls = append(s.setStatusCalls, setStatusCall{policyVersionID, status})
	return nil
}
func (s *stubPolicyClient) Publish(_ context.Context, policyVersionID string) error {
	s.publishCalls = append(s.publishCalls, policyVersionID)
	return nil
}
func (s *stubPolicyClient) ResolveDecision(_ context.Context, _ string, _ int) (string, error) {
	if s.decisionErr != nil {
		return "", s.decisionErr
	}
	return s.decision, nil
}
func (s *stubPolicyClient) EscalateStage(_ context.Context, _ string, stageIndex int) error {
	s.escalateCalls = append(s.escalateCalls, stageIndex)
	return nil
}

// Compile-time: the stub must satisfy the worker's client interface.
var _ PolicyServiceClient = (*stubPolicyClient)(nil)

// --- policy.set_status ---

func TestSetStatusHandler(t *testing.T) {
	for _, status := range []string{"approved", "rejected", "published"} {
		client := &stubPolicyClient{}
		res, err := HandleAction(context.Background(), client, "policy.set_status", "run-1",
			map[string]any{"policy_version_id": "pv-1", "status": status})
		if err != nil {
			t.Fatalf("status %s: %v", status, err)
		}
		if len(client.setStatusCalls) != 1 || client.setStatusCalls[0].status != status {
			t.Fatalf("status %s: calls=%+v", status, client.setStatusCalls)
		}
		if res["ok"] != true || res["status"] != status {
			t.Fatalf("status %s: result %v", status, res)
		}
	}
}

func TestSetStatusHandler_MissingInputs(t *testing.T) {
	if _, err := HandleAction(context.Background(), &stubPolicyClient{}, "policy.set_status", "run-1",
		map[string]any{}); err == nil {
		t.Fatal("expected error for missing policy_version_id and status")
	}
}

func TestSetStatusHandler_InvalidTransition(t *testing.T) {
	// published → approved is not a legal transition; current_status guards it.
	_, err := HandleAction(context.Background(), &stubPolicyClient{}, "policy.set_status", "run-1",
		map[string]any{"policy_version_id": "pv-1", "status": "approved", "current_status": "published"})
	if err == nil {
		t.Fatal("expected invalid_transition error")
	}
}

// --- policy.resolve_decision ---

func TestResolveDecisionHandler(t *testing.T) {
	for _, d := range []string{"approve", "reject", "request_changes"} {
		client := &stubPolicyClient{decision: d}
		res, err := HandleAction(context.Background(), client, "policy.resolve_decision", "run-1",
			map[string]any{"stage_index": float64(0)}) // JSON numbers arrive as float64
		if err != nil {
			t.Fatalf("decision %s: %v", d, err)
		}
		if res["decision"] != d {
			t.Fatalf("decision %s: result %v", d, res)
		}
	}
}

func TestResolveDecisionHandler_BadDecision(t *testing.T) {
	if _, err := HandleAction(context.Background(), &stubPolicyClient{decision: "maybe"}, "policy.resolve_decision", "r",
		map[string]any{"stage_index": 0}); err == nil {
		t.Fatal("expected error for unrecognised decision")
	}
}

func TestResolveDecisionHandler_ClientError(t *testing.T) {
	if _, err := HandleAction(context.Background(), &stubPolicyClient{decisionErr: errors.New("boom")}, "policy.resolve_decision", "r",
		map[string]any{"stage_index": 0}); err == nil {
		t.Fatal("expected error propagated from client")
	}
}

// --- policy.escalate_stage ---

func TestEscalateHandler(t *testing.T) {
	client := &stubPolicyClient{}
	res, err := HandleAction(context.Background(), client, "policy.escalate_stage", "r",
		map[string]any{"stage_index": float64(2)})
	if err != nil {
		t.Fatalf("escalate: %v", err)
	}
	if res["ok"] != true {
		t.Fatalf("escalate result %v", res)
	}
	if len(client.escalateCalls) != 1 || client.escalateCalls[0] != 2 {
		t.Fatalf("escalate calls %+v", client.escalateCalls)
	}
}

// --- CorePolicyClient (the real, core-backed client) ---

type fakeCore struct {
	calls []*corev1.SetVersionStatusRequest
	err   error
}

func (f *fakeCore) SetVersionStatus(_ context.Context, in *corev1.SetVersionStatusRequest, _ ...grpc.CallOption) (*corev1.SetVersionStatusResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.calls = append(f.calls, in)
	return &corev1.SetVersionStatusResponse{}, nil
}

type fakeRunStatus struct {
	calls []struct{ pvID, status string }
}

func (f *fakeRunStatus) UpdateRunStatus(_ context.Context, pvID, status string) error {
	f.calls = append(f.calls, struct{ pvID, status string }{pvID, status})
	return nil
}

type fakeResolver struct {
	decision string
	gotRun   string
	gotStage int
}

func (f *fakeResolver) ResolveStageDecision(_ context.Context, runID string, stage int) (string, error) {
	f.gotRun = runID
	f.gotStage = stage
	return f.decision, nil
}

type fakeAudit struct{ actions []string }

func (f *fakeAudit) Emit(_ context.Context, action, _ string) { f.actions = append(f.actions, action) }

type fakeRunRefs struct{ pvID string }

func (f *fakeRunRefs) PolicyVersionIDForRun(_ context.Context, _ string) (string, error) {
	return f.pvID, nil
}

func TestCorePolicyClient_SetStatusPublishedHitsCore(t *testing.T) {
	core := &fakeCore{}
	runs := &fakeRunStatus{}
	c := NewCorePolicyClient(CorePolicyClientOpts{Core: core, Runs: runs})
	if err := c.SetStatus(context.Background(), "pv-1", "published"); err != nil {
		t.Fatalf("SetStatus: %v", err)
	}
	if len(core.calls) != 1 || core.calls[0].Status != "published" || core.calls[0].PolicyVersionId != "pv-1" {
		t.Fatalf("core calls: %+v", core.calls)
	}
	if core.calls[0].ActorUserId != "system" {
		t.Fatalf("expected system actor, got %q", core.calls[0].ActorUserId)
	}
	if len(runs.calls) != 0 {
		t.Fatalf("published should not touch run status: %+v", runs.calls)
	}
}

func TestCorePolicyClient_SetStatusApprovedHitsRunStore(t *testing.T) {
	core := &fakeCore{}
	runs := &fakeRunStatus{}
	c := NewCorePolicyClient(CorePolicyClientOpts{Core: core, Runs: runs})
	if err := c.SetStatus(context.Background(), "pv-1", "approved"); err != nil {
		t.Fatalf("SetStatus: %v", err)
	}
	if len(core.calls) != 0 {
		t.Fatalf("approved (lifecycle) must not hit core: %+v", core.calls)
	}
	if len(runs.calls) != 1 || runs.calls[0].status != "approved" {
		t.Fatalf("run calls: %+v", runs.calls)
	}
}

func TestCorePolicyClient_SetStatusApprovedScheduledMapsToScheduled(t *testing.T) {
	runs := &fakeRunStatus{}
	c := NewCorePolicyClient(CorePolicyClientOpts{Core: &fakeCore{}, Runs: runs})
	if err := c.SetStatus(context.Background(), "pv-1", "approved_scheduled"); err != nil {
		t.Fatalf("SetStatus: %v", err)
	}
	if len(runs.calls) != 1 || runs.calls[0].status != "scheduled" {
		t.Fatalf("expected scheduled run status, got %+v", runs.calls)
	}
}

func TestCorePolicyClient_PublishHitsCore(t *testing.T) {
	core := &fakeCore{}
	c := NewCorePolicyClient(CorePolicyClientOpts{Core: core})
	if err := c.Publish(context.Background(), "pv-9"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(core.calls) != 1 || core.calls[0].Status != "published" {
		t.Fatalf("core calls: %+v", core.calls)
	}
}

func TestCorePolicyClient_ResolveDecisionDelegatesToResolver(t *testing.T) {
	res := &fakeResolver{decision: "reject"}
	c := NewCorePolicyClient(CorePolicyClientOpts{Core: &fakeCore{}, Resolver: res})
	got, err := c.ResolveDecision(context.Background(), "run-7", 2)
	if err != nil {
		t.Fatalf("ResolveDecision: %v", err)
	}
	if got != "reject" {
		t.Fatalf("decision = %q, want reject", got)
	}
	if res.gotRun != "run-7" || res.gotStage != 2 {
		t.Fatalf("resolver got run=%q stage=%d", res.gotRun, res.gotStage)
	}
}

func TestCorePolicyClient_ResolveDecisionNoResolverErrors(t *testing.T) {
	c := NewCorePolicyClient(CorePolicyClientOpts{Core: &fakeCore{}})
	if _, err := c.ResolveDecision(context.Background(), "r", 0); err == nil {
		t.Fatal("expected error when no resolver is wired (never auto-approve)")
	}
}

func TestCorePolicyClient_EscalateEmitsAudit(t *testing.T) {
	au := &fakeAudit{}
	c := NewCorePolicyClient(CorePolicyClientOpts{Core: &fakeCore{}, Audit: au, RunRefs: &fakeRunRefs{pvID: "pv-x"}})
	if err := c.EscalateStage(context.Background(), "run-1", 1); err != nil {
		t.Fatalf("EscalateStage: %v", err)
	}
	if len(au.actions) != 1 || au.actions[0] != "workflow.stage.escalated" {
		t.Fatalf("audit actions: %+v", au.actions)
	}
}
