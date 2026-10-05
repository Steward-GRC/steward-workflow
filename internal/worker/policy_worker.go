// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package worker handles the policy.* saga actions the compiled workflows
// run: set_status (with the target status in Inputs["status"]),
// resolve_decision and escalate_stage.
package worker

import (
	"context"
	"fmt"

	corev1 "github.com/Steward-GRC/steward-workflow/gen/go/thirdparty/core/v1"
	"github.com/Steward-GRC/steward-workflow/internal/statemachine"
	"google.golang.org/grpc"
)

// PolicyServiceClient is what the saga actions need from the policy side.
type PolicyServiceClient interface {
	SetStatus(ctx context.Context, policyVersionID, status string) error
	Publish(ctx context.Context, policyVersionID string) error
	// ResolveDecision returns the resolved outcome of a stage ("approve",
	// "reject" or "request_changes"). The saga engine drops the payload a human
	// submits to a manual_approval task, so the compiled outcome_switch cannot
	// read the decision from run variables directly; the policy.resolve_decision
	// action calls this to read the outcome recorded in
	// approval_assignments / quorum and feeds it back into the run as the
	// "decision" variable. Keyed by saga run id (the run ↔ policy_version
	// mapping lives in approval_runs).
	ResolveDecision(ctx context.Context, runID string, stageIndex int) (string, error)
	// EscalateStage notifies the stage's group owner that the SLA elapsed.
	EscalateStage(ctx context.Context, runID string, stageIndex int) error
}

// --- real (core-backed) policy client ---------------------------------------

// CorePolicyVersionClient is the slice of the generated core PolicyService client
// the real worker client uses. Declared as an interface so tests can inject a
// fake without dialing a real gRPC server; the generated
// corev1.PolicyServiceClient satisfies it.
type CorePolicyVersionClient interface {
	SetVersionStatus(ctx context.Context, in *corev1.SetVersionStatusRequest, opts ...grpc.CallOption) (*corev1.SetVersionStatusResponse, error)
}

// stageDecider resolves a stage's recorded outcome (approve/reject/pending) from
// approval_assignments + quorum. Implemented by saga.DecisionResolver. The real
// client delegates ResolveDecision to it so the worker-client method, even when
// the embedded engine bypasses it, is never an auto-approve stub.
type stageDecider interface {
	ResolveStageDecision(ctx context.Context, runID string, stageIndex int) (string, error)
}

// runStatusUpdater writes the approval-run lifecycle status (approval_runs).
// Implemented by store.AssignmentStore.UpdateRunStatus.
type runStatusUpdater interface {
	UpdateRunStatus(ctx context.Context, policyVersionID, status string) error
}

// runResolver maps a saga run id back to its policy version id. EscalateStage
// uses it to label the audit subject; an adapter over
// store.AssignmentStore.GetRunByRunID implements it.
type runResolver interface {
	PolicyVersionIDForRun(ctx context.Context, runID string) (string, error)
}

// auditEmitter emits an audit event for escalation. Implemented by an adapter
// over the audit emitter (see cmd/server).
type auditEmitter interface {
	Emit(ctx context.Context, action, subject string)
}

// CorePolicyClient is the production PolicyServiceClient. It writes status back
// to core and reads decisions from the approval store:
//
//   - SetStatus: a CORE version status (published/draft/superseded/archived) is
//     written to core via SetVersionStatus; an approval-lifecycle status
//     (approved/approved_scheduled/rejected/in_review/withdrawn) is written to
//     the local approval_runs row (the workflow-side source of truth). This
//     split exists because core's policy_version_status enum only has the four
//     stored statuses; the lifecycle statuses live in approval_runs.
//   - Publish: SetVersionStatus(status=published).
//   - ResolveDecision: delegates to the DecisionResolver (real recorded outcome).
//   - EscalateStage: emits an audit event (full SLA notify is a follow-up).
type CorePolicyClient struct {
	core     CorePolicyVersionClient
	runs     runStatusUpdater
	runRefs  runResolver
	resolver stageDecider
	audit    auditEmitter
	actor    string // actor_user_id stamped on core SetVersionStatus calls (system actor)
}

// CorePolicyClientOpts collects the dependencies for NewCorePolicyClient.
type CorePolicyClientOpts struct {
	Core     CorePolicyVersionClient
	Runs     runStatusUpdater
	RunRefs  runResolver
	Resolver stageDecider
	Audit    auditEmitter
	Actor    string // defaults to "system" when empty
}

// NewCorePolicyClient builds the production client.
func NewCorePolicyClient(o CorePolicyClientOpts) *CorePolicyClient {
	if o.Actor == "" {
		o.Actor = "system"
	}
	return &CorePolicyClient{
		core:     o.Core,
		runs:     o.Runs,
		runRefs:  o.RunRefs,
		resolver: o.Resolver,
		audit:    o.Audit,
		actor:    o.Actor,
	}
}

// isCoreVersionStatus reports whether status is one of core's four stored
// policy_version_status values (the only statuses SetVersionStatus accepts).
func isCoreVersionStatus(status string) bool {
	switch status {
	case "draft", "published", "superseded", "archived":
		return true
	default:
		return false
	}
}

// SetStatus writes the status back to the correct system of record. Core version
// statuses go to core; approval-lifecycle statuses update the approval_runs row.
func (c *CorePolicyClient) SetStatus(ctx context.Context, policyVersionID, status string) error {
	if isCoreVersionStatus(status) {
		_, err := c.core.SetVersionStatus(ctx, &corev1.SetVersionStatusRequest{
			PolicyVersionId: policyVersionID,
			Status:          status,
			ActorUserId:     c.actor,
		})
		if err != nil {
			return fmt.Errorf("core SetVersionStatus(%s): %w", status, err)
		}
		return nil
	}
	// Approval-lifecycle status (approved/approved_scheduled/rejected/in_review/
	// withdrawn): persist on the approval_runs row, which is the workflow-side
	// source of truth surfaced via GetStatus. "approved_scheduled" maps to the
	// "scheduled" run status; everything else passes through unchanged.
	runStatus := status
	if status == "approved_scheduled" {
		runStatus = "scheduled"
	}
	if c.runs == nil {
		return nil
	}
	if err := c.runs.UpdateRunStatus(ctx, policyVersionID, runStatus); err != nil {
		return fmt.Errorf("update run status(%s): %w", runStatus, err)
	}
	return nil
}

// Publish flips the policy version to published in core.
func (c *CorePolicyClient) Publish(ctx context.Context, policyVersionID string) error {
	_, err := c.core.SetVersionStatus(ctx, &corev1.SetVersionStatusRequest{
		PolicyVersionId: policyVersionID,
		Status:          "published",
		ActorUserId:     c.actor,
	})
	if err != nil {
		return fmt.Errorf("core Publish: %w", err)
	}
	return nil
}

// ResolveDecision delegates to the DecisionResolver, which reads the recorded
// approve/reject/pending outcome from approval_assignments + stage quorum. It
// NEVER auto-approves. In the embedded-engine wiring the engine calls the
// resolver directly (so this method is normally not on the hot path), but it is
// implemented correctly here so any caller that does go through the
// PolicyServiceClient gets the real outcome.
func (c *CorePolicyClient) ResolveDecision(ctx context.Context, runID string, stageIndex int) (string, error) {
	if c.resolver == nil {
		return "", fmt.Errorf("resolve_decision: no decision resolver wired")
	}
	return c.resolver.ResolveStageDecision(ctx, runID, stageIndex)
}

// EscalateStage records an SLA escalation as an audit event. Notifying the
// category's owners is not built yet.
func (c *CorePolicyClient) EscalateStage(ctx context.Context, runID string, stageIndex int) error {
	pvID := runID
	if c.runRefs != nil {
		if id, err := c.runRefs.PolicyVersionIDForRun(ctx, runID); err == nil && id != "" {
			pvID = id
		}
	}
	if c.audit != nil {
		c.audit.Emit(ctx, "workflow.stage.escalated",
			fmt.Sprintf("policy_version:%s stage:%d run:%s", pvID, stageIndex, runID))
	}
	return nil
}

// Compile-time: CorePolicyClient must satisfy the worker client interface.
var _ PolicyServiceClient = (*CorePolicyClient)(nil)

// HandleAction is the transport-agnostic core for every policy.* saga action.
// Invoked by the embedded in-process action verb (internal/saga). runID + inputs
// come from the engine's action step; the returned map is merged into the run's
// variables by the engine.
func HandleAction(ctx context.Context, client PolicyServiceClient, action, runID string, inputs map[string]any) (map[string]any, error) {
	switch action {
	case "policy.set_status":
		pvID, _ := inputs["policy_version_id"].(string)
		status, _ := inputs["status"].(string)
		currentStatus, _ := inputs["current_status"].(string) // injected by the saga run
		if pvID == "" || status == "" {
			return nil, fmt.Errorf("set_status: missing policy_version_id or status")
		}
		// Guard the transition using the approval state machine to catch invalid saga routing.
		if currentStatus != "" {
			event := statemachine.StatusToEvent(statemachine.Status(status))
			if _, err := statemachine.Transition(statemachine.Status(currentStatus), event); err != nil {
				return nil, fmt.Errorf("set_status: transition %s→%s not allowed: %w", currentStatus, status, err)
			}
		}
		if err := client.SetStatus(ctx, pvID, status); err != nil {
			return nil, fmt.Errorf("SetStatus(%s): %w", status, err)
		}
		return map[string]any{"ok": true, "status": status}, nil

	case "policy.resolve_decision":
		stageIdx := inputInt(inputs["stage_index"])
		decision, err := client.ResolveDecision(ctx, runID, stageIdx)
		if err != nil {
			return nil, fmt.Errorf("ResolveDecision(stage=%d): %w", stageIdx, err)
		}
		switch decision {
		case "approve", "reject", "request_changes", "pending":
			// valid; "pending" loops the stage back to await the next approver.
		default:
			return nil, fmt.Errorf("resolve_decision returned %q (want approve/reject/request_changes/pending)", decision)
		}
		return map[string]any{"decision": decision}, nil

	case "policy.escalate_stage":
		stageIdx := inputInt(inputs["stage_index"])
		if err := client.EscalateStage(ctx, runID, stageIdx); err != nil {
			return nil, fmt.Errorf("EscalateStage(stage=%d): %w", stageIdx, err)
		}
		return map[string]any{"ok": true}, nil

	default:
		return nil, fmt.Errorf("unknown policy action %q", action)
	}
}

// inputInt coerces a step-input value to int. JSON-decoded action payloads carry
// numbers as float64; compile-time literals may arrive as int. Unknown → 0.
func inputInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	default:
		return 0
	}
}
