// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package saga

import (
	"context"
	"fmt"

	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/store"
	"github.com/Steward-GRC/steward-workflow/internal/tally"
)

// The stage outcomes policy.resolve_decision returns to the outcome switch.
const (
	DecisionApprove = tally.DecisionApprove
	DecisionReject  = tally.DecisionReject
	DecisionPending = tally.DecisionPending
)

type runLookup interface {
	GetRunByRunID(ctx context.Context, runID string) (store.ApprovalRun, error)
}

type defLookup interface {
	Get(ctx context.Context, id string) (builder.WorkflowDef, error)
	// GetVersion returns the definition at the version a run is pinned to.
	GetVersion(ctx context.Context, id string, version int) (builder.WorkflowDef, error)
}

type stageAssignmentLister interface {
	ListStageAssignments(ctx context.Context, pvID string, stageIdx int) ([]store.AssignmentRow, error)
}

// DecisionResolver works out a stage's outcome from its seats and the pinned
// definition's quorum, after every decision.
type DecisionResolver struct {
	runs   runLookup
	defs   defLookup
	asgmts stageAssignmentLister
}

// NewDecisionResolver returns a DecisionResolver.
func NewDecisionResolver(runs runLookup, defs defLookup, asgmts stageAssignmentLister) *DecisionResolver {
	return &DecisionResolver{runs: runs, defs: defs, asgmts: asgmts}
}

// ResolveStageDecision returns approve, reject or pending for the run's stage.
func (r *DecisionResolver) ResolveStageDecision(ctx context.Context, runID string, stageIndex int) (string, error) {
	run, err := r.runs.GetRunByRunID(ctx, runID)
	if err != nil {
		return "", fmt.Errorf("resolve decision: lookup run %q: %w", runID, err)
	}
	wd, err := r.defs.GetVersion(ctx, run.WorkflowDefID, run.WorkflowVersion)
	if err != nil {
		return "", fmt.Errorf("resolve decision: get def %q@v%d: %w", run.WorkflowDefID, run.WorkflowVersion, err)
	}
	if stageIndex < 0 || stageIndex >= len(wd.Stages) {
		return "", fmt.Errorf("resolve decision: stage_index %d out of range (def has %d stages)", stageIndex, len(wd.Stages))
	}
	rows, err := r.asgmts.ListStageAssignments(ctx, run.PolicyVersionID, stageIndex)
	if err != nil {
		return "", fmt.Errorf("resolve decision: list assignments: %w", err)
	}
	return tally.Resolve(wd.Stages[stageIndex], rows), nil
}
