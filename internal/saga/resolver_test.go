// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package saga

import (
	"context"
	"fmt"
	"testing"

	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/store"
)

type fakeRuns struct {
	defID   string
	version int
}

func (f fakeRuns) GetRunByRunID(_ context.Context, runID string) (store.ApprovalRun, error) {
	return store.ApprovalRun{RunID: runID, PolicyVersionID: "pv-1", WorkflowDefID: f.defID, WorkflowVersion: f.version}, nil
}

// fakeVersionedDefs holds multiple versions keyed by version number.
// Version 0 in the map means "the current (latest) version" returned by Get.
type fakeVersionedDefs struct {
	versions map[int]builder.WorkflowDef // version -> def; 0 = current
}

func (f fakeVersionedDefs) Get(_ context.Context, _ string) (builder.WorkflowDef, error) {
	wd, ok := f.versions[0]
	if !ok {
		return builder.WorkflowDef{}, fmt.Errorf("not found")
	}
	return wd, nil
}

func (f fakeVersionedDefs) GetVersion(_ context.Context, _ string, version int) (builder.WorkflowDef, error) {
	wd, ok := f.versions[version]
	if !ok {
		return builder.WorkflowDef{}, fmt.Errorf("version %d not found", version)
	}
	return wd, nil
}

// fakeDefs is retained for the existing resolverFor helper (single-version tests).
type fakeDefs struct{ wd builder.WorkflowDef }

func (f fakeDefs) Get(_ context.Context, _ string) (builder.WorkflowDef, error) { return f.wd, nil }

func (f fakeDefs) GetVersion(_ context.Context, _ string, _ int) (builder.WorkflowDef, error) {
	return f.wd, nil
}

type fakeAssignments struct{ rows []store.AssignmentRow }

func (f fakeAssignments) ListStageAssignments(_ context.Context, _ string, _ int) ([]store.AssignmentRow, error) {
	return f.rows, nil
}

func row(state string) store.AssignmentRow { return store.AssignmentRow{State: state} }

func resolverFor(quorum builder.Quorum, n int, rows []store.AssignmentRow) *DecisionResolver {
	wd := builder.WorkflowDef{
		ID: "def-1", Version: 1, Stages: []builder.Stage{{Name: "s0", Quorum: quorum, QuorumN: n,
			ApproverIDs: []string{"u0"}}},
	}
	return NewDecisionResolver(fakeRuns{defID: "def-1", version: 1}, fakeDefs{wd: wd}, fakeAssignments{rows: rows})
}

func TestResolver(t *testing.T) {
	cases := []struct {
		name   string
		quorum builder.Quorum
		n      int
		rows   []store.AssignmentRow
		want   string
	}{
		{"any: one approval", builder.QuorumAny, 0, []store.AssignmentRow{row("approved"), row("pending")}, DecisionApprove},
		{"any: none yet", builder.QuorumAny, 0, []store.AssignmentRow{row("pending"), row("pending")}, DecisionPending},
		{"any rejection short-circuits", builder.QuorumAny, 0, []store.AssignmentRow{row("rejected"), row("approved")}, DecisionReject},
		{"all: not all approved", builder.QuorumAll, 0, []store.AssignmentRow{row("approved"), row("pending")}, DecisionPending},
		{"all: every active approved", builder.QuorumAll, 0, []store.AssignmentRow{row("approved"), row("approved")}, DecisionApprove},
		{"all: superseded excluded from pool", builder.QuorumAll, 0, []store.AssignmentRow{row("approved"), row("superseded")}, DecisionApprove},
		{"nofm: 2 of 3 reached", builder.QuorumNofM, 2, []store.AssignmentRow{row("approved"), row("approved"), row("pending")}, DecisionApprove},
		{"nofm: only 1 of 2 needed-2", builder.QuorumNofM, 2, []store.AssignmentRow{row("approved"), row("pending")}, DecisionPending},
		{"majority: 2 of 3", builder.QuorumMajority, 0, []store.AssignmentRow{row("approved"), row("approved"), row("pending")}, DecisionApprove},
		{"majority: 1 of 3 not yet", builder.QuorumMajority, 0, []store.AssignmentRow{row("approved"), row("pending"), row("pending")}, DecisionPending},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolverFor(tc.quorum, tc.n, tc.rows).ResolveStageDecision(context.Background(), "run-1", 0)
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestResolver_UsePinnedVersion asserts that a run pinned to version 1 resolves
// its quorum against the v1 stage definition even after the def has been edited
// (v2 has a different quorum policy): in-flight runs must not silently adopt the new def after an edit.
func TestResolver_UsePinnedVersion(t *testing.T) {
	// v1: QuorumAny (one approval = approve)
	// v2 (current): QuorumAll  (both approvers must approve)
	v1 := builder.WorkflowDef{
		ID: "def-pinned", Version: 1,
		Stages: []builder.Stage{{Name: "review-v1", Quorum: builder.QuorumAny, ApproverIDs: []string{"u1", "u2"}}},
	}
	v2 := builder.WorkflowDef{
		ID: "def-pinned", Version: 2,
		Stages: []builder.Stage{{Name: "review-v2", Quorum: builder.QuorumAll, ApproverIDs: []string{"u1", "u2"}}},
	}

	defs := fakeVersionedDefs{versions: map[int]builder.WorkflowDef{
		0: v2, // current version = v2
		1: v1,
		2: v2,
	}}

	// The run was started when version was 1.
	runs := fakeRuns{defID: "def-pinned", version: 1}
	// Only u1 has approved; u2 is still pending.
	asgmts := fakeAssignments{rows: []store.AssignmentRow{row("approved"), row("pending")}}

	resolver := NewDecisionResolver(runs, defs, asgmts)
	got, err := resolver.ResolveStageDecision(context.Background(), "run-v1-pinned", 0)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	// Under v1 (QuorumAny): one approval → approve.
	// Under v2 (QuorumAll): one of two approved → pending.
	// The resolver MUST use v1 (pinned), so the result must be "approve".
	if got != DecisionApprove {
		t.Fatalf("resolver should use pinned v1 (QuorumAny); got %q, want %q", got, DecisionApprove)
	}
}
