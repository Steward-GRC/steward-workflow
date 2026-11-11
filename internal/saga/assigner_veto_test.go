// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package saga

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Steward-GRC/steward-workflow/internal/builder"
)

// The veto seat (steward-workflow#26). Each test names the mock server test it
// mirrors (steward-mock-server src/product/veto.test.ts).

func vetoAssigner(creator assignmentCreator) *StageAssigner {
	wd := builder.WorkflowDef{ID: "def-1", Version: 1, Stages: []builder.Stage{{
		Name: "Review", Quorum: builder.QuorumAny, ApproverIDs: []string{"charlie", "dave"},
		VetoApproverIDs: []string{"dave"},
		VetoGroupIDs:    []string{"g-finance"},
		GroupUnits: []builder.GroupUnit{
			{GroupID: "g-finance", InternalQuorum: builder.QuorumAny, Members: []string{"erin", "frank"}},
			{GroupID: "g-legal", InternalQuorum: builder.QuorumAny, Members: []string{"gina"}},
		},
	}}}
	return NewStageAssigner(fakeDefs{wd: wd}, creator, builder.CompileOptions{StageSLAHours: 72, StageReminderHours: 48})
}

func vetoSeatsFor(t *testing.T, enabled any) map[string]bool {
	t.Helper()
	creator := &recordingCreator{}
	extra := map[string]any{"approvers_s0": []any{"charlie", "dave"}}
	if enabled != nil {
		extra["veto_enabled"] = enabled
	}
	run := fakeSagaRun("run-veto", baseInputs(extra))
	require.NoError(t, vetoAssigner(creator).AssignStage(context.Background(), run, 0))
	got := map[string]bool{}
	for _, c := range creator.calls {
		got[c.UserID+"|"+c.GroupID] = c.Veto
	}
	return got
}

// Mock: "carries the marks into a run only when the workflow has opted in" (an
// opted-in run: users and the members of marked group seats hold the veto).
func TestStageAssigner_MarksVetoSeatsWhenTheWorkflowOptedIn(t *testing.T) {
	require.Equal(t, map[string]bool{
		"charlie|":        false,
		"dave|":           true,
		"erin|g-finance":  true,
		"frank|g-finance": true,
		"gina|g-legal":    false,
	}, vetoSeatsFor(t, true))
}

// Mock: the same call, not opted in: no seat carries a mark, and with no flag
// everything behaves as before.
func TestStageAssigner_MarksNoVetoSeatsWithoutTheOptIn(t *testing.T) {
	for _, enabled := range []any{false, nil} {
		for seat, veto := range vetoSeatsFor(t, enabled) {
			require.False(t, veto, seat)
		}
	}
}
