// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package saga

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/fixture"
)

func groupStageAssigner(creator assignmentCreator) *StageAssigner {
	wd := builder.WorkflowDef{ID: "def-1", Version: 1, Stages: []builder.Stage{{
		Name: "Sign-off", Quorum: builder.QuorumAll, ApproverIDs: []string{fixture.Carol},
		GroupUnits: []builder.GroupUnit{{GroupID: fixture.FinanceTeam, InternalQuorum: builder.QuorumAny,
			Members: []string{fixture.Carol, fixture.Dave}}},
	}}}
	return NewStageAssigner(fakeDefs{wd: wd}, creator, builder.CompileOptions{StageSLAHours: 72, StageReminderHours: 48})
}

func TestStageAssigner_SeatsIndividualsAndGroupMembersSeparately(t *testing.T) {
	creator := &recordingCreator{}
	run := fakeSagaRun("run-1", baseInputs(map[string]any{"approvers_s0": []any{fixture.Carol}}))
	require.NoError(t, groupStageAssigner(creator).AssignStage(context.Background(), run, 0))

	got := map[string]bool{}
	for _, c := range creator.calls {
		got[c.UserID+"|"+c.GroupID] = true
	}
	require.Equal(t, map[string]bool{
		fixture.Carol + "|":                       true,
		fixture.Carol + "|" + fixture.FinanceTeam: true,
		fixture.Dave + "|" + fixture.FinanceTeam:  true,
	}, got, "Carol holds an individual seat and a group seat")
}

type denyFilter struct{ deny map[string]bool }

func (f denyFilter) Eligible(_ context.Context, _ string, candidates []string) ([]string, []string, error) {
	var out []string
	for _, c := range candidates {
		if !f.deny[c] {
			out = append(out, c)
		}
	}
	return out, []string{fixture.Grace}, nil
}

func TestStageAssigner_FilterDropsGroupMembersWhoCantRead(t *testing.T) {
	creator := &recordingCreator{}
	run := fakeSagaRun("run-1", baseInputs(map[string]any{"approvers_s0": []any{fixture.Carol}}))
	a := groupStageAssigner(creator).WithAccessFilter(denyFilter{deny: map[string]bool{fixture.Dave: true}})
	require.NoError(t, a.AssignStage(context.Background(), run, 0))
	for _, c := range creator.calls {
		require.NotEqual(t, fixture.Dave, c.UserID)
	}
	require.Len(t, creator.calls, 2)
}

func TestStageAssigner_GroupOnlyStageIsSeated(t *testing.T) {
	creator := &recordingCreator{}
	run := fakeSagaRun("run-1", baseInputs(nil))
	require.NoError(t, groupStageAssigner(creator).AssignStage(context.Background(), run, 0))
	require.Len(t, creator.calls, 2, "the group's members are seated even with no individual pool")
}
