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

// seatsOf renders the recorded seats as "user" (individual) or "user|group".
func seatsOf(creator *recordingCreator) []string {
	out := make([]string, 0, len(creator.calls))
	for _, c := range creator.calls {
		out = append(out, c.UserID+"|"+c.GroupID)
	}
	return out
}

// mixedNofM is an n-of-m stage with the given individual pool and one group unit.
func mixedNofM(n int, pool, members []string, creator assignmentCreator) *StageAssigner {
	wd := builder.WorkflowDef{ID: "def-1", Version: 1, Stages: []builder.Stage{{
		Name: "Mixed", Quorum: builder.QuorumNofM, QuorumN: n, ApproverIDs: pool,
		GroupUnits: []builder.GroupUnit{{GroupID: fixture.FinanceTeam, InternalQuorum: builder.QuorumAny, Members: members}},
	}}}
	return NewStageAssigner(fakeDefs{wd: wd}, creator, builder.CompileOptions{StageSLAHours: 72, StageReminderHours: 48})
}

// Mock submit-seats.test.ts: "leaves a unit with no reader out of the run".
func TestStageAssigner_UnitWithNoReaderGetsNoSeats(t *testing.T) {
	creator := &recordingCreator{}
	run := fakeSagaRun("run-1", baseInputs(map[string]any{"approvers_s0": []any{fixture.Carol}}))
	wd := builder.WorkflowDef{ID: "def-1", Version: 1, Stages: []builder.Stage{{
		Name: "Sign-off", Quorum: builder.QuorumAll, ApproverIDs: []string{fixture.Carol},
		GroupUnits: []builder.GroupUnit{{GroupID: fixture.FinanceTeam, InternalQuorum: builder.QuorumAny,
			Members: []string{fixture.Dave}}},
	}}}
	a := NewStageAssigner(fakeDefs{wd: wd}, creator, builder.CompileOptions{StageSLAHours: 72, StageReminderHours: 48}).
		WithAccessFilter(fakeFilter{deny: map[string]bool{fixture.Dave: true}, owners: []string{fixture.Alice}})
	require.NoError(t, a.AssignStage(context.Background(), run, 0))
	require.Equal(t, []string{fixture.Carol + "|"}, seatsOf(creator),
		"the unit with no reader is left out; quorum all is met by Carol alone, so no owner joins")
}

// Mock: "seats the owners as individuals when the readers left cannot meet N".
func TestStageAssigner_OwnersJoinWhenReadersLeftCannotMeetN(t *testing.T) {
	creator := &recordingCreator{}
	run := fakeSagaRun("run-1", baseInputs(map[string]any{"approvers_s0": []any{fixture.Carol, fixture.Frank}}))
	a := mixedNofM(2, []string{fixture.Carol, fixture.Frank}, []string{fixture.Erin}, creator).
		WithAccessFilter(fakeFilter{deny: map[string]bool{fixture.Frank: true, fixture.Erin: true}, owners: []string{fixture.Alice}})
	require.NoError(t, a.AssignStage(context.Background(), run, 0))
	// Carol is one vote; the unit has no reader and Frank cannot read, so Alice joins.
	require.Equal(t, []string{fixture.Carol + "|", fixture.Alice + "|"}, seatsOf(creator))
}

// Mock: "counts a unit with a reader as a vote, so no owner joins".
func TestStageAssigner_UnitWithAReaderCountsAsAVote(t *testing.T) {
	creator := &recordingCreator{}
	run := fakeSagaRun("run-1", baseInputs(map[string]any{"approvers_s0": []any{fixture.Carol, fixture.Frank}}))
	a := mixedNofM(2, []string{fixture.Carol, fixture.Frank}, []string{fixture.Dave}, creator).
		WithAccessFilter(fakeFilter{deny: map[string]bool{fixture.Frank: true}, owners: []string{fixture.Alice}})
	require.NoError(t, a.AssignStage(context.Background(), run, 0))
	require.ElementsMatch(t, []string{fixture.Carol + "|", fixture.Dave + "|" + fixture.FinanceTeam}, seatsOf(creator),
		"Carol and the unit make two votes, so no owner joins")
}
