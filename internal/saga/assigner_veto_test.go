// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package saga

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Steward-GRC/steward-workflow/internal/builder"
)

// The veto seat, per stage (steward-workflow#26, #64). Each test names the mock
// server test it mirrors (steward-mock-server src/product/veto.test.ts, "a run's
// veto holders" and "two veto people at one stage").

func vetoStage(mut func(*builder.Stage)) builder.Stage {
	st := builder.Stage{
		Name: "Review", Quorum: builder.QuorumAny, ApproverIDs: []string{"charlie", "dave"},
		VetoUserIDs: []string{"dave"},
		GroupUnits: []builder.GroupUnit{
			{GroupID: "g-finance", InternalQuorum: builder.QuorumAny, Members: []string{"erin", "frank"}},
			{GroupID: "g-legal", InternalQuorum: builder.QuorumAny, Members: []string{"gina"}},
		},
	}
	if mut != nil {
		mut(&st)
	}
	return st
}

// vetoSeatsFor runs the assigner over one stage and answers which seats carry a
// veto, keyed "user|group". extra are added to the run inputs.
func vetoSeatsFor(t *testing.T, st builder.Stage, filter approverFilter, extra map[string]any) map[string]bool {
	t.Helper()
	creator := &recordingCreator{}
	wd := builder.WorkflowDef{ID: "def-1", Version: 1, Stages: []builder.Stage{st}}
	a := NewStageAssigner(fakeDefs{wd: wd}, creator, builder.CompileOptions{StageSLAHours: 72, StageReminderHours: 48})
	if filter != nil {
		a = a.WithAccessFilter(filter)
	}
	in := map[string]any{"approvers_s0": []any{"charlie", "dave"}}
	for k, v := range extra {
		in[k] = v
	}
	run := fakeSagaRun("run-veto", baseInputs(in))
	require.NoError(t, a.AssignStage(context.Background(), run, 0))
	got := map[string]bool{}
	for _, c := range creator.calls {
		got[c.UserID+"|"+c.GroupID] = c.Veto
	}
	return got
}

func holders(seats map[string]bool) []string {
	var out []string
	for seat, veto := range seats {
		if veto {
			out = append(out, seat)
		}
	}
	return out
}

// Mock: "are the stage's named people who are seated" (no workflow switch is
// pinned into the run, so a named person holds the veto by being on the list).
func TestStageAssigner_NamedPeopleHoldTheVetoWithoutAnyWorkflowSwitch(t *testing.T) {
	require.Equal(t, map[string]bool{
		"charlie|":        false,
		"dave|":           true,
		"erin|g-finance":  false,
		"frank|g-finance": false,
		"gina|g-legal":    false,
	}, vetoSeatsFor(t, vetoStage(nil), nil, nil))
}

// Mock: "apply without any workflow switch": a run pinned with the old flag off
// still seats the named person with a veto.
func TestStageAssigner_TheOldPinnedFlagGatesNothing(t *testing.T) {
	for _, flag := range []any{false, true, nil} {
		seats := vetoSeatsFor(t, vetoStage(nil), nil, map[string]any{"veto_enabled": flag})
		require.Equal(t, []string{"dave|"}, holders(seats), "flag %v", flag)
	}
}

// A member of a seated group unit is in the pool, so a named group member holds
// the veto on their group seat.
func TestStageAssigner_ANamedGroupMemberHoldsTheVeto(t *testing.T) {
	st := vetoStage(func(s *builder.Stage) { s.VetoUserIDs = []string{"frank"} })
	require.Equal(t, []string{"frank|g-finance"}, holders(vetoSeatsFor(t, st, nil, nil)))
}

// A person who sits on an individual seat and a group seat holds the veto on
// both: a reject on either seat is the same person's veto.
func TestStageAssigner_APersonOnTwoSeatsHoldsTheVetoOnBoth(t *testing.T) {
	st := vetoStage(func(s *builder.Stage) {
		s.VetoUserIDs = []string{"dave"}
		s.GroupUnits[0].Members = []string{"dave", "frank"}
	})
	require.ElementsMatch(t, []string{"dave|", "dave|g-finance"}, holders(vetoSeatsFor(t, st, nil, nil)))
}

// Mock: "two veto people at one stage": both are marked, anyone else is not.
func TestStageAssigner_TwoPeopleHoldTheVetoAtOneStage(t *testing.T) {
	st := vetoStage(func(s *builder.Stage) {
		s.ApproverIDs = []string{"charlie", "dave", "alice"}
		s.VetoUserIDs = []string{"charlie", "dave"}
	})
	seats := vetoSeatsFor(t, st, nil, map[string]any{"approvers_s0": []any{"charlie", "dave", "alice"}})
	require.ElementsMatch(t, []string{"charlie|", "dave|"}, holders(seats))
	require.False(t, seats["alice|"])
}

// Mock: "do not include a named person who is not in the pool at seating": the
// pool for this lineage is [charlie, dave]; erin is only listed for another
// category, so she holds nothing and nothing is raised.
func TestStageAssigner_ANamedPersonOutsideThePoolHoldsNoVeto(t *testing.T) {
	st := vetoStage(func(s *builder.Stage) { s.VetoUserIDs = []string{"zed", "dave"} })
	require.Equal(t, []string{"dave|"}, holders(vetoSeatsFor(t, st, nil, nil)))
}

// Mock: "do not include a named person who is not in the pool", via the access
// filter: a vetoer the category rules do not let approve is not seated.
func TestStageAssigner_AVetoerTheAccessFilterDropsHoldsNoVeto(t *testing.T) {
	st := vetoStage(func(s *builder.Stage) { s.VetoUserIDs = []string{"charlie", "dave"} })
	filter := fakeFilter{deny: map[string]bool{"dave": true}, owners: []string{"owner"}}
	seats := vetoSeatsFor(t, st, filter, nil)
	require.Equal(t, []string{"charlie|"}, holders(seats))
	require.NotContains(t, seats, "dave|")
}

// Mock: "never include site admin or root who is not in the pool": owners and
// root join as a backstop only to make up the quorum, and hold no veto even if
// the list names them.
func TestStageAssigner_BackstopOwnersAndRootNeverHoldAVeto(t *testing.T) {
	st := vetoStage(func(s *builder.Stage) {
		s.Quorum, s.QuorumN, s.GroupUnits = builder.QuorumNofM, 2, nil
		s.ApproverIDs = []string{"charlie", "dave", "alice"}
		s.VetoUserIDs = []string{"dave", "owner", "root"}
	})
	// charlie and alice are denied, so one vote is left for an n-of-m of 2.
	filter := fakeFilter{deny: map[string]bool{"charlie": true, "alice": true}, owners: []string{"owner", "root"}}
	seats := vetoSeatsFor(t, st, filter, map[string]any{"approvers_s0": []any{"charlie", "dave", "alice"}})
	require.Contains(t, seats, "owner|", "the owner was seated to make up the quorum")
	require.Contains(t, seats, "root|")
	require.Equal(t, []string{"dave|"}, holders(seats))
}

// Mock: "are empty for a stage without veto people".
func TestStageAssigner_AStageWithoutVetoPeopleHasNoVetoSeats(t *testing.T) {
	st := vetoStage(func(s *builder.Stage) { s.VetoUserIDs = nil })
	require.Empty(t, holders(vetoSeatsFor(t, st, nil, nil)))
}

// Mock: "the old switch and lists are accepted and ignored": stored marks from
// an older build seat nobody with a veto, with the old flag on.
func TestStageAssigner_TheOldMarksAreIgnored(t *testing.T) {
	st := vetoStage(func(s *builder.Stage) {
		s.VetoUserIDs = nil
		s.VetoApproverIDs = []string{"dave"}
		s.VetoGroupIDs = []string{"g-finance"}
	})
	require.Empty(t, holders(vetoSeatsFor(t, st, nil, map[string]any{"veto_enabled": true})))
}
