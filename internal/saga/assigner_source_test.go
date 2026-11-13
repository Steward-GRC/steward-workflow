// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package saga

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/store"
)

// The seat source (steward-workflow#80): each seat records why the person holds
// it, so the gateway can label an approver "Category owner" or "Approver".

// sourcesFor runs the assigner over one stage and answers each seat's source,
// keyed "user|group".
func sourcesFor(t *testing.T, st builder.Stage, filter approverFilter, pool []any) map[string]string {
	t.Helper()
	creator := &recordingCreator{}
	wd := builder.WorkflowDef{ID: "def-1", Version: 1, Stages: []builder.Stage{st}}
	a := NewStageAssigner(fakeDefs{wd: wd}, creator, builder.CompileOptions{StageSLAHours: 72, StageReminderHours: 48})
	if filter != nil {
		a = a.WithAccessFilter(filter)
	}
	run := fakeSagaRun("run-source", baseInputs(map[string]any{"approvers_s0": pool}))
	require.NoError(t, a.AssignStage(context.Background(), run, 0))
	got := map[string]string{}
	for _, c := range creator.calls {
		got[c.UserID+"|"+c.GroupID] = c.Source
	}
	return got
}

func TestStageAssigner_NamedApproversAndGroupMembersAreSeatedAsApprovers(t *testing.T) {
	st := builder.Stage{
		Name: "Review", Quorum: builder.QuorumAny, ApproverIDs: []string{"charlie", "dave"},
		GroupUnits: []builder.GroupUnit{{GroupID: "g-finance", InternalQuorum: builder.QuorumAny, Members: []string{"erin"}}},
	}
	got := sourcesFor(t, st, nil, []any{"charlie", "dave"})
	require.Equal(t, map[string]string{
		"charlie|":       store.SeatSourceApprover,
		"dave|":          store.SeatSourceApprover,
		"erin|g-finance": store.SeatSourceApprover,
	}, got)
}

func TestStageAssigner_ABackstopOwnerIsSeatedAsACategoryOwner(t *testing.T) {
	st := builder.Stage{
		Name: "Review", Quorum: builder.QuorumNofM, QuorumN: 2, ApproverIDs: []string{"charlie", "dave", "alice"},
	}
	// charlie and alice are denied, so one vote is left for an n-of-m of 2 and
	// the owners join to make up the quorum.
	filter := fakeFilter{deny: map[string]bool{"charlie": true, "alice": true}, owners: []string{"owner"}}
	got := sourcesFor(t, st, filter, []any{"charlie", "dave", "alice"})
	require.Equal(t, map[string]string{
		"dave|":  store.SeatSourceApprover,
		"owner|": store.SeatSourceCategoryOwner,
	}, got)
}

func TestStageAssigner_AnOwnerWhoIsAlsoANamedApproverStaysAnApprover(t *testing.T) {
	st := builder.Stage{
		Name: "Review", Quorum: builder.QuorumNofM, QuorumN: 2, ApproverIDs: []string{"dave", "owner", "alice"},
	}
	filter := fakeFilter{deny: map[string]bool{"alice": true}, owners: []string{"owner", "boss"}}
	// Two named people remain, which already meets 2 of m: no backstop joins, and
	// the owner holds the seat the workflow named.
	got := sourcesFor(t, st, filter, []any{"dave", "owner", "alice"})
	require.Equal(t, store.SeatSourceApprover, got["owner|"])
	require.NotContains(t, got, "boss|")

	// With a third vote needed the backstop adds boss but never re-labels owner.
	st.QuorumN = 3
	got = sourcesFor(t, st, filter, []any{"dave", "owner", "alice"})
	require.Equal(t, store.SeatSourceApprover, got["owner|"])
	require.Equal(t, store.SeatSourceCategoryOwner, got["boss|"])
}
