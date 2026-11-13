// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc

import (
	"testing"

	"github.com/stretchr/testify/require"

	workflowv1 "github.com/Steward-GRC/steward-workflow/gen/go/steward/workflow/v1"
	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/store"
)

// The seat source (steward-workflow#80): GetStatus tells the gateway why each
// person holds a seat.

func sourcesOf(resp *workflowv1.GetStatusResponse) map[string]workflowv1.SeatSource {
	out := map[string]workflowv1.SeatSource{}
	for _, a := range resp.GetStageAssignees()[0].GetAssignees() {
		out[a.GetUserId()] = a.GetSource()
	}
	return out
}

func TestSeatSource_GetStatusReturnsEachSeatsSource(t *testing.T) {
	srv := reviewServer(t, builder.QuorumAny, 0,
		map[string]string{"charlie": "pending", "owner": "pending", "old": "pending"})
	as := srv.assignments.(*stubAssignments)
	for user, source := range map[string]string{
		"charlie": store.SeatSourceApprover,
		"owner":   store.SeatSourceCategoryOwner,
		"old":     "", // a seat made before the source was recorded
	} {
		k := keyOf(reviewPV, 0, user)
		row := as.rows[k]
		row.Source = source
		as.rows[k] = row
	}
	require.Equal(t, map[string]workflowv1.SeatSource{
		"charlie": workflowv1.SeatSource_SEAT_SOURCE_APPROVER,
		"owner":   workflowv1.SeatSource_SEAT_SOURCE_CATEGORY_OWNER,
		"old":     workflowv1.SeatSource_SEAT_SOURCE_UNSPECIFIED,
	}, sourcesOf(vetoStatus(t, srv)))
}

// A stage not entered yet has no seats; the preview lists the people the
// definition names, who will hold the seats as approvers.
func TestSeatSource_APreviewedStageNamesApprovers(t *testing.T) {
	srv := reviewServer(t, builder.QuorumAny, 0, nil)
	srv.defStore = &stubWorkflowDefStore{wd: builder.WorkflowDef{
		ID: "def-1", Name: "Review", Version: 1,
		Stages: []builder.Stage{{
			Name: "Review", Quorum: builder.QuorumAny, ApproverIDs: []string{"charlie"},
			GroupUnits: []builder.GroupUnit{{GroupID: "g1", InternalQuorum: builder.QuorumAny, Members: []string{"erin"}}},
		}},
	}}
	require.Equal(t, map[string]workflowv1.SeatSource{
		"charlie": workflowv1.SeatSource_SEAT_SOURCE_APPROVER,
		"erin":    workflowv1.SeatSource_SEAT_SOURCE_APPROVER,
	}, sourcesOf(vetoStatus(t, srv)))
}
