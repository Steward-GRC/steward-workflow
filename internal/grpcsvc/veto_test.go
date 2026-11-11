// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	workflowv1 "github.com/Steward-GRC/steward-workflow/gen/go/steward/workflow/v1"
	"github.com/Steward-GRC/steward-workflow/internal/builder"
)

// The veto seat (steward-workflow#26). Each test names the mock server test it
// mirrors (steward-mock-server src/product/veto.test.ts).

// vetoServer is the mock's openRun: one stage where any one of three approvers
// is enough, so a lone reject is only a vote, with dave holding the veto.
func vetoServer(t *testing.T, vetoUsers ...string) *WorkflowServer {
	t.Helper()
	srv := reviewServer(t, builder.QuorumAny, 0,
		map[string]string{"charlie": "pending", "alice": "pending", "dave": "pending"})
	as := srv.assignments.(*stubAssignments)
	for _, u := range vetoUsers {
		k := keyOf(reviewPV, 0, u)
		row := as.rows[k]
		row.Veto = true
		as.rows[k] = row
	}
	return srv
}

func vetoStatus(t *testing.T, srv *WorkflowServer) *workflowv1.GetStatusResponse {
	t.Helper()
	resp, err := srv.GetStatus(context.Background(), &workflowv1.GetStatusRequest{PolicyVersionId: reviewPV})
	require.NoError(t, err)
	return resp
}

// Mock: "a reject by a veto approver ends the version as rejected although the
// quorum could still be reached".
func TestVeto_RejectEndsTheRunAlthoughTheQuorumCouldBeReached(t *testing.T) {
	srv := vetoServer(t, "dave")
	require.NoError(t, reviewSignal(srv, "dave", workflowv1.SignalType_SIGNAL_TYPE_REJECT, "No."))
	require.Equal(t, "rejected", reviewSeat(srv, "dave"))
	require.NotEmpty(t, srv.sagaClient.(*stubSagaClient).cancelledRuns, "the run ends at once")
}

// Mock: "cancels the other seats".
func TestVeto_RejectCancelsTheOtherSeats(t *testing.T) {
	srv := vetoServer(t, "dave")
	require.NoError(t, reviewSignal(srv, "dave", workflowv1.SignalType_SIGNAL_TYPE_REJECT, "No."))
	require.Equal(t, "superseded", reviewSeat(srv, "charlie"))
	require.Equal(t, "superseded", reviewSeat(srv, "alice"))
	require.Equal(t, "rejected", reviewSeat(srv, "dave"))
}

// Mock: "shows which seats hold a veto", and the run record says whose veto
// ended it.
func TestVeto_GetStatusShowsWhichSeatsHoldAVetoAndWhoVetoed(t *testing.T) {
	srv := vetoServer(t, "dave")
	resp := vetoStatus(t, srv)
	veto := map[string]bool{}
	for _, a := range resp.GetStageAssignees()[0].GetAssignees() {
		veto[a.GetUserId()] = a.GetVeto()
	}
	require.Equal(t, map[string]bool{"charlie": false, "alice": false, "dave": true}, veto)
	require.Empty(t, resp.GetVetoedBy(), "nobody has vetoed yet")

	require.NoError(t, reviewSignal(srv, "dave", workflowv1.SignalType_SIGNAL_TYPE_REJECT, "No."))
	require.Equal(t, "dave", vetoStatus(t, srv).GetVetoedBy())
}

// Mock: "a reject by anyone else is only a vote while the quorum can still be
// reached".
func TestVeto_RejectByAnyoneElseStaysAVote(t *testing.T) {
	srv := vetoServer(t, "dave")
	require.NoError(t, reviewSignal(srv, "charlie", workflowv1.SignalType_SIGNAL_TYPE_REJECT, "No."))
	require.Empty(t, srv.sagaClient.(*stubSagaClient).cancelledRuns, "the run goes on")
	require.Equal(t, "pending", reviewSeat(srv, "alice"))
	require.Equal(t, "pending", reviewSeat(srv, "dave"))
	require.Empty(t, vetoStatus(t, srv).GetVetoedBy())
}

// With no veto flag everything behaves as before: a reject by dave is a vote.
func TestVeto_WithoutTheFlagARejectIsAVote(t *testing.T) {
	srv := vetoServer(t)
	require.NoError(t, reviewSignal(srv, "dave", workflowv1.SignalType_SIGNAL_TYPE_REJECT, "No."))
	require.Empty(t, srv.sagaClient.(*stubSagaClient).cancelledRuns)
	require.Equal(t, "pending", reviewSeat(srv, "charlie"))
}

func setVetoReq(id string, enabled bool, actor string) *workflowv1.SetWorkflowVetoEnabledRequest {
	return &workflowv1.SetWorkflowVetoEnabledRequest{Id: id, Enabled: enabled, ActorUserId: actor}
}

// Mock: "opting a workflow in is off by default and set by a template admin".
func TestVeto_OptInIsOffByDefaultAndSetByAnAdmin(t *testing.T) {
	fds := newFakeDefStore()
	fds.defs["wf-2"] = builder.WorkflowDef{ID: "wf-2", Name: "Review", Version: 1,
		Stages: []builder.Stage{{Name: "s", Quorum: builder.QuorumAny, ApproverIDs: []string{"u1"}}}}
	srv := newServerWithFakeDefStore(fds)
	srv.admins = stubAdmins{admins: map[string]bool{"grace": true}}

	got, err := srv.GetWorkflowDef(context.Background(), &workflowv1.GetWorkflowDefRequest{Id: "wf-2"})
	require.NoError(t, err)
	require.False(t, got.GetDef().GetVetoEnabled())

	_, err = srv.SetWorkflowVetoEnabled(context.Background(), setVetoReq("wf-2", true, "bob"))
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.False(t, fds.defs["wf-2"].VetoEnabled)

	done, err := srv.SetWorkflowVetoEnabled(context.Background(), setVetoReq("wf-2", true, "grace"))
	require.NoError(t, err)
	require.True(t, done.GetDef().GetVetoEnabled())
	require.True(t, fds.defs["wf-2"].VetoEnabled)
}

// Mock: "marks users and group seats on a stage", and saving stages leaves the
// opt-in alone.
func TestVeto_StageMarksRoundTripThroughUpdate(t *testing.T) {
	fds := newFakeDefStore()
	fds.defs["wf-1"] = builder.WorkflowDef{ID: "wf-1", Name: "Finance", Version: 1, VetoEnabled: true,
		Stages: []builder.Stage{{Name: "s", Quorum: builder.QuorumAny, ApproverIDs: []string{"u1"}}}}
	srv := newServerWithFakeDefStore(fds)
	stage := stageWithApprovers("Finance sign-off", "user-charlie")
	stage.VetoApproverIds = []string{"user-charlie"}
	stage.VetoGroupIds = []string{"group-finance"}

	resp, err := srv.UpdateWorkflowDef(context.Background(), &workflowv1.UpdateWorkflowDefRequest{
		Id: "wf-1", Name: "Finance sign-off", Stages: []*workflowv1.WorkflowStage{stage}, ActorUserId: "grace",
	})
	require.NoError(t, err)
	got := resp.GetDef().GetStages()[0]
	require.Equal(t, []string{"user-charlie"}, got.GetVetoApproverIds())
	require.Equal(t, []string{"group-finance"}, got.GetVetoGroupIds())
	require.True(t, fds.defs["wf-1"].VetoEnabled, "an update keeps the opt-in")
}

// Mock: "carries the marks into a run only when the workflow has opted in"
// (the submit half: the run is pinned to the opt-in it was submitted under).
func TestVeto_SubmitCarriesTheOptInIntoTheRun(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		srv := newTestServer()
		srv.defStore = &stubWorkflowDefStore{wd: builder.WorkflowDef{
			ID: "def-1", Name: "Finance", Version: 1, VetoEnabled: enabled,
			Stages: []builder.Stage{{Name: "s", Quorum: builder.QuorumAny, ApproverIDs: []string{"u1"}, VetoApproverIDs: []string{"u1"}}},
		}}
		_, err := srv.Submit(context.Background(), &workflowv1.SubmitRequest{
			PolicyVersionId: "pv-1", PolicyId: "pol-1", SubmittedBy: "bob",
		})
		require.NoError(t, err)
		require.Equal(t, enabled, srv.sagaClient.(*stubSagaClient).startedInputs["veto_enabled"])
	}
}
