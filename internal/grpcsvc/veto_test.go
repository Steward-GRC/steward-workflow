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

func vetoSaveServer() (*WorkflowServer, *fakeDefStore) {
	fds := newFakeDefStore()
	fds.defs["wf-1"] = builder.WorkflowDef{ID: "wf-1", Name: "Finance", Version: 1,
		Stages: []builder.Stage{{Name: "s", Quorum: builder.QuorumAny, ApproverIDs: []string{"u1"}}}}
	return newServerWithFakeDefStore(fds), fds
}

func vetoUpdate(srv *WorkflowServer, stages ...*workflowv1.WorkflowStage) (*workflowv1.UpdateWorkflowDefResponse, error) {
	return srv.UpdateWorkflowDef(context.Background(), &workflowv1.UpdateWorkflowDefRequest{
		Id: "wf-1", Name: "Finance sign-off", Stages: stages, ActorUserId: "grace",
	})
}

func vetoStage2(name string, mut func(*workflowv1.WorkflowStage)) *workflowv1.WorkflowStage {
	st := stageWithApprovers(name, "user-charlie", "user-dave")
	if mut != nil {
		mut(st)
	}
	return st
}

// Mock: "stores a list of people per stage and answers it".
func TestVeto_StageVetoPeopleRoundTripThroughUpdate(t *testing.T) {
	srv, fds := vetoSaveServer()
	resp, err := vetoUpdate(srv,
		vetoStage2("First", func(s *workflowv1.WorkflowStage) { s.VetoUserIds = []string{"user-charlie", "user-dave"} }),
		vetoStage2("Second", func(s *workflowv1.WorkflowStage) { s.VetoUserIds = []string{"user-dave"} }),
		vetoStage2("Third", nil),
	)
	require.NoError(t, err)
	var got [][]string
	for _, st := range resp.GetDef().GetStages() {
		got = append(got, st.GetVetoUserIds())
	}
	require.Equal(t, [][]string{{"user-charlie", "user-dave"}, {"user-dave"}, nil}, got)
	require.Equal(t, []string{"user-charlie", "user-dave"}, fds.defs["wf-1"].Stages[0].VetoUserIDs)
}

// Mock: "refuses a person who is not one of the stage's approvers", and
// "refuses site admin and root unless they are an approver".
func TestVeto_UpdateRefusesAVetoPersonWhoIsNotAnApprover(t *testing.T) {
	srv, fds := vetoSaveServer()
	before := fds.defs["wf-1"]
	for _, stranger := range []string{"user-bob", "user-alice"} {
		_, err := vetoUpdate(srv, vetoStage2("First", func(s *workflowv1.WorkflowStage) {
			s.VetoUserIds = []string{"user-dave", stranger}
		}))
		require.Equal(t, codes.InvalidArgument, status.Code(err), stranger)
		require.Contains(t, status.Convert(err).Message(), "veto")
		require.Contains(t, status.Convert(err).Message(), "approver")
		require.Contains(t, status.Convert(err).Message(), stranger)
	}
	require.Equal(t, before, fds.defs["wf-1"], "a refused save changes nothing")

	// Named as an approver, the same person is accepted.
	_, err := vetoUpdate(srv, vetoStage2("First", func(s *workflowv1.WorkflowStage) {
		s.ApproverIds = []string{"user-charlie", "user-alice"}
		s.VetoUserIds = []string{"user-alice"}
	}))
	require.NoError(t, err)
}

// Mock: "accepts a person only the category or group lists name".
func TestVeto_UpdateAcceptsAPersonOnlyTheCategoryOrGroupListsName(t *testing.T) {
	srv, _ := vetoSaveServer()
	_, err := vetoUpdate(srv, vetoStage2("First", func(s *workflowv1.WorkflowStage) {
		s.ApproversByCategory = map[string]*workflowv1.ApproverList{"category-people": {UserIds: []string{"user-erin"}}}
		s.GroupUnits = []*workflowv1.GroupUnit{{GroupId: "group-finance", InternalQuorum: "one", MemberUserIds: []string{"user-frank"}}}
		s.VetoUserIds = []string{"user-erin", "user-frank"}
	}))
	require.NoError(t, err)
}

// Mock: "creates a workflow with the list, and refuses a stranger there too".
func TestVeto_CreateStoresTheListAndRefusesAStranger(t *testing.T) {
	srv := newServerWithFakeDefStore(newFakeDefStore())
	create := func(veto ...string) (*workflowv1.CreateWorkflowDefResponse, error) {
		return srv.CreateWorkflowDef(context.Background(), &workflowv1.CreateWorkflowDefRequest{
			Name: "New", ActorUserId: "grace",
			Stages: []*workflowv1.WorkflowStage{vetoStage2("First", func(s *workflowv1.WorkflowStage) { s.VetoUserIds = veto })},
		})
	}
	_, err := create("user-bob")
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	made, err := create("user-dave")
	require.NoError(t, err)
	require.Equal(t, []string{"user-dave"}, made.GetDef().GetStages()[0].GetVetoUserIds())
}
