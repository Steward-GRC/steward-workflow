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

func vetoSaveServer() (*WorkflowServer, *fakeDefStore) {
	fds := newFakeDefStore()
	fds.defs["wf-1"] = builder.WorkflowDef{ID: "wf-1", Name: "Finance", Version: 1, VetoEnabled: true,
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

// Mock: "stores a list of people per stage and answers it", and saving stages
// leaves the old opt-in alone.
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
	require.True(t, fds.defs["wf-1"].VetoEnabled, "an update keeps the stored flag, which now gates nothing")
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

// Mock: "are accepted and ignored on a stage": the old lists are not stored and
// are answered empty.
func TestVeto_TheOldListsAreAcceptedAndIgnored(t *testing.T) {
	srv, fds := vetoSaveServer()
	resp, err := vetoUpdate(srv, vetoStage2("First", func(s *workflowv1.WorkflowStage) {
		// The deprecated fields are exactly what this test sends.
		s.VetoApproverIds = []string{"user-charlie"} //nolint:staticcheck // SA1019: proves the old fields are ignored
		s.VetoGroupIds = []string{"group-finance"}   //nolint:staticcheck // SA1019: proves the old fields are ignored
	}))
	require.NoError(t, err)
	got := resp.GetDef().GetStages()[0]
	require.Empty(t, got.GetVetoApproverIds())
	require.Empty(t, got.GetVetoGroupIds())
	require.Empty(t, got.GetVetoUserIds())
	require.Empty(t, fds.defs["wf-1"].Stages[0].VetoApproverIDs)
	require.Empty(t, fds.defs["wf-1"].Stages[0].VetoGroupIDs)
}

// A definition stored before the change answers its old marks as empty and its
// people as stored.
func TestVeto_AStoredDefinitionWithOldMarksAnswersThemEmpty(t *testing.T) {
	fds := newFakeDefStore()
	fds.defs["wf-1"] = builder.WorkflowDef{ID: "wf-1", Name: "Finance", Version: 1, VetoEnabled: true,
		Stages: []builder.Stage{{Name: "s", Quorum: builder.QuorumAny, ApproverIDs: []string{"u1"},
			VetoApproverIDs: []string{"u1"}, VetoGroupIDs: []string{"g1"}, VetoUserIDs: []string{"u1"}}}}
	got, err := newServerWithFakeDefStore(fds).GetWorkflowDef(context.Background(), &workflowv1.GetWorkflowDefRequest{Id: "wf-1"})
	require.NoError(t, err)
	st := got.GetDef().GetStages()[0]
	require.Empty(t, st.GetVetoApproverIds())
	require.Empty(t, st.GetVetoGroupIds())
	require.Equal(t, []string{"u1"}, st.GetVetoUserIds())
}

// Mock: "apply without any workflow switch" (the submit half): the old flag is
// no longer pinned into the run, whatever it holds.
func TestVeto_SubmitNoLongerPinsTheOldFlag(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		srv := newTestServer()
		srv.defStore = &stubWorkflowDefStore{wd: builder.WorkflowDef{
			ID: "def-1", Name: "Finance", Version: 1, VetoEnabled: enabled,
			Stages: []builder.Stage{{Name: "s", Quorum: builder.QuorumAny, ApproverIDs: []string{"u1"}, VetoUserIDs: []string{"u1"}}},
		}}
		_, err := srv.Submit(context.Background(), &workflowv1.SubmitRequest{
			PolicyVersionId: "pv-1", PolicyId: "pol-1", SubmittedBy: "bob",
		})
		require.NoError(t, err)
		require.NotContains(t, srv.sagaClient.(*stubSagaClient).startedInputs, "veto_enabled")
	}
}
