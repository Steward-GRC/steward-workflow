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
	"github.com/Steward-GRC/steward-workflow/internal/fixture"
	"github.com/Steward-GRC/steward-workflow/internal/store"
)

// Semantic versions (steward-workflow#10): the author's change type pick rides
// the submit to core; a pick below core's suggestion waits for the first
// stage's quorum to agree, and workflow records who agreed.

const ctPV = "pv-ct"

func pickReq(ct workflowv1.ChangeType, reason string) *workflowv1.SubmitRequest {
	r := submitReq()
	r.ChangeType = ct
	r.ChangeTypeReason = reason
	return r
}

func TestSubmit_PassesTheChangeTypePickToCore(t *testing.T) {
	srv := newTestServer()
	core := &stubCoreStatus{}
	srv.coreStatus = core
	_, err := srv.Submit(context.Background(), pickReq(workflowv1.ChangeType_CHANGE_TYPE_MINOR, "wording only"))
	require.NoError(t, err)
	require.Equal(t, []submitPick{{pvID: "pv-1", actor: "user-1", changeType: workflowv1.ChangeType_CHANGE_TYPE_MINOR, reason: "wording only"}}, core.picks)
}

func TestSubmit_NoPickLeavesTheSubmitUnchanged(t *testing.T) {
	srv := newTestServer()
	core := &stubCoreStatus{}
	srv.coreStatus = core
	_, err := srv.Submit(context.Background(), submitReq())
	require.NoError(t, err)
	require.Len(t, core.picks, 1)
	require.Equal(t, workflowv1.ChangeType_CHANGE_TYPE_UNSPECIFIED, core.picks[0].changeType)
	require.Empty(t, core.picks[0].reason)
	rs := srv.runStore.(*stubRunStore)
	require.Empty(t, rs.tracked.RequestedChangeType, "no pick, nothing pending on the run")
	require.Nil(t, rs.tracked.ChangeTypeAgreedAt)
	require.NotContains(t, srv.auditEmit.(*stubAuditEmitter).emitted, "workflow.change_type.requested")
}

func TestSubmit_CarriesAPendingLoweringOnTheRun(t *testing.T) {
	srv := newTestServer()
	srv.coreStatus = &stubCoreStatus{requested: workflowv1.ChangeType_CHANGE_TYPE_MINOR}
	_, err := srv.Submit(context.Background(), pickReq(workflowv1.ChangeType_CHANGE_TYPE_MINOR, "wording only"))
	require.NoError(t, err)
	require.Equal(t, "minor", srv.runStore.(*stubRunStore).tracked.RequestedChangeType)
	au := srv.auditEmit.(*stubAuditEmitter)
	require.Contains(t, au.emitted, "workflow.change_type.requested")
	require.Equal(t, "user-1", au.actors["workflow.change_type.requested"])
	require.Equal(t, "minor", au.attrs["workflow.change_type.requested"]["change_type"])
}

func TestSubmit_APickCoreAppliedAtOnceLeavesNothingPending(t *testing.T) {
	srv := newTestServer()
	srv.coreStatus = &stubCoreStatus{} // core reports no pending lowering
	_, err := srv.Submit(context.Background(), pickReq(workflowv1.ChangeType_CHANGE_TYPE_MAJOR, ""))
	require.NoError(t, err)
	require.Empty(t, srv.runStore.(*stubRunStore).tracked.RequestedChangeType)
}

func TestSubmit_CoreRefusalOfThePickPassesThroughAndStartsNoRun(t *testing.T) {
	srv := newTestServer()
	srv.coreStatus = &stubCoreStatus{err: status.Error(codes.InvalidArgument, "a change type below the suggestion needs a reason")}
	_, err := srv.Submit(context.Background(), pickReq(workflowv1.ChangeType_CHANGE_TYPE_FIX, ""))
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "needs a reason")
	require.Empty(t, srv.sagaClient.(*stubSagaClient).startedRunID)
	require.Nil(t, srv.runStore.(*stubRunStore).tracked)
}

// ctServer is a one-stage-pair workflow: stage 0 wants two of its three
// approvers, and the run carries a pending lowering to minor.
func ctServer(t *testing.T) (*WorkflowServer, *stubCoreStatus, *stubAssignments) {
	t.Helper()
	srv := newTestServer()
	srv.defStore = &stubWorkflowDefStore{wd: builder.WorkflowDef{
		ID: "def-ct", Name: "Two stage", Version: 1,
		Stages: []builder.Stage{
			{Name: "First", Quorum: builder.QuorumNofM, QuorumN: 2, ApproverIDs: []string{fixture.Carol, fixture.Dave, fixture.Erin}},
			{Name: "Second", Quorum: builder.QuorumAll, ApproverIDs: []string{fixture.Frank}},
		},
	}}
	core := &stubCoreStatus{}
	srv.coreStatus = core
	srv.runStore.(*stubRunStore).tracked = &store.ApprovalRun{
		PolicyVersionID: ctPV, RunID: "run-ct", WorkflowDefID: "def-ct", WorkflowVersion: 1,
		Status: "in_review", SubmittedBy: fixture.Bob, RequestedChangeType: "minor",
	}
	as := srv.assignments.(*stubAssignments)
	seat := func(stage int, user string) {
		_, err := as.CreateAssignment(context.Background(), store.AssignmentRow{PolicyVersionID: ctPV, StageIndex: stage, UserID: user}, fixture.Bob)
		require.NoError(t, err)
	}
	seat(0, fixture.Carol)
	seat(0, fixture.Dave)
	seat(0, fixture.Erin)
	seat(1, fixture.Frank)
	return srv, core, as
}

func ctSignal(srv *WorkflowServer, st workflowv1.SignalType, user string) error {
	_, err := srv.Signal(context.Background(), &workflowv1.SignalRequest{
		PolicyVersionId: ctPV, Signal: st, ActorUserId: user, Comment: "reviewed",
	})
	return err
}

func TestLowering_NeedsTheFirstStagesQuorum(t *testing.T) {
	srv, core, _ := ctServer(t)
	require.NoError(t, ctSignal(srv, workflowv1.SignalType_SIGNAL_TYPE_APPROVE, fixture.Carol))
	require.Empty(t, core.agreements, "one of two approvals is not a quorum, nothing is agreed")
	require.Nil(t, srv.runStore.(*stubRunStore).tracked.ChangeTypeAgreedAt)
}

func TestLowering_QuorumRecordsTheAgreeingUsersOnce(t *testing.T) {
	srv, core, _ := ctServer(t)
	require.NoError(t, ctSignal(srv, workflowv1.SignalType_SIGNAL_TYPE_APPROVE, fixture.Carol))
	require.NoError(t, ctSignal(srv, workflowv1.SignalType_SIGNAL_TYPE_APPROVE, fixture.Dave))
	require.Len(t, core.agreements, 1)
	require.Equal(t, ctPV, core.agreements[0].pvID)
	require.Equal(t, fixture.Dave, core.agreements[0].actor, "the user whose decision completed the quorum")
	require.ElementsMatch(t, []string{fixture.Carol, fixture.Dave}, core.agreements[0].agreedBy)
	require.NotNil(t, srv.runStore.(*stubRunStore).tracked.ChangeTypeAgreedAt)

	// A later approval on the same stage records nothing more.
	require.NoError(t, ctSignal(srv, workflowv1.SignalType_SIGNAL_TYPE_APPROVE, fixture.Erin))
	require.Len(t, core.agreements, 1, "agreement is recorded once")
	au := srv.auditEmit.(*stubAuditEmitter)
	require.Equal(t, fixture.Dave, au.actors["workflow.change_type.agreed"])
	require.Equal(t, "policy_version:"+ctPV, au.subjects["workflow.change_type.agreed"])
}

func TestLowering_NoPendingRequestRecordsNothing(t *testing.T) {
	srv, core, _ := ctServer(t)
	srv.runStore.(*stubRunStore).tracked.RequestedChangeType = ""
	require.NoError(t, ctSignal(srv, workflowv1.SignalType_SIGNAL_TYPE_APPROVE, fixture.Carol))
	require.NoError(t, ctSignal(srv, workflowv1.SignalType_SIGNAL_TYPE_APPROVE, fixture.Dave))
	require.Empty(t, core.agreements, "no pick below the suggestion, so no agreement to record")
}

func TestLowering_OnlyTheFirstStageAgrees(t *testing.T) {
	srv, core, as := ctServer(t)
	require.NoError(t, as.setSeat(ctPV, 0, fixture.Carol, "approved"))
	require.NoError(t, as.setSeat(ctPV, 0, fixture.Dave, "approved"))
	require.NoError(t, ctSignal(srv, workflowv1.SignalType_SIGNAL_TYPE_APPROVE, fixture.Frank))
	require.Equal(t, "approved", as.rows[seatKey(ctPV, 1, fixture.Frank, "")].State)
	require.Empty(t, core.agreements, "a later stage's quorum does not agree to a lowering")
}

func TestLowering_ARejectionRecordsNothing(t *testing.T) {
	srv, core, _ := ctServer(t)
	require.NoError(t, ctSignal(srv, workflowv1.SignalType_SIGNAL_TYPE_REJECT, fixture.Carol))
	require.Empty(t, core.agreements)
}

func TestLowering_CoreRefusalPassesThroughAndKeepsTheSeatOpen(t *testing.T) {
	srv, core, as := ctServer(t)
	require.NoError(t, ctSignal(srv, workflowv1.SignalType_SIGNAL_TYPE_APPROVE, fixture.Carol))
	core.agreeErr = status.Error(codes.InvalidArgument, "invalid agreed_by_user_ids entry")
	err := ctSignal(srv, workflowv1.SignalType_SIGNAL_TYPE_APPROVE, fixture.Dave)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "agreed_by_user_ids")
	require.Equal(t, "pending", as.rows[seatKey(ctPV, 0, fixture.Dave, "")].State, "the decision is not recorded when core refuses the agreement")
	require.Nil(t, srv.runStore.(*stubRunStore).tracked.ChangeTypeAgreedAt)

	// Once core accepts, the same approval goes through and agrees.
	core.agreeErr = nil
	require.NoError(t, ctSignal(srv, workflowv1.SignalType_SIGNAL_TYPE_APPROVE, fixture.Dave))
	require.Len(t, core.agreements, 1)
}

// Core answers FAILED_PRECONDITION when no lowering is pending any more (the
// author dropped the pick, or it was applied): nothing is left to agree, so
// the approver's decision must not be blocked.
func TestLowering_NothingPendingInCoreDoesNotBlockTheDecision(t *testing.T) {
	srv, core, as := ctServer(t)
	core.agreeErr = status.Error(codes.FailedPrecondition, "no change type lowering is pending")
	require.NoError(t, ctSignal(srv, workflowv1.SignalType_SIGNAL_TYPE_APPROVE, fixture.Carol))
	require.NoError(t, ctSignal(srv, workflowv1.SignalType_SIGNAL_TYPE_APPROVE, fixture.Dave))
	require.Equal(t, "approved", as.rows[seatKey(ctPV, 0, fixture.Dave, "")].State)
	require.NotNil(t, srv.runStore.(*stubRunStore).tracked.ChangeTypeAgreedAt, "marked done so it is not retried")
	require.Empty(t, core.agreements)
}

func TestLowering_BulkApprovalRecordsTheAgreementToo(t *testing.T) {
	srv, core, _ := ctServer(t)
	bulk := func(user string) *workflowv1.BulkDecideResponse {
		srv.actorExtractor = func(context.Context) (ActorClaims, bool) { return ActorClaims{UserID: user}, true }
		resp, err := srv.BulkDecide(context.Background(), &workflowv1.BulkDecideRequest{Decisions: []*workflowv1.Decision{{
			PolicyVersionId: ctPV, StageIndex: 0, Decision: workflowv1.DecisionType_DECISION_TYPE_APPROVE, Comment: "ok",
		}}})
		require.NoError(t, err)
		return resp
	}
	require.True(t, bulk(fixture.Carol).Results[0].Ok)
	require.Empty(t, core.agreements)
	require.True(t, bulk(fixture.Dave).Results[0].Ok)
	require.Len(t, core.agreements, 1)
	require.ElementsMatch(t, []string{fixture.Carol, fixture.Dave}, core.agreements[0].agreedBy)
}
