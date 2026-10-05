// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	workflowv1 "github.com/Steward-GRC/steward-workflow/gen/go/steward/workflow/v1"
	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/fixture"
	"github.com/Steward-GRC/steward-workflow/internal/store"
)

// Individual and group approvals are separate: Carol is an individual
// approver on the stage and a member of the finance team, which approves as
// a group by majority.

const seatsPV = "pv-seats"

func seatsServer(t *testing.T) (*WorkflowServer, *stubAssignments) {
	t.Helper()
	srv := newTestServer()
	srv.defStore = &stubWorkflowDefStore{wd: builder.WorkflowDef{
		ID: "def-seats", Name: fixture.FinanceSignOff, Version: 1,
		Stages: []builder.Stage{{
			Name: "Sign-off", Quorum: builder.QuorumAll, ApproverIDs: []string{fixture.Carol},
			GroupUnits: []builder.GroupUnit{{GroupID: fixture.FinanceTeam, InternalQuorum: builder.QuorumMajority,
				Members: []string{fixture.Carol, fixture.Dave, fixture.Erin}}},
		}},
	}}
	srv.runStore.(*stubRunStore).tracked = &store.ApprovalRun{
		PolicyVersionID: seatsPV, RunID: "run-seats", WorkflowDefID: "def-seats", WorkflowVersion: 1, Status: "in_review", SubmittedBy: fixture.Bob,
	}
	as := srv.assignments.(*stubAssignments)
	seed := func(user, group string) {
		now := time.Now().UTC()
		_, err := as.CreateAssignment(context.Background(), store.AssignmentRow{
			PolicyVersionID: seatsPV, StageIndex: 0, UserID: user, GroupID: group,
			SLADeadlineAt: now.Add(time.Hour), ReminderAt: now.Add(time.Hour),
		}, fixture.Bob)
		require.NoError(t, err)
	}
	seed(fixture.Carol, "")
	seed(fixture.Carol, fixture.FinanceTeam)
	seed(fixture.Dave, fixture.FinanceTeam)
	seed(fixture.Erin, fixture.FinanceTeam)
	return srv, as
}

func seatState(as *stubAssignments, user, group string) string {
	return as.rows[seatKey(seatsPV, 0, user, group)].State
}

func signal(srv *WorkflowServer, st workflowv1.SignalType, user, group string) error {
	_, err := srv.Signal(context.Background(), &workflowv1.SignalRequest{
		PolicyVersionId: seatsPV, Signal: st, ActorUserId: user, Comment: "reviewed", GroupId: group,
	})
	return err
}

func TestSeats_SignalWithoutGroupDecidesTheIndividualSeat(t *testing.T) {
	srv, as := seatsServer(t)
	require.NoError(t, signal(srv, workflowv1.SignalType_SIGNAL_TYPE_APPROVE, fixture.Carol, ""))
	require.Equal(t, "approved", seatState(as, fixture.Carol, ""))
	require.Equal(t, "pending", seatState(as, fixture.Carol, fixture.FinanceTeam), "an individual approval never decides the group seat")
}

func TestSeats_SignalWithGroupDecidesTheGroupSeat(t *testing.T) {
	srv, as := seatsServer(t)
	require.NoError(t, signal(srv, workflowv1.SignalType_SIGNAL_TYPE_APPROVE, fixture.Carol, fixture.FinanceTeam))
	require.Equal(t, "approved", seatState(as, fixture.Carol, fixture.FinanceTeam))
	require.Equal(t, "pending", seatState(as, fixture.Carol, ""))
}

func TestSeats_SignalWithoutGroupUsesTheOnlyGroupSeat(t *testing.T) {
	srv, as := seatsServer(t)
	require.NoError(t, signal(srv, workflowv1.SignalType_SIGNAL_TYPE_APPROVE, fixture.Dave, ""))
	require.Equal(t, "approved", seatState(as, fixture.Dave, fixture.FinanceTeam))
}

func TestSeats_SignalWithoutGroupRefusesSeveralGroupSeats(t *testing.T) {
	srv, as := seatsServer(t)
	now := time.Now().UTC()
	_, err := as.CreateAssignment(context.Background(), store.AssignmentRow{
		PolicyVersionID: seatsPV, StageIndex: 0, UserID: fixture.Dave, GroupID: fixture.Approvers,
		SLADeadlineAt: now, ReminderAt: now,
	}, fixture.Bob)
	require.NoError(t, err)

	err = signal(srv, workflowv1.SignalType_SIGNAL_TYPE_APPROVE, fixture.Dave, "")
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestSeats_SignalForAGroupTheActorIsNotInIsRefused(t *testing.T) {
	srv, _ := seatsServer(t)
	err := signal(srv, workflowv1.SignalType_SIGNAL_TYPE_APPROVE, fixture.Carol, fixture.Approvers)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestSeats_GroupMemberRejectionLeavesTheStageOpen(t *testing.T) {
	srv, as := seatsServer(t)
	require.NoError(t, signal(srv, workflowv1.SignalType_SIGNAL_TYPE_REJECT, fixture.Dave, fixture.FinanceTeam))

	require.Equal(t, "rejected", seatState(as, fixture.Dave, fixture.FinanceTeam))
	require.Equal(t, "pending", seatState(as, fixture.Erin, fixture.FinanceTeam), "two of three can still make a majority")
	require.Equal(t, "pending", seatState(as, fixture.Carol, ""))
	require.Empty(t, srv.sagaClient.(*stubSagaClient).cancelledRuns, "the run goes on")
}

func TestSeats_GroupRejectionPastItsQuorumRejectsTheStage(t *testing.T) {
	srv, as := seatsServer(t)
	notes := &recordingRunNotifier{}
	srv.runNotifier = notes
	require.NoError(t, signal(srv, workflowv1.SignalType_SIGNAL_TYPE_REJECT, fixture.Dave, fixture.FinanceTeam))
	require.NoError(t, signal(srv, workflowv1.SignalType_SIGNAL_TYPE_REJECT, fixture.Erin, fixture.FinanceTeam))

	require.Equal(t, "superseded", seatState(as, fixture.Carol, ""), "the stage is rejected once the group can't reach a majority")
	require.Equal(t, "superseded", seatState(as, fixture.Carol, fixture.FinanceTeam))
	require.Equal(t, []string{"run-seats"}, srv.sagaClient.(*stubSagaClient).cancelledRuns)
	require.Len(t, notes.denied, 1)
}

func TestSeats_IndividualRejectionRejectsTheStage(t *testing.T) {
	srv, as := seatsServer(t)
	require.NoError(t, signal(srv, workflowv1.SignalType_SIGNAL_TYPE_REJECT, fixture.Carol, ""))
	require.Equal(t, "superseded", seatState(as, fixture.Dave, fixture.FinanceTeam))
	require.Equal(t, []string{"run-seats"}, srv.sagaClient.(*stubSagaClient).cancelledRuns)
}

func TestSeats_SubmitSeatsGroupsFromTheDefinitionNotThePool(t *testing.T) {
	srv, _ := seatsServer(t)
	srv.defStore = &stubWorkflowDefStore{wd: builder.WorkflowDef{
		ID: "def-g", Name: fixture.FinanceSignOff, Version: 1,
		Stages: []builder.Stage{{Name: "Sign-off", Quorum: builder.QuorumAny,
			GroupUnits: []builder.GroupUnit{{GroupID: fixture.FinanceTeam, InternalQuorum: builder.QuorumAll, Members: []string{fixture.Dave}}}}},
	}}
	_, err := srv.Submit(context.Background(), &workflowv1.SubmitRequest{
		PolicyVersionId: "pv-g", PolicyId: "pol-g", SubmittedBy: fixture.Bob, AncestorCategoryIds: []string{fixture.Finance},
	})
	require.NoError(t, err, "a stage staffed only by a group is staffed")
	pool, _ := srv.sagaClient.(*stubSagaClient).startedInputs["approvers_s0"].([]string)
	require.Empty(t, pool, "group members get group seats, not individual ones")
}

func TestSeats_GetStatusReportsSeatsAndTheGroupTally(t *testing.T) {
	srv, _ := seatsServer(t)
	require.NoError(t, signal(srv, workflowv1.SignalType_SIGNAL_TYPE_APPROVE, fixture.Carol, ""))
	require.NoError(t, signal(srv, workflowv1.SignalType_SIGNAL_TYPE_APPROVE, fixture.Dave, fixture.FinanceTeam))

	resp, err := srv.GetStatus(context.Background(), &workflowv1.GetStatusRequest{PolicyVersionId: seatsPV})
	require.NoError(t, err)
	var groups []string
	for _, a := range resp.GetStageAssignees()[0].GetAssignees() {
		if a.GetUserId() == fixture.Carol {
			groups = append(groups, a.GetGroupId())
		}
	}
	slices.Sort(groups)
	require.Equal(t, []string{"", fixture.FinanceTeam}, groups, "Carol holds two seats")

	units := resp.GetStageUnitProgress()[0].GetUnits()
	require.Len(t, units, 1)
	require.Equal(t, fixture.FinanceTeam, units[0].GetGroupId())
	require.Equal(t, int32(1), units[0].GetApprovals(), "Carol's individual approval is not a group vote")
	require.Equal(t, int32(2), units[0].GetRequired())
	require.Equal(t, int32(3), units[0].GetRoster())
	require.Equal(t, "PENDING", units[0].GetStatus())
}

func TestSeats_EligiblePoolForAGroupSeatIsTheGroup(t *testing.T) {
	srv, _ := seatsServer(t)
	resp, err := srv.GetStageEligiblePool(context.Background(), &workflowv1.GetStageEligiblePoolRequest{
		PolicyVersionId: seatsPV, StageIndex: 0, GroupId: fixture.FinanceTeam,
	})
	require.NoError(t, err)
	require.Equal(t, []string{fixture.Carol, fixture.Dave, fixture.Erin}, resp.GetEligibleUserIds())
}

func TestSeats_SwapAGroupSeatWithinTheGroup(t *testing.T) {
	srv, as := seatsServer(t)
	srv.defStore = &stubWorkflowDefStore{wd: builder.WorkflowDef{
		ID: "def-seats", Name: fixture.FinanceSignOff, Version: 1,
		Stages: []builder.Stage{{Name: "Sign-off", Quorum: builder.QuorumAll, ApproverIDs: []string{fixture.Carol},
			GroupUnits: []builder.GroupUnit{{GroupID: fixture.FinanceTeam, InternalQuorum: builder.QuorumMajority,
				Members: []string{fixture.Carol, fixture.Dave, fixture.Erin, fixture.Grace}}}}},
	}}
	_, err := srv.SwapAssignee(context.Background(), &workflowv1.SwapAssigneeRequest{
		PolicyVersionId: seatsPV, StageIndex: 0, CurrentUserId: fixture.Dave, NewUserId: fixture.Grace, GroupId: fixture.FinanceTeam,
		Reason: "on leave", InitiatorRole: workflowv1.InitiatorRole_INITIATOR_ROLE_ADMIN,
	})
	require.NoError(t, err)
	require.Equal(t, "swapped_out", seatState(as, fixture.Dave, fixture.FinanceTeam))
	require.Equal(t, "pending", seatState(as, fixture.Grace, fixture.FinanceTeam))

	_, err = srv.SwapAssignee(context.Background(), &workflowv1.SwapAssigneeRequest{
		PolicyVersionId: seatsPV, StageIndex: 0, CurrentUserId: fixture.Erin, NewUserId: fixture.Frank, GroupId: fixture.FinanceTeam,
		Reason: "on leave", InitiatorRole: workflowv1.InitiatorRole_INITIATOR_ROLE_ADMIN,
	})
	requireCoded(t, err, codes.FailedPrecondition, entry(6010), map[string]string{"new_user_id": fixture.Frank})
}

func TestSeats_BulkDecideAGroupSeat(t *testing.T) {
	srv, as := seatsServer(t)
	srv.actorExtractor = func(context.Context) (ActorClaims, bool) { return ActorClaims{UserID: fixture.Carol}, true }
	resp, err := srv.BulkDecide(context.Background(), &workflowv1.BulkDecideRequest{Decisions: []*workflowv1.Decision{
		{PolicyVersionId: seatsPV, StageIndex: 0, Decision: workflowv1.DecisionType_DECISION_TYPE_APPROVE, Comment: "ok", GroupId: fixture.FinanceTeam},
	}})
	require.NoError(t, err)
	require.True(t, resp.GetResults()[0].GetOk())
	require.Equal(t, fixture.FinanceTeam, resp.GetResults()[0].GetGroupId())
	require.Equal(t, "approved", seatState(as, fixture.Carol, fixture.FinanceTeam))
	require.Equal(t, "pending", seatState(as, fixture.Carol, ""))
}

type stubAdmins struct {
	admins map[string]bool
	err    error
}

func (s stubAdmins) IsWorkflowAdmin(_ context.Context, userID string) (bool, error) {
	return s.admins[userID], s.err
}

func TestSwap_AdminDecidedByTheAdminChecker(t *testing.T) {
	srv, _ := setupServerForSwap(t)
	srv.actorExtractor = func(context.Context) (ActorClaims, bool) { return ActorClaims{UserID: fixture.Frank}, true }
	req := &workflowv1.SwapAssigneeRequest{
		PolicyVersionId: "pv-swap", StageIndex: 0, CurrentUserId: "u1", NewUserId: "u2",
		Reason: "PTO", InitiatorRole: workflowv1.InitiatorRole_INITIATOR_ROLE_ADMIN,
	}

	srv.admins = stubAdmins{admins: map[string]bool{}}
	_, err := srv.SwapAssignee(context.Background(), req)
	requireCoded(t, err, codes.PermissionDenied, entry(6002), map[string]string{"role": "admin"})

	srv.admins = stubAdmins{err: errors.New("identity unavailable")}
	_, err = srv.SwapAssignee(context.Background(), req)
	require.Equal(t, codes.Internal, status.Code(err))

	srv.admins = stubAdmins{admins: map[string]bool{fixture.Frank: true}}
	_, err = srv.SwapAssignee(context.Background(), req)
	require.NoError(t, err)
}

func TestListPendingTasks_CarriesTheDueTime(t *testing.T) {
	srv := newTestServer()
	due := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	srv.sagaClient = &stubSagaClient{pendingTasks: []PendingTaskInfo{{TaskID: "t1", RunID: "r1", PolicyVersionID: "pv1", DueAt: &due}}}
	resp, err := srv.ListPendingTasks(context.Background(), &workflowv1.ListPendingTasksRequest{})
	require.NoError(t, err)
	require.Equal(t, due, resp.GetTasks()[0].GetDueAt().AsTime())
}

func TestListUpcomingTasks_IncludesGroupMembers(t *testing.T) {
	srv, _ := seatsServer(t)
	srv.runStore.(*stubRunStore).activeRuns = []store.ApprovalRun{{PolicyVersionID: "pv-up", WorkflowDefID: "def-seats", WorkflowVersion: 1}}
	resp, err := srv.ListUpcomingTasks(context.Background(), &workflowv1.ListUpcomingTasksRequest{ApproverUserId: fixture.Erin})
	require.NoError(t, err)
	require.Len(t, resp.GetTasks(), 1)
}
