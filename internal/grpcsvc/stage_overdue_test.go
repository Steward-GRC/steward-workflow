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
)

// The overdue settings (steward-workflow#72): saved and read back on create and
// update, refused when they could never fire.

func overdueStage(mutate func(*workflowv1.WorkflowStage)) *workflowv1.WorkflowStage {
	s := &workflowv1.WorkflowStage{Id: "ws-1", Name: "Review", ApproverIds: []string{"u1"}, Quorum: "one", SlaDays: 3}
	if mutate != nil {
		mutate(s)
	}
	return s
}

func TestStageOverdue_ProtoRoundTrip(t *testing.T) {
	in := overdueStage(func(s *workflowv1.WorkflowStage) { s.RemindEveryDays = 1; s.EscalateAfterDays = 2 })
	st := stageFromProto(in)
	require.Equal(t, 1, st.RemindEveryDays)
	require.Equal(t, 2, st.EscalateAfterDays)
	back := stageToProto(st)
	require.EqualValues(t, 1, back.GetRemindEveryDays())
	require.EqualValues(t, 2, back.GetEscalateAfterDays())

	off := stageToProto(stageFromProto(overdueStage(nil)))
	require.Zero(t, off.GetRemindEveryDays(), "unset stays off")
	require.Zero(t, off.GetEscalateAfterDays())
}

func TestStageOverdue_CreateStoresAndReturnsThem(t *testing.T) {
	srv := newServerWithFakeDefStore(newFakeDefStore())
	made, err := srv.CreateWorkflowDef(context.Background(), &workflowv1.CreateWorkflowDefRequest{
		Name: "New", ActorUserId: "grace",
		Stages: []*workflowv1.WorkflowStage{overdueStage(func(s *workflowv1.WorkflowStage) { s.RemindEveryDays = 1; s.EscalateAfterDays = 2 })},
	})
	require.NoError(t, err)
	got := made.GetDef().GetStages()[0]
	require.EqualValues(t, 1, got.GetRemindEveryDays())
	require.EqualValues(t, 2, got.GetEscalateAfterDays())
}

func TestStageOverdue_CreateAndUpdateRefuseWhatCouldNeverFire(t *testing.T) {
	refused := map[string]func(*workflowv1.WorkflowStage){
		"negative reminder":           func(s *workflowv1.WorkflowStage) { s.RemindEveryDays = -1 },
		"escalation above the limit":  func(s *workflowv1.WorkflowStage) { s.EscalateAfterDays = 366 },
		"reminder without a due date": func(s *workflowv1.WorkflowStage) { s.SlaDays = 0; s.RemindEveryDays = 1 },
		"escalation with reject":      func(s *workflowv1.WorkflowStage) { s.RejectOnSlaBreach = true; s.EscalateAfterDays = 1 },
	}
	for name, mutate := range refused {
		t.Run(name, func(t *testing.T) {
			fds := newFakeDefStore()
			srv := newServerWithFakeDefStore(fds)
			stages := []*workflowv1.WorkflowStage{overdueStage(mutate)}

			_, err := srv.CreateWorkflowDef(context.Background(), &workflowv1.CreateWorkflowDefRequest{Name: "New", Stages: stages})
			require.Equal(t, codes.InvalidArgument, status.Code(err), "create")

			ok, err := srv.CreateWorkflowDef(context.Background(), &workflowv1.CreateWorkflowDefRequest{
				Name: "Ok", Stages: []*workflowv1.WorkflowStage{overdueStage(nil)}})
			require.NoError(t, err)
			_, err = srv.UpdateWorkflowDef(context.Background(), &workflowv1.UpdateWorkflowDefRequest{
				Id: ok.GetDef().GetId(), Name: "Ok", Stages: stages})
			require.Equal(t, codes.InvalidArgument, status.Code(err), "update")
		})
	}
}

func TestStageOverdue_ReminderWithRejectOnBreachMessageIsPlain(t *testing.T) {
	srv := newServerWithFakeDefStore(newFakeDefStore())
	_, err := srv.CreateWorkflowDef(context.Background(), &workflowv1.CreateWorkflowDefRequest{
		Name: "New",
		Stages: []*workflowv1.WorkflowStage{overdueStage(func(s *workflowv1.WorkflowStage) {
			s.RejectOnSlaBreach = true
			s.RemindEveryDays = 1
		})},
	})
	require.Error(t, err)
	require.Contains(t, status.Convert(err).Message(), "Review")
}
