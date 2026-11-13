// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc

import (
	"context"
	"errors"
	"testing"

	"github.com/Bugs5382/go-apperr/apperrgrpc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	workflowv1 "github.com/Steward-GRC/steward-workflow/gen/go/steward/workflow/v1"
	"github.com/Steward-GRC/steward-workflow/internal/store"
)

func newDeleteServer(fds *fakeDefStore) (*WorkflowServer, *stubAuditEmitter) {
	audit := &stubAuditEmitter{}
	srv := newServerWithFakeDefStore(fds)
	srv.auditEmit = audit
	return srv, audit
}

func TestDeleteWorkflowDef_DeletesAndAudits(t *testing.T) {
	fds := newFakeDefStore()
	srv, audit := newDeleteServer(fds)
	created, err := srv.CreateWorkflowDef(context.Background(), &workflowv1.CreateWorkflowDefRequest{
		Name: "Workflow", Stages: []*workflowv1.WorkflowStage{stageWithApprovers("S0", "u1")},
	})
	require.NoError(t, err)

	_, err = srv.DeleteWorkflowDef(context.Background(), &workflowv1.DeleteWorkflowDefRequest{
		Id: created.Def.Id, ActorUserId: "admin-1",
	})
	require.NoError(t, err)
	require.Equal(t, []string{created.Def.Id}, fds.deleted)
	require.Equal(t, "id:"+created.Def.Id, audit.subjects["workflow_def.deleted"], "ids only")
	require.Equal(t, "admin-1", audit.actors["workflow_def.deleted"])
}

func TestDeleteWorkflowDef_EmptyIDIsInvalid(t *testing.T) {
	srv, audit := newDeleteServer(newFakeDefStore())
	_, err := srv.DeleteWorkflowDef(context.Background(), &workflowv1.DeleteWorkflowDefRequest{})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.NotContains(t, audit.emitted, "workflow_def.deleted")
}

func TestDeleteWorkflowDef_InUseIsRefusedWithCounts(t *testing.T) {
	fds := newFakeDefStore()
	fds.delErr = &store.DefInUseError{DefID: "d1", Runs: 3, Overrides: 1}
	srv, audit := newDeleteServer(fds)

	_, err := srv.DeleteWorkflowDef(context.Background(), &workflowv1.DeleteWorkflowDefRequest{Id: "d1"})
	st := status.Convert(err)
	require.Equal(t, codes.FailedPrecondition, st.Code())
	info, ok := apperrgrpc.FromStatus(st)
	require.True(t, ok)
	require.Equal(t, "WORKFLOW_DEF_IN_USE", info.Symbol)
	require.Equal(t, 6013, info.Code)
	require.Equal(t, "3", info.Metadata["run_count"])
	require.Equal(t, "1", info.Metadata["override_count"])
	require.Contains(t, st.Message(), "3 approval runs")
	require.NotContains(t, audit.emitted, "workflow_def.deleted", "a refused delete is not audited as a delete")
}

func TestDeleteWorkflowDef_UnknownIDIsNotFound(t *testing.T) {
	fds := newFakeDefStore()
	fds.delErr = store.ErrWorkflowDefNotFound
	srv, _ := newDeleteServer(fds)

	_, err := srv.DeleteWorkflowDef(context.Background(), &workflowv1.DeleteWorkflowDefRequest{Id: "nope"})
	st := status.Convert(err)
	require.Equal(t, codes.NotFound, st.Code())
	info, _ := apperrgrpc.FromStatus(st)
	require.Equal(t, "WORKFLOW_DEF_NOT_FOUND", info.Symbol)
	require.Equal(t, "nope", info.Metadata["id"])
}

func TestDeleteWorkflowDef_StoreFailureIsInternal(t *testing.T) {
	fds := newFakeDefStore()
	fds.delErr = errors.New("db down")
	srv, audit := newDeleteServer(fds)

	_, err := srv.DeleteWorkflowDef(context.Background(), &workflowv1.DeleteWorkflowDefRequest{Id: "d1"})
	require.Equal(t, codes.Internal, status.Code(err))
	require.NotContains(t, audit.emitted, "workflow_def.deleted")
}
