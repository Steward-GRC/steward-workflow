// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"testing"

	grpcactor "github.com/Bugs5382/go-grpc-actor"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	workflowv1 "github.com/Steward-GRC/steward-workflow/gen/go/steward/workflow/v1"
	corev1 "github.com/Steward-GRC/steward-workflow/gen/go/thirdparty/core/v1"
	"github.com/Steward-GRC/steward-workflow/internal/grpcsvc"
)

// During act-as every permission check runs as the target, so the extractor
// returns the subject; the admin is recorded from the same actor by the store
// and the audit emitter.
func TestActorFromContextIsTheEffectiveSubject(t *testing.T) {
	ctx := grpcactor.WithActor(context.Background(), grpcactor.Actor{Subject: "user-carol", Impersonator: "user-alice"})
	got, ok := actorFromContext(ctx)
	require.True(t, ok)
	require.Equal(t, grpcsvc.ActorClaims{UserID: "user-carol"}, got)
}

func TestActorFromContextWithoutAnActor(t *testing.T) {
	_, ok := actorFromContext(context.Background())
	require.False(t, ok, "a call with no admitted actor has no caller")
}

// fakeCorePolicies records the SetVersionStatus and RecordChangeTypeAgreement
// requests the coreStatus adapter sends.
type fakeCorePolicies struct {
	corev1.PolicyServiceClient
	status    *corev1.SetVersionStatusRequest
	agreement *corev1.RecordChangeTypeAgreementRequest
	requested corev1.ChangeType
}

func (f *fakeCorePolicies) SetVersionStatus(_ context.Context, in *corev1.SetVersionStatusRequest, _ ...grpc.CallOption) (*corev1.SetVersionStatusResponse, error) {
	f.status = in
	return &corev1.SetVersionStatusResponse{Version: &corev1.PolicyVersion{RequestedChangeType: f.requested}}, nil
}

func (f *fakeCorePolicies) RecordChangeTypeAgreement(_ context.Context, in *corev1.RecordChangeTypeAgreementRequest, _ ...grpc.CallOption) (*corev1.RecordChangeTypeAgreementResponse, error) {
	f.agreement = in
	return &corev1.RecordChangeTypeAgreementResponse{}, nil
}

// The workflow and core change types share their numbers, so a pick and a
// pending lowering cross the adapter unchanged.
func TestCoreStatusPassesTheChangeTypeBothWays(t *testing.T) {
	for _, tc := range []struct {
		wf   workflowv1.ChangeType
		core corev1.ChangeType
	}{
		{workflowv1.ChangeType_CHANGE_TYPE_UNSPECIFIED, corev1.ChangeType_CHANGE_TYPE_UNSPECIFIED},
		{workflowv1.ChangeType_CHANGE_TYPE_MAJOR, corev1.ChangeType_CHANGE_TYPE_MAJOR},
		{workflowv1.ChangeType_CHANGE_TYPE_MINOR, corev1.ChangeType_CHANGE_TYPE_MINOR},
		{workflowv1.ChangeType_CHANGE_TYPE_FIX, corev1.ChangeType_CHANGE_TYPE_FIX},
	} {
		fake := &fakeCorePolicies{requested: tc.core}
		got, err := coreStatus{fake}.SubmitVersion(context.Background(), "pv-1", "user-1", tc.wf, "why")
		require.NoError(t, err)
		require.Equal(t, tc.core, fake.status.GetChangeType())
		require.Equal(t, "why", fake.status.GetChangeTypeReason())
		require.Equal(t, "in_review", fake.status.GetStatus())
		require.Equal(t, tc.wf, got)
	}
}

func TestCoreStatusRecordsTheAgreement(t *testing.T) {
	fake := &fakeCorePolicies{}
	require.NoError(t, coreStatus{fake}.RecordChangeTypeAgreement(context.Background(), "pv-1", []string{"u1", "u2"}, "u2"))
	require.Equal(t, "pv-1", fake.agreement.GetPolicyVersionId())
	require.Equal(t, []string{"u1", "u2"}, fake.agreement.GetAgreedByUserIds())
	require.Equal(t, "u2", fake.agreement.GetActorUserId())
}
