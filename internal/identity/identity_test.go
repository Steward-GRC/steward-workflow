// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package identity_test

import (
	"context"
	"errors"
	"testing"

	stewardauthz "github.com/Steward-GRC/steward-authz"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	identityv1 "github.com/Steward-GRC/steward-workflow/gen/go/thirdparty/identity/v1"
	"github.com/Steward-GRC/steward-workflow/internal/fixture"
	"github.com/Steward-GRC/steward-workflow/internal/identity"
)

type fakeRead struct {
	identityv1.IdentityReadServiceClient
	users map[string]*identityv1.User
	err   error
	asked []string
}

func (f *fakeRead) GetUser(_ context.Context, in *identityv1.GetUserRequest, _ ...grpc.CallOption) (*identityv1.GetUserResponse, error) {
	f.asked = append(f.asked, in.GetUserId())
	if f.err != nil {
		return nil, f.err
	}
	return &identityv1.GetUserResponse{User: f.users[in.GetUserId()]}, nil
}

func TestSubject_MapsTheUsersAccess(t *testing.T) {
	read := &fakeRead{users: map[string]*identityv1.User{fixture.Carol: {
		Id:                 fixture.Carol,
		Roles:              []string{"template-admin", "admin"},
		ScopedRoles:        []*identityv1.ScopedRole{{Role: "approver", Category: "Facilities"}},
		IdpGroups:          []string{fixture.FacilitiesTeam},
		ReadSensitiveGrant: true,
		PolicyOverrides: []*identityv1.PolicyOverride{
			{PolicyNumber: fixture.DeskBookingPolicyNumber, Effect: identityv1.OverrideEffect_OVERRIDE_EFFECT_DENY},
			{PolicyNumber: "POL-EXPENSES-000002", Effect: identityv1.OverrideEffect_OVERRIDE_EFFECT_ALLOW},
			{PolicyNumber: "POL-TRAVEL-000003", Effect: identityv1.OverrideEffect_OVERRIDE_EFFECT_UNSPECIFIED},
		},
	}}}

	s, err := identity.New(read).Subject(context.Background(), fixture.Carol)
	require.NoError(t, err)
	require.Equal(t, fixture.Carol, s.UserID)
	require.Equal(t, []stewardauthz.Role{stewardauthz.RoleTemplateAdmin}, s.Roles, "a role steward-authz doesn't know grants nothing")
	require.Equal(t, []stewardauthz.ScopedGrant{{Role: stewardauthz.RoleApprover, Category: "Facilities"}}, s.ScopedGrants)
	require.Equal(t, []string{fixture.FacilitiesTeam}, s.Groups)
	require.True(t, s.ReadSensitive)
	require.Equal(t, []stewardauthz.Override{
		{ResourceID: fixture.DeskBookingPolicyNumber, Grant: stewardauthz.GrantDeny},
		{ResourceID: "POL-EXPENSES-000002", Grant: stewardauthz.GrantAllow},
	}, s.Overrides)
}

func TestSubject_Root(t *testing.T) {
	read := &fakeRead{users: map[string]*identityv1.User{fixture.Alice: {Id: fixture.Alice, IsRoot: true, Roles: []string{"site-admin"}}}}
	s, err := identity.New(read).Subject(context.Background(), fixture.Alice)
	require.NoError(t, err)
	require.True(t, s.Root)
	require.True(t, s.SiteAdmin())
}

func TestSubject_PropagatesTheError(t *testing.T) {
	boom := errors.New("identity unavailable")
	_, err := identity.New(&fakeRead{err: boom}).Subject(context.Background(), fixture.Carol)
	require.ErrorIs(t, err, boom)
}

func TestSubject_UnknownUserIsAnError(t *testing.T) {
	_, err := identity.New(&fakeRead{users: map[string]*identityv1.User{}}).Subject(context.Background(), fixture.Erin)
	require.ErrorIs(t, err, identity.ErrNoUser)
}

func TestIsWorkflowAdmin(t *testing.T) {
	read := &fakeRead{users: map[string]*identityv1.User{
		fixture.Frank: {Id: fixture.Frank, Roles: []string{"template-admin"}},
		fixture.Alice: {Id: fixture.Alice, Roles: []string{"site-admin"}},
		fixture.Carol: {Id: fixture.Carol, ScopedRoles: []*identityv1.ScopedRole{{Role: "approver", Category: "Facilities"}}},
	}}
	c := identity.New(read)
	for user, want := range map[string]bool{fixture.Frank: true, fixture.Alice: true, fixture.Carol: false} {
		got, err := c.IsWorkflowAdmin(context.Background(), user)
		require.NoError(t, err)
		require.Equal(t, want, got, user)
	}
	_, err := identity.New(&fakeRead{err: errors.New("down")}).IsWorkflowAdmin(context.Background(), fixture.Frank)
	require.Error(t, err)
}
