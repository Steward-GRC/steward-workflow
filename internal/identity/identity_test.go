// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package identity_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Bugs5382/go-authz/ordered"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	identityv1 "github.com/Steward-GRC/steward-workflow/gen/go/thirdparty/identity/v1"
	"github.com/Steward-GRC/steward-workflow/internal/authz"
	"github.com/Steward-GRC/steward-workflow/internal/fixture"
	"github.com/Steward-GRC/steward-workflow/internal/identity"
)

type fakeRead struct {
	identityv1.IdentityReadServiceClient
	users map[string]*identityv1.User
	err   error
	asked []string
	all   []*identityv1.User
}

func (f *fakeRead) ListAllUsers(context.Context, *identityv1.ListAllUsersRequest, ...grpc.CallOption) (*identityv1.ListAllUsersResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &identityv1.ListAllUsersResponse{Users: f.all}, nil
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
	require.Equal(t, []authz.Role{authz.RoleTemplateAdmin}, s.Roles, "a role the catalog doesn't know grants nothing")
	require.Equal(t, []authz.ScopedGrant{{Role: authz.RoleApprover, Category: "Facilities"}}, s.ScopedGrants)
	require.Equal(t, []string{fixture.FacilitiesTeam}, s.Groups)
	require.True(t, s.ReadSensitive)
	require.Equal(t, []authz.Override{
		{ResourceID: fixture.DeskBookingPolicyNumber, Grant: authz.GrantDeny},
		{ResourceID: "POL-EXPENSES-000002", Grant: authz.GrantAllow},
	}, s.Overrides)
}

func TestSubject_Root(t *testing.T) {
	read := &fakeRead{users: map[string]*identityv1.User{fixture.Alice: {Id: fixture.Alice, IsRoot: true, Roles: []string{"site-admin"}}}}
	s, err := identity.New(read).Subject(context.Background(), fixture.Alice)
	require.NoError(t, err)
	require.True(t, s.Root)
	require.Contains(t, s.Roles, authz.RoleSiteAdmin)
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

func TestPrincipal_PersonCarriesGroupsAndTheRootRole(t *testing.T) {
	read := &fakeRead{users: map[string]*identityv1.User{
		fixture.Alice: {Id: fixture.Alice, IsRoot: true, Roles: []string{"site-admin"}, IdpGroups: []string{"Facilities"}},
		fixture.Frank: {Id: fixture.Frank, Roles: []string{"site-admin"}},
	}}
	c := identity.New(read)

	root, err := c.Principal(context.Background(), fixture.Alice)
	require.NoError(t, err)
	require.Equal(t, fixture.Alice, root.Person.ID)
	require.Equal(t, []string{"Facilities"}, root.Person.Groups)
	require.Equal(t, []string{authz.RootRole}, root.Person.Roles)
	require.True(t, root.Subject.Root)

	admin, err := c.Principal(context.Background(), fixture.Frank)
	require.NoError(t, err)
	require.Empty(t, admin.Person.Roles, "a non-root site admin has no engine role: the rules apply to them")
	require.Contains(t, admin.Subject.Roles, authz.RoleSiteAdmin)
}

func TestPeople_ListsLiveAccounts(t *testing.T) {
	read := &fakeRead{all: []*identityv1.User{
		{Id: fixture.Alice, IsRoot: true, Enabled: true},
		{Id: fixture.Carol, Enabled: true, IdpGroups: []string{"Approvers"}},
		{Id: fixture.Dave, Enabled: false},
		{Id: fixture.Erin, Enabled: true, DeletedAt: "2026-01-02T03:04:05Z"},
	}}
	people, err := identity.New(read).People(context.Background())
	require.NoError(t, err)
	require.Equal(t, []ordered.Person{
		{ID: fixture.Alice, Roles: []string{authz.RootRole}},
		{ID: fixture.Carol, Groups: []string{"Approvers"}},
	}, people)

	_, err = identity.New(&fakeRead{err: errors.New("down")}).People(context.Background())
	require.Error(t, err)
}
