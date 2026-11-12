// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package identity reads users from steward-identity and turns them into the
// rbac subjects and ordered people the access decisions are made for.
package identity

import (
	"context"
	"errors"
	"fmt"

	"github.com/Bugs5382/go-authz/ordered"

	identityv1 "github.com/Steward-GRC/steward-workflow/gen/go/thirdparty/identity/v1"
	"github.com/Steward-GRC/steward-workflow/internal/authz"
)

// ErrNoUser means identity answered without a user.
var ErrNoUser = errors.New("identity: no such user")

// Client reads users through identity's read service.
type Client struct {
	read identityv1.IdentityReadServiceClient
}

// New returns a Client on read.
func New(read identityv1.IdentityReadServiceClient) *Client { return &Client{read: read} }

// Subject returns the user's access as an rbac subject. Per-policy overrides
// are keyed by policy number.
func (c *Client) Subject(ctx context.Context, userID string) (authz.Subject, error) {
	p, err := c.Principal(ctx, userID)
	return p.Subject, err
}

// Principal returns the user as an rbac subject and as an ordered person.
func (c *Client) Principal(ctx context.Context, userID string) (authz.Principal, error) {
	resp, err := c.read.GetUser(ctx, &identityv1.GetUserRequest{UserId: userID})
	if err != nil {
		return authz.Principal{}, fmt.Errorf("identity GetUser %q: %w", userID, err)
	}
	u := resp.GetUser()
	if u == nil {
		return authz.Principal{}, fmt.Errorf("%w: %q", ErrNoUser, userID)
	}
	return principalOf(u), nil
}

// People lists the live accounts as ordered people, the set an approver pool is
// drawn from. A disabled or deleted account is left out.
func (c *Client) People(ctx context.Context) ([]ordered.Person, error) {
	resp, err := c.read.ListAllUsers(ctx, &identityv1.ListAllUsersRequest{})
	if err != nil {
		return nil, fmt.Errorf("identity ListAllUsers: %w", err)
	}
	out := make([]ordered.Person, 0, len(resp.GetUsers()))
	for _, u := range resp.GetUsers() {
		if !u.GetEnabled() || u.GetDeletedAt() != "" {
			continue
		}
		out = append(out, personOf(u))
	}
	return out, nil
}

// personOf is the user as the category rules see them: the groups the identity
// provider asserted, and the root role only for a root account.
func personOf(u *identityv1.User) ordered.Person {
	p := ordered.Person{ID: u.GetId(), Groups: u.GetIdpGroups()}
	if u.GetIsRoot() {
		p.Roles = []string{authz.RootRole}
	}
	return p
}

func principalOf(u *identityv1.User) authz.Principal {
	s := authz.Subject{
		UserID:        u.GetId(),
		Groups:        u.GetIdpGroups(),
		ReadSensitive: u.GetReadSensitiveGrant(),
		Root:          u.GetIsRoot(),
	}
	for _, name := range u.GetRoles() {
		if r, err := authz.ParseRole(name); err == nil {
			s.Roles = append(s.Roles, r)
		}
	}
	for _, sr := range u.GetScopedRoles() {
		if r, err := authz.ParseRole(sr.GetRole()); err == nil {
			s.ScopedGrants = append(s.ScopedGrants, authz.ScopedGrant{Role: r, Category: sr.GetCategory()})
		}
	}
	for _, o := range u.GetPolicyOverrides() {
		switch o.GetEffect() {
		case identityv1.OverrideEffect_OVERRIDE_EFFECT_ALLOW:
			s.Overrides = append(s.Overrides, authz.Override{ResourceID: o.GetPolicyNumber(), Grant: authz.GrantAllow})
		case identityv1.OverrideEffect_OVERRIDE_EFFECT_DENY:
			s.Overrides = append(s.Overrides, authz.Override{ResourceID: o.GetPolicyNumber(), Grant: authz.GrantDeny})
		}
	}
	return authz.Principal{Subject: s, Person: personOf(u)}
}

// IsWorkflowAdmin reports whether the user may manage workflows, and so
// reassign any approval as an admin: the catalog's workflow.manage.
func (c *Client) IsWorkflowAdmin(ctx context.Context, userID string) (bool, error) {
	s, err := c.Subject(ctx, userID)
	if err != nil {
		return false, err
	}
	return authz.HasCapability(s, authz.WorkflowManage), nil
}
