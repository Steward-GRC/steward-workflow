// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package identity reads users from steward-identity and turns them into
// steward-authz subjects.
package identity

import (
	"context"
	"errors"
	"fmt"

	stewardauthz "github.com/Steward-GRC/steward-authz"

	identityv1 "github.com/Steward-GRC/steward-workflow/gen/go/thirdparty/identity/v1"
)

// ErrNoUser means identity answered without a user.
var ErrNoUser = errors.New("identity: no such user")

// Client reads users through identity's read service.
type Client struct {
	read identityv1.IdentityReadServiceClient
}

// New returns a Client on read.
func New(read identityv1.IdentityReadServiceClient) *Client { return &Client{read: read} }

// Subject returns the user's access as a steward-authz subject. Per-policy
// overrides are keyed by policy number.
func (c *Client) Subject(ctx context.Context, userID string) (stewardauthz.Subject, error) {
	resp, err := c.read.GetUser(ctx, &identityv1.GetUserRequest{UserId: userID})
	if err != nil {
		return stewardauthz.Subject{}, fmt.Errorf("identity GetUser %q: %w", userID, err)
	}
	u := resp.GetUser()
	if u == nil {
		return stewardauthz.Subject{}, fmt.Errorf("%w: %q", ErrNoUser, userID)
	}
	s := stewardauthz.Subject{
		UserID:        u.GetId(),
		Groups:        u.GetIdpGroups(),
		ReadSensitive: u.GetReadSensitiveGrant(),
		Root:          u.GetIsRoot(),
	}
	for _, name := range u.GetRoles() {
		if r, err := stewardauthz.ParseRole(name); err == nil {
			s.Roles = append(s.Roles, r)
		}
	}
	for _, sr := range u.GetScopedRoles() {
		if r, err := stewardauthz.ParseRole(sr.GetRole()); err == nil {
			s.ScopedGrants = append(s.ScopedGrants, stewardauthz.ScopedGrant{Role: r, Category: sr.GetCategory()})
		}
	}
	for _, o := range u.GetPolicyOverrides() {
		switch o.GetEffect() {
		case identityv1.OverrideEffect_OVERRIDE_EFFECT_ALLOW:
			s.Overrides = append(s.Overrides, stewardauthz.Override{ResourceID: o.GetPolicyNumber(), Grant: stewardauthz.GrantAllow})
		case identityv1.OverrideEffect_OVERRIDE_EFFECT_DENY:
			s.Overrides = append(s.Overrides, stewardauthz.Override{ResourceID: o.GetPolicyNumber(), Grant: stewardauthz.GrantDeny})
		}
	}
	return s, nil
}
