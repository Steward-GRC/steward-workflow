// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package authz_test

import (
	"testing"

	"github.com/Bugs5382/go-authz/ordered"
	"github.com/stretchr/testify/require"

	"github.com/Steward-GRC/steward-workflow/internal/authz"
)

func TestAccessModel_RootIsExemptExceptAck(t *testing.T) {
	rs := ordered.RuleSet{Categories: map[string]ordered.Category{
		"c": {Entries: []ordered.Entry{
			{Subject: ordered.Everyone, Action: authz.ActApprove, Effect: ordered.Deny},
			{Subject: ordered.Everyone, Action: authz.ActAck, Effect: ordered.Deny},
		}},
	}}
	root := ordered.Person{ID: "r", Roles: []string{authz.RootRole}}
	m := authz.AccessModel()
	require.True(t, m.Decide(rs, root, "c", authz.ActApprove).Allowed, "root approves past a deny")
	require.False(t, m.Decide(rs, root, "c", authz.ActAck).Allowed, "root follows the Ack rule")
}

func TestAccessModel_NonRootSiteAdminFollowsTheRules(t *testing.T) {
	rs := ordered.RuleSet{Categories: map[string]ordered.Category{
		"c": {Entries: []ordered.Entry{{Subject: "user:a", Action: authz.ActApprove, Effect: ordered.Deny}}},
	}}
	admin := ordered.Person{ID: "a", Roles: []string{string(authz.RoleSiteAdmin)}}
	require.False(t, authz.AccessModel().Decide(rs, admin, "c", authz.ActApprove).Allowed)
}

func TestAccessModel_OwnersApproveAndAreImmuneToDeny(t *testing.T) {
	rs := ordered.RuleSet{Categories: map[string]ordered.Category{
		"c": {
			Owners:  []string{"user:o"},
			Entries: []ordered.Entry{{Subject: "user:o", Action: authz.ActApprove, Effect: ordered.Deny}},
		},
	}}
	o := ordered.Person{ID: "o"}
	m := authz.AccessModel()
	require.True(t, m.Decide(rs, o, "c", authz.ActApprove).Allowed)
	require.False(t, m.Decide(rs, o, "c", authz.ActAck).Allowed, "owners do not acknowledge")
}
