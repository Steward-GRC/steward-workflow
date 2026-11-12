// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package authz_test

import (
	"testing"

	"github.com/Bugs5382/go-authz/ordered"
	"github.com/stretchr/testify/require"

	"github.com/Steward-GRC/steward-workflow/internal/authz"
)

func denyAll(subject string) ordered.RuleSet {
	var es []ordered.Entry
	for _, a := range []string{authz.ActRead, authz.ActAuthor, authz.ActApprove, authz.ActManage, authz.ActAck} {
		es = append(es, ordered.Entry{Subject: subject, Action: a, Effect: ordered.Deny})
	}
	return ordered.RuleSet{Categories: map[string]ordered.Category{"c": {Entries: es}}}
}

// Root always has Read, Author and Manage: a deny saved against root, as a user
// or through a group, is reported but never applied.
func TestAccessModel_RootAlwaysReadAuthorManage(t *testing.T) {
	m := authz.AccessModel()
	root := ordered.Person{ID: "r", Groups: []string{"Staff"}, Roles: []string{authz.RootRole}}
	for _, subject := range []string{"user:r", "group:Staff"} {
		rs := denyAll(subject)
		for _, act := range []string{authz.ActRead, authz.ActAuthor, authz.ActManage} {
			d := m.Decide(rs, root, "c", act)
			require.True(t, d.Allowed, "%s %s", subject, act)
			require.Equal(t, ordered.ReasonSuperuser, d.Reason, "%s %s", subject, act)
			require.NotNil(t, d.ConfiguredBlock, "%s %s: the saved deny is reported", subject, act)
			require.Equal(t, subject, d.ConfiguredBlock.Subject)
		}
	}
}

// Approve and Ack are not global for root: they follow the rows.
func TestAccessModel_ApproveAndAckFollowTheRowsForRoot(t *testing.T) {
	m := authz.AccessModel()
	root := ordered.Person{ID: "r", Roles: []string{authz.RootRole}}
	empty := ordered.RuleSet{Categories: map[string]ordered.Category{"c": {}}}
	require.False(t, m.Decide(empty, root, "c", authz.ActApprove).Allowed, "no Approve cell, no approval")
	deny := denyAll("user:r")
	require.False(t, m.Decide(deny, root, "c", authz.ActApprove).Allowed, "an Approve deny applies")
	require.False(t, m.Decide(deny, root, "c", authz.ActAck).Allowed, "an Ack deny applies")
	allow := ordered.RuleSet{Categories: map[string]ordered.Category{"c": {Entries: []ordered.Entry{
		{Subject: "user:r", Action: authz.ActApprove, Effect: ordered.Allow},
	}}}}
	require.True(t, m.Decide(allow, root, "c", authz.ActApprove).Allowed, "an Approve allow applies")
}

// A site admin who is not root follows the rules in every action.
func TestAccessModel_NonRootSiteAdminFollowsTheRules(t *testing.T) {
	admin := ordered.Person{ID: "a", Roles: []string{string(authz.RoleSiteAdmin)}}
	rs := denyAll("user:a")
	for _, act := range []string{authz.ActRead, authz.ActAuthor, authz.ActApprove, authz.ActManage, authz.ActAck} {
		require.False(t, authz.AccessModel().Decide(rs, admin, "c", act).Allowed, act)
	}
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
