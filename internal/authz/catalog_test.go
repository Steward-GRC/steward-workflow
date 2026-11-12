// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package authz_test

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/Bugs5382/go-authz/rbac"
	"github.com/stretchr/testify/require"

	"github.com/Steward-GRC/steward-workflow/internal/authz"
)

// The embedded catalog is a copy of the canonical one; any drift is a failure.
func TestEmbeddedCatalogMatchesTheCanonicalCatalog(t *testing.T) {
	canonical, err := os.ReadFile("testdata/catalog.canonical.json")
	require.NoError(t, err)
	want, err := rbac.Parse(canonical)
	require.NoError(t, err)

	require.Empty(t, rbac.Diff(authz.Catalog, want), "embedded catalog drifted from testdata/catalog.canonical.json")
}

func TestEmbeddedCatalogIsByteIdenticalToTheCanonicalFile(t *testing.T) {
	canonical, err := os.ReadFile("testdata/catalog.canonical.json")
	require.NoError(t, err)
	embedded, err := os.ReadFile("catalog.json")
	require.NoError(t, err)
	require.True(t, bytes.Equal(canonical, embedded), "internal/authz/catalog.json differs from the canonical copy")
}

func TestCatalogSourceNamesTheCommit(t *testing.T) {
	b, err := os.ReadFile("testdata/CATALOG_SOURCE")
	require.NoError(t, err)
	src := string(b)
	require.Contains(t, src, "repo: Steward-GRC/steward-gateway")
	require.Contains(t, src, "path: internal/authz/catalog.json")
	var commit string
	for _, l := range strings.Split(src, "\n") {
		if v, ok := strings.CutPrefix(l, "commit: "); ok {
			commit = v
		}
	}
	require.Regexp(t, `^[0-9a-f]{40}$`, commit)
}

func TestCatalogPermissionsUsedHere(t *testing.T) {
	for _, p := range []authz.Permission{authz.PolicyRead, authz.WorkflowManage} {
		_, err := authz.Catalog.ParsePermission(string(p))
		require.NoError(t, err, p)
	}
}

func TestParseRoleRejectsAnUnknownRole(t *testing.T) {
	_, err := authz.ParseRole("admin")
	require.ErrorIs(t, err, rbac.ErrUnknownRole)
	r, err := authz.ParseRole("template-admin")
	require.NoError(t, err)
	require.Equal(t, authz.RoleTemplateAdmin, r)
}

func TestHasCapability(t *testing.T) {
	require.True(t, authz.HasCapability(authz.Subject{Roles: []authz.Role{authz.RoleTemplateAdmin}}, authz.WorkflowManage))
	require.True(t, authz.HasCapability(authz.Subject{Roles: []authz.Role{authz.RoleSiteAdmin}}, authz.WorkflowManage))
	require.False(t, authz.HasCapability(authz.Subject{UserID: "u"}, authz.WorkflowManage))
}
