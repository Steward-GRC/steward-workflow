// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package authz holds workflow's permission catalog and its category access
// model. The catalog is the canonical Steward catalog (catalog.json, a
// byte-identical copy of the one in steward-gateway, checked against
// testdata/catalog.canonical.json); the mechanism is go-authz's rbac package.
// Category rules are decided by go-authz's ordered package, configured here.
package authz

import (
	_ "embed"

	"github.com/Bugs5382/go-authz/ordered"
	"github.com/Bugs5382/go-authz/rbac"
)

//go:embed catalog.json
var catalogJSON []byte

// Catalog is the parsed canonical catalog. It is read-only; a catalog that does
// not parse stops the process at start-up.
var Catalog = rbac.MustParse(catalogJSON)

// The rbac types workflow uses, so callers import one package.
type (
	Subject     = rbac.Subject
	Resource    = rbac.Resource
	Decision    = rbac.Decision
	Override    = rbac.Override
	ScopedGrant = rbac.ScopedGrant
	Permission  = rbac.Permission
	Role        = rbac.Role
)

// The override values.
const (
	GrantAllow = rbac.GrantAllow
	GrantDeny  = rbac.GrantDeny
)

// The catalog permissions workflow checks.
const (
	PolicyRead     Permission = "policy.read"
	WorkflowManage Permission = "workflow.manage"
)

// The catalog roles workflow names.
const (
	RoleApprover      Role = "approver"
	RoleTemplateAdmin Role = "template-admin"
	RoleSiteAdmin     Role = "site-admin"
)

// Authorize decides one permission for s on r (nil when the action is not aimed
// at one resource).
func Authorize(s Subject, p Permission, r *Resource) Decision {
	return Catalog.Authorize(s, p, r)
}

// HasCapability reports whether s could hold p in at least one scope.
func HasCapability(s Subject, p Permission) bool {
	return Catalog.HasCapability(s, p)
}

// ParseRole returns the catalog role named s, or an error for a role the
// catalog does not know.
func ParseRole(s string) (Role, error) { return Catalog.ParseRole(s) }

// The five actions of the category access model, as the ordered engine spells
// them.
const (
	ActRead    = "read"
	ActAuthor  = "author"
	ActApprove = "approve"
	ActManage  = "manage"
	ActAck     = "ack"
)

// RootRole is the engine role of the root seat. It comes from identity's
// is_root flag, never from a user name or id.
const RootRole = "root"

// AccessModel is the ordered engine configured for Steward: the five default
// actions (Manage implies Read and Author, Approve and Author imply Read, Ack
// requires Read; owners hold Read, Author, Approve and Manage but not Ack) and
// the root seat. Root always holds Read, Author and Manage: no row can take
// those away, and a deny saved against root is reported as the decision's
// ConfiguredBlock but never applied. Approve and Ack are not global for root:
// they follow the rows like anyone else's, and so does a site admin who is not
// root, in every action.
func AccessModel() ordered.Model {
	m := ordered.DefaultModel()
	m.Superusers = &ordered.Superusers{
		Subjects: []string{"role:" + RootRole},
		Actions:  []string{ActRead, ActAuthor, ActManage},
	}
	return m
}

// Principal is one signed-in person as both engines see them: the rbac subject
// (roles, scoped grants, per-document overrides) and the ordered person
// (groups and the root role) the category rules are read against.
type Principal struct {
	Subject Subject
	Person  ordered.Person
}
