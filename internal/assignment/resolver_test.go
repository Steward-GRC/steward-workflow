// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package assignment

import (
	"context"
	"testing"

	corev1 "github.com/Steward-GRC/steward-workflow/gen/go/thirdparty/core/v1"
)

// coreCategoryFetcher builds a CoreCategoryFetcher backed by an in-memory fake core
// CategoryService, so the resolver tests exercise the real core adapter.
func coreCategoryFetcher(defaults map[string]string) *CoreCategoryFetcher {
	groups := make(map[string]*corev1.Category, len(defaults))
	for gid, wf := range defaults {
		groups[gid] = &corev1.Category{Id: gid, DefaultWorkflowId: wf}
	}
	return &CoreCategoryFetcher{client: &fakeCoreCategoryClient{categories: groups}}
}

// stubOverrideFetcher implements PolicyOverrideFetcher for tests.
type stubOverrideFetcher struct {
	defID  string
	exists bool
}

func (s *stubOverrideFetcher) GetPolicyOverride(ctx context.Context, policyID string) (string, bool, error) {
	return s.defID, s.exists, nil
}

func TestResolve_PolicyOverrideTakesPrecedence(t *testing.T) {
	r := NewResolver(
		coreCategoryFetcher(map[string]string{"leaf": "group-def-id"}),
		&stubOverrideFetcher{defID: "override-def-id", exists: true},
	)
	defID, err := r.Resolve(context.Background(), "policy-1", []string{"leaf", "root"})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if defID != "override-def-id" {
		t.Fatalf("expected override-def-id, got %q", defID)
	}
}

func TestResolve_PolicyOverrideExplicitNoWorkflow(t *testing.T) {
	// override exists but is empty string → policy explicitly requires no approval
	r := NewResolver(
		coreCategoryFetcher(map[string]string{"leaf": "group-def-id"}),
		&stubOverrideFetcher{defID: "", exists: true},
	)
	defID, err := r.Resolve(context.Background(), "policy-1", []string{"leaf", "root"})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if defID != "" {
		t.Fatalf("expected empty (no workflow), got %q", defID)
	}
}

func TestResolve_HomeCategoryOwnAssignment(t *testing.T) {
	r := NewResolver(
		coreCategoryFetcher(map[string]string{"leaf": "leaf-def", "root": "root-def"}),
		&stubOverrideFetcher{}, // no override
	)
	defID, err := r.Resolve(context.Background(), "policy-1", []string{"leaf", "root"})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if defID != "leaf-def" {
		t.Fatalf("expected leaf-def (home category's own workflow), got %q", defID)
	}
}

func TestResolve_NoInheritanceFromParent(t *testing.T) {
	// The home (leaf) category has no workflow; an ancestor does. Workflows are NOT
	// inherited between categories, so this resolves to "" (no workflow) — NOT the
	// parent's; the policy publishes without approval.
	r := NewResolver(
		coreCategoryFetcher(map[string]string{"root": "root-def"}), // only the ancestor has one
		&stubOverrideFetcher{},
	)
	defID, err := r.Resolve(context.Background(), "policy-1", []string{"leaf", "root"})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if defID != "" {
		t.Fatalf("expected empty (no inheritance from parent), got %q", defID)
	}
}

// TestResolve_HomeCategoryFromCoreDefault: a policy whose home category's
// default workflow is set in core resolves to it.
func TestResolve_HomeCategoryFromCoreDefault(t *testing.T) {
	const (
		facilities = "6f1d2c3b-0000-4000-8000-000000000001"
		standard   = "6f1d2c3b-0000-4000-8000-0000000000a1"
	)
	r := NewResolver(
		coreCategoryFetcher(map[string]string{facilities: standard}),
		&stubOverrideFetcher{},
	)
	defID, err := r.Resolve(context.Background(), "policy-desk-booking", []string{facilities})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if defID != standard {
		t.Fatalf("expected %q, got %q", standard, defID)
	}
}

func TestResolve_NoAssignmentAnywhere(t *testing.T) {
	r := NewResolver(
		coreCategoryFetcher(map[string]string{}),
		&stubOverrideFetcher{},
	)
	defID, err := r.Resolve(context.Background(), "policy-1", []string{"leaf", "root"})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if defID != "" {
		t.Fatalf("expected empty (no workflow), got %q", defID)
	}
}
