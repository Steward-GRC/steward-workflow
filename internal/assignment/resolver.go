// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package assignment works out which workflow a policy is submitted under.
package assignment

import "context"

// CategoryAssignmentFetcher returns a category's default workflow, "" for
// none.
type CategoryAssignmentFetcher interface {
	GetCategoryAssignment(ctx context.Context, categoryID string) (string, error)
}

// PolicyOverrideFetcher returns a policy's own workflow. exists is false with
// no override; exists with defID "" means the policy needs no approval.
type PolicyOverrideFetcher interface {
	GetPolicyOverride(ctx context.Context, policyID string) (defID string, exists bool, err error)
}

// Resolver resolves a policy's workflow.
type Resolver struct {
	categories CategoryAssignmentFetcher
	policies   PolicyOverrideFetcher
}

// NewResolver returns a Resolver.
func NewResolver(categories CategoryAssignmentFetcher, policies PolicyOverrideFetcher) *Resolver {
	return &Resolver{categories: categories, policies: policies}
}

// Resolve returns the workflow for policyID, whose categories run from its
// home category (index 0) to the root: the policy's override when it has one
// (even "no workflow"), else the home category's own default. Workflows are
// not inherited from ancestor categories. "" means no workflow.
func (r *Resolver) Resolve(ctx context.Context, policyID string, ancestorCategoryIDs []string) (string, error) {
	defID, exists, err := r.policies.GetPolicyOverride(ctx, policyID)
	if err != nil {
		return "", err
	}
	if exists {
		return defID, nil
	}
	if len(ancestorCategoryIDs) == 0 {
		return "", nil
	}
	return r.categories.GetCategoryAssignment(ctx, ancestorCategoryIDs[0])
}
