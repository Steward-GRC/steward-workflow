// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package access drops the approvers who can't read the policy they would
// approve, using steward-authz's read decision, and supplies the home
// category's owners as the backstop when too few are left.
package access

import (
	"context"
	"fmt"

	stewardauthz "github.com/Steward-GRC/steward-authz"
	"google.golang.org/grpc"

	corev1 "github.com/Steward-GRC/steward-workflow/gen/go/thirdparty/core/v1"
)

// PolicyRef is what the read decision needs to know about a policy.
type PolicyRef struct {
	Number         string
	HomeCategoryID string
	Sensitive      bool
}

// SubjectReader returns a user's access. Implemented by identity.Client.
type SubjectReader interface {
	Subject(ctx context.Context, userID string) (stewardauthz.Subject, error)
}

// CoreReader reads the policy and its category from core.
type CoreReader interface {
	PolicyRefForVersion(ctx context.Context, policyVersionID string) (PolicyRef, error)
	CategoryOwners(ctx context.Context, categoryID string) ([]string, error)
}

// CanRead reports whether s may read the policy. assigned says s is one of
// its approvers, which lets them read a sensitive policy.
func CanRead(s stewardauthz.Subject, p PolicyRef, assigned bool) bool {
	r := &stewardauthz.Resource{ID: p.Number, Category: p.HomeCategoryID, Sensitive: p.Sensitive}
	if assigned {
		r.Approvers = []string{s.UserID}
	}
	return stewardauthz.Authorize(s, stewardauthz.PolicyRead, r).Allowed()
}

// Resolver filters approver pools.
type Resolver struct {
	subjects SubjectReader
	core     CoreReader
}

// NewResolver returns a Resolver.
func NewResolver(subjects SubjectReader, core CoreReader) *Resolver {
	return &Resolver{subjects: subjects, core: core}
}

// Eligible returns the candidates who can read the version's policy as its
// approvers, and the owners of its home category.
func (r *Resolver) Eligible(ctx context.Context, policyVersionID string, candidates []string) (eligible, owners []string, err error) {
	ref, err := r.core.PolicyRefForVersion(ctx, policyVersionID)
	if err != nil {
		return nil, nil, err
	}
	eligible = make([]string, 0, len(candidates))
	for _, uid := range candidates {
		s, err := r.subjects.Subject(ctx, uid)
		if err != nil {
			return nil, nil, err
		}
		if CanRead(s, ref, true) {
			eligible = append(eligible, uid)
		}
	}
	owners, err = r.core.CategoryOwners(ctx, ref.HomeCategoryID)
	if err != nil {
		return nil, nil, err
	}
	return eligible, owners, nil
}

// MeetsQuorum reports whether a pool of poolSize can still satisfy the quorum.
func MeetsQuorum(quorum string, quorumN, poolSize int) bool {
	if quorum == "nofm" {
		return poolSize >= quorumN
	}
	return poolSize > 0
}

// PolicyClient is the part of core's PolicyService the adapter calls.
type PolicyClient interface {
	GetPolicyVersion(ctx context.Context, in *corev1.GetPolicyVersionRequest, opts ...grpc.CallOption) (*corev1.GetPolicyVersionResponse, error)
	GetPolicy(ctx context.Context, in *corev1.GetPolicyRequest, opts ...grpc.CallOption) (*corev1.GetPolicyResponse, error)
}

// CategoryClient is the part of core's CategoryService the adapter calls.
type CategoryClient interface {
	GetCategory(ctx context.Context, in *corev1.GetCategoryRequest, opts ...grpc.CallOption) (*corev1.GetCategoryResponse, error)
}

// CoreAdapter is the CoreReader on core's gRPC clients.
type CoreAdapter struct {
	policies   PolicyClient
	categories CategoryClient
}

// NewCoreAdapter returns a CoreAdapter.
func NewCoreAdapter(policies PolicyClient, categories CategoryClient) *CoreAdapter {
	return &CoreAdapter{policies: policies, categories: categories}
}

// PolicyRefForVersion reads the version's policy.
func (a *CoreAdapter) PolicyRefForVersion(ctx context.Context, policyVersionID string) (PolicyRef, error) {
	pv, err := a.policies.GetPolicyVersion(ctx, &corev1.GetPolicyVersionRequest{Id: policyVersionID})
	if err != nil {
		return PolicyRef{}, fmt.Errorf("core GetPolicyVersion %q: %w", policyVersionID, err)
	}
	pol, err := a.policies.GetPolicy(ctx, &corev1.GetPolicyRequest{Id: pv.GetVersion().GetPolicyId()})
	if err != nil {
		return PolicyRef{}, fmt.Errorf("core GetPolicy %q: %w", pv.GetVersion().GetPolicyId(), err)
	}
	p := pol.GetPolicy()
	return PolicyRef{
		Number:         p.GetNumber(),
		HomeCategoryID: p.GetHomeCategoryId(),
		Sensitive:      p.GetSensitivity() == corev1.Sensitivity_SENSITIVITY_SENSITIVE,
	}, nil
}

// CategoryOwners reads a category's owners.
func (a *CoreAdapter) CategoryOwners(ctx context.Context, categoryID string) ([]string, error) {
	resp, err := a.categories.GetCategory(ctx, &corev1.GetCategoryRequest{Id: categoryID})
	if err != nil {
		return nil, fmt.Errorf("core GetCategory %q: %w", categoryID, err)
	}
	return resp.GetCategory().GetOwners(), nil
}
