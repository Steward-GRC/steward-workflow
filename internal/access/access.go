// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package access decides who may approve a policy. The category's rules, read
// from core as the effective ruleset chain and decided by go-authz's ordered
// engine, say who holds Approve; the catalog's read decision (rbac) still
// applies, so a per-document deny override or a sensitive policy keeps an
// approver out. The home category's owners and root back the stage up when too
// few designated approvers are left.
package access

import (
	"context"
	"fmt"

	"github.com/Bugs5382/go-authz/ordered"
	"google.golang.org/grpc"

	corev1 "github.com/Steward-GRC/steward-workflow/gen/go/thirdparty/core/v1"
	"github.com/Steward-GRC/steward-workflow/internal/authz"
)

// PolicyRef is what the read and approve decisions need to know about a policy.
type PolicyRef struct {
	Number         string
	HomeCategoryID string
	Sensitive      bool
}

// PrincipalReader reads people from identity. Implemented by identity.Client.
type PrincipalReader interface {
	Principal(ctx context.Context, userID string) (authz.Principal, error)
	People(ctx context.Context) ([]ordered.Person, error)
}

// CoreReader reads the policy and its category rules from core.
type CoreReader interface {
	PolicyRefForVersion(ctx context.Context, policyVersionID string) (PolicyRef, error)
	// Ruleset returns the category's rules and owners together with its
	// ancestors', as the engine reads them.
	Ruleset(ctx context.Context, categoryID string) (ordered.RuleSet, error)
}

// CanRead reports whether s may read the policy. assigned says s is one of
// its approvers, which lets them read a sensitive policy.
func CanRead(s authz.Subject, p PolicyRef, assigned bool) bool {
	r := &authz.Resource{ID: p.Number, Category: p.HomeCategoryID, Sensitive: p.Sensitive}
	if assigned {
		r.Approvers = []string{s.UserID}
	}
	return authz.Authorize(s, authz.PolicyRead, r).Allowed()
}

// Resolver filters approver pools.
type Resolver struct {
	people PrincipalReader
	core   CoreReader
	model  ordered.Model
}

// NewResolver returns a Resolver.
func NewResolver(people PrincipalReader, core CoreReader) *Resolver {
	return &Resolver{people: people, core: core, model: authz.AccessModel()}
}

// Eligible returns the candidates, in their order, who may approve the
// version's policy: the category rules allow them Approve and they can read the
// policy as one of its approvers. It never adds anyone; the candidates are the
// designated approvers (ApproverIDs, the category list or group members).
func (r *Resolver) Eligible(ctx context.Context, policyVersionID string, candidates []string) ([]string, error) {
	ref, err := r.core.PolicyRefForVersion(ctx, policyVersionID)
	if err != nil {
		return nil, err
	}
	rs, err := r.core.Ruleset(ctx, ref.HomeCategoryID)
	if err != nil {
		return nil, err
	}
	eligible := make([]string, 0, len(candidates))
	for _, uid := range candidates {
		p, err := r.people.Principal(ctx, uid)
		if err != nil {
			return nil, err
		}
		if r.model.Decide(rs, p.Person, ref.HomeCategoryID, authz.ActApprove).Allowed && CanRead(p.Subject, ref, true) {
			eligible = append(eligible, uid)
		}
	}
	return eligible, nil
}

// Backstop returns the people who stand in when too few designated approvers
// are left: the owners of the version's home category (and its ancestors),
// then root.
func (r *Resolver) Backstop(ctx context.Context, policyVersionID string) ([]string, error) {
	ref, err := r.core.PolicyRefForVersion(ctx, policyVersionID)
	if err != nil {
		return nil, err
	}
	pool, err := r.ApproverPool(ctx, ref.HomeCategoryID)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(pool))
	for _, e := range pool {
		if e.Reason == ordered.ReasonOwner || e.Reason == ordered.ReasonSuperuser {
			out = append(out, e.Person)
		}
	}
	return out, nil
}

// ApproverPool lists everyone who may approve in the category: its owners, then
// root as a global approver, then each person the rules allow Approve. It is
// drawn from the accounts identity lists.
func (r *Resolver) ApproverPool(ctx context.Context, categoryID string) ([]ordered.PoolEntry, error) {
	rs, err := r.core.Ruleset(ctx, categoryID)
	if err != nil {
		return nil, err
	}
	everyone, err := r.people.People(ctx)
	if err != nil {
		return nil, err
	}
	return r.model.Pool(rs, everyone, categoryID, authz.ActApprove), nil
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
	GetEffectiveRuleset(ctx context.Context, in *corev1.GetEffectiveRulesetRequest, opts ...grpc.CallOption) (*corev1.GetEffectiveRulesetResponse, error)
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

// Ruleset reads the category's whole chain in one call and builds the engine's
// rule set from it. A policy with no home category has no rules: nothing is
// allowed but what root, being exempt, holds.
func (a *CoreAdapter) Ruleset(ctx context.Context, categoryID string) (ordered.RuleSet, error) {
	if categoryID == "" {
		return ordered.RuleSet{Categories: map[string]ordered.Category{}, Parents: map[string]string{}}, nil
	}
	resp, err := a.categories.GetEffectiveRuleset(ctx, &corev1.GetEffectiveRulesetRequest{CategoryId: categoryID})
	if err != nil {
		return ordered.RuleSet{}, fmt.Errorf("core GetEffectiveRuleset %q: %w", categoryID, err)
	}
	if len(resp.GetLevels()) == 0 {
		return ordered.RuleSet{}, fmt.Errorf("core GetEffectiveRuleset %q: no levels", categoryID)
	}
	return ruleSetOf(resp.GetLevels()), nil
}

// ruleSetOf builds the rule set from the chain, nearest first. Owners are
// people: core stores their user ids and the engine matches "user:<id>".
func ruleSetOf(levels []*corev1.EffectiveLevel) ordered.RuleSet {
	rs := ordered.RuleSet{Categories: make(map[string]ordered.Category, len(levels)), Parents: map[string]string{}}
	for i, l := range levels {
		id := l.GetCategoryId()
		owners := make([]string, 0, len(l.GetOwners()))
		for _, o := range l.GetOwners() {
			owners = append(owners, "user:"+o)
		}
		rs.Categories[id] = ordered.Category{Entries: entriesOf(l.GetRules()), Owners: owners}
		if i+1 < len(levels) {
			rs.Parents[id] = levels[i+1].GetCategoryId()
		}
	}
	return rs
}

// entriesOf turns the ordered rows into cells, one per action the row has an
// opinion on. A blank cell is skipped by the engine.
func entriesOf(rules []*corev1.CategoryRule) []ordered.Entry {
	var out []ordered.Entry
	for _, r := range rules {
		subject := subjectOf(r)
		if subject == "" {
			continue
		}
		for _, c := range []struct {
			action string
			effect corev1.GrantEffect
		}{
			{authz.ActRead, r.GetRead()},
			{authz.ActAuthor, r.GetAuthor()},
			{authz.ActApprove, r.GetApprove()},
			{authz.ActManage, r.GetManage()},
			{authz.ActAck, r.GetAck()},
		} {
			switch c.effect {
			case corev1.GrantEffect_GRANT_EFFECT_ALLOW:
				out = append(out, ordered.Entry{Subject: subject, Action: c.action, Effect: ordered.Allow})
			case corev1.GrantEffect_GRANT_EFFECT_DENY:
				out = append(out, ordered.Entry{Subject: subject, Action: c.action, Effect: ordered.Deny})
			}
		}
	}
	return out
}

func subjectOf(r *corev1.CategoryRule) string {
	switch r.GetSubjectKind() {
	case corev1.RuleSubjectKind_RULE_SUBJECT_KIND_EVERYONE:
		return ordered.Everyone
	case corev1.RuleSubjectKind_RULE_SUBJECT_KIND_GROUP:
		return "group:" + r.GetSubjectRef()
	case corev1.RuleSubjectKind_RULE_SUBJECT_KIND_USER:
		return "user:" + r.GetSubjectRef()
	default:
		return ""
	}
}
