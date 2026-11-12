// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package access

import (
	"context"
	"errors"
	"testing"

	"github.com/Bugs5382/go-authz/ordered"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	corev1 "github.com/Steward-GRC/steward-workflow/gen/go/thirdparty/core/v1"
	"github.com/Steward-GRC/steward-workflow/internal/authz"
	"github.com/Steward-GRC/steward-workflow/internal/fixture"
)

// Test people. The ids are the fixture's invented users.
const (
	owner    = fixture.Dave
	member   = fixture.Carol // in the Approvers group
	outsider = fixture.Erin  // in no group
	rootID   = fixture.Alice
	admin    = fixture.Frank // a non-root site admin
)

type fakePrincipals struct {
	principals map[string]authz.Principal
	people     []ordered.Person
	err        error
}

func (f fakePrincipals) Principal(_ context.Context, id string) (authz.Principal, error) {
	if f.err != nil {
		return authz.Principal{}, f.err
	}
	if p, ok := f.principals[id]; ok {
		return p, nil
	}
	person := ordered.Person{ID: id}
	for _, q := range f.people {
		if q.ID == id {
			person = q
		}
	}
	return authz.Principal{Subject: authz.Subject{UserID: id}, Person: person}, nil
}

func (f fakePrincipals) People(context.Context) ([]ordered.Person, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.people, nil
}

type fakeCore struct {
	ref PolicyRef
	rs  ordered.RuleSet
}

func (f fakeCore) PolicyRefForVersion(context.Context, string) (PolicyRef, error) { return f.ref, nil }
func (f fakeCore) Ruleset(context.Context, string) (ordered.RuleSet, error)       { return f.rs, nil }

func people() []ordered.Person {
	return []ordered.Person{
		{ID: owner},
		{ID: member, Groups: []string{"Approvers"}},
		{ID: outsider},
		{ID: rootID, Roles: []string{authz.RootRole}},
		{ID: admin, Groups: []string{"Approvers"}, Roles: []string{string(authz.RoleSiteAdmin)}},
	}
}

func ruleset(owners []string, entries ...ordered.Entry) ordered.RuleSet {
	return ordered.RuleSet{
		Categories: map[string]ordered.Category{fixture.Facilities: {Owners: owners, Entries: entries}},
		Parents:    map[string]string{},
	}
}

func ids(es []ordered.PoolEntry) []string {
	out := make([]string, 0, len(es))
	for _, e := range es {
		out = append(out, e.Person)
	}
	return out
}

var approversMayApprove = ordered.Entry{Subject: "group:Approvers", Action: authz.ActApprove, Effect: ordered.Allow}

func newResolver(rs ordered.RuleSet) *Resolver {
	pr := fakePrincipals{people: people()}
	core := fakeCore{ref: PolicyRef{Number: fixture.DeskBookingPolicyNumber, HomeCategoryID: fixture.Facilities}, rs: rs}
	return NewResolver(pr, core)
}

// The pool is the owners, then root, then whoever Approve allows.
func TestApproverPool_OwnersRootAndApproveAllowed(t *testing.T) {
	rs := ruleset([]string{"user:" + owner}, approversMayApprove)
	pool, err := newResolver(rs).ApproverPool(context.Background(), fixture.Facilities)
	require.NoError(t, err)
	require.Equal(t, []string{owner, rootID, member, admin}, ids(pool))
	require.Equal(t, ordered.ReasonOwner, pool[0].Reason)
	require.Equal(t, ordered.ReasonSuperuser, pool[1].Reason)
}

func TestApproverPool_ADenyRemovesSomeoneFromThePool(t *testing.T) {
	rs := ruleset(nil,
		ordered.Entry{Subject: "user:" + member, Action: authz.ActApprove, Effect: ordered.Deny},
		approversMayApprove,
	)
	pool, err := newResolver(rs).ApproverPool(context.Background(), fixture.Facilities)
	require.NoError(t, err)
	require.NotContains(t, ids(pool), member, "the user row above the group row denies")
	require.Contains(t, ids(pool), admin)
}

func TestApproverPool_ANonRootAdminFollowsADeny(t *testing.T) {
	rs := ruleset(nil,
		ordered.Entry{Subject: "user:" + admin, Action: authz.ActApprove, Effect: ordered.Deny},
		ordered.Entry{Subject: "user:" + rootID, Action: authz.ActApprove, Effect: ordered.Deny},
		approversMayApprove,
	)
	pool, err := newResolver(rs).ApproverPool(context.Background(), fixture.Facilities)
	require.NoError(t, err)
	require.NotContains(t, ids(pool), admin, "a site admin who is not root is bound by the deny")
	require.Contains(t, ids(pool), rootID, "root is a global approver")
}

func TestApproverPool_EachActionIsStrict(t *testing.T) {
	// Read, author and manage cells never put anyone in the approve pool, and a
	// read deny does not take an approve allow away.
	rs := ruleset(nil,
		ordered.Entry{Subject: "user:" + member, Action: authz.ActRead, Effect: ordered.Deny},
		ordered.Entry{Subject: "user:" + member, Action: authz.ActApprove, Effect: ordered.Allow},
		ordered.Entry{Subject: "user:" + outsider, Action: authz.ActRead, Effect: ordered.Allow},
		ordered.Entry{Subject: "user:" + outsider, Action: authz.ActAuthor, Effect: ordered.Allow},
	)
	pool, err := newResolver(rs).ApproverPool(context.Background(), fixture.Facilities)
	require.NoError(t, err)
	require.Contains(t, ids(pool), member)
	require.NotContains(t, ids(pool), outsider)
}

func TestApproverPool_ManageDoesNotImplyApprove(t *testing.T) {
	rs := ruleset(nil, ordered.Entry{Subject: "user:" + outsider, Action: authz.ActManage, Effect: ordered.Allow})
	m := authz.AccessModel()
	require.True(t, m.Decide(rs, ordered.Person{ID: outsider}, fixture.Facilities, authz.ActAuthor).Allowed, "manage gives author")
	require.True(t, m.Decide(rs, ordered.Person{ID: outsider}, fixture.Facilities, authz.ActRead).Allowed, "manage gives read")
	require.False(t, m.Decide(rs, ordered.Person{ID: outsider}, fixture.Facilities, authz.ActApprove).Allowed, "manage does not give approve")

	pool, err := newResolver(rs).ApproverPool(context.Background(), fixture.Facilities)
	require.NoError(t, err)
	require.NotContains(t, ids(pool), outsider)
}

func TestApproverPool_ChildRowsComeFirstAndParentsInherit(t *testing.T) {
	rs := ordered.RuleSet{
		Categories: map[string]ordered.Category{
			fixture.Facilities: {Entries: []ordered.Entry{{Subject: "user:" + member, Action: authz.ActApprove, Effect: ordered.Deny}}},
			"Top":              {Owners: []string{"user:" + owner}, Entries: []ordered.Entry{approversMayApprove}},
		},
		Parents: map[string]string{fixture.Facilities: "Top"},
	}
	pool, err := newResolver(rs).ApproverPool(context.Background(), fixture.Facilities)
	require.NoError(t, err)
	got := ids(pool)
	require.Contains(t, got, owner, "owners are inherited")
	require.Contains(t, got, admin, "the parent's allow is inherited")
	require.NotContains(t, got, member, "the child's deny is read first")
}

func TestApproverPool_PeopleErrorPropagates(t *testing.T) {
	sentinel := errors.New("identity unavailable")
	r := NewResolver(fakePrincipals{err: sentinel}, fakeCore{})
	_, err := r.ApproverPool(context.Background(), fixture.Facilities)
	require.ErrorIs(t, err, sentinel)
}

// Eligible keeps only the designated approvers (ApproverIDs or the category
// list, passed in as candidates) whose Approve is allowed. It never adds anyone.
func TestEligible_FiltersTheDesignatedApproversByApprove(t *testing.T) {
	rs := ruleset([]string{"user:" + owner}, approversMayApprove)
	elig, err := newResolver(rs).Eligible(context.Background(), "pv1", []string{outsider, member, owner, rootID, admin})
	require.NoError(t, err)
	require.Equal(t, []string{member, owner, rootID, admin}, elig, "order kept, the outsider dropped, nobody added")
}

func TestEligible_ADenyDropsADesignatedApprover(t *testing.T) {
	rs := ruleset(nil,
		ordered.Entry{Subject: "user:" + member, Action: authz.ActApprove, Effect: ordered.Deny},
		approversMayApprove,
	)
	elig, err := newResolver(rs).Eligible(context.Background(), "pv1", []string{member, admin})
	require.NoError(t, err)
	require.Equal(t, []string{admin}, elig)
}

func TestEligible_APerDocumentDenyOverrideStillBlocks(t *testing.T) {
	deny := []authz.Override{{ResourceID: fixture.DeskBookingPolicyNumber, Grant: authz.GrantDeny}}
	pr := fakePrincipals{principals: map[string]authz.Principal{
		member: {Subject: authz.Subject{UserID: member, Overrides: deny}, Person: ordered.Person{ID: member, Groups: []string{"Approvers"}}},
	}}
	core := fakeCore{ref: PolicyRef{Number: fixture.DeskBookingPolicyNumber, HomeCategoryID: fixture.Facilities}, rs: ruleset(nil, approversMayApprove)}
	elig, err := NewResolver(pr, core).Eligible(context.Background(), "pv1", []string{member})
	require.NoError(t, err)
	require.Empty(t, elig)
}

func TestEligible_AnAssignedApproverReadsASensitiveDocument(t *testing.T) {
	pr := fakePrincipals{principals: map[string]authz.Principal{
		member: {Subject: authz.Subject{UserID: member}, Person: ordered.Person{ID: member, Groups: []string{"Approvers"}}},
	}}
	core := fakeCore{ref: PolicyRef{Number: "POL-FACILITIES-000004", HomeCategoryID: fixture.Facilities, Sensitive: true}, rs: ruleset(nil, approversMayApprove)}
	elig, err := NewResolver(pr, core).Eligible(context.Background(), "pv1", []string{member})
	require.NoError(t, err)
	require.Equal(t, []string{member}, elig)
}

func TestEligible_NoHomeCategoryLeavesOnlyRoot(t *testing.T) {
	pr := fakePrincipals{principals: map[string]authz.Principal{
		rootID: {Person: ordered.Person{ID: rootID, Roles: []string{authz.RootRole}}},
	}}
	core := fakeCore{ref: PolicyRef{Number: "POL-X"}, rs: ordered.RuleSet{}}
	elig, err := NewResolver(pr, core).Eligible(context.Background(), "pv1", []string{member, rootID})
	require.NoError(t, err)
	require.Equal(t, []string{rootID}, elig)
}

func TestEligible_PrincipalErrorPropagates(t *testing.T) {
	sentinel := errors.New("identity unavailable")
	r := NewResolver(fakePrincipals{err: sentinel}, fakeCore{ref: PolicyRef{Number: "POL-1"}})
	elig, err := r.Eligible(context.Background(), "pv1", []string{member})
	require.ErrorIs(t, err, sentinel)
	require.Nil(t, elig)
}

// The backstop is the category's owners and root, not everyone Approve allows.
func TestBackstop_OwnersAndRootOnly(t *testing.T) {
	rs := ruleset([]string{"user:" + owner}, approversMayApprove)
	got, err := newResolver(rs).Backstop(context.Background(), "pv1")
	require.NoError(t, err)
	require.Equal(t, []string{owner, rootID}, got)
}

func TestMeetsQuorum(t *testing.T) {
	cases := []struct {
		q    string
		n    int
		pool int
		want bool
	}{
		{"any", 0, 1, true}, {"any", 0, 0, false},
		{"all", 0, 2, true}, {"all", 0, 0, false},
		{"majority", 0, 3, true}, {"majority", 0, 0, false},
		{"nofm", 2, 2, true}, {"nofm", 2, 1, false},
	}
	for _, c := range cases {
		if got := MeetsQuorum(c.q, c.n, c.pool); got != c.want {
			t.Errorf("MeetsQuorum(%q,%d,%d)=%v want %v", c.q, c.n, c.pool, got, c.want)
		}
	}
}

type fakePolicies struct {
	corev1.PolicyServiceClient
	version *corev1.PolicyVersion
	policy  *corev1.Policy
}

func (f fakePolicies) GetPolicyVersion(context.Context, *corev1.GetPolicyVersionRequest, ...grpc.CallOption) (*corev1.GetPolicyVersionResponse, error) {
	return &corev1.GetPolicyVersionResponse{Version: f.version}, nil
}

func (f fakePolicies) GetPolicy(_ context.Context, in *corev1.GetPolicyRequest, _ ...grpc.CallOption) (*corev1.GetPolicyResponse, error) {
	if in.GetId() != f.policy.GetId() {
		return nil, errors.New("unexpected policy id " + in.GetId())
	}
	return &corev1.GetPolicyResponse{Policy: f.policy}, nil
}

type fakeCategories struct {
	corev1.CategoryServiceClient
	levels []*corev1.EffectiveLevel
	err    error
	asked  []string
}

func (f *fakeCategories) GetEffectiveRuleset(_ context.Context, in *corev1.GetEffectiveRulesetRequest, _ ...grpc.CallOption) (*corev1.GetEffectiveRulesetResponse, error) {
	f.asked = append(f.asked, in.GetCategoryId())
	if f.err != nil {
		return nil, f.err
	}
	return &corev1.GetEffectiveRulesetResponse{Levels: f.levels}, nil
}

func TestCoreAdapter_PolicyRef(t *testing.T) {
	a := NewCoreAdapter(
		fakePolicies{
			version: &corev1.PolicyVersion{Id: "pv1", PolicyId: "p1"},
			policy:  &corev1.Policy{Id: "p1", Number: fixture.DeskBookingPolicyNumber, HomeCategoryId: fixture.Facilities, Sensitivity: corev1.Sensitivity_SENSITIVITY_SENSITIVE},
		},
		&fakeCategories{},
	)
	ref, err := a.PolicyRefForVersion(context.Background(), "pv1")
	require.NoError(t, err)
	require.Equal(t, PolicyRef{Number: fixture.DeskBookingPolicyNumber, HomeCategoryID: fixture.Facilities, Sensitive: true}, ref)
}

func TestCoreAdapter_RulesetBuildsTheChainNearestFirst(t *testing.T) {
	allow, deny := corev1.GrantEffect_GRANT_EFFECT_ALLOW, corev1.GrantEffect_GRANT_EFFECT_DENY
	cat := &fakeCategories{levels: []*corev1.EffectiveLevel{
		{
			CategoryId: fixture.Facilities, Name: "Facilities", Owners: []string{owner},
			Rules: []*corev1.CategoryRule{
				{SubjectKind: corev1.RuleSubjectKind_RULE_SUBJECT_KIND_USER, SubjectRef: member, Approve: deny},
				{SubjectKind: corev1.RuleSubjectKind_RULE_SUBJECT_KIND_GROUP, SubjectRef: "Approvers", Approve: allow, Manage: allow},
			},
		},
		{
			CategoryId: "Top", Name: "Top",
			Rules: []*corev1.CategoryRule{
				{SubjectKind: corev1.RuleSubjectKind_RULE_SUBJECT_KIND_EVERYONE, Read: allow, Ack: allow},
			},
		},
	}}
	rs, err := NewCoreAdapter(fakePolicies{}, cat).Ruleset(context.Background(), fixture.Facilities)
	require.NoError(t, err)
	require.Equal(t, []string{fixture.Facilities}, cat.asked)
	require.Equal(t, map[string]string{fixture.Facilities: "Top"}, rs.Parents)
	require.Equal(t, []string{"user:" + owner}, rs.Categories[fixture.Facilities].Owners)
	require.Equal(t, []ordered.Entry{
		{Subject: "user:" + member, Action: authz.ActApprove, Effect: ordered.Deny},
		{Subject: "group:Approvers", Action: authz.ActApprove, Effect: ordered.Allow},
		{Subject: "group:Approvers", Action: authz.ActManage, Effect: ordered.Allow},
	}, rs.Categories[fixture.Facilities].Entries)
	require.Equal(t, []ordered.Entry{
		{Subject: ordered.Everyone, Action: authz.ActRead, Effect: ordered.Allow},
		{Subject: ordered.Everyone, Action: authz.ActAck, Effect: ordered.Allow},
	}, rs.Categories["Top"].Entries)

	m := authz.AccessModel()
	require.False(t, m.Decide(rs, ordered.Person{ID: member, Groups: []string{"Approvers"}}, fixture.Facilities, authz.ActApprove).Allowed)
	require.True(t, m.Decide(rs, ordered.Person{ID: admin, Groups: []string{"Approvers"}}, fixture.Facilities, authz.ActApprove).Allowed)
	require.True(t, m.Decide(rs, ordered.Person{ID: outsider}, fixture.Facilities, authz.ActRead).Allowed, "the parent's Everyone row is inherited")
}

func TestCoreAdapter_RulesetErrors(t *testing.T) {
	sentinel := errors.New("core unavailable")
	_, err := NewCoreAdapter(fakePolicies{}, &fakeCategories{err: sentinel}).Ruleset(context.Background(), fixture.Facilities)
	require.ErrorIs(t, err, sentinel)

	_, err = NewCoreAdapter(fakePolicies{}, &fakeCategories{}).Ruleset(context.Background(), fixture.Facilities)
	require.Error(t, err, "an empty chain is an error")
}

func TestCoreAdapter_NoHomeCategoryHasNoRules(t *testing.T) {
	cat := &fakeCategories{}
	rs, err := NewCoreAdapter(fakePolicies{}, cat).Ruleset(context.Background(), "")
	require.NoError(t, err)
	require.Empty(t, cat.asked)
	require.Empty(t, rs.Categories)
}
