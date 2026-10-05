// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package access

import (
	"context"
	"errors"
	"testing"

	stewardauthz "github.com/Steward-GRC/steward-authz"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	corev1 "github.com/Steward-GRC/steward-workflow/gen/go/thirdparty/core/v1"
	"github.com/Steward-GRC/steward-workflow/internal/fixture"
)

type fakeSubjects struct {
	subjects map[string]stewardauthz.Subject
	err      error
}

func (f fakeSubjects) Subject(_ context.Context, id string) (stewardauthz.Subject, error) {
	if f.err != nil {
		return stewardauthz.Subject{}, f.err
	}
	s, ok := f.subjects[id]
	if !ok {
		s = stewardauthz.Subject{UserID: id}
	}
	return s, nil
}

type fakeCore struct {
	ref    PolicyRef
	owners []string
}

func (f fakeCore) PolicyRefForVersion(context.Context, string) (PolicyRef, error) { return f.ref, nil }
func (f fakeCore) CategoryOwners(context.Context, string) ([]string, error)       { return f.owners, nil }

func TestCanRead(t *testing.T) {
	pol := PolicyRef{Number: fixture.DeskBookingPolicyNumber, HomeCategoryID: fixture.Facilities}
	sens := PolicyRef{Number: "POL-FACILITIES-000004", HomeCategoryID: fixture.Facilities, Sensitive: true}
	deny := []stewardauthz.Override{{ResourceID: pol.Number, Grant: stewardauthz.GrantDeny}}
	cases := []struct {
		name     string
		s        stewardauthz.Subject
		p        PolicyRef
		assigned bool
		want     bool
	}{
		{"plain member reads", stewardauthz.Subject{UserID: fixture.Carol}, pol, true, true},
		{"deny override blocks", stewardauthz.Subject{UserID: fixture.Carol, Overrides: deny}, pol, true, false},
		{"deny override blocks a site admin too", stewardauthz.Subject{UserID: fixture.Alice, Roles: []stewardauthz.Role{stewardauthz.RoleSiteAdmin}, Overrides: deny}, pol, true, false},
		{"an assigned approver reads a sensitive document", stewardauthz.Subject{UserID: fixture.Carol}, sens, true, true},
		{"sensitive needs a grant when not assigned", stewardauthz.Subject{UserID: fixture.Carol}, sens, false, false},
		{"the read-sensitive grant reads it", stewardauthz.Subject{UserID: fixture.Carol, ReadSensitive: true}, sens, false, true},
	}
	for _, c := range cases {
		if got := CanRead(c.s, c.p, c.assigned); got != c.want {
			t.Errorf("%s: CanRead=%v want %v", c.name, got, c.want)
		}
	}
}

func TestResolver_Eligible(t *testing.T) {
	subjects := fakeSubjects{subjects: map[string]stewardauthz.Subject{
		fixture.Erin: {UserID: fixture.Erin, Overrides: []stewardauthz.Override{{ResourceID: fixture.DeskBookingPolicyNumber, Grant: stewardauthz.GrantDeny}}},
	}}
	core := fakeCore{ref: PolicyRef{Number: fixture.DeskBookingPolicyNumber, HomeCategoryID: fixture.Facilities}, owners: []string{fixture.Dave}}

	elig, owners, err := NewResolver(subjects, core).Eligible(context.Background(), "pv1", []string{fixture.Carol, fixture.Erin})
	require.NoError(t, err)
	require.Equal(t, []string{fixture.Carol}, elig)
	require.Equal(t, []string{fixture.Dave}, owners)
}

func TestResolver_SubjectErrorPropagates(t *testing.T) {
	sentinel := errors.New("identity unavailable")
	r := NewResolver(fakeSubjects{err: sentinel}, fakeCore{ref: PolicyRef{Number: "POL-1"}, owners: []string{"owner"}})
	elig, owners, err := r.Eligible(context.Background(), "pv1", []string{fixture.Carol})
	require.ErrorIs(t, err, sentinel)
	require.Nil(t, elig)
	require.Nil(t, owners)
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
	category *corev1.Category
}

func (f fakeCategories) GetCategory(context.Context, *corev1.GetCategoryRequest, ...grpc.CallOption) (*corev1.GetCategoryResponse, error) {
	return &corev1.GetCategoryResponse{Category: f.category}, nil
}

func TestCoreAdapter(t *testing.T) {
	a := NewCoreAdapter(
		fakePolicies{
			version: &corev1.PolicyVersion{Id: "pv1", PolicyId: "p1"},
			policy:  &corev1.Policy{Id: "p1", Number: fixture.DeskBookingPolicyNumber, HomeCategoryId: fixture.Facilities, Sensitivity: corev1.Sensitivity_SENSITIVITY_SENSITIVE},
		},
		fakeCategories{category: &corev1.Category{Id: fixture.Facilities, Owners: []string{fixture.Dave}}},
	)
	ref, err := a.PolicyRefForVersion(context.Background(), "pv1")
	require.NoError(t, err)
	require.Equal(t, PolicyRef{Number: fixture.DeskBookingPolicyNumber, HomeCategoryID: fixture.Facilities, Sensitive: true}, ref)
	owners, err := a.CategoryOwners(context.Background(), fixture.Facilities)
	require.NoError(t, err)
	require.Equal(t, []string{fixture.Dave}, owners)
}
