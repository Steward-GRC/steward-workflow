// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package assignment

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	corev1 "github.com/Steward-GRC/steward-workflow/gen/go/thirdparty/core/v1"
)

// fakeCoreCategoryClient is a minimal stand-in for corev1.CategoryServiceClient's
// GetCategory, driven by an in-memory map keyed by category id.
type fakeCoreCategoryClient struct {
	categories map[string]*corev1.Category
	err        error
}

func (f *fakeCoreCategoryClient) GetCategory(_ context.Context, in *corev1.GetCategoryRequest, _ ...grpc.CallOption) (*corev1.GetCategoryResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	g, ok := f.categories[in.GetId()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "category not found: %q", in.GetId())
	}
	return &corev1.GetCategoryResponse{Category: g}, nil
}

func TestCoreCategoryFetcher_DefaultWorkflowSet(t *testing.T) {
	f := &CoreCategoryFetcher{client: &fakeCoreCategoryClient{categories: map[string]*corev1.Category{
		"cat-facilities": {Id: "cat-facilities", DefaultWorkflowId: "wf-facilities"},
	}}}

	got, err := f.GetCategoryAssignment(context.Background(), "cat-facilities")
	if err != nil {
		t.Fatalf("GetCategoryAssignment: %v", err)
	}
	if got != "wf-facilities" {
		t.Fatalf("want %q, got %q", "wf-facilities", got)
	}
}

func TestCoreCategoryFetcher_DefaultWorkflowUnset(t *testing.T) {
	f := &CoreCategoryFetcher{client: &fakeCoreCategoryClient{categories: map[string]*corev1.Category{
		"cat-travel": {Id: "cat-travel"}, // no DefaultWorkflowId
	}}}

	got, err := f.GetCategoryAssignment(context.Background(), "cat-travel")
	if err != nil {
		t.Fatalf("GetCategoryAssignment: %v", err)
	}
	if got != "" {
		t.Fatalf("want empty, got %q", got)
	}
}

func TestCoreCategoryFetcher_CategoryNotFound(t *testing.T) {
	f := &CoreCategoryFetcher{client: &fakeCoreCategoryClient{categories: map[string]*corev1.Category{}}}

	got, err := f.GetCategoryAssignment(context.Background(), "missing")
	if err != nil {
		t.Fatalf("expected nil error for not-found, got %v", err)
	}
	if got != "" {
		t.Fatalf("want empty for not-found, got %q", got)
	}
}

func TestCoreCategoryFetcher_NonNotFoundErrorPropagates(t *testing.T) {
	boom := status.Error(codes.Unavailable, "core down")
	f := &CoreCategoryFetcher{client: &fakeCoreCategoryClient{err: boom}}

	_, err := f.GetCategoryAssignment(context.Background(), "cat-facilities")
	if err == nil {
		t.Fatal("expected error to propagate")
	}
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("want Unavailable, got %v", err)
	}
}
