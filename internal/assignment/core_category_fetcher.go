// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package assignment

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	corev1 "github.com/Steward-GRC/steward-workflow/gen/go/thirdparty/core/v1"
)

// CoreCategoryGetter is the part of core's CategoryService the fetcher calls.
type CoreCategoryGetter interface {
	GetCategory(ctx context.Context, in *corev1.GetCategoryRequest, opts ...grpc.CallOption) (*corev1.GetCategoryResponse, error)
}

// CoreCategoryFetcher reads a category's default workflow from core, where it
// is set.
type CoreCategoryFetcher struct{ client CoreCategoryGetter }

// NewCoreCategoryFetcher returns a CoreCategoryFetcher on client.
func NewCoreCategoryFetcher(client CoreCategoryGetter) *CoreCategoryFetcher {
	return &CoreCategoryFetcher{client: client}
}

// GetCategoryAssignment returns the category's default workflow id, "" when it
// has none or doesn't exist.
func (f *CoreCategoryFetcher) GetCategoryAssignment(ctx context.Context, categoryID string) (string, error) {
	resp, err := f.client.GetCategory(ctx, &corev1.GetCategoryRequest{Id: categoryID})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return "", nil
		}
		return "", err
	}
	return resp.GetCategory().GetDefaultWorkflowId(), nil
}
