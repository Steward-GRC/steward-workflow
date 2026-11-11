// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc

import (
	"context"
	"fmt"
	"testing"

	workflowv1 "github.com/Steward-GRC/steward-workflow/gen/go/steward/workflow/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func submitReq() *workflowv1.SubmitRequest {
	return &workflowv1.SubmitRequest{
		PolicyVersionId: "pv-1", PolicyId: "pol-1", CategoryId: "g1",
		SubmittedBy: "user-1", AncestorCategoryIds: []string{"g1", "root"},
	}
}

// A submitted version becomes in_review in core (#41), written by the submitter.
func TestSubmit_SetsInReviewInCore(t *testing.T) {
	srv := newTestServer()
	core := &stubCoreStatus{}
	srv.coreStatus = core
	if _, err := srv.Submit(context.Background(), submitReq()); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if len(core.calls) != 1 || core.calls[0].pvID != "pv-1" || core.calls[0].status != "in_review" || core.calls[0].actor != "user-1" {
		t.Fatalf("core calls = %+v, want one (pv-1, in_review, user-1)", core.calls)
	}
}

// Core refusing the submit (for example a document with no text) stops it
// before a run starts.
func TestSubmit_CoreRefusalStartsNoRun(t *testing.T) {
	srv := newTestServer()
	srv.coreStatus = &stubCoreStatus{err: status.Error(codes.InvalidArgument, "submit refused: the document holds no text")}
	_, err := srv.Submit(context.Background(), submitReq())
	if status.Code(err) != codes.Internal && status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want an error from core, got %v", err)
	}
	if got := srv.sagaClient.(*stubSagaClient).startedRunID; got != "" {
		t.Fatalf("a run started (%q) after core refused the submit", got)
	}
	if srv.runStore.(*stubRunStore).tracked != nil {
		t.Fatal("a run was tracked after core refused the submit")
	}
}

// When the run cannot start, the version goes back to draft rather than stay in
// review with nothing running.
func TestSubmit_StartFailureReturnsVersionToDraft(t *testing.T) {
	srv := newTestServer()
	core := &stubCoreStatus{}
	srv.coreStatus = core
	srv.sagaClient.(*stubSagaClient).startErr = fmt.Errorf("engine down")
	if _, err := srv.Submit(context.Background(), submitReq()); err == nil {
		t.Fatal("want the start error")
	}
	if len(core.calls) != 2 || core.calls[0].status != "in_review" || core.calls[1].status != "draft" {
		t.Fatalf("core calls = %+v, want in_review then draft", core.calls)
	}
}

func TestSubmit_WithoutCoreSetterIsUnimplemented(t *testing.T) {
	srv := newTestServer()
	srv.coreStatus = nil
	_, err := srv.Submit(context.Background(), submitReq())
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("want Unimplemented, got %v", err)
	}
	if got := srv.sagaClient.(*stubSagaClient).startedRunID; got != "" {
		t.Fatalf("a run started (%q) with no core setter", got)
	}
}
