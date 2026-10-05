// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc

import (
	"context"
	"testing"

	workflowv1 "github.com/Steward-GRC/steward-workflow/gen/go/steward/workflow/v1"
	"github.com/Steward-GRC/steward-workflow/internal/store"
)

// recordingRunNotifier captures the started/denied events the handlers emit so a
// test can assert the submitter is notified with the right payload.
type recordingRunNotifier struct {
	started []RunStartedEvent
	denied  []RunDeniedEvent
	err     error
}

func (n *recordingRunNotifier) NotifyRunStarted(_ context.Context, e RunStartedEvent) error {
	n.started = append(n.started, e)
	return n.err
}

func (n *recordingRunNotifier) NotifyRunDenied(_ context.Context, e RunDeniedEvent) error {
	n.denied = append(n.denied, e)
	return n.err
}

// TestSubmit_EmitsRunStarted asserts submit-for-approval emits exactly one
// workflow.started event addressed to the SUBMITTER, carrying the ids
// obligations needs to resolve the recipient + policy title.
func TestSubmit_EmitsRunStarted(t *testing.T) {
	srv := newTestServer()
	notifier := &recordingRunNotifier{}
	srv.runNotifier = notifier

	resp, err := srv.Submit(context.Background(), &workflowv1.SubmitRequest{
		PolicyVersionId:     "pv-1",
		PolicyId:            "pol-9",
		CategoryId:          "g1",
		SubmittedBy:         "sub-1",
		AncestorCategoryIds: []string{"g1"},
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if len(notifier.started) != 1 {
		t.Fatalf("want exactly 1 workflow.started event, got %d: %+v", len(notifier.started), notifier.started)
	}
	e := notifier.started[0]
	if e.EventType != "workflow.started" {
		t.Errorf("event_type: want workflow.started, got %q", e.EventType)
	}
	if e.SubmittedByUserID != "sub-1" {
		t.Errorf("submitted_by_user_id: want sub-1, got %q", e.SubmittedByUserID)
	}
	if e.PolicyID != "pol-9" || e.PolicyVersionID != "pv-1" {
		t.Errorf("policy ids: want pol-9/pv-1, got %q/%q", e.PolicyID, e.PolicyVersionID)
	}
	if e.RunID != resp.RunId {
		t.Errorf("run_id: want %q, got %q", resp.RunId, e.RunID)
	}
	if e.CurrentStep != "s" {
		t.Errorf("current_step: want first stage name 's', got %q", e.CurrentStep)
	}

	// The submitter + policy id are also persisted on the run so the reject path
	// can address the denied email later.
	rs := srv.runStore.(*stubRunStore)
	if rs.tracked == nil || rs.tracked.SubmittedBy != "sub-1" || rs.tracked.PolicyID != "pol-9" {
		t.Fatalf("run must persist submitter+policy id, got %+v", rs.tracked)
	}
}

// TestSubmit_NoSubmitterSkipsStarted asserts a submit without a submitter id
// (no addressable recipient) emits nothing rather than a recipient-less event.
func TestSubmit_NoSubmitterSkipsStarted(t *testing.T) {
	srv := newTestServer()
	notifier := &recordingRunNotifier{}
	srv.runNotifier = notifier

	if _, err := srv.Submit(context.Background(), &workflowv1.SubmitRequest{
		PolicyVersionId: "pv-1", PolicyId: "pol-9", AncestorCategoryIds: []string{"g1"},
	}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if len(notifier.started) != 0 {
		t.Fatalf("want no started event without a submitter, got %d", len(notifier.started))
	}
}

// TestSubmit_EmitFailureDoesNotFailSubmit asserts a publish error is swallowed:
// Submit still succeeds and the run is tracked (the run is authoritative; the
// notification is best-effort).
func TestSubmit_EmitFailureDoesNotFailSubmit(t *testing.T) {
	srv := newTestServer()
	srv.runNotifier = &recordingRunNotifier{err: context.DeadlineExceeded}

	resp, err := srv.Submit(context.Background(), &workflowv1.SubmitRequest{
		PolicyVersionId: "pv-1", PolicyId: "pol-9", SubmittedBy: "sub-1", AncestorCategoryIds: []string{"g1"},
	})
	if err != nil {
		t.Fatalf("emit failure must not fail Submit, got: %v", err)
	}
	if resp.GetRunId() == "" {
		t.Fatal("expected a tracked run despite emit failure")
	}
}

// TestSignal_RejectEmitsRunDenied asserts a reject emits exactly one
// workflow.denied event addressed to the SUBMITTER (resolved off the run),
// carrying the reviewer + reason.
func TestSignal_RejectEmitsRunDenied(t *testing.T) {
	srv := newTestServer()
	notifier := &recordingRunNotifier{}
	srv.runNotifier = notifier
	srv.runStore.(*stubRunStore).tracked = &store.ApprovalRun{
		PolicyVersionID: "pv-1", RunID: "run-9", WorkflowDefID: "def-1", WorkflowVersion: 1,
		Status: "in_review", SubmittedBy: "sub-1", PolicyID: "pol-9",
	}

	_, err := srv.Signal(context.Background(), &workflowv1.SignalRequest{
		PolicyVersionId: "pv-1",
		Signal:          workflowv1.SignalType_SIGNAL_TYPE_REJECT,
		ActorUserId:     "rev-1",
		Comment:         "Section 4 conflicts with the retention schedule.",
	})
	if err != nil {
		t.Fatalf("Signal(reject): %v", err)
	}
	if len(notifier.denied) != 1 {
		t.Fatalf("want exactly 1 workflow.denied event, got %d: %+v", len(notifier.denied), notifier.denied)
	}
	e := notifier.denied[0]
	if e.EventType != "workflow.denied" {
		t.Errorf("event_type: want workflow.denied, got %q", e.EventType)
	}
	if e.SubmittedByUserID != "sub-1" {
		t.Errorf("submitted_by_user_id (recipient): want sub-1, got %q", e.SubmittedByUserID)
	}
	if e.ReviewedByUserID != "rev-1" {
		t.Errorf("reviewed_by_user_id: want rev-1, got %q", e.ReviewedByUserID)
	}
	if e.PolicyID != "pol-9" {
		t.Errorf("policy_id: want pol-9, got %q", e.PolicyID)
	}
	if e.Reason != "Section 4 conflicts with the retention schedule." {
		t.Errorf("reason: got %q", e.Reason)
	}
	if e.WorkflowName != "Test" {
		t.Errorf("workflow_name: want resolved def name 'Test', got %q", e.WorkflowName)
	}
}

// TestSignal_ApproveDoesNotEmitDenied asserts an approve never emits a denied
// event (only reject/return does).
func TestSignal_ApproveDoesNotEmitDenied(t *testing.T) {
	srv := newTestServer()
	notifier := &recordingRunNotifier{}
	srv.runNotifier = notifier
	srv.runStore.(*stubRunStore).tracked = &store.ApprovalRun{
		PolicyVersionID: "pv-1", RunID: "run-9", WorkflowDefID: "def-1", Status: "in_review", SubmittedBy: "sub-1", PolicyID: "pol-9",
	}
	if _, err := srv.Signal(context.Background(), &workflowv1.SignalRequest{
		PolicyVersionId: "pv-1", Signal: workflowv1.SignalType_SIGNAL_TYPE_APPROVE, ActorUserId: "rev-1", Comment: "ok",
	}); err != nil {
		t.Fatalf("Signal(approve): %v", err)
	}
	if len(notifier.denied) != 0 {
		t.Fatalf("approve must not emit a denied event, got %d", len(notifier.denied))
	}
}

// TestSignal_RejectNoSubmitterSkipsDenied asserts a reject on a legacy run with
// no persisted submitter (pre-migration-0003) emits nothing rather than a
// recipient-less event.
func TestSignal_RejectNoSubmitterSkipsDenied(t *testing.T) {
	srv := newTestServer()
	notifier := &recordingRunNotifier{}
	srv.runNotifier = notifier
	srv.runStore.(*stubRunStore).tracked = &store.ApprovalRun{
		PolicyVersionID: "pv-1", RunID: "run-9", WorkflowDefID: "def-1", Status: "in_review", // no SubmittedBy
	}
	if _, err := srv.Signal(context.Background(), &workflowv1.SignalRequest{
		PolicyVersionId: "pv-1", Signal: workflowv1.SignalType_SIGNAL_TYPE_REJECT, ActorUserId: "rev-1", Comment: "no",
	}); err != nil {
		t.Fatalf("Signal(reject): %v", err)
	}
	if len(notifier.denied) != 0 {
		t.Fatalf("reject on a run with no submitter must not emit, got %d", len(notifier.denied))
	}
}
