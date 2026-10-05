// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package statemachine

import "testing"

func TestTransition_DraftToInReview(t *testing.T) {
	next, err := Transition(StatusDraft, EventSubmit)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if next != StatusInReview {
		t.Fatalf("expected InReview, got %q", next)
	}
}

func TestTransition_InReviewApproved(t *testing.T) {
	next, err := Transition(StatusInReview, EventAllApproved)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if next != StatusApproved {
		t.Fatalf("expected Approved, got %q", next)
	}
}

func TestTransition_InReviewRejected(t *testing.T) {
	next, err := Transition(StatusInReview, EventReject)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if next != StatusRejected {
		t.Fatalf("expected Rejected, got %q", next)
	}
}

func TestTransition_InReviewRequestChanges(t *testing.T) {
	next, err := Transition(StatusInReview, EventRequestChanges)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if next != StatusDraft {
		t.Fatalf("expected Draft, got %q", next)
	}
}

func TestTransition_InReviewWithdrawn(t *testing.T) {
	next, err := Transition(StatusInReview, EventWithdraw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if next != StatusWithdrawn {
		t.Fatalf("expected Withdrawn, got %q", next)
	}
}

func TestTransition_ApprovedScheduled(t *testing.T) {
	next, err := Transition(StatusApproved, EventSchedule)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if next != StatusScheduled {
		t.Fatalf("expected Scheduled, got %q", next)
	}
}

func TestTransition_ApprovedPublish(t *testing.T) {
	next, err := Transition(StatusApproved, EventPublish)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if next != StatusPublished {
		t.Fatalf("expected Published, got %q", next)
	}
}

func TestTransition_ScheduledPublish(t *testing.T) {
	next, err := Transition(StatusScheduled, EventPublish)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if next != StatusPublished {
		t.Fatalf("expected Published, got %q", next)
	}
}

func TestTransition_PublishedSuperseded(t *testing.T) {
	next, err := Transition(StatusPublished, EventSupersede)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if next != StatusSuperseded {
		t.Fatalf("expected Superseded, got %q", next)
	}
}

func TestTransition_PublishedRetired(t *testing.T) {
	next, err := Transition(StatusPublished, EventRetire)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if next != StatusArchived {
		t.Fatalf("expected Archived, got %q", next)
	}
}

func TestTransition_InvalidFromTerminal(t *testing.T) {
	if _, err := Transition(StatusRejected, EventSubmit); err == nil {
		t.Fatal("expected error transitioning from terminal state Rejected")
	}
}

func TestTransition_InvalidEvent(t *testing.T) {
	if _, err := Transition(StatusDraft, EventReject); err == nil {
		t.Fatal("expected error for invalid Draft→Reject transition")
	}
}
