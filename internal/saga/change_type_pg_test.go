// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package saga_test

import (
	"context"
	"testing"

	workflowv1 "github.com/Steward-GRC/steward-workflow/gen/go/steward/workflow/v1"
	"github.com/Steward-GRC/steward-workflow/internal/store"
)

// A pick below core's suggestion is held on the run, and the first stage's
// quorum records the agreement in core once (steward-workflow#10), on the
// real stores.
func TestChangeTypeAgreement_RecordedOnceOnTheRealStores(t *testing.T) {
	h := newLifecycleHarness(t)
	h.coreStatus.requested = workflowv1.ChangeType_CHANGE_TYPE_MINOR
	pv := "pv-ct-pg"

	h.submit(t, pv)
	runID := h.waitForPause(t, pv)

	run, err := store.NewAssignmentStore(h.pool).GetRun(context.Background(), pv)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.RequestedChangeType != "minor" || run.ChangeTypeAgreedAt != nil {
		t.Fatalf("run = requested %q agreed %v, want minor and not agreed", run.RequestedChangeType, run.ChangeTypeAgreedAt)
	}
	if h.coreStatus.agreements != 0 {
		t.Fatalf("agreement recorded before any approval")
	}

	taskID := h.pendingTaskID(t, runID)
	if _, err := h.srv.Signal(context.Background(), &workflowv1.SignalRequest{
		RunId: runID, TaskId: taskID, Signal: workflowv1.SignalType_SIGNAL_TYPE_APPROVE,
		ActorUserId: "approverA", Comment: "ok",
	}); err != nil {
		t.Fatalf("approve: %v", err)
	}
	if h.coreStatus.agreements != 1 || len(h.coreStatus.agreedBy) != 1 || h.coreStatus.agreedBy[0] != "approverA" {
		t.Fatalf("agreements = %d by %v, want one by approverA", h.coreStatus.agreements, h.coreStatus.agreedBy)
	}
	run, err = store.NewAssignmentStore(h.pool).GetRun(context.Background(), pv)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.ChangeTypeAgreedAt == nil {
		t.Fatal("the run was not marked agreed")
	}
}
