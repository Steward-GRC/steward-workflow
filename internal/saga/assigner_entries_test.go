// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package saga

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/fixture"
)

// recordingEntries records every stage entry the assigner reports.
type recordingEntries struct {
	calls []string
	err   error
}

func (r *recordingEntries) RecordStageEntry(_ context.Context, runID string, stageIdx int, _ time.Time) error {
	r.calls = append(r.calls, runID+"|"+string(rune('0'+stageIdx)))
	return r.err
}

// A stage with nobody to seat is still entered, so the status can tell a run
// waiting on it from one that has not reached it (steward-workflow#50).
func TestStageAssigner_EmptyPoolStillRecordsTheEntry(t *testing.T) {
	creator, entries := &recordingCreator{}, &recordingEntries{}
	wd := builder.WorkflowDef{ID: "def-1", Version: 1, Stages: []builder.Stage{{Name: "Empty", Quorum: builder.QuorumAny}}}
	a := NewStageAssigner(fakeDefs{wd: wd}, creator, builder.CompileOptions{}).WithStageEntries(entries)
	run := fakeSagaRun("run-1", baseInputs(nil))

	require.NoError(t, a.AssignStage(context.Background(), run, 0))
	require.Empty(t, creator.calls, "nobody to seat")
	require.Equal(t, []string{run.ID.String() + "|0"}, entries.calls)
}

func TestStageAssigner_NoEligibleApproverStillRecordsTheEntry(t *testing.T) {
	creator, entries := &recordingCreator{}, &recordingEntries{}
	a := stageAssignerFor(nil, creator).WithStageEntries(entries).
		WithAccessFilter(fakeFilter{deny: map[string]bool{fixture.Carol: true}})
	run := fakeSagaRun("run-1", baseInputs(map[string]any{"approvers_s0": []any{fixture.Carol}}))

	require.NoError(t, a.AssignStage(context.Background(), run, 0))
	require.Empty(t, creator.calls)
	require.Len(t, entries.calls, 1, "the filter emptied the pool, the stage was still entered")
}

func TestStageAssigner_SeatingStageRecordsTheEntryOnceAndAgainOnRetry(t *testing.T) {
	creator, entries := &recordingCreator{}, &recordingEntries{}
	a := stageAssignerFor(nil, creator).WithStageEntries(entries)
	run := fakeSagaRun("run-1", baseInputs(map[string]any{"approvers_s1": []any{"u1"}}))

	require.NoError(t, a.AssignStage(context.Background(), run, 1))
	require.NoError(t, a.AssignStage(context.Background(), run, 1))
	require.Len(t, entries.calls, 2, "each pass reports it; the store keeps one row")
	require.Equal(t, entries.calls[0], entries.calls[1])
}

func TestStageAssigner_FailedEntryWriteFailsTheStepSoTheEngineRetries(t *testing.T) {
	creator := &recordingCreator{}
	entries := &recordingEntries{err: errors.New("db down")}
	a := stageAssignerFor(nil, creator).WithStageEntries(entries)
	run := fakeSagaRun("run-1", baseInputs(map[string]any{"approvers_s1": []any{"u1"}}))

	require.Error(t, a.AssignStage(context.Background(), run, 1))
}
