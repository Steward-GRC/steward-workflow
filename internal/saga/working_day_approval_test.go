// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package saga

import (
	"context"
	"errors"
	"testing"
	"time"

	log "github.com/Bugs5382/go-log"
	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine/verbs"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/workcal"
)

func pinned(t *testing.T, days []int, tz string, holidays ...string) any {
	t.Helper()
	c, err := workcal.New(days, tz, holidays)
	require.NoError(t, err)
	return c.Pin()
}

// runVerb executes the wrapped manual_approval step and returns the due time
// the engine verb stored on the user task (nil for none).
func runVerb(t *testing.T, now time.Time, inputs map[string]any, stepInputs map[string]any) *time.Time {
	t.Helper()
	st := memory.New()
	fc := clock.NewFakeClock(now)
	v := NewWorkingDayApprovalVerb(verbs.ManualApprovalVerb{S: st, Clock: fc}, fc, log.Nop())
	run := domain.SagaRun{ID: uuid.New(), WorkflowID: "wf", State: domain.RunStateRunning, Inputs: inputs, StartedAt: now, LastEventAt: now}
	require.NoError(t, st.CreateRun(context.Background(), run))
	step := domain.Step{ID: "s0_task_0", Type: domain.StepTypeManualApproval, Inputs: stepInputs}
	_, err := v.Execute(context.Background(), run, step)
	require.True(t, errors.Is(err, verbs.ErrSagaPaused), "err = %v", err)
	tasks, err := st.ListUserTasksByRun(context.Background(), run.ID)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	return tasks[0].DueAt
}

func TestWorkingDayApprovalVerb_DueFromPinnedCalendar(t *testing.T) {
	monFri := []int{1, 2, 3, 4, 5}
	fri := time.Date(2026, 11, 13, 17, 0, 0, 0, time.UTC)
	cases := []struct {
		name   string
		now    time.Time
		pin    any
		sla    any
		legacy string
		want   time.Time
	}{
		{"weekend skipped", fri, pinned(t, monFri, "UTC"), 2, "48h", time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)},
		{"holiday skipped", fri, pinned(t, monFri, "UTC", "2026-11-16"), 2, "48h", time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC)},
		{"time zone", time.Date(2026, 11, 12, 20, 0, 0, 0, time.UTC), pinned(t, monFri, "Pacific/Auckland"), 1, "24h",
			time.Date(2026, 11, 16, 11, 0, 0, 0, time.UTC)},
		{"sla as float after the store round trip", fri, pinned(t, monFri, "UTC"), float64(2), "48h", time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)},
		{"no pinned calendar keeps due_in", fri, nil, 2, "48h", fri.Add(48 * time.Hour)},
		{"no sla_days keeps due_in", fri, pinned(t, monFri, "UTC"), nil, "72h", fri.Add(72 * time.Hour)},
		{"over the day limit keeps due_in", fri, pinned(t, monFri, "UTC"), 400, "9600h", fri.Add(9600 * time.Hour)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := map[string]any{}
			if tc.pin != nil {
				in[workcal.PinKey] = tc.pin
			}
			step := map[string]any{"assignee": "stage:0", "due_in": tc.legacy}
			if tc.sla != nil {
				step["sla_days"] = tc.sla
			}
			due := runVerb(t, tc.now, in, step)
			require.NotNil(t, due)
			require.True(t, due.Equal(tc.want), "due = %s, want %s", due.UTC(), tc.want.UTC())
		})
	}
}

// A calendar edited in core after submit changes nothing for a run in flight:
// the run carries its own copy, and the verb reads only that.
func TestWorkingDayApprovalVerb_PinnedCalendarIgnoresLaterEdits(t *testing.T) {
	fri := time.Date(2026, 11, 13, 17, 0, 0, 0, time.UTC)
	live := workcal.Default()
	runInputs := map[string]any{workcal.PinKey: live.Pin()}
	before := runVerb(t, fri, runInputs, map[string]any{"assignee": "stage:0", "sla_days": 2, "due_in": "48h"})

	// The site admin now adds Tuesday 17 Nov as a holiday and drops Friday.
	edited, err := workcal.New([]int{1, 2, 3, 4}, "UTC", []string{"2026-11-17"})
	require.NoError(t, err)
	_ = edited

	after := runVerb(t, fri, runInputs, map[string]any{"assignee": "stage:0", "sla_days": 2, "due_in": "48h"})
	require.True(t, before.Equal(*after))
	require.True(t, before.Equal(time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)))

	// A run submitted after the edit pins the new calendar and differs.
	fresh := runVerb(t, fri, map[string]any{workcal.PinKey: edited.Pin()}, map[string]any{"assignee": "stage:0", "sla_days": 2, "due_in": "48h"})
	require.True(t, fresh.Equal(time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC)), "fresh = %s", fresh.UTC())
}

func TestWorkingDayApprovalVerb_BadPinFallsBackToDueIn(t *testing.T) {
	fri := time.Date(2026, 11, 13, 17, 0, 0, 0, time.UTC)
	due := runVerb(t, fri, map[string]any{workcal.PinKey: "garbage"}, map[string]any{"assignee": "stage:0", "sla_days": 2, "due_in": "48h"})
	require.NotNil(t, due)
	require.True(t, due.Equal(fri.Add(48*time.Hour)))
}

func TestWorkingDayApprovalVerb_InnerErrorsPass(t *testing.T) {
	fc := clock.NewFakeClock(time.Now())
	v := NewWorkingDayApprovalVerb(verbs.ManualApprovalVerb{S: memory.New(), Clock: fc}, fc, log.Nop())
	_, err := v.Execute(context.Background(), domain.SagaRun{ID: uuid.New()}, domain.Step{ID: "x", Inputs: map[string]any{"sla_days": 1}})
	require.Error(t, err, "the engine verb still requires an assignee")
}

// Through the real engine: a compiled stage with sla_days, started with a
// pinned calendar, gets a task due at the end of the Nth working day, not
// N*24h from now.
func TestBuild_EngineUsesTheWorkingDayVerb(t *testing.T) {
	sc, err := BuildInMemory(nil)
	require.NoError(t, err)
	wd := builder.WorkflowDef{ID: "wf-wd", Name: "wd", Version: 1, Stages: []builder.Stage{
		{Name: "S0", ApproverIDs: []string{"u1"}, Quorum: builder.QuorumAny, SLADays: 3},
	}}
	def, err := builder.Compile(wd, builder.CompileOptions{})
	require.NoError(t, err)
	require.NoError(t, sc.Register(def))

	cal, err := workcal.New([]int{1, 2, 3, 4, 5}, "Europe/London", []string{"2026-12-25"})
	require.NoError(t, err)
	before := time.Now()
	id, err := sc.Start(context.Background(), "wf-wd", map[string]any{
		"policy_version_id": "pv-1", workcal.PinKey: cal.Pin(),
	})
	require.NoError(t, err)
	after := time.Now()

	tasks, err := sc.Store().ListUserTasksByRun(context.Background(), id)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.NotNil(t, tasks[0].DueAt)
	w1, _ := cal.AddWorkingDays(before, 3)
	w2, _ := cal.AddWorkingDays(after, 3)
	got := *tasks[0].DueAt
	// Due at a local midnight (give or take the microseconds between the verb
	// and the engine reading the clock), not at now + 72h.
	require.True(t, got.Sub(w1).Abs() < time.Second || got.Sub(w2).Abs() < time.Second, "due = %s, want %s", got.UTC(), w1.UTC())
	require.False(t, got.Sub(before) == 72*time.Hour)
}
