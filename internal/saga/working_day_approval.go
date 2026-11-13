// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package saga

import (
	"context"
	"errors"
	"maps"
	"time"

	log "github.com/Bugs5382/go-log"
	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine/verbs"

	"github.com/Steward-GRC/steward-workflow/internal/workcal"
)

// WorkingDayApprovalVerb is the workflow's manual_approval step. It wraps the
// engine's verb and, when the stage carries sla_days and the run pinned a work
// calendar, replaces the static due_in with the time left until the end of the
// Nth working day, counted when the step runs. Anything else (no sla_days, no
// pinned calendar, a calendar or count that is unusable) passes the step
// through unchanged, so the compiled calendar-day due_in applies, exactly as
// for a run from before calendars were pinned.
type WorkingDayApprovalVerb struct {
	inner verbs.Handler
	clock clock.Clock
	log   log.Logger
}

// NewWorkingDayApprovalVerb wraps inner. clk is the time sla_days is counted
// from.
func NewWorkingDayApprovalVerb(inner verbs.Handler, clk clock.Clock, lg log.Logger) WorkingDayApprovalVerb {
	if lg == nil {
		lg = log.Nop()
	}
	return WorkingDayApprovalVerb{inner: inner, clock: clk, log: lg}
}

// Execute implements verbs.Handler.
func (v WorkingDayApprovalVerb) Execute(ctx context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
	lg := v.log.Ctx(ctx)
	days := runIntInput(step.Inputs, "sla_days")
	if days <= 0 {
		lg.Debug("manual_approval: no sla_days, using due_in as compiled", log.F("run_id", run.ID.String()), log.F("step_id", step.ID))
		return v.inner.Execute(ctx, run, step)
	}
	cal, err := workcal.FromPin(run.Inputs[workcal.PinKey])
	switch {
	case errors.Is(err, workcal.ErrNoPin):
		lg.Debug("manual_approval: run has no pinned calendar, counting calendar days",
			log.F("run_id", run.ID.String()), log.F("step_id", step.ID), log.F("sla_days", days))
		return v.inner.Execute(ctx, run, step)
	case err != nil:
		lg.Error(err, "manual_approval: pinned calendar is unusable, counting calendar days",
			log.F("run_id", run.ID.String()), log.F("step_id", step.ID))
		return v.inner.Execute(ctx, run, step)
	}
	now := v.clock.Now()
	deadline, err := cal.AddWorkingDays(now, days)
	if err != nil {
		lg.Error(err, "manual_approval: cannot count working days, counting calendar days",
			log.F("run_id", run.ID.String()), log.F("step_id", step.ID), log.F("sla_days", days))
		return v.inner.Execute(ctx, run, step)
	}
	dueIn := workcal.DueIn(now, deadline)
	lg.Debug("manual_approval: due at the end of the working day",
		log.F("run_id", run.ID.String()), log.F("step_id", step.ID), log.F("sla_days", days),
		log.F("due_at", deadline.UTC().Format(time.RFC3339)), log.F("due_in", dueIn.String()))

	inputs := make(map[string]any, len(step.Inputs))
	maps.Copy(inputs, step.Inputs)
	inputs["due_in"] = dueIn.String()
	step.Inputs = inputs
	return v.inner.Execute(ctx, run, step)
}
