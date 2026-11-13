// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package sweeper tells approvers and category owners that a stage is overdue
// (steward-workflow#76). A periodic sweep reads the pending seats past their
// due date and, for a stage whose definition sets remind_every_days or
// escalate_after_days, publishes workflow.approval_reminder to each approver
// still pending and workflow.stage_escalated to each category owner.
//
// The sweeper only notifies. It never approves, rejects, reassigns or adds a
// seat; the only rows it writes are its own progress marks (reminders_sent and
// reminder_at on a seat, and the stage_escalations row).
package sweeper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	log "github.com/Bugs5382/go-log"
	"github.com/Bugs5382/go-saga-orchestration/clock"

	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/store"
	"github.com/Steward-GRC/steward-workflow/internal/tally"
	"github.com/Steward-GRC/steward-workflow/internal/workcal"
)

// The routing keys obligations binds to on the "jobs" exchange, and the event
// types they carry.
const (
	ReminderRoutingKey   = "workflow.approval_reminder"
	EscalationRoutingKey = "workflow.stage_escalated"
)

// The audit actions. Ids only.
const (
	AuditReminderSent   = "workflow.stage_reminder.sent"
	AuditStageEscalated = "workflow.stage_escalated"
)

// ReminderEvent asks obligations to remind one approver that a stage is past
// due. N is the reminder number for the seat: 1 at the due date, then 2, 3, ...
// Obligations de-duplicates on TaskID and N, so skipping a number is safe.
type ReminderEvent struct {
	EventType       string `json:"event_type"`
	TaskID          string `json:"task_id"`
	PolicyID        string `json:"policy_id"`
	PolicyVersionID string `json:"policy_version_id"`
	StageIndex      int    `json:"stage_index"`
	StageName       string `json:"stage_name"`
	ApproverUserID  string `json:"approver_user_id"`
	WorkflowName    string `json:"workflow_name"`
	DueBy           string `json:"due_by"`
	N               int    `json:"n"`
}

// EscalationEvent tells one category owner that a stage is still undecided
// escalate_after_days working days after its due date. PendingCount is how many
// distinct people have yet to decide.
type EscalationEvent struct {
	EventType       string `json:"event_type"`
	TaskID          string `json:"task_id"`
	PolicyID        string `json:"policy_id"`
	PolicyVersionID string `json:"policy_version_id"`
	StageIndex      int    `json:"stage_index"`
	StageName       string `json:"stage_name"`
	OwnerUserID     string `json:"owner_user_id"`
	WorkflowName    string `json:"workflow_name"`
	DueBy           string `json:"due_by"`
	PendingCount    int    `json:"pending_count"`
}

// Store is what the sweeper reads and records.
type Store interface {
	ListOverdueSeats(ctx context.Context, now time.Time) ([]store.OverdueSeat, error)
	ListStageAssignments(ctx context.Context, policyVersionID string, stageIdx int) ([]store.AssignmentRow, error)
	RecordReminderSent(ctx context.Context, assignmentID string, n int, nextAt time.Time) (bool, error)
	StageEscalated(ctx context.Context, runID string, stageIdx int) (bool, error)
	RecordStageEscalation(ctx context.Context, runID, policyVersionID string, stageIdx int, at time.Time) (bool, error)
}

// Defs reads the definition version a run is pinned to.
type Defs interface {
	GetVersion(ctx context.Context, id string, version int) (builder.WorkflowDef, error)
}

// Runs reads a run's inputs, which hold its pinned work calendar.
type Runs interface {
	RunInputs(ctx context.Context, runID string) (map[string]any, error)
}

// Owners returns who a stage escalates to: the owners of the policy version's
// home category and its ancestors (the access resolver's Backstop).
type Owners interface {
	Backstop(ctx context.Context, policyVersionID string) ([]string, error)
}

// Publisher publishes to the "jobs" exchange.
type Publisher interface {
	Publish(ctx context.Context, routingKey string, body []byte) error
}

// Audit records an audit event. A failure to publish it is the adapter's to
// log; it never fails a sweep.
type Audit interface {
	EmitActorAttrs(ctx context.Context, action, subject, actorUserID, groupID string, attrs map[string]string)
}

// Deps are the sweeper's collaborators.
type Deps struct {
	Store     Store
	Defs      Defs
	Runs      Runs
	Owners    Owners
	Publisher Publisher
	Audit     Audit
	// Clock is the time overdue is judged at; nil is the system clock.
	Clock  clock.Clock
	Logger log.Logger
}

// Sweeper runs one sweep at a time.
type Sweeper struct {
	d  Deps
	lg log.Logger
}

// New returns a Sweeper.
func New(d Deps) *Sweeper {
	if d.Clock == nil {
		d.Clock = clock.SystemClock{}
	}
	if d.Logger == nil {
		d.Logger = log.Nop()
	}
	return &Sweeper{d: d, lg: d.Logger}
}

// stageKey is a stage of a run.
type stageKey struct {
	runID string
	index int
}

// Sweep does one pass. A problem with one stage is logged and the pass goes on
// to the next; the error returned is only for not being able to read the seats.
func (s *Sweeper) Sweep(ctx context.Context) error {
	lg := s.lg.Ctx(ctx)
	now := s.d.Clock.Now()
	started := time.Now()
	lg.Debug("stage notice sweep: start", log.F("now", now.UTC().Format(time.RFC3339)))

	seats, err := s.d.Store.ListOverdueSeats(ctx, now)
	if err != nil {
		lg.Error(err, "stage notice sweep: list overdue seats failed")
		return fmt.Errorf("sweep: %w", err)
	}
	var order []stageKey
	byStage := map[stageKey][]store.OverdueSeat{}
	for _, seat := range seats {
		k := stageKey{seat.RunID, seat.StageIndex}
		if _, ok := byStage[k]; !ok {
			order = append(order, k)
		}
		byStage[k] = append(byStage[k], seat)
	}
	log.Trace(lg, "stage notice sweep: overdue seats read", log.F("seats", len(seats)), log.F("stages", len(order)))

	var reminders, escalations int
	for _, k := range order {
		r, e := s.sweepStage(ctx, now, byStage[k])
		reminders += r
		escalations += e
	}
	lg.Info("stage notice sweep: done",
		log.F("stages", len(order)), log.F("reminders_sent", reminders), log.F("escalations_sent", escalations),
		log.F("duration_ms", time.Since(started).Milliseconds()))
	return nil
}

// sweepStage handles one stage's overdue seats. It returns how many reminders
// and escalation events went out.
func (s *Sweeper) sweepStage(ctx context.Context, now time.Time, seats []store.OverdueSeat) (reminders, escalations int) {
	first := seats[0]
	lg := s.lg.Ctx(ctx).With(
		log.F("run_id", first.RunID), log.F("policy_version_id", first.PolicyVersionID), log.F("stage_index", first.StageIndex))

	wd, err := s.d.Defs.GetVersion(ctx, first.WorkflowDefID, first.WorkflowVersion)
	if err != nil {
		lg.Error(err, "stage notice: read the pinned definition failed", log.F("workflow_def_id", first.WorkflowDefID), log.F("workflow_version", first.WorkflowVersion))
		return 0, 0
	}
	if first.StageIndex < 0 || first.StageIndex >= len(wd.Stages) {
		lg.Error(nil, "stage notice: stage index is outside the pinned definition", log.F("stages", len(wd.Stages)))
		return 0, 0
	}
	stage := wd.Stages[first.StageIndex]
	if reason := notSwept(stage); reason != "" {
		log.Trace(lg, "stage notice: stage skipped", log.F("reason", reason))
		return 0, 0
	}

	rows, err := s.d.Store.ListStageAssignments(ctx, first.PolicyVersionID, first.StageIndex)
	if err != nil {
		lg.Error(err, "stage notice: list the stage's seats failed")
		return 0, 0
	}
	if outcome := tally.Resolve(stage, rows); outcome != tally.DecisionPending {
		lg.Debug("stage notice: stage already decided by its quorum, skipped", log.F("outcome", outcome))
		return 0, 0
	}

	inputs, err := s.d.Runs.RunInputs(ctx, first.RunID)
	if err != nil {
		lg.Error(err, "stage notice: read the run inputs failed, stage skipped until the next sweep")
		return 0, 0
	}
	cal := pinnedCalendar(lg, inputs)
	lg.Debug("stage notice: sweeping stage", log.F("overdue_seats", len(seats)), log.F("working_days", cal != nil),
		log.F("remind_every_days", stage.RemindEveryDays), log.F("escalate_after_days", stage.EscalateAfterDays))

	if stage.RemindEveryDays > 0 {
		reminders = s.remind(ctx, lg, now, wd, stage, seats, cal)
	}
	if stage.EscalateAfterDays > 0 {
		escalations = s.escalate(ctx, lg, now, wd, stage, seats, rows, cal)
	}
	return reminders, escalations
}

// notSwept says why a stage is never swept, or "" when it is: it needs a due
// date, at least one of the two settings, and must not reject on breach (the
// run is rejected at the due date, so it is never overdue).
func notSwept(stage builder.Stage) string {
	switch {
	case stage.RejectOnSLABreach:
		return "reject_on_sla_breach"
	case stage.SLADays <= 0:
		return "no sla_days"
	case stage.RemindEveryDays <= 0 && stage.EscalateAfterDays <= 0:
		return "no reminder or escalation setting"
	}
	return ""
}

// pinnedCalendar reads the run's pinned work calendar; nil counts calendar days,
// the same fallback the due date uses.
func pinnedCalendar(lg log.Logger, inputs map[string]any) *workcal.Calendar {
	cal, err := workcal.FromPin(inputs[workcal.PinKey])
	switch {
	case errors.Is(err, workcal.ErrNoPin):
		return nil
	case err != nil:
		lg.Error(err, "stage notice: pinned work calendar is unusable, counting calendar days")
		return nil
	}
	return &cal
}

func taskID(pvID string, stageIdx int) string { return fmt.Sprintf("%s:%d", pvID, stageIdx) }

func auditSubject(pvID string, stageIdx int) string {
	return fmt.Sprintf("policy_version:%s stage:%d", pvID, stageIdx)
}

func rfc3339(t time.Time) string { return t.UTC().Format(time.RFC3339) }

// remind publishes the newest elapsed reminder number to each approver who has
// not had it. A number is recorded only after its publish succeeded.
func (s *Sweeper) remind(ctx context.Context, lg log.Logger, now time.Time, wd builder.WorkflowDef, stage builder.Stage, seats []store.OverdueSeat, cal *workcal.Calendar) int {
	published := map[string]int{} // approver -> highest number published in this pass
	sent := 0
	for _, seat := range seats {
		sl := lg.With(log.F("seat_id", seat.ID))
		n, next, err := reminderProgress(seat.SLADeadlineAt, stage.RemindEveryDays, cal, now)
		if err != nil {
			sl.Error(err, "stage notice: cannot work out the seat's reminder number")
			continue
		}
		if n <= seat.RemindersSent {
			log.Trace(sl, "stage notice: reminder already sent for this number", log.F("n", n), log.F("reminders_sent", seat.RemindersSent))
			continue
		}
		if published[seat.UserID] < n {
			evt := ReminderEvent{
				EventType:       ReminderRoutingKey,
				TaskID:          taskID(seat.PolicyVersionID, seat.StageIndex),
				PolicyID:        seat.PolicyID,
				PolicyVersionID: seat.PolicyVersionID,
				StageIndex:      seat.StageIndex,
				StageName:       stage.Name,
				ApproverUserID:  seat.UserID,
				WorkflowName:    wd.Name,
				DueBy:           rfc3339(seat.SLADeadlineAt),
				N:               n,
			}
			if err := s.publish(ctx, sl, ReminderRoutingKey, evt); err != nil {
				sl.Error(err, "stage notice: publish reminder failed, retried on the next sweep",
					log.F("approver_user_id", seat.UserID), log.F("n", n))
				continue
			}
			published[seat.UserID] = n
			sent++
			sl.Debug("stage notice: reminder published", log.F("approver_user_id", seat.UserID), log.F("n", n), log.F("skipped_numbers", n-seat.RemindersSent-1))
			s.d.Audit.EmitActorAttrs(ctx, AuditReminderSent, auditSubject(seat.PolicyVersionID, seat.StageIndex), "", "",
				map[string]string{"run_id": seat.RunID, "approver_user_id": seat.UserID, "n": strconv.Itoa(n)})
		}
		recorded, err := s.d.Store.RecordReminderSent(ctx, seat.ID, n, next)
		if err != nil {
			sl.Error(err, "stage notice: record reminder failed, the same number may go out once more",
				log.F("approver_user_id", seat.UserID), log.F("n", n))
			continue
		}
		log.Trace(sl, "stage notice: reminder recorded", log.F("n", n), log.F("recorded", recorded), log.F("next_at", rfc3339(next)))
	}
	return sent
}

// escalate tells every category owner once that the stage is still undecided
// escalate_after_days working days after its due date.
func (s *Sweeper) escalate(ctx context.Context, lg log.Logger, now time.Time, wd builder.WorkflowDef, stage builder.Stage, seats []store.OverdueSeat, rows []store.AssignmentRow, cal *workcal.Calendar) int {
	first := seats[0]
	done, err := s.d.Store.StageEscalated(ctx, first.RunID, first.StageIndex)
	if err != nil {
		lg.Error(err, "stage notice: check the escalation record failed")
		return 0
	}
	if done {
		log.Trace(lg, "stage notice: stage already escalated")
		return 0
	}
	// The stage is due when its earliest pending seat is.
	due := seats[0].SLADeadlineAt
	for _, seat := range seats {
		if seat.SLADeadlineAt.Before(due) {
			due = seat.SLADeadlineAt
		}
	}
	at, err := stepDays(due, stage.EscalateAfterDays, cal)
	if err != nil {
		lg.Error(err, "stage notice: cannot work out when the stage escalates")
		return 0
	}
	if at.After(now) {
		log.Trace(lg, "stage notice: escalation not due yet", log.F("escalates_at", rfc3339(at)))
		return 0
	}

	owners, err := s.d.Owners.Backstop(ctx, first.PolicyVersionID)
	if err != nil {
		lg.Error(err, "stage notice: read the category owners failed, retried on the next sweep")
		return 0
	}
	owners = distinct(owners)
	if len(owners) == 0 {
		lg.Warn("stage notice: the stage is due for escalation but the category has no owner to tell; nothing recorded")
		return 0
	}
	pending := pendingPeople(rows)
	lg.Debug("stage notice: escalating", log.F("owners", len(owners)), log.F("pending_count", pending), log.F("escalated_due_at", rfc3339(at)))

	sent, failed := 0, 0
	for _, owner := range owners {
		evt := EscalationEvent{
			EventType:       EscalationRoutingKey,
			TaskID:          taskID(first.PolicyVersionID, first.StageIndex),
			PolicyID:        first.PolicyID,
			PolicyVersionID: first.PolicyVersionID,
			StageIndex:      first.StageIndex,
			StageName:       stage.Name,
			OwnerUserID:     owner,
			WorkflowName:    wd.Name,
			DueBy:           rfc3339(due),
			PendingCount:    pending,
		}
		if err := s.publish(ctx, lg, EscalationRoutingKey, evt); err != nil {
			failed++
			lg.Error(err, "stage notice: publish escalation failed", log.F("owner_user_id", owner))
			continue
		}
		sent++
		lg.Debug("stage notice: escalation published", log.F("owner_user_id", owner))
	}
	if failed > 0 {
		lg.Warn("stage notice: escalation not recorded, retried on the next sweep (an owner already told is not told twice)",
			log.F("sent", sent), log.F("failed", failed))
		return sent
	}
	recorded, err := s.d.Store.RecordStageEscalation(ctx, first.RunID, first.PolicyVersionID, first.StageIndex, now)
	if err != nil {
		lg.Error(err, "stage notice: record the escalation failed, the owners may be told once more")
		return sent
	}
	lg.Info("stage notice: stage escalated", log.F("owners", sent), log.F("pending_count", pending), log.F("recorded", recorded))
	s.d.Audit.EmitActorAttrs(ctx, AuditStageEscalated, auditSubject(first.PolicyVersionID, first.StageIndex), "", "",
		map[string]string{"run_id": first.RunID, "owners": strconv.Itoa(sent), "pending_count": strconv.Itoa(pending)})
	return sent
}

func (s *Sweeper) publish(ctx context.Context, lg log.Logger, key string, evt any) error {
	body, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", key, err)
	}
	started := time.Now()
	err = s.d.Publisher.Publish(ctx, key, body)
	log.Trace(lg, "stage notice: publish", log.F("routing_key", key), log.F("duration_ms", time.Since(started).Milliseconds()), log.F("ok", err == nil))
	if err != nil {
		return fmt.Errorf("publish %s: %w", key, err)
	}
	return nil
}

// pendingPeople counts the distinct people on the stage who have not decided.
func pendingPeople(rows []store.AssignmentRow) int {
	people := map[string]bool{}
	for _, r := range rows {
		if r.State == "pending" || r.State == "paused" {
			people[r.UserID] = true
		}
	}
	return len(people)
}

func distinct(ids []string) []string {
	seen := make(map[string]bool, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if id != "" && !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out
}
