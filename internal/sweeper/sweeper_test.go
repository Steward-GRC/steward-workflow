// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package sweeper_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	log "github.com/Bugs5382/go-log"
	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/stretchr/testify/require"

	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/store"
	"github.com/Steward-GRC/steward-workflow/internal/sweeper"
	"github.com/Steward-GRC/steward-workflow/internal/workcal"
)

// The test week. 2026-03-02 is a Monday. The seats are due at the end of
// Thursday the 5th (Friday 00:00 UTC), on a Monday to Friday calendar.
var (
	dueFri  = time.Date(2026, 3, 6, 0, 0, 0, 0, time.UTC)
	satNoon = time.Date(2026, 3, 7, 12, 0, 0, 0, time.UTC)
)

func at(day, hour int) time.Time { return time.Date(2026, 3, day, hour, 0, 0, 0, time.UTC) }

// world is the fakes a sweep runs against.
type world struct {
	t     *testing.T
	clk   *clock.FakeClock
	seats []*seatRec
	stage builder.Stage
	// run is the run every seat belongs to.
	runStatus string
	pin       any
	pinErr    error
	owners    []string
	ownersErr error

	published  []published
	publishErr func(routingKey string, body []byte) error
	audits     []auditRec
	escalated  map[string]bool
	recordFail bool
}

type seatRec struct {
	row   store.AssignmentRow
	state string
}

type published struct {
	key  string
	body map[string]any
}

type auditRec struct {
	action, subject string
	attrs           map[string]string
}

func newWorld(t *testing.T, stage builder.Stage) *world {
	t.Helper()
	cal := workcal.Default()
	return &world{
		t: t, clk: clock.NewFakeClock(dueFri), stage: stage, runStatus: "in_review",
		pin: cal.Pin(), escalated: map[string]bool{},
	}
}

func overdueStage() builder.Stage {
	return builder.Stage{
		Name: "Legal review", Quorum: builder.QuorumAll, ApproverIDs: []string{"u1", "u2"},
		SLADays: 3, RemindEveryDays: 1, EscalateAfterDays: 2,
	}
}

// seat adds a seat due at due.
func (w *world) seat(user, group, state string, due time.Time) *seatRec {
	r := &seatRec{state: state, row: store.AssignmentRow{
		ID: "seat-" + user + "-" + group, PolicyVersionID: "pv-1", StageIndex: 0, UserID: user, GroupID: group,
		SLADeadlineAt: due, ReminderAt: due, State: state,
	}}
	w.seats = append(w.seats, r)
	return r
}

func (w *world) now(t time.Time) { w.clk.Advance(t.Sub(w.clk.Now())) }

func (w *world) sweeper() *sweeper.Sweeper {
	return sweeper.New(sweeper.Deps{
		Store: (*fakeStore)(w), Defs: (*fakeDefs)(w), Runs: (*fakeRuns)(w), Owners: (*fakeOwners)(w),
		Publisher: (*fakePub)(w), Audit: (*fakeAudit)(w), Clock: w.clk, Logger: log.Nop(),
	})
}

func (w *world) sweep() {
	w.t.Helper()
	require.NoError(w.t, w.sweeper().Sweep(context.Background()))
}

func (w *world) reminders() []published   { return w.byKey("workflow.approval_reminder") }
func (w *world) escalations() []published { return w.byKey("workflow.stage_escalated") }

func (w *world) byKey(key string) []published {
	var out []published
	for _, p := range w.published {
		if p.key == key {
			out = append(out, p)
		}
	}
	return out
}

func (w *world) reminderNumbers(user string) []int {
	var out []int
	for _, p := range w.reminders() {
		if p.body["approver_user_id"] == user {
			out = append(out, int(p.body["n"].(float64)))
		}
	}
	return out
}

func (w *world) sent(user, group string) int {
	for _, s := range w.seats {
		if s.row.UserID == user && s.row.GroupID == group {
			return s.row.RemindersSent
		}
	}
	w.t.Fatalf("no seat %s/%s", user, group)
	return 0
}

type (
	fakeStore  world
	fakeDefs   world
	fakeRuns   world
	fakeOwners world
	fakePub    world
	fakeAudit  world
)

func (f *fakeStore) ListOverdueSeats(_ context.Context, now time.Time) ([]store.OverdueSeat, error) {
	var out []store.OverdueSeat
	if f.runStatus != "in_review" {
		return nil, nil
	}
	for _, s := range f.seats {
		if s.state == "pending" && !s.row.SLADeadlineAt.After(now) {
			row := s.row
			row.State = s.state
			out = append(out, store.OverdueSeat{
				AssignmentRow: row, RunID: "run-1", WorkflowDefID: "def-1", WorkflowVersion: 2,
				PolicyID: "pol-1", SubmittedBy: "u-author",
			})
		}
	}
	return out, nil
}

func (f *fakeStore) ListStageAssignments(_ context.Context, pv string, idx int) ([]store.AssignmentRow, error) {
	var out []store.AssignmentRow
	for _, s := range f.seats {
		if s.row.PolicyVersionID == pv && s.row.StageIndex == idx {
			row := s.row
			row.State = s.state
			out = append(out, row)
		}
	}
	return out, nil
}

func (f *fakeStore) RecordReminderSent(_ context.Context, id string, n int, next time.Time) (bool, error) {
	if f.recordFail {
		return false, errors.New("db down")
	}
	for _, s := range f.seats {
		if s.row.ID == id && s.state == "pending" && s.row.RemindersSent < n {
			s.row.RemindersSent, s.row.ReminderAt = n, next
			return true, nil
		}
	}
	return false, nil
}

func (f *fakeStore) StageEscalated(_ context.Context, runID string, idx int) (bool, error) {
	return f.escalated[runID+idxKey(idx)], nil
}

func (f *fakeStore) RecordStageEscalation(_ context.Context, runID, _ string, idx int, _ time.Time) (bool, error) {
	k := runID + idxKey(idx)
	if f.escalated[k] {
		return false, nil
	}
	f.escalated[k] = true
	return true, nil
}

func idxKey(i int) string { return string(rune('a' + i)) }

func (f *fakeDefs) GetVersion(_ context.Context, id string, version int) (builder.WorkflowDef, error) {
	if id != "def-1" || version != 2 {
		return builder.WorkflowDef{}, errors.New("unexpected def")
	}
	return builder.WorkflowDef{ID: id, Name: "Policy sign-off", Version: version, Stages: []builder.Stage{f.stage}}, nil
}

func (f *fakeRuns) RunInputs(_ context.Context, runID string) (map[string]any, error) {
	if f.pinErr != nil {
		return nil, f.pinErr
	}
	in := map[string]any{"policy_version_id": "pv-1"}
	if f.pin != nil {
		// The saga store keeps inputs as JSON.
		raw, _ := json.Marshal(f.pin)
		var back any
		_ = json.Unmarshal(raw, &back)
		in[workcal.PinKey] = back
	}
	return in, nil
}

func (f *fakeOwners) Backstop(context.Context, string) ([]string, error) {
	return f.owners, f.ownersErr
}

func (f *fakePub) Publish(_ context.Context, key string, body []byte) error {
	if f.publishErr != nil {
		if err := f.publishErr(key, body); err != nil {
			return err
		}
	}
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return err
	}
	f.published = append(f.published, published{key: key, body: m})
	return nil
}

func (f *fakeAudit) EmitActorAttrs(_ context.Context, action, subject, _, _ string, attrs map[string]string) {
	f.audits = append(f.audits, auditRec{action: action, subject: subject, attrs: attrs})
}

// Reminder 1 falls at the due instant, then one per working day across a weekend
// (Friday 00:00, Saturday 00:00, Tuesday 00:00): nothing falls on Sunday or
// Monday.
func TestSweep_ReminderNumberIncrementsOnTheWorkingDayBoundary(t *testing.T) {
	w := newWorld(t, overdueStage())
	w.seat("u1", "", "pending", dueFri)

	w.now(dueFri.Add(-time.Second))
	w.sweep()
	require.Empty(t, w.reminders(), "nothing before the due instant")

	w.now(dueFri)
	w.sweep()
	require.Equal(t, []int{1}, w.reminderNumbers("u1"), "reminder 1 falls at the due instant")

	w.now(at(6, 12))
	w.sweep()
	require.Equal(t, []int{1}, w.reminderNumbers("u1"), "still inside the day after the due day")

	w.now(satNoon) // reminder 2 fell at Saturday 00:00, the end of Friday
	w.sweep()
	w.now(at(8, 12))
	w.sweep()
	w.now(at(9, 12))
	w.sweep()
	require.Equal(t, []int{1, 2}, w.reminderNumbers("u1"), "Sunday and Monday add nothing")

	w.now(at(10, 0)) // the end of Monday
	w.sweep()
	require.Equal(t, []int{1, 2, 3}, w.reminderNumbers("u1"))
	require.Equal(t, 3, w.sent("u1", ""))
}

// A holiday is not a working day, so the reminder after it waits one more day.
func TestSweep_HolidayPushesTheNextReminder(t *testing.T) {
	w := newWorld(t, overdueStage())
	cal := workcal.Default()
	cal.Holidays = []workcal.Date{{Year: 2026, Month: time.March, Day: 9}} // the Monday
	w.pin = cal.Pin()
	w.seat("u1", "", "pending", dueFri)

	for _, tm := range []time.Time{dueFri, satNoon, at(10, 12)} {
		w.now(tm)
		w.sweep()
	}
	require.Equal(t, []int{1, 2}, w.reminderNumbers("u1"), "Tuesday noon: the holiday Monday did not count")

	w.now(at(11, 0)) // the end of Tuesday
	w.sweep()
	require.Equal(t, []int{1, 2, 3}, w.reminderNumbers("u1"))
}

// remind_every_days counts N working days between reminders.
func TestSweep_RemindEveryNWorkingDays(t *testing.T) {
	st := overdueStage()
	st.RemindEveryDays, st.EscalateAfterDays = 3, 0
	w := newWorld(t, st)
	w.seat("u1", "", "pending", dueFri)

	w.now(dueFri)
	w.sweep()
	// Reminder 2 is 3 working days after the due day (Thu): Fri, Mon, Tue,
	// so the end of Tuesday, Wednesday 00:00.
	w.now(at(10, 12))
	w.sweep()
	require.Equal(t, []int{1}, w.reminderNumbers("u1"))
	w.now(at(11, 0))
	w.sweep()
	require.Equal(t, []int{1, 2}, w.reminderNumbers("u1"))
}

// Running the sweep again at the same time publishes nothing new.
func TestSweep_RerunDoesNotRepublish(t *testing.T) {
	w := newWorld(t, overdueStage())
	w.seat("u1", "", "pending", dueFri)
	w.now(dueFri.Add(time.Hour))
	w.sweep()
	w.sweep()
	w.sweep()
	require.Len(t, w.reminders(), 1)
}

// After downtime only the newest elapsed number goes out, not a burst.
func TestSweep_CatchUpPublishesOnlyTheNewestNumber(t *testing.T) {
	w := newWorld(t, overdueStage())
	w.seat("u1", "", "pending", dueFri)
	w.now(at(11, 12)) // numbers 1 to 4 have fallen (Fri, Sat, Tue, Wed)
	w.sweep()
	require.Equal(t, []int{4}, w.reminderNumbers("u1"))
	require.Equal(t, 4, w.sent("u1", ""))
}

// A failed publish is not counted: the next sweep sends the same number, and
// the progress moves only then.
func TestSweep_PublishFailureIsRetriedWithoutCounting(t *testing.T) {
	w := newWorld(t, overdueStage())
	seat := w.seat("u1", "", "pending", dueFri)
	w.publishErr = func(string, []byte) error { return errors.New("broker down") }
	w.now(dueFri.Add(time.Hour))
	w.sweep()
	require.Empty(t, w.reminders())
	require.Zero(t, seat.row.RemindersSent, "nothing recorded for a number that did not go out")
	require.True(t, seat.row.ReminderAt.Equal(dueFri), "reminder_at has not moved")
	require.Empty(t, w.audits, "no audit for a reminder that did not go out")

	w.publishErr = nil
	w.sweep()
	require.Equal(t, []int{1}, w.reminderNumbers("u1"))
	require.Equal(t, 1, seat.row.RemindersSent)
	w.sweep()
	require.Len(t, w.reminders(), 1, "counted once")
}

// reminder_at moves to when the next reminder is due.
func TestSweep_MovesReminderAtToTheNextReminder(t *testing.T) {
	w := newWorld(t, overdueStage())
	seat := w.seat("u1", "", "pending", dueFri)
	w.now(dueFri)
	w.sweep()
	require.True(t, seat.row.ReminderAt.Equal(at(7, 0)), "reminder 2 falls at the end of Friday, got %s", seat.row.ReminderAt)
}

// A crash after the publish and before the record sends the same number again
// at most once more; the consumer dedups on it.
func TestSweep_RecordFailureRepublishesTheSameNumber(t *testing.T) {
	w := newWorld(t, overdueStage())
	w.seat("u1", "", "pending", dueFri)
	w.recordFail = true
	w.now(dueFri.Add(time.Hour))
	w.sweep()
	w.recordFail = false
	w.sweep()
	require.Equal(t, []int{1, 1}, w.reminderNumbers("u1"))
	w.sweep()
	require.Len(t, w.reminders(), 2)
}

// Seats that are decided, paused, superseded or withdrawn get nothing, and
// the pending one beside them still does.
func TestSweep_SkipsSeatsThatAreNotPending(t *testing.T) {
	st := overdueStage()
	st.Quorum = builder.QuorumAny
	w := newWorld(t, st)
	w.seat("u-pending", "", "pending", dueFri)
	w.seat("u-paused", "", "paused", dueFri)
	w.seat("u-superseded", "", "superseded", dueFri)
	w.seat("u-withdrawn", "", "withdrawn", dueFri)
	w.seat("u-swapped", "", "swapped_out", dueFri)
	w.now(dueFri.Add(time.Hour))
	w.sweep()
	require.Len(t, w.reminders(), 1)
	require.Equal(t, "u-pending", w.reminders()[0].body["approver_user_id"])
}

// A stage that is already decided by its quorum is left alone, even with a
// seat still pending.
func TestSweep_SkipsAStageItsQuorumAlreadyDecided(t *testing.T) {
	for name, states := range map[string][2]string{
		"approved by quorum": {"approved", "pending"},
		"rejected":           {"rejected", "pending"},
	} {
		t.Run(name, func(t *testing.T) {
			st := overdueStage()
			if name == "approved by quorum" {
				st.Quorum = builder.QuorumAny
			}
			w := newWorld(t, st)
			w.seat("u1", "", states[0], dueFri)
			w.seat("u2", "", states[1], dueFri)
			w.now(at(12, 12))
			w.sweep()
			require.Empty(t, w.published)
			require.Empty(t, w.audits)
		})
	}
}

// A run that is not in review is never swept.
func TestSweep_SkipsRunsNotInReview(t *testing.T) {
	w := newWorld(t, overdueStage())
	w.seat("u1", "", "pending", dueFri)
	w.runStatus = "approved"
	w.now(at(12, 12))
	w.sweep()
	require.Empty(t, w.published)
}

// reject_on_sla_breach stages reject at the due date, a stage with no sla_days
// has no due date, and a stage with neither setting has nothing to send.
func TestSweep_SkipsStagesThatCannotBeOverdue(t *testing.T) {
	cases := map[string]func(*builder.Stage){
		"rejects on breach": func(s *builder.Stage) { s.RejectOnSLABreach = true },
		"no sla_days":       func(s *builder.Stage) { s.SLADays = 0 },
		"both off":          func(s *builder.Stage) { s.RemindEveryDays, s.EscalateAfterDays = 0, 0 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			st := overdueStage()
			mutate(&st)
			w := newWorld(t, st)
			w.owners = []string{"o1"}
			w.seat("u1", "", "pending", dueFri)
			w.now(at(20, 12))
			w.sweep()
			require.Empty(t, w.published)
			require.Empty(t, w.audits)
		})
	}
}

// With reminders off, escalation still works; with escalation off, reminders do.
func TestSweep_EachSettingWorksAlone(t *testing.T) {
	st := overdueStage()
	st.RemindEveryDays = 0
	w := newWorld(t, st)
	w.owners = []string{"o1"}
	w.seat("u1", "", "pending", dueFri)
	w.now(at(11, 12))
	w.sweep()
	require.Empty(t, w.reminders())
	require.Len(t, w.escalations(), 1)

	st = overdueStage()
	st.EscalateAfterDays = 0
	w = newWorld(t, st)
	w.owners = []string{"o1"}
	w.seat("u1", "", "pending", dueFri)
	w.now(at(11, 12))
	w.sweep()
	require.Len(t, w.reminders(), 1)
	require.Empty(t, w.escalations())
}

// The run has no pinned calendar: days are calendar days, the same fallback the
// due date uses. Due Friday 00:00, every 2 days, escalate after 3.
func TestSweep_UnpinnedRunCountsCalendarDays(t *testing.T) {
	st := overdueStage()
	st.RemindEveryDays, st.EscalateAfterDays = 2, 3
	w := newWorld(t, st)
	w.pin = nil
	w.owners = []string{"o1"}
	w.seat("u1", "", "pending", dueFri)

	w.now(dueFri)
	w.sweep()
	w.now(at(7, 23)) // Saturday evening: reminder 2 is due Sunday 00:00
	w.sweep()
	require.Equal(t, []int{1}, w.reminderNumbers("u1"))
	require.Empty(t, w.escalations())

	w.now(at(8, 0))
	w.sweep()
	require.Equal(t, []int{1, 2}, w.reminderNumbers("u1"), "Sunday counts: no weekend skip")

	w.now(at(9, 0)) // three calendar days after the due day: Monday 00:00
	w.sweep()
	require.Len(t, w.escalations(), 1)
}

// A pinned calendar that cannot be read is the same fallback, logged.
func TestSweep_UnusablePinFallsBackToCalendarDays(t *testing.T) {
	w := newWorld(t, overdueStage())
	w.pin = map[string]any{"weekdays": []int{}, "timezone": "UTC"}
	w.seat("u1", "", "pending", dueFri)
	w.now(at(8, 0))
	w.sweep()
	require.Equal(t, []int{3}, w.reminderNumbers("u1"), "Friday, Saturday and Sunday 00:00 have fallen: calendar days, newest only")
}

// A run whose inputs cannot be read is skipped, not guessed.
func TestSweep_RunInputsErrorSkipsTheStage(t *testing.T) {
	w := newWorld(t, overdueStage())
	w.owners = []string{"o1"}
	w.seat("u1", "", "pending", dueFri)
	w.pinErr = errors.New("saga store down")
	w.now(at(20, 12))
	w.sweep()
	require.Empty(t, w.published)
	w.pinErr = nil
	w.sweep()
	require.NotEmpty(t, w.reminders())
}

// One person holding an individual and a group seat gets one reminder.
func TestSweep_OnePersonWithTwoSeatsGetsOneReminder(t *testing.T) {
	w := newWorld(t, overdueStage())
	w.seat("u1", "", "pending", dueFri)
	w.seat("u1", "grp-1", "pending", dueFri)
	w.seat("u2", "", "pending", dueFri)
	w.now(dueFri.Add(time.Hour))
	w.sweep()
	require.Len(t, w.reminders(), 2)
	require.Equal(t, 1, w.sent("u1", ""))
	require.Equal(t, 1, w.sent("u1", "grp-1"), "both seats are counted")
	w.sweep()
	require.Len(t, w.reminders(), 2)
}

// The reminder carries ids and the due date, no names.
func TestSweep_ReminderPayloadAndAudit(t *testing.T) {
	w := newWorld(t, overdueStage())
	w.seat("u1", "", "pending", dueFri)
	w.now(dueFri.Add(time.Hour))
	w.sweep()
	require.Len(t, w.reminders(), 1)
	require.Equal(t, map[string]any{
		"event_type":        "workflow.approval_reminder",
		"task_id":           "pv-1:0",
		"policy_id":         "pol-1",
		"policy_version_id": "pv-1",
		"stage_index":       float64(0),
		"stage_name":        "Legal review",
		"approver_user_id":  "u1",
		"workflow_name":     "Policy sign-off",
		"due_by":            "2026-03-06T00:00:00Z",
		"n":                 float64(1),
	}, w.reminders()[0].body)
	require.Equal(t, []auditRec{{
		action: "workflow.stage_reminder.sent", subject: "policy_version:pv-1 stage:0",
		attrs: map[string]string{"run_id": "run-1", "approver_user_id": "u1", "n": "1"},
	}}, w.audits)
}

// The escalation falls 2 working days after the due day (Fri, Mon: the end of
// Monday, Tuesday 00:00), goes to every owner once, and is recorded.
func TestSweep_EscalatesOnceToEachOwner(t *testing.T) {
	st := overdueStage()
	st.RemindEveryDays = 0
	w := newWorld(t, st)
	w.owners = []string{"o1", "o2", "o1"}
	w.seat("u1", "", "pending", dueFri)
	w.seat("u1", "grp-1", "pending", dueFri)
	w.seat("u2", "", "pending", dueFri)
	w.seat("u3", "", "approved", dueFri)

	w.now(at(9, 23))
	w.sweep()
	require.Empty(t, w.escalations(), "not before 2 working days have passed")

	w.now(at(10, 0))
	w.sweep()
	got := w.escalations()
	require.Len(t, got, 2, "one per distinct owner")
	var owners []string
	for _, p := range got {
		owners = append(owners, p.body["owner_user_id"].(string))
		require.Equal(t, float64(2), p.body["pending_count"], "two people still undecided, one of them with two seats")
	}
	slices.Sort(owners)
	require.Equal(t, []string{"o1", "o2"}, owners)
	require.Equal(t, map[string]any{
		"event_type": "workflow.stage_escalated", "task_id": "pv-1:0", "policy_id": "pol-1",
		"policy_version_id": "pv-1", "stage_index": float64(0), "stage_name": "Legal review",
		"owner_user_id": got[0].body["owner_user_id"], "workflow_name": "Policy sign-off",
		"due_by": "2026-03-06T00:00:00Z", "pending_count": float64(2),
	}, got[0].body)

	w.now(at(16, 12))
	w.sweep()
	w.sweep()
	require.Len(t, w.escalations(), 2, "never repeats")
	require.True(t, w.escalated["run-1a"])
	require.Equal(t, "workflow.stage_escalated", w.audits[len(w.audits)-1].action)
	require.Equal(t, "policy_version:pv-1 stage:0", w.audits[len(w.audits)-1].subject)
}

// A holiday moves the escalation too.
func TestSweep_HolidayPushesTheEscalation(t *testing.T) {
	st := overdueStage()
	st.RemindEveryDays = 0
	w := newWorld(t, st)
	cal := workcal.Default()
	cal.Holidays = []workcal.Date{{Year: 2026, Month: time.March, Day: 9}}
	w.pin = cal.Pin()
	w.owners = []string{"o1"}
	w.seat("u1", "", "pending", dueFri)
	w.now(at(10, 12))
	w.sweep()
	require.Empty(t, w.escalations())
	w.now(at(11, 0))
	w.sweep()
	require.Len(t, w.escalations(), 1)
}

// A stage that was decided before the escalation fell is never escalated.
func TestSweep_NoEscalationOnceTheStageIsDecided(t *testing.T) {
	st := overdueStage()
	w := newWorld(t, st)
	w.owners = []string{"o1"}
	a := w.seat("u1", "", "pending", dueFri)
	b := w.seat("u2", "", "pending", dueFri)
	w.now(at(9, 12))
	w.sweep()
	a.state, b.state = "approved", "approved"
	w.now(at(12, 12))
	w.sweep()
	require.Empty(t, w.escalations())
}

// If any owner's notice fails the stage is not recorded, so the next sweep
// retries; the owner who already got it is deduped by the consumer.
func TestSweep_EscalationPublishFailureIsRetried(t *testing.T) {
	st := overdueStage()
	st.RemindEveryDays = 0
	w := newWorld(t, st)
	w.owners = []string{"o1", "o2"}
	w.seat("u1", "", "pending", dueFri)
	w.publishErr = func(_ string, body []byte) error {
		var m map[string]any
		_ = json.Unmarshal(body, &m)
		if m["owner_user_id"] == "o2" {
			return errors.New("broker down")
		}
		return nil
	}
	w.now(at(10, 0))
	w.sweep()
	require.False(t, w.escalated["run-1a"], "not recorded while an owner's notice failed")
	require.Empty(t, w.audits, "no audit until the escalation is complete")

	w.publishErr = nil
	w.sweep()
	require.True(t, w.escalated["run-1a"])
	w.sweep()
	var o2 int
	for _, p := range w.escalations() {
		if p.body["owner_user_id"] == "o2" {
			o2++
		}
	}
	require.Equal(t, 1, o2)
}

// No owner to tell: nothing is recorded, so an owner added later is still told.
func TestSweep_NoOwnersRecordsNothing(t *testing.T) {
	st := overdueStage()
	st.RemindEveryDays = 0
	w := newWorld(t, st)
	w.seat("u1", "", "pending", dueFri)
	w.now(at(10, 0))
	w.sweep()
	require.Empty(t, w.escalations())
	require.False(t, w.escalated["run-1a"])

	w.owners = []string{"o1"}
	w.sweep()
	require.Len(t, w.escalations(), 1)

	w2 := newWorld(t, st)
	w2.ownersErr = errors.New("core down")
	w2.seat("u1", "", "pending", dueFri)
	w2.now(at(10, 0))
	w2.sweep()
	require.Empty(t, w2.escalations())
	require.False(t, w2.escalated["run-1a"])
}

// A failure on one stage does not stop the others.
func TestSweep_OneBadStageDoesNotStopTheRest(t *testing.T) {
	w := newWorld(t, overdueStage())
	w.seat("u1", "", "pending", dueFri)
	w.publishErr = func(_ string, body []byte) error {
		var m map[string]any
		_ = json.Unmarshal(body, &m)
		if m["approver_user_id"] == "u1" {
			return errors.New("boom")
		}
		return nil
	}
	w.seat("u2", "", "pending", dueFri)
	w.now(dueFri.Add(time.Hour))
	w.sweep()
	require.Equal(t, []int{1}, w.reminderNumbers("u2"))
	require.Empty(t, w.reminderNumbers("u1"))
}

// The sweeper's only effects are publishes, the audit and its own progress
// marks: no seat changes state, and none is added.
func TestSweep_OnlyNotifies(t *testing.T) {
	st := overdueStage()
	w := newWorld(t, st)
	w.owners = []string{"o1"}
	w.seat("u1", "", "pending", dueFri)
	w.seat("u2", "", "pending", dueFri)
	w.now(at(20, 12))
	w.sweep()
	require.Len(t, w.seats, 2)
	for _, s := range w.seats {
		require.Equal(t, "pending", s.state)
		require.Equal(t, "pending", s.row.State)
	}
}
