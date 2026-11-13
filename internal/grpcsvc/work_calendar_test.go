// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	workflowv1 "github.com/Steward-GRC/steward-workflow/gen/go/steward/workflow/v1"
	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/workcal"
)

// stubCalendarSource is the site calendar as core would answer it.
type stubCalendarSource struct {
	cal   workcal.Calendar
	err   error
	calls int
}

func (s *stubCalendarSource) WorkCalendar(context.Context) (workcal.Calendar, error) {
	s.calls++
	return s.cal, s.err
}

// withSLA returns a test server whose only stage has sla_days set.
func withSLA(t *testing.T, days int, src WorkCalendarSource) *WorkflowServer {
	t.Helper()
	srv := newTestServer()
	srv.defStore = &stubWorkflowDefStore{wd: builder.WorkflowDef{
		ID: "def-1", Name: "Test", Version: 1, AuthorUserID: "author-1",
		Stages: []builder.Stage{{Name: "s", Quorum: builder.QuorumAny, ApproverIDs: []string{"u1", "u2", "u3"}, SLADays: days}},
	}}
	srv.workCalendar = src
	return srv
}

func calOf(t *testing.T, days []int, tz string, hol ...string) workcal.Calendar {
	t.Helper()
	c, err := workcal.New(days, tz, hol)
	require.NoError(t, err)
	return c
}

func TestSubmit_PinsTheSiteWorkCalendar(t *testing.T) {
	src := &stubCalendarSource{cal: calOf(t, []int{1, 2, 3, 4, 5}, "Europe/London", "2026-12-25")}
	srv := withSLA(t, 3, src)
	_, err := srv.Submit(context.Background(), submitReq())
	require.NoError(t, err)

	in := srv.sagaClient.(*stubSagaClient).startedInputs
	got, err := workcal.FromPin(in[workcal.PinKey])
	require.NoError(t, err)
	from := time.Date(2026, 12, 23, 9, 0, 0, 0, time.UTC)
	want, _ := src.cal.AddWorkingDays(from, 2)
	have, _ := got.AddWorkingDays(from, 2)
	require.True(t, want.Equal(have))
	require.Equal(t, 1, src.calls)
}

// A calendar edit after submit does not reach the run already pinned; the next
// submit pins the new one.
func TestSubmit_PinnedCalendarDoesNotChangeAfterAnEdit(t *testing.T) {
	src := &stubCalendarSource{cal: calOf(t, []int{1, 2, 3, 4, 5}, "UTC")}
	srv := withSLA(t, 2, src)
	saga := srv.sagaClient.(*stubSagaClient)

	_, err := srv.Submit(context.Background(), submitReq())
	require.NoError(t, err)
	first := saga.startedInputs[workcal.PinKey]

	src.cal = calOf(t, []int{1, 2, 3, 4}, "Asia/Tokyo", "2026-11-17")
	saga.startedRunID = "run-2"
	_, err = srv.Submit(context.Background(), submitReq())
	require.NoError(t, err)
	second := saga.startedInputs[workcal.PinKey]

	f1, err := workcal.FromPin(first)
	require.NoError(t, err)
	f2, err := workcal.FromPin(second)
	require.NoError(t, err)
	from := time.Date(2026, 11, 13, 17, 0, 0, 0, time.UTC)
	d1, _ := f1.AddWorkingDays(from, 2)
	d2, _ := f2.AddWorkingDays(from, 2)
	require.True(t, d1.Equal(time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)), "first run kept Mon-Fri UTC, got %s", d1)
	require.False(t, d1.Equal(d2))
}

func TestSubmit_NoSLADaysDoesNotAskCore(t *testing.T) {
	src := &stubCalendarSource{cal: workcal.Default()}
	srv := withSLA(t, 0, src)
	_, err := srv.Submit(context.Background(), submitReq())
	require.NoError(t, err)
	require.Zero(t, src.calls, "no stage counts days, so core is not asked")
	_, pinned := srv.sagaClient.(*stubSagaClient).startedInputs[workcal.PinKey]
	require.False(t, pinned)
}

func TestSubmit_NoCalendarSourceKeepsLegacyDays(t *testing.T) {
	srv := withSLA(t, 3, nil)
	_, err := srv.Submit(context.Background(), submitReq())
	require.NoError(t, err)
	_, pinned := srv.sagaClient.(*stubSagaClient).startedInputs[workcal.PinKey]
	require.False(t, pinned)
}

// Core down: the submit fails, retryable, and nothing was started, cancelled or
// written, because pinning a guessed calendar would misjudge the whole run.
func TestSubmit_CoreUnavailableFailsTheSubmit(t *testing.T) {
	for _, code := range []codes.Code{codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted} {
		t.Run(code.String(), func(t *testing.T) {
			src := &stubCalendarSource{err: status.Error(code, "core is down")}
			srv := withSLA(t, 3, src)
			core := &stubCoreStatus{}
			srv.coreStatus = core
			_, err := srv.Submit(context.Background(), submitReq())
			require.Equal(t, codes.Unavailable, status.Code(err), "err = %v", err)
			saga := srv.sagaClient.(*stubSagaClient)
			require.Empty(t, saga.startedRunID)
			require.Empty(t, saga.cancelledForVersion, "a prior run must survive a failed resubmit")
			require.Empty(t, core.calls)
			require.Nil(t, srv.runStore.(*stubRunStore).tracked)
		})
	}
}

// A calendar core cannot or will not give (the caller is not allowed, core
// predates the field, the data is invalid) is a settings fault, not an outage:
// the submit goes ahead on the legacy calendar-day math, loudly logged.
func TestSubmit_UnusableCalendarFallsBackToLegacyDays(t *testing.T) {
	for name, err := range map[string]error{
		"permission denied": status.Error(codes.PermissionDenied, "not allowed"),
		"unimplemented":     status.Error(codes.Unimplemented, "no such method"),
		"not found":         status.Error(codes.NotFound, "none"),
		"invalid calendar":  workcal.ErrInvalidTimezone,
		"plain error":       errors.New("boom"),
	} {
		t.Run(name, func(t *testing.T) {
			srv := withSLA(t, 3, &stubCalendarSource{err: err})
			_, serr := srv.Submit(context.Background(), submitReq())
			require.NoError(t, serr)
			saga := srv.sagaClient.(*stubSagaClient)
			require.NotEmpty(t, saga.startedRunID)
			_, pinned := saga.startedInputs[workcal.PinKey]
			require.False(t, pinned)
		})
	}
}

// Reassigning a seat counts the new deadline on the run's pinned calendar, the
// same one the timer and the stage entry used.
func TestSwap_DeadlineUsesTheRunsPinnedCalendar(t *testing.T) {
	cal := calOf(t, []int{1, 2, 3, 4, 5}, "UTC", "2026-12-25")
	srv, as := setupServerForSwap(t)
	srv.defStore = &stubWorkflowDefStore{wd: builder.WorkflowDef{
		ID: "def-1", Name: "Test", Version: 1, AuthorUserID: "author-1",
		Stages: []builder.Stage{{Name: "s", Quorum: builder.QuorumAny, ApproverIDs: []string{"u1", "u2", "u3"}, SLADays: 4}},
	}}
	srv.sagaClient.(*stubSagaClient).runInputs = map[string]any{workcal.PinKey: cal.Pin()}

	now := time.Now().UTC()
	_, err := srv.SwapAssignee(context.Background(), &workflowv1.SwapAssigneeRequest{
		PolicyVersionId: "pv-swap", StageIndex: 0, CurrentUserId: "u1", NewUserId: "u2",
		Reason: "PTO", InitiatorRole: workflowv1.InitiatorRole_INITIATOR_ROLE_ADMIN,
	})
	require.NoError(t, err)
	row := as.rows[seatKey("pv-swap", 0, "u2", "")]
	w1, _ := cal.AddWorkingDays(now, 4)
	w2, _ := cal.AddWorkingDays(time.Now().UTC(), 4)
	require.True(t, row.SLADeadlineAt.Equal(w1) || row.SLADeadlineAt.Equal(w2), "deadline = %s, want %s", row.SLADeadlineAt, w1)
}

// A run with no pinned calendar (one from before this change) keeps calendar days.
func TestSwap_RunWithoutPinKeepsLegacyDays(t *testing.T) {
	srv, as := setupServerForSwap(t)
	srv.defStore = &stubWorkflowDefStore{wd: builder.WorkflowDef{
		ID: "def-1", Name: "Test", Version: 1, AuthorUserID: "author-1",
		Stages: []builder.Stage{{Name: "s", Quorum: builder.QuorumAny, ApproverIDs: []string{"u1", "u2", "u3"}, SLADays: 4}},
	}}
	now := time.Now().UTC()
	_, err := srv.SwapAssignee(context.Background(), &workflowv1.SwapAssigneeRequest{
		PolicyVersionId: "pv-swap", StageIndex: 0, CurrentUserId: "u1", NewUserId: "u2",
		Reason: "PTO", InitiatorRole: workflowv1.InitiatorRole_INITIATOR_ROLE_ADMIN,
	})
	require.NoError(t, err)
	row := as.rows[seatKey("pv-swap", 0, "u2", "")]
	require.WithinDuration(t, now.Add(4*24*time.Hour), row.SLADeadlineAt, 5*time.Second)
}
