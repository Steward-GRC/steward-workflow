// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package builder

import (
	"testing"
	"time"

	"github.com/Steward-GRC/steward-workflow/internal/workcal"
)

func mustCal(t *testing.T, days []int, tz string, hol ...string) *workcal.Calendar {
	t.Helper()
	c, err := workcal.New(days, tz, hol)
	if err != nil {
		t.Fatal(err)
	}
	return &c
}

func TestStageSLA_WorkingDays(t *testing.T) {
	monFri := []int{1, 2, 3, 4, 5}
	opts := CompileOptions{StageSLAHours: 72, StageReminderHours: 48}
	cases := []struct {
		name string
		days int
		cal  *workcal.Calendar
		now  time.Time
		want time.Time
	}{
		{"no calendar keeps calendar days", 5, nil,
			time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC), time.Date(2026, 11, 17, 10, 0, 0, 0, time.UTC)},
		{"weekend is skipped", 2, mustCal(t, monFri, "UTC"),
			time.Date(2026, 11, 13, 17, 0, 0, 0, time.UTC), time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)},
		{"holiday is skipped", 2, mustCal(t, monFri, "UTC", "2026-11-16"),
			time.Date(2026, 11, 13, 17, 0, 0, 0, time.UTC), time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC)},
		{"time zone moves the day", 1, mustCal(t, monFri, "Pacific/Auckland"),
			time.Date(2026, 11, 12, 20, 0, 0, 0, time.UTC),  // Fri in UTC, Sat 09:00 in Auckland
			time.Date(2026, 11, 16, 11, 0, 0, 0, time.UTC)}, // end of Mon 16 Nov in Auckland: Tue 00:00 NZDT
		{"no sla_days uses the service hours even with a calendar", 0, mustCal(t, monFri, "UTC"),
			time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC), time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC)},
		{"over the day limit falls back to calendar days", 400, mustCal(t, monFri, "UTC"),
			time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC), time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC).Add(400 * 24 * time.Hour)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, reminder := opts.StageSLA(Stage{SLADays: tc.days}, tc.now, tc.cal)
			if !got.Equal(tc.want) {
				t.Fatalf("deadline = %s, want %s", got.UTC(), tc.want.UTC())
			}
			if tc.days > 0 && !reminder.Equal(got) {
				t.Fatalf("reminder = %s, want it at the deadline %s", reminder, got)
			}
		})
	}
}
