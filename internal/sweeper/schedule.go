// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package sweeper

import (
	"errors"
	"fmt"
	"time"

	"github.com/Steward-GRC/steward-workflow/internal/workcal"
)

// maxReminders bounds the reminder count for one seat so a bad value can never
// spin the sweep. A reminder is at least a day apart, so this is decades.
const maxReminders = 10000

// stepDays returns the instant n days after the day that ends at from.
//
// from is an exclusive end of day: the due instant, or the instant a reminder
// fell. With a calendar that is n working days after that day, ending at the
// next local midnight, the same end-of-day rule the due date uses. With none it
// is n calendar days, the fallback the due date uses for a run with no pin.
func stepDays(from time.Time, n int, cal *workcal.Calendar) (time.Time, error) {
	if cal == nil {
		return from.Add(time.Duration(n) * 24 * time.Hour), nil
	}
	// A nanosecond back is inside the day that ended at from.
	next, err := cal.AddWorkingDays(from.Add(-time.Nanosecond), n)
	if err != nil {
		return time.Time{}, fmt.Errorf("count %d working days after %s: %w", n, from.UTC().Format(time.RFC3339), err)
	}
	return next, nil
}

// reminderProgress returns how many reminders have fallen at now and when the
// next one falls. Reminder 1 falls at due; reminder k+1 falls every days after
// the day of reminder k. A due date still ahead is 0 reminders, next at due.
func reminderProgress(due time.Time, every int, cal *workcal.Calendar, now time.Time) (n int, next time.Time, err error) {
	if every <= 0 {
		return 0, time.Time{}, errors.New("remind_every_days is not set")
	}
	next = due
	for next.Compare(now) <= 0 {
		n++
		if n > maxReminders {
			return 0, time.Time{}, fmt.Errorf("more than %d reminders have fallen since %s", maxReminders, due.UTC().Format(time.RFC3339))
		}
		if next, err = stepDays(next, every, cal); err != nil {
			return 0, time.Time{}, err
		}
	}
	return n, next, nil
}
