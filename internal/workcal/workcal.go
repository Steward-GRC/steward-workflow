// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package workcal counts whole working days against one site calendar:
// which weekdays work, which civil dates are holidays, and the time zone the
// dates are read in.
//
// It uses only the standard library and holds no state, so it tests without a
// database or a clock and ports line for line to another language.
//
// # Semantics
//
// AddWorkingDays(from, n) is the deadline "n working days after entry":
//
//   - The day of entry is the civil date of from in the calendar's zone. It is
//     never counted, whether or not it is a working day, and whatever the time
//     of day (a Friday evening and a Monday morning both start counting from
//     the next day).
//   - Each following date that is a working weekday and not a holiday counts as
//     one. The nth such date is the due date.
//   - The result is the exclusive end of that due date: the instant of local
//     midnight that starts the next civil date. A task is late from that
//     instant on. Using the next midnight, not 23:59:59.999999999, keeps the
//     value exact in every language and sub-second precision.
//   - Days are stepped as civil dates, never as 24 hour blocks, so a day with
//     a daylight saving shift (23, 24, 24.5 or 25 hours) still counts as one
//     day and the result is always local midnight.
//   - n <= 0 returns from unchanged.
//
// Examples with Monday to Friday: Monday 10:00 with n=1 is due at the start of
// Wednesday (end of Tuesday); Friday 22:00 with n=1 is due at the start of
// Tuesday (end of Monday); Saturday with n=1 gives the same.
package workcal

import (
	"errors"
	"fmt"
	"time"
	// The zone database ships inside the binary so a minimal image with no
	// system zoneinfo still loads the site time zone.
	_ "time/tzdata"
)

const (
	// MaxWorkingDays is the largest n AddWorkingDays accepts.
	MaxWorkingDays = 365
	// MaxHolidays is the most holidays a calendar may list.
	MaxHolidays = 366
)

var (
	// ErrNoWorkingWeekday means the calendar has no working weekday, so no
	// deadline can ever be reached.
	ErrNoWorkingWeekday = errors.New("workcal: at least one working weekday is required")
	// ErrInvalidWeekday means a weekday number is outside 1 (Monday) to 7
	// (Sunday) or is listed twice.
	ErrInvalidWeekday = errors.New("workcal: weekday must be 1 (Monday) to 7 (Sunday), once each")
	// ErrInvalidTimezone means the time zone is not a loadable IANA name.
	ErrInvalidTimezone = errors.New("workcal: time zone is not a known IANA name")
	// ErrInvalidHoliday means a holiday is not a real YYYY-MM-DD date.
	ErrInvalidHoliday = errors.New("workcal: holiday is not a valid YYYY-MM-DD date")
	// ErrDuplicateHoliday means the same date is listed twice.
	ErrDuplicateHoliday = errors.New("workcal: holiday dates must be unique")
	// ErrTooManyHolidays means the list is longer than MaxHolidays.
	ErrTooManyHolidays = errors.New("workcal: too many holidays")
	// ErrTooManyDays means n is larger than MaxWorkingDays.
	ErrTooManyDays = errors.New("workcal: too many working days requested")
)

// Date is a civil (calendar) date with no time of day and no zone.
type Date struct {
	Year  int
	Month time.Month
	Day   int
}

// ParseDate reads a strict "YYYY-MM-DD" date. Anything else is rejected,
// including padding, other separators, a time part and impossible dates.
func ParseDate(s string) (Date, error) {
	if len(s) != 10 || s[4] != '-' || s[7] != '-' {
		return Date{}, fmt.Errorf("%w: %q", ErrInvalidHoliday, s)
	}
	for i := 0; i < len(s); i++ {
		if i == 4 || i == 7 {
			continue
		}
		if s[i] < '0' || s[i] > '9' {
			return Date{}, fmt.Errorf("%w: %q", ErrInvalidHoliday, s)
		}
	}
	t, err := time.Parse(time.DateOnly, s)
	if err != nil || t.Year() < 1 {
		return Date{}, fmt.Errorf("%w: %q", ErrInvalidHoliday, s)
	}
	return Date{t.Year(), t.Month(), t.Day()}, nil
}

// DateOf returns the civil date of t in t's own location.
func DateOf(t time.Time) Date {
	y, m, d := t.Date()
	return Date{y, m, d}
}

// String formats the date as YYYY-MM-DD.
func (d Date) String() string {
	return fmt.Sprintf("%04d-%02d-%02d", d.Year, int(d.Month), d.Day)
}

// valid reports whether d is a real date.
func (d Date) valid() bool {
	if d.Year < 1 || d.Month < time.January || d.Month > time.December || d.Day < 1 {
		return false
	}
	t := time.Date(d.Year, d.Month, d.Day, 0, 0, 0, 0, time.UTC)
	return t.Year() == d.Year && t.Month() == d.Month && t.Day() == d.Day
}

// weekday is the day of the week of a civil date. It is computed in UTC, where
// a day is always 24 hours, so it never depends on a zone.
func (d Date) weekday() time.Weekday {
	return time.Date(d.Year, d.Month, d.Day, 12, 0, 0, 0, time.UTC).Weekday()
}

// Weekdays is a set of days of the week.
type Weekdays uint8

// MonToFri is the default working week.
var MonToFri = NewWeekdays(time.Monday, time.Tuesday, time.Wednesday, time.Thursday, time.Friday)

// NewWeekdays builds a set from the given days.
func NewWeekdays(days ...time.Weekday) Weekdays {
	var w Weekdays
	for _, d := range days {
		w |= 1 << uint(d)
	}
	return w
}

// WeekdaysFromISO builds a set from ISO numbers (1 Monday to 7 Sunday). It
// rejects an empty list, a number out of range and a repeated number.
func WeekdaysFromISO(days []int) (Weekdays, error) {
	if len(days) == 0 {
		return 0, ErrNoWorkingWeekday
	}
	var w Weekdays
	for _, n := range days {
		if n < 1 || n > 7 {
			return 0, fmt.Errorf("%w: %d", ErrInvalidWeekday, n)
		}
		day := time.Weekday(n % 7) // 7 is Sunday, which is 0 in package time
		if w.Has(day) {
			return 0, fmt.Errorf("%w: %d listed twice", ErrInvalidWeekday, n)
		}
		w |= NewWeekdays(day)
	}
	return w, nil
}

// Has reports whether d is in the set.
func (w Weekdays) Has(d time.Weekday) bool {
	return d >= time.Sunday && d <= time.Saturday && w&(1<<uint(d)) != 0
}

// ISO lists the set as ISO numbers (1 Monday to 7 Sunday) in ascending order.
func (w Weekdays) ISO() []int {
	out := make([]int, 0, 7)
	for n := 1; n <= 7; n++ {
		if w.Has(time.Weekday(n % 7)) {
			out = append(out, n)
		}
	}
	return out
}

// ParseLocation loads an IANA zone name such as "Europe/London" or "UTC". The
// empty string and "Local" are rejected: a site calendar must not depend on
// the host.
func ParseLocation(name string) (*time.Location, error) {
	if name == "" || name == "Local" {
		return nil, fmt.Errorf("%w: %q", ErrInvalidTimezone, name)
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, fmt.Errorf("%w: %q", ErrInvalidTimezone, name)
	}
	return loc, nil
}

// Calendar says which days count as working days.
type Calendar struct {
	// Weekdays are the working days of the week. Empty is invalid.
	Weekdays Weekdays
	// Holidays are civil dates that never count, even on a working weekday.
	Holidays []Date
	// Location is the zone the dates are read in. Nil means UTC.
	Location *time.Location
}

// Default is Monday to Friday, no holidays, UTC.
func Default() Calendar {
	return Calendar{Weekdays: MonToFri, Location: time.UTC}
}

// New builds and validates a calendar from the shapes the site setting is
// stored in: ISO weekday numbers, an IANA zone name and YYYY-MM-DD holidays.
func New(isoWeekdays []int, timezone string, holidays []string) (Calendar, error) {
	w, err := WeekdaysFromISO(isoWeekdays)
	if err != nil {
		return Calendar{}, err
	}
	loc, err := ParseLocation(timezone)
	if err != nil {
		return Calendar{}, err
	}
	days := make([]Date, 0, len(holidays))
	for _, h := range holidays {
		d, err := ParseDate(h)
		if err != nil {
			return Calendar{}, err
		}
		days = append(days, d)
	}
	c := Calendar{Weekdays: w, Holidays: days, Location: loc}
	if err := c.Validate(); err != nil {
		return Calendar{}, err
	}
	return c, nil
}

// Validate checks the calendar: at least one working weekday, only real and
// unique holiday dates, no more than MaxHolidays of them. A nil Location is
// UTC and valid; a Location value is already loaded, so it cannot be bad.
func (c Calendar) Validate() error {
	if c.Weekdays == 0 {
		return ErrNoWorkingWeekday
	}
	if c.Weekdays > 0x7f {
		return fmt.Errorf("%w: unknown bits in %#x", ErrInvalidWeekday, uint8(c.Weekdays))
	}
	if len(c.Holidays) > MaxHolidays {
		return fmt.Errorf("%w: %d (limit %d)", ErrTooManyHolidays, len(c.Holidays), MaxHolidays)
	}
	seen := make(map[Date]struct{}, len(c.Holidays))
	for _, h := range c.Holidays {
		if !h.valid() {
			return fmt.Errorf("%w: %v", ErrInvalidHoliday, h)
		}
		if _, dup := seen[h]; dup {
			return fmt.Errorf("%w: %s", ErrDuplicateHoliday, h)
		}
		seen[h] = struct{}{}
	}
	return nil
}

func (c Calendar) location() *time.Location {
	if c.Location == nil {
		return time.UTC
	}
	return c.Location
}

func (c Calendar) holidaySet() map[Date]struct{} {
	set := make(map[Date]struct{}, len(c.Holidays))
	for _, h := range c.Holidays {
		set[h] = struct{}{}
	}
	return set
}

// isWorking is IsWorkingDay with the holiday set already built.
func (c Calendar) isWorking(d Date, holidays map[Date]struct{}) bool {
	if !c.Weekdays.Has(d.weekday()) {
		return false
	}
	_, off := holidays[d]
	return !off
}

// IsWorkingDay reports whether the civil date counts as a working day: its
// weekday is a working weekday and it is not a holiday. An invalid date is
// not a working day.
func (c Calendar) IsWorkingDay(d Date) bool {
	if !d.valid() {
		return false
	}
	return c.isWorking(d, c.holidaySet())
}

// AddWorkingDays returns the deadline n working days after from: the instant
// of local midnight that ends the nth working day after the day of entry. See
// the package documentation for the exact rules. n <= 0 returns from
// unchanged. A calendar with no working weekday returns ErrNoWorkingWeekday and
// n above MaxWorkingDays returns ErrTooManyDays; the search always ends because
// every seven days hold a working weekday and the holiday list is finite.
func (c Calendar) AddWorkingDays(from time.Time, n int) (time.Time, error) {
	if n <= 0 {
		return from, nil
	}
	if n > MaxWorkingDays {
		return time.Time{}, fmt.Errorf("%w: %d (limit %d)", ErrTooManyDays, n, MaxWorkingDays)
	}
	if c.Weekdays&0x7f == 0 {
		return time.Time{}, ErrNoWorkingWeekday
	}
	loc := c.location()
	holidays := c.holidaySet()

	// Step as civil dates in UTC (a day is always one step there) and only go
	// back to the zone for the final midnight.
	y, m, d := from.In(loc).Date()
	cursor := time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
	for counted := 0; counted < n; {
		cursor = cursor.AddDate(0, 0, 1)
		if c.isWorking(DateOf(cursor), holidays) {
			counted++
		}
	}
	cy, cm, cd := cursor.Date()
	return time.Date(cy, cm, cd+1, 0, 0, 0, 0, loc), nil
}

// DueIn converts a deadline into the time left from now, the form a timer
// takes. It never returns a negative value: a deadline already passed (or the
// zero time) is 0.
func DueIn(now, deadline time.Time) time.Duration {
	if d := deadline.Sub(now); d > 0 {
		return d
	}
	return 0
}
