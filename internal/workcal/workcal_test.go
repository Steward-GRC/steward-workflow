// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package workcal

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

type fixtureCalendar struct {
	Weekdays []int    `json:"weekdays"`
	Timezone string   `json:"timezone"`
	Holidays []string `json:"holidays"`
}

type fixture struct {
	Calendars map[string]fixtureCalendar `json:"calendars"`
	Add       []struct {
		Name     string `json:"name"`
		Calendar string `json:"calendar"`
		From     string `json:"from"`
		N        int    `json:"n"`
		Want     string `json:"want"`
	} `json:"add_working_days"`
	Days []struct {
		Calendar string `json:"calendar"`
		Date     string `json:"date"`
		Want     bool   `json:"want"`
	} `json:"is_working_day"`
}

func loadFixture(t *testing.T) (fixture, map[string]Calendar) {
	t.Helper()
	raw, err := os.ReadFile("testdata/workcal_cases.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var fx fixture
	if err := json.Unmarshal(raw, &fx); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	cals := make(map[string]Calendar, len(fx.Calendars))
	for name, fc := range fx.Calendars {
		c, err := New(fc.Weekdays, fc.Timezone, fc.Holidays)
		if err != nil {
			t.Fatalf("calendar %q: %v", name, err)
		}
		cals[name] = c
	}
	return fx, cals
}

func TestAddWorkingDaysFixture(t *testing.T) {
	fx, cals := loadFixture(t)
	if len(fx.Add) < 40 {
		t.Fatalf("fixture has %d cases, expected the full set", len(fx.Add))
	}
	for _, tc := range fx.Add {
		t.Run(tc.Name, func(t *testing.T) {
			cal, ok := cals[tc.Calendar]
			if !ok {
				t.Fatalf("unknown calendar %q", tc.Calendar)
			}
			from, err := time.Parse(time.RFC3339Nano, tc.From)
			if err != nil {
				t.Fatalf("from: %v", err)
			}
			want, err := time.Parse(time.RFC3339Nano, tc.Want)
			if err != nil {
				t.Fatalf("want: %v", err)
			}
			got, err := cal.AddWorkingDays(from, tc.N)
			if err != nil {
				t.Fatalf("AddWorkingDays: %v", err)
			}
			if !got.Equal(want) {
				t.Fatalf("AddWorkingDays(%s, %d) = %s, want %s", tc.From, tc.N,
					got.UTC().Format(time.RFC3339Nano), tc.Want)
			}
		})
	}
}

func TestIsWorkingDayFixture(t *testing.T) {
	fx, cals := loadFixture(t)
	for _, tc := range fx.Days {
		t.Run(tc.Calendar+"/"+tc.Date, func(t *testing.T) {
			d, err := ParseDate(tc.Date)
			if err != nil {
				t.Fatalf("date: %v", err)
			}
			if got := cals[tc.Calendar].IsWorkingDay(d); got != tc.Want {
				t.Fatalf("IsWorkingDay(%s) = %v, want %v", tc.Date, got, tc.Want)
			}
		})
	}
}

func TestAddWorkingDaysNoWorkingWeekday(t *testing.T) {
	cal := Calendar{Location: time.UTC}
	done := make(chan error, 1)
	go func() {
		_, err := cal.AddWorkingDays(time.Date(2026, 11, 12, 9, 0, 0, 0, time.UTC), 1)
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrNoWorkingWeekday) {
			t.Fatalf("err = %v, want ErrNoWorkingWeekday", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("AddWorkingDays did not return for a calendar with no working weekday")
	}
}

func TestAddWorkingDaysNoWorkingWeekdayNonPositive(t *testing.T) {
	// Nothing is counted for n <= 0, so the input comes back even for an
	// unusable calendar; callers validate the calendar when they accept it.
	from := time.Date(2026, 11, 12, 9, 0, 0, 0, time.UTC)
	got, err := Calendar{}.AddWorkingDays(from, 0)
	if err != nil || !got.Equal(from) {
		t.Fatalf("got %v, %v; want the input and no error", got, err)
	}
}

func TestAddWorkingDaysLimit(t *testing.T) {
	cal := Default()
	from := time.Date(2026, 11, 12, 9, 0, 0, 0, time.UTC)
	if _, err := cal.AddWorkingDays(from, MaxWorkingDays); err != nil {
		t.Fatalf("n = MaxWorkingDays: %v", err)
	}
	if _, err := cal.AddWorkingDays(from, MaxWorkingDays+1); !errors.Is(err, ErrTooManyDays) {
		t.Fatalf("n over the limit: err = %v, want ErrTooManyDays", err)
	}
}

func TestAddWorkingDaysEveryWeekdayHoliday(t *testing.T) {
	// Every Wednesday for a year is a holiday and Wednesday is the only
	// working weekday: the search must still end (holidays are finite).
	var hol []Date
	d := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 52; i++ {
		hol = append(hol, DateOf(d.AddDate(0, 0, 7*i)))
	}
	cal := Calendar{Weekdays: NewWeekdays(time.Wednesday), Holidays: hol, Location: time.UTC}
	got, err := cal.AddWorkingDays(time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC), 1)
	if err != nil {
		t.Fatalf("AddWorkingDays: %v", err)
	}
	want := time.Date(2027, 11, 18, 0, 0, 0, 0, time.UTC) // Wed 2027-11-17 is working, end of that day
	if !got.Equal(want) {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestAddWorkingDaysNilLocationIsUTC(t *testing.T) {
	cal := Calendar{Weekdays: MonToFri}
	got, err := cal.AddWorkingDays(time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC), 1)
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestAddWorkingDaysIsStrictlyIncreasingAndAtLocalMidnight(t *testing.T) {
	loc, err := ParseLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	cal := Calendar{Weekdays: MonToFri, Location: loc}
	from := time.Date(2026, 3, 1, 15, 0, 0, 0, time.UTC)
	prev := from
	for n := 1; n <= 120; n++ {
		got, err := cal.AddWorkingDays(from, n)
		if err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		if !got.After(prev) {
			t.Fatalf("n=%d: %s is not after %s", n, got, prev)
		}
		l := got.In(loc)
		if l.Hour() != 0 || l.Minute() != 0 || l.Second() != 0 || l.Nanosecond() != 0 {
			t.Fatalf("n=%d: %s is not local midnight", n, l)
		}
		prev = got
	}
}

func TestAddWorkingDaysDoesNotMutateCalendar(t *testing.T) {
	cal := Default()
	cal.Holidays = []Date{{2026, time.November, 16}}
	before := append([]Date(nil), cal.Holidays...)
	if _, err := cal.AddWorkingDays(time.Date(2026, 11, 13, 22, 0, 0, 0, time.UTC), 3); err != nil {
		t.Fatal(err)
	}
	if len(cal.Holidays) != len(before) || cal.Holidays[0] != before[0] {
		t.Fatal("holidays were modified")
	}
}

func TestParseDate(t *testing.T) {
	good := map[string]Date{
		"2026-11-12": {2026, time.November, 12},
		"2028-02-29": {2028, time.February, 29},
		"0001-01-01": {1, time.January, 1},
	}
	for in, want := range good {
		got, err := ParseDate(in)
		if err != nil || got != want {
			t.Errorf("ParseDate(%q) = %v, %v; want %v", in, got, err, want)
		}
		if got.String() != in {
			t.Errorf("String() = %q, want %q", got.String(), in)
		}
	}
	bad := []string{"", "2026-2-3", "2026-02-30", "2027-02-29", "2026-13-01", "2026-00-10",
		"26-11-12", " 2026-11-12", "2026-11-12 ", "2026/11/12", "2026-11-12T00:00:00Z", "12-11-2026",
		"2026-11-1", "+2026-11-12"}
	for _, in := range bad {
		if _, err := ParseDate(in); err == nil {
			t.Errorf("ParseDate(%q) accepted", in)
		}
	}
}

func TestParseLocation(t *testing.T) {
	for _, name := range []string{"UTC", "America/New_York", "Asia/Kolkata", "Australia/Lord_Howe", "Europe/London"} {
		loc, err := ParseLocation(name)
		if err != nil || loc == nil || loc.String() != name {
			t.Errorf("ParseLocation(%q) = %v, %v", name, loc, err)
		}
	}
	for _, name := range []string{"", " ", "Local", "Not/AZone", "../etc/passwd", "/etc/localtime", "america/new_york "} {
		if _, err := ParseLocation(name); err == nil {
			t.Errorf("ParseLocation(%q) accepted", name)
		}
	}
}

func TestWeekdaysFromISO(t *testing.T) {
	w, err := WeekdaysFromISO([]int{1, 2, 3, 4, 5})
	if err != nil || w != MonToFri {
		t.Fatalf("got %v, %v", w, err)
	}
	w, err = WeekdaysFromISO([]int{7, 6})
	if err != nil || !w.Has(time.Sunday) || !w.Has(time.Saturday) || w.Has(time.Monday) {
		t.Fatalf("got %v, %v", w, err)
	}
	if _, err := WeekdaysFromISO([]int{0}); err == nil {
		t.Error("0 accepted")
	}
	if _, err := WeekdaysFromISO([]int{8}); err == nil {
		t.Error("8 accepted")
	}
	if _, err := WeekdaysFromISO([]int{1, 1}); err == nil {
		t.Error("duplicate accepted")
	}
	if _, err := WeekdaysFromISO(nil); !errors.Is(err, ErrNoWorkingWeekday) {
		t.Errorf("empty: err = %v, want ErrNoWorkingWeekday", err)
	}
}

func TestISOWeekdaysRoundTrip(t *testing.T) {
	got := NewWeekdays(time.Sunday, time.Monday, time.Thursday).ISO()
	want := []int{1, 4, 7}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestNew(t *testing.T) {
	c, err := New([]int{1, 2, 3, 4, 5}, "UTC", []string{"2026-12-25"})
	if err != nil {
		t.Fatal(err)
	}
	if c.Weekdays != MonToFri || len(c.Holidays) != 1 || c.Location.String() != "UTC" {
		t.Fatalf("unexpected calendar %+v", c)
	}
	cases := []struct {
		name string
		days []int
		tz   string
		hol  []string
		want error
	}{
		{"no weekday", nil, "UTC", nil, ErrNoWorkingWeekday},
		{"bad weekday", []int{9}, "UTC", nil, ErrInvalidWeekday},
		{"bad zone", []int{1}, "Nowhere/Land", nil, ErrInvalidTimezone},
		{"bad holiday", []int{1}, "UTC", []string{"2026-02-30"}, ErrInvalidHoliday},
		{"duplicate holiday", []int{1}, "UTC", []string{"2026-12-25", "2026-12-25"}, ErrDuplicateHoliday},
	}
	for _, tc := range cases {
		if _, err := New(tc.days, tc.tz, tc.hol); !errors.Is(err, tc.want) {
			t.Errorf("%s: err = %v, want %v", tc.name, err, tc.want)
		}
	}
}

func TestValidate(t *testing.T) {
	ok := Default()
	if err := ok.Validate(); err != nil {
		t.Fatalf("default calendar: %v", err)
	}
	if err := (Calendar{Weekdays: MonToFri}).Validate(); err != nil {
		t.Fatalf("nil location is UTC: %v", err)
	}
	tooMany := Default()
	for i := 0; i <= MaxHolidays; i++ {
		tooMany.Holidays = append(tooMany.Holidays, DateOf(time.Date(2026, 1, 1+i, 0, 0, 0, 0, time.UTC)))
	}
	cases := []struct {
		name string
		cal  Calendar
		want error
	}{
		{"no weekday", Calendar{Location: time.UTC}, ErrNoWorkingWeekday},
		{"duplicate holiday", Calendar{Weekdays: MonToFri, Holidays: []Date{{2026, 12, 25}, {2026, 12, 25}}}, ErrDuplicateHoliday},
		{"impossible holiday", Calendar{Weekdays: MonToFri, Holidays: []Date{{2026, 2, 30}}}, ErrInvalidHoliday},
		{"month zero", Calendar{Weekdays: MonToFri, Holidays: []Date{{2026, 0, 1}}}, ErrInvalidHoliday},
		{"too many holidays", tooMany, ErrTooManyHolidays},
	}
	for _, tc := range cases {
		if err := tc.cal.Validate(); !errors.Is(err, tc.want) {
			t.Errorf("%s: err = %v, want %v", tc.name, err, tc.want)
		}
	}
}

func TestDueIn(t *testing.T) {
	now := time.Date(2026, 11, 12, 9, 0, 0, 0, time.UTC)
	cases := []struct {
		name     string
		deadline time.Time
		want     time.Duration
	}{
		{"future", now.Add(90 * time.Minute), 90 * time.Minute},
		{"equal", now, 0},
		{"past", now.Add(-time.Hour), 0},
		{"far past", now.Add(-1000 * time.Hour), 0},
		{"zero deadline", time.Time{}, 0},
		{"other zone", now.Add(2 * time.Hour).In(time.FixedZone("x", 5*3600+1800)), 2 * time.Hour},
	}
	for _, tc := range cases {
		if got := DueIn(now, tc.deadline); got != tc.want {
			t.Errorf("%s: DueIn = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestDueInFromAddWorkingDays(t *testing.T) {
	now := time.Date(2026, 11, 13, 22, 0, 0, 0, time.UTC) // Friday evening
	deadline, err := Default().AddWorkingDays(now, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := DueIn(now, deadline), 74*time.Hour; got != want { // to Tuesday 00:00Z
		t.Fatalf("DueIn = %v, want %v", got, want)
	}
}

func TestErrorsAreDescriptiveAndNeverEmpty(t *testing.T) {
	for _, e := range []error{ErrNoWorkingWeekday, ErrInvalidWeekday, ErrInvalidTimezone, ErrInvalidHoliday,
		ErrDuplicateHoliday, ErrTooManyHolidays, ErrTooManyDays} {
		if strings.TrimSpace(e.Error()) == "" {
			t.Errorf("empty error text for %#v", e)
		}
	}
}
