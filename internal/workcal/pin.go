// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package workcal

import (
	"errors"
	"fmt"
)

// PinKey is the run input that carries a run's pinned calendar.
const PinKey = "work_calendar"

// ErrNoPin means the run carries no pinned calendar (a run from before
// calendars were pinned, or a submit that could not read one). Callers keep the
// calendar-day math for it.
var ErrNoPin = errors.New("workcal: no pinned calendar")

// Pin returns the calendar in the shape a run's inputs hold it: ISO weekdays,
// the zone name and YYYY-MM-DD holidays. The values are copies, so the pin does
// not change if the caller edits the calendar afterwards.
func (c Calendar) Pin() map[string]any {
	holidays := make([]string, len(c.Holidays))
	for i, h := range c.Holidays {
		holidays[i] = h.String()
	}
	return map[string]any{
		"weekdays": c.Weekdays.ISO(),
		"timezone": c.location().String(),
		"holidays": holidays,
	}
}

// FromPin rebuilds the calendar from a run input. The saga store keeps inputs
// as JSON, so numbers come back as float64 and lists as []any; both that and
// the in-memory shape of Pin are read. nil is ErrNoPin; anything else that does
// not make a valid calendar is an error.
func FromPin(v any) (Calendar, error) {
	if v == nil {
		return Calendar{}, ErrNoPin
	}
	m, ok := v.(map[string]any)
	if !ok {
		return Calendar{}, fmt.Errorf("workcal: pinned calendar is %T, want an object", v)
	}
	days, err := intList(m["weekdays"])
	if err != nil {
		return Calendar{}, fmt.Errorf("workcal: pinned weekdays: %w", err)
	}
	holidays, err := stringList(m["holidays"])
	if err != nil {
		return Calendar{}, fmt.Errorf("workcal: pinned holidays: %w", err)
	}
	tz, _ := m["timezone"].(string)
	return New(days, tz, holidays)
}

func intList(v any) ([]int, error) {
	switch l := v.(type) {
	case nil:
		return nil, nil
	case []int:
		return append([]int(nil), l...), nil
	case []any:
		out := make([]int, 0, len(l))
		for _, e := range l {
			switch n := e.(type) {
			case int:
				out = append(out, n)
			case int64:
				out = append(out, int(n))
			case float64:
				if n != float64(int(n)) {
					return nil, fmt.Errorf("%v is not a whole number", n)
				}
				out = append(out, int(n))
			default:
				return nil, fmt.Errorf("%v (%T) is not a number", e, e)
			}
		}
		return out, nil
	default:
		return nil, fmt.Errorf("%T is not a list", v)
	}
}

func stringList(v any) ([]string, error) {
	switch l := v.(type) {
	case nil:
		return nil, nil
	case []string:
		return append([]string(nil), l...), nil
	case []any:
		out := make([]string, 0, len(l))
		for _, e := range l {
			s, ok := e.(string)
			if !ok {
				return nil, fmt.Errorf("%v (%T) is not a string", e, e)
			}
			out = append(out, s)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("%T is not a list", v)
	}
}
