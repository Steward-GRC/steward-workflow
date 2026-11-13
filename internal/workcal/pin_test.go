// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package workcal

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestPinRoundTrip(t *testing.T) {
	cal, err := New([]int{1, 2, 3, 4, 7}, "America/New_York", []string{"2026-12-25", "2027-01-01"})
	if err != nil {
		t.Fatal(err)
	}
	pin := cal.Pin()

	// Straight from memory, and after the JSON round trip the saga store does
	// (numbers become float64, lists become []any).
	raw, err := json.Marshal(map[string]any{PinKey: pin})
	if err != nil {
		t.Fatal(err)
	}
	var stored map[string]any
	if err := json.Unmarshal(raw, &stored); err != nil {
		t.Fatal(err)
	}
	for name, v := range map[string]any{"memory": pin, "json": stored[PinKey]} {
		t.Run(name, func(t *testing.T) {
			got, err := FromPin(v)
			if err != nil {
				t.Fatalf("FromPin: %v", err)
			}
			from := time.Date(2026, 12, 24, 12, 0, 0, 0, time.UTC)
			want, _ := cal.AddWorkingDays(from, 2)
			have, _ := got.AddWorkingDays(from, 2)
			if !want.Equal(have) {
				t.Fatalf("round trip changed the deadline: %s vs %s", have, want)
			}
		})
	}
}

func TestFromPinAbsentAndBad(t *testing.T) {
	if _, err := FromPin(nil); !errors.Is(err, ErrNoPin) {
		t.Fatalf("nil: err = %v, want ErrNoPin", err)
	}
	bad := []any{
		"nope",
		map[string]any{"weekdays": []any{}, "timezone": "UTC"},
		map[string]any{"weekdays": []any{float64(9)}, "timezone": "UTC"},
		map[string]any{"weekdays": []any{float64(1)}, "timezone": "Not/AZone"},
		map[string]any{"weekdays": []any{float64(1)}, "timezone": "UTC", "holidays": []any{"2026-02-30"}},
		map[string]any{"weekdays": []any{"mon"}, "timezone": "UTC"},
	}
	for i, v := range bad {
		if _, err := FromPin(v); err == nil || errors.Is(err, ErrNoPin) {
			t.Fatalf("case %d: err = %v, want a validation error", i, err)
		}
	}
}
