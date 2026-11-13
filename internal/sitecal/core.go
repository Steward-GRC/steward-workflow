// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package sitecal reads the site work calendar from core.
package sitecal

import (
	"context"
	"errors"
	"fmt"

	corev1 "github.com/Steward-GRC/steward-workflow/gen/go/thirdparty/core/v1"
	"github.com/Steward-GRC/steward-workflow/internal/workcal"
)

// ErrNoCalendar means core answered without a work calendar (a core that
// predates the field).
var ErrNoCalendar = errors.New("sitecal: core returned no work calendar")

// CoreSource reads the calendar through core's SettingsService. Core always
// answers with the effective calendar (Monday to Friday, UTC when none was
// saved), so a missing one means core is older than the field.
type CoreSource struct {
	client corev1.SettingsServiceClient
}

// NewCoreSource returns a CoreSource on client.
func NewCoreSource(client corev1.SettingsServiceClient) *CoreSource {
	return &CoreSource{client: client}
}

// WorkCalendar fetches and validates the site calendar. A failed call keeps
// its gRPC status, so the caller can tell an outage from a refusal; a calendar
// that does not validate is a plain error.
func (s *CoreSource) WorkCalendar(ctx context.Context) (workcal.Calendar, error) {
	resp, err := s.client.GetGlobalSettings(ctx, &corev1.GetGlobalSettingsRequest{})
	if err != nil {
		return workcal.Calendar{}, err
	}
	wc := resp.GetSettings().GetWorkCalendar()
	if wc == nil {
		return workcal.Calendar{}, ErrNoCalendar
	}
	days := make([]int, len(wc.GetWeekdays()))
	for i, d := range wc.GetWeekdays() {
		days[i] = int(d)
	}
	holidays := make([]string, len(wc.GetHolidays()))
	for i, h := range wc.GetHolidays() {
		holidays[i] = h.GetDate()
	}
	cal, err := workcal.New(days, wc.GetTimezone(), holidays)
	if err != nil {
		return workcal.Calendar{}, fmt.Errorf("sitecal: core's work calendar is not usable: %w", err)
	}
	return cal, nil
}
