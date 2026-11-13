// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package sitecal

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	corev1 "github.com/Steward-GRC/steward-workflow/gen/go/thirdparty/core/v1"
)

type fakeSettings struct {
	corev1.SettingsServiceClient
	resp *corev1.GetGlobalSettingsResponse
	err  error
}

func (f fakeSettings) GetGlobalSettings(context.Context, *corev1.GetGlobalSettingsRequest, ...grpc.CallOption) (*corev1.GetGlobalSettingsResponse, error) {
	return f.resp, f.err
}

func resp(c *corev1.WorkCalendar) *corev1.GetGlobalSettingsResponse {
	return &corev1.GetGlobalSettingsResponse{Settings: &corev1.GlobalSettings{WorkCalendar: c}}
}

func TestCoreSource_ReadsTheCalendar(t *testing.T) {
	src := NewCoreSource(fakeSettings{resp: resp(&corev1.WorkCalendar{
		Weekdays: []int32{1, 2, 3, 4, 7}, Timezone: "Europe/London",
		Holidays: []*corev1.Holiday{{Date: "2026-12-25", Label: "Christmas"}, {Date: "2026-12-28"}},
	})})
	cal, err := src.WorkCalendar(context.Background())
	require.NoError(t, err)
	from := time.Date(2026, 12, 24, 12, 0, 0, 0, time.UTC)
	got, err := cal.AddWorkingDays(from, 1)
	require.NoError(t, err)
	// Thu 24 Dec entry; Fri 25 is a holiday, Sat off, Sun 27 is working day 1.
	require.True(t, got.Equal(time.Date(2026, 12, 28, 0, 0, 0, 0, time.UTC)), "got %s", got)
}

func TestCoreSource_Errors(t *testing.T) {
	boom := status.Error(codes.Unavailable, "down")
	_, err := NewCoreSource(fakeSettings{err: boom}).WorkCalendar(context.Background())
	require.Equal(t, codes.Unavailable, status.Code(err), "the gRPC code must survive so the caller can tell an outage")

	for name, r := range map[string]*corev1.GetGlobalSettingsResponse{
		"no settings":      {},
		"no calendar":      resp(nil),
		"no weekdays":      resp(&corev1.WorkCalendar{Timezone: "UTC"}),
		"bad weekday":      resp(&corev1.WorkCalendar{Weekdays: []int32{8}, Timezone: "UTC"}),
		"bad timezone":     resp(&corev1.WorkCalendar{Weekdays: []int32{1}, Timezone: "Mars/Base"}),
		"empty timezone":   resp(&corev1.WorkCalendar{Weekdays: []int32{1}}),
		"bad holiday date": resp(&corev1.WorkCalendar{Weekdays: []int32{1}, Timezone: "UTC", Holidays: []*corev1.Holiday{{Date: "tomorrow"}}}),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewCoreSource(fakeSettings{resp: r}).WorkCalendar(context.Background())
			require.Error(t, err)
			require.Equal(t, codes.Unknown, status.Code(err), "an unusable calendar is not an outage: %v", err)
		})
	}
}
