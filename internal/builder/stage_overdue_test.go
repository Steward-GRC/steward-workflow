// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package builder

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The overdue settings (steward-workflow#72): reminders and escalation are whole
// working days counted past the due date, so they need a due date and cannot
// sit with a stage that rejects at its due date.

func TestStage_ValidateOverdue(t *testing.T) {
	cases := []struct {
		name    string
		stage   Stage
		wantErr string // substring; empty means valid
	}{
		{"nothing set", Stage{Name: "S"}, ""},
		{"sla only", Stage{Name: "S", SLADays: 3}, ""},
		{"reject on breach alone", Stage{Name: "S", SLADays: 3, RejectOnSLABreach: true}, ""},
		{"reminder with a due date", Stage{Name: "S", SLADays: 3, RemindEveryDays: 1}, ""},
		{"escalation with a due date", Stage{Name: "S", SLADays: 3, EscalateAfterDays: 2}, ""},
		{"both at the limit", Stage{Name: "S", SLADays: 3, RemindEveryDays: 365, EscalateAfterDays: 365}, ""},
		{"negative reminder", Stage{Name: "S", SLADays: 3, RemindEveryDays: -1}, "remind_every_days"},
		{"negative escalation", Stage{Name: "S", SLADays: 3, EscalateAfterDays: -2}, "escalate_after_days"},
		{"reminder above the limit", Stage{Name: "S", SLADays: 3, RemindEveryDays: 366}, "remind_every_days"},
		{"escalation above the limit", Stage{Name: "S", SLADays: 3, EscalateAfterDays: 366}, "escalate_after_days"},
		{"reminder without a due date", Stage{Name: "S", RemindEveryDays: 1}, "due date"},
		{"escalation without a due date", Stage{Name: "S", EscalateAfterDays: 1}, "due date"},
		{"reminder with reject on breach", Stage{Name: "S", SLADays: 3, RejectOnSLABreach: true, RemindEveryDays: 1}, "rejects"},
		{"escalation with reject on breach", Stage{Name: "S", SLADays: 3, RejectOnSLABreach: true, EscalateAfterDays: 1}, "rejects"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.stage.ValidateOverdue()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.True(t, strings.Contains(err.Error(), tc.wantErr), "got %q, want it to mention %q", err.Error(), tc.wantErr)
			require.Contains(t, err.Error(), `"S"`, "the message names the stage")
		})
	}
}

func TestStage_OverdueFieldsRoundTripJSON(t *testing.T) {
	in := WorkflowDef{Name: "W", Stages: []Stage{{Name: "S", Quorum: QuorumAny, SLADays: 3, RemindEveryDays: 1, EscalateAfterDays: 2}}}
	raw, err := marshalStagesForTest(in.Stages)
	require.NoError(t, err)
	require.Contains(t, string(raw), `"remind_every_days":1`)
	require.Contains(t, string(raw), `"escalate_after_days":2`)

	off, err := marshalStagesForTest([]Stage{{Name: "S", Quorum: QuorumAny, SLADays: 3}})
	require.NoError(t, err)
	require.NotContains(t, string(off), "remind_every_days", "off is omitted, so stored stages stay as they were")
	require.NotContains(t, string(off), "escalate_after_days")
}

func marshalStagesForTest(stages []Stage) ([]byte, error) {
	return json.Marshal(stages)
}
