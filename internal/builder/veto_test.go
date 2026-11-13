// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package builder

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// The save-time check on a stage's veto people (steward-workflow#64). Mock:
// checkVetoPeople in src/product/resolvers/library.ts.

func TestStage_ValidateVetoUsers(t *testing.T) {
	base := Stage{
		Name: "Finance", Quorum: QuorumAny, ApproverIDs: []string{"a", "b"},
		ApproversByCategory: map[string][]string{"cat": {"c"}},
		GroupUnits:          []GroupUnit{{GroupID: "g", InternalQuorum: QuorumAny, Members: []string{"m"}}},
	}
	for name, tc := range map[string]struct {
		veto []string
		ok   bool
	}{
		"none":                    {nil, true},
		"an approver":             {[]string{"a"}, true},
		"a per-category approver": {[]string{"c"}, true},
		"a group unit member":     {[]string{"m"}, true},
		"all three":               {[]string{"a", "c", "m"}, true},
		"a stranger":              {[]string{"a", "zed"}, false},
		"only strangers":          {[]string{"x", "y"}, false},
	} {
		t.Run(name, func(t *testing.T) {
			s := base
			s.VetoUserIDs = tc.veto
			err := s.ValidateVetoUsers()
			if tc.ok {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), "veto")
			require.Contains(t, err.Error(), "approver")
			require.Contains(t, err.Error(), `"Finance"`)
		})
	}
}

// A stage stored before the change has no veto_user_ids and still loads, the
// old keys ignored; one with the new key round-trips.
func TestStage_OldStoredJSONStillLoads(t *testing.T) {
	var old Stage
	require.NoError(t, json.Unmarshal([]byte(
		`{"name":"s","approver_ids":["a"],"quorum":"any","veto_approver_ids":["a"],"veto_group_ids":["g"]}`), &old))
	require.Empty(t, old.VetoUserIDs)

	b, err := json.Marshal(Stage{Name: "s", Quorum: QuorumAny, ApproverIDs: []string{"a"}, VetoUserIDs: []string{"a"}})
	require.NoError(t, err)
	require.Contains(t, string(b), `"veto_user_ids":["a"]`)
	b, err = json.Marshal(Stage{Name: "s", Quorum: QuorumAny, ApproverIDs: []string{"a"}})
	require.NoError(t, err)
	require.NotContains(t, string(b), "veto_user_ids")
}
