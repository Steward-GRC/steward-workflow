// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package builder

import "testing"

func TestWorkflowDefValidate_OK(t *testing.T) {
	wd := WorkflowDef{
		Name: "HR Approval",
		Stages: []Stage{
			{
				Name:        "Line Manager",
				Quorum:      QuorumAny,
				ApproverIDs: []string{"u1"},
			},
			{
				Name:        "HR Director",
				Quorum:      QuorumAll,
				ApproverIDs: []string{"u2", "u3"},
				Condition:   "policy.sensitivity == 'HIGH'",
			},
		},
	}
	if err := wd.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWorkflowDefValidate_OK_Majority(t *testing.T) {
	wd := WorkflowDef{
		Name: "Majority",
		Stages: []Stage{{
			Name: "s", Quorum: QuorumMajority,
			ApproverIDs: []string{"u1"},
		}},
	}
	if err := wd.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWorkflowDefValidate_OK_NofM(t *testing.T) {
	wd := WorkflowDef{
		Name: "NofM",
		Stages: []Stage{{
			Name: "s", Quorum: QuorumNofM, QuorumN: 2,
			ApproverIDs: []string{"u1", "u2", "u3"},
		}},
	}
	if err := wd.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWorkflowDefValidate_NofMMissingN(t *testing.T) {
	wd := WorkflowDef{
		Name: "NofM",
		Stages: []Stage{{
			Name: "s", Quorum: QuorumNofM,
			ApproverIDs: []string{"u1"},
		}},
	}
	if err := wd.Validate(); err == nil {
		t.Fatal("expected error for nofm with quorum_n=0")
	}
}

func TestWorkflowDefValidate_QuorumNOnNonNofM(t *testing.T) {
	wd := WorkflowDef{
		Name: "Bad",
		Stages: []Stage{{
			Name: "s", Quorum: QuorumAny, QuorumN: 2,
			ApproverIDs: []string{"u1"},
		}},
	}
	if err := wd.Validate(); err == nil {
		t.Fatal("expected error for quorum_n on non-nofm stage")
	}
}

func TestWorkflowDefValidate_NoName(t *testing.T) {
	wd := WorkflowDef{Stages: []Stage{{Name: "s", Quorum: QuorumAny, ApproverIDs: []string{"u1"}}}}
	if err := wd.Validate(); err == nil {
		t.Fatal("expected error for missing Name")
	}
}

func TestWorkflowDefValidate_NoStages(t *testing.T) {
	wd := WorkflowDef{Name: "x"}
	if err := wd.Validate(); err == nil {
		t.Fatal("expected error for no stages")
	}
}

func TestWorkflowDefValidate_StageNoApproverIDs_AllowedAsDraft(t *testing.T) {
	// an empty-approver stage is a DRAFT — valid to save/compile.
	// The "must be staffed" rule is enforced at run time (Submit), not here.
	wd := WorkflowDef{Name: "x", Stages: []Stage{{Name: "s", Quorum: QuorumAny}}}
	if err := wd.Validate(); err != nil {
		t.Fatalf("empty-approver stage should be a valid draft, got: %v", err)
	}
}

func TestWorkflowDefValidate_InvalidQuorum(t *testing.T) {
	wd := WorkflowDef{Name: "x", Stages: []Stage{{Name: "s", Quorum: "bad", ApproverIDs: []string{"u1"}}}}
	if err := wd.Validate(); err == nil {
		t.Fatal("expected error for invalid quorum")
	}
}

func TestEvaluateQuorum_All(t *testing.T) {
	cases := []struct {
		approved, pool int
		want           bool
	}{
		{0, 0, false}, // empty pool never ok
		{0, 3, false},
		{2, 3, false},
		{3, 3, true},
	}
	for _, c := range cases {
		got := EvaluateQuorum(QuorumAll, 0, c.approved, c.pool)
		if got != c.want {
			t.Errorf("All approved=%d pool=%d got %v want %v", c.approved, c.pool, got, c.want)
		}
	}
}

func TestEvaluateQuorum_Any(t *testing.T) {
	if EvaluateQuorum(QuorumAny, 0, 0, 5) {
		t.Error("Any with 0 approved should be false")
	}
	if !EvaluateQuorum(QuorumAny, 0, 1, 5) {
		t.Error("Any with 1 approved should be true")
	}
	if !EvaluateQuorum(QuorumAny, 0, 9, 5) {
		t.Error("Any with many approved should be true")
	}
}

func TestEvaluateQuorum_Majority(t *testing.T) {
	cases := []struct {
		approved, pool int
		want           bool
	}{
		{0, 1, false},
		{1, 1, true}, // majority of 1 = 1
		{1, 2, false},
		{2, 2, true},
		{1, 3, false},
		{2, 3, true},  // majority of 3 = 2 (>50%)
		{3, 5, true},  // 3/5
		{2, 5, false}, // 2/5 not majority
		{4, 6, true},  // 4/6 majority (rounds up: need >3)
		{3, 6, false}, // 3/6 is exactly half, not majority
		{0, 0, false}, // empty pool false
	}
	for _, c := range cases {
		got := EvaluateQuorum(QuorumMajority, 0, c.approved, c.pool)
		if got != c.want {
			t.Errorf("Majority approved=%d pool=%d got %v want %v", c.approved, c.pool, got, c.want)
		}
	}
}

func TestEvaluateQuorum_NofM(t *testing.T) {
	cases := []struct {
		n, approved, pool int
		want              bool
	}{
		{2, 1, 5, false},
		{2, 2, 5, true},
		{2, 3, 5, true},
		{5, 5, 5, true}, // n = pool
		{1, 1, 1, true},
		{3, 2, 10, false},
	}
	for _, c := range cases {
		got := EvaluateQuorum(QuorumNofM, c.n, c.approved, c.pool)
		if got != c.want {
			t.Errorf("NofM n=%d approved=%d pool=%d got %v want %v", c.n, c.approved, c.pool, got, c.want)
		}
	}
}

func TestEvaluateQuorum_Unknown(t *testing.T) {
	if EvaluateQuorum("bogus", 0, 5, 5) {
		t.Error("unknown quorum should not satisfy")
	}
}

func TestStage_Validate_UserBased(t *testing.T) {
	s := Stage{Name: "Review", ApproverIDs: []string{"u1"}, Quorum: QuorumAny}
	if err := s.Validate(); err != nil {
		t.Fatalf("valid user-based stage rejected: %v", err)
	}
	// an empty-approver stage is a valid DRAFT at the builder level;
	// the staffing requirement is enforced at run time (Submit), not in Validate.
	empty := Stage{Name: "Review", ApproverIDs: nil, Quorum: QuorumAny}
	if err := empty.Validate(); err != nil {
		t.Fatalf("empty-approver stage should be a valid draft, got: %v", err)
	}
}
