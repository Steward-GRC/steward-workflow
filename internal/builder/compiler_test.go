// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package builder

import (
	"testing"

	"github.com/Bugs5382/go-saga-orchestration/domain"
)

func TestCompile_SingleStageAny(t *testing.T) {
	wd := WorkflowDef{
		ID:      "wf-1",
		Name:    "Simple",
		Version: 1,
		Stages: []Stage{
			{Name: "Managers", Quorum: QuorumAny, ApproverIDs: []string{"mgr"}},
		},
	}
	def, err := Compile(wd, CompileOptions{StageSLAHours: 72, StageReminderHours: 48})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if def.ID != "wf-1" {
		t.Fatalf("ID: got %q", def.ID)
	}
	if !def.Published {
		t.Fatal("expected Published=true")
	}
	// Topology (engine-verified — see saga_harness_test.go):
	//   manual_approval (due_in SLA, timeout→escalate) → policy.resolve_decision
	//     → outcome_switch (expr "decision") → set_status_* → end
	stepTypes := map[string]int{}
	stepActions := map[string]int{}
	byID := map[string]domain.Step{}
	for _, s := range def.Steps {
		stepTypes[string(s.Type)]++
		byID[s.ID] = s
		if s.Type == "action" {
			stepActions[s.Action]++
		}
	}
	if stepTypes["manual_approval"] == 0 {
		t.Fatal("expected at least one manual_approval step")
	}
	// The human task carries the engine-native SLA (due_in) and a timeout→escalate branch.
	task := byID["s0_task_0"]
	if _, ok := task.Inputs["due_in"].(string); !ok {
		t.Fatal("manual_approval s0_task_0 must carry inputs[\"due_in\"] (engine-native SLA)")
	}
	if task.Branches["timeout"].Next != "s0_escalate" {
		t.Fatalf("manual_approval timeout branch must escalate, got %+v", task.Branches)
	}
	// The decision must be resolved into a run variable by a worker action, because the
	// engine drops the human's manual_approval submission. The switch then branches on it.
	if stepActions["policy.resolve_decision"] == 0 {
		t.Fatal("expected a policy.resolve_decision action that populates the \"decision\" variable")
	}
	sw := byID["s0_outcome_switch"]
	if expr, _ := sw.Inputs["expr"].(string); expr != "decision" {
		t.Fatalf("outcome_switch must branch on the \"decision\" variable, got expr=%q", expr)
	}
	if sw.Branches["approve"].Next == "" || sw.Branches["reject"].Next == "" {
		t.Fatalf("outcome_switch approve/reject branches must be wired, got %+v", sw.Branches)
	}
	if stepActions["policy.set_status"] == 0 {
		t.Fatal("expected at least one action step with Action=\"policy.set_status\"")
	}
	if stepTypes[string(domain.StepTypeEnd)] == 0 {
		t.Fatal("expected an end step")
	}
}

func TestCompile_MultiStageAll(t *testing.T) {
	wd := WorkflowDef{
		ID:      "wf-2",
		Name:    "Two-Stage All",
		Version: 1,
		Stages: []Stage{
			{Name: "S1", Quorum: QuorumAll, ApproverIDs: []string{"g1", "g2"}},
			{Name: "S2", Quorum: QuorumAny, ApproverIDs: []string{"g3"}},
		},
	}
	def, err := Compile(wd, CompileOptions{StageSLAHours: 72, StageReminderHours: 48})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	// Quorum is per-USER over the unified pool, not per group, so each stage emits a
	// single manual_approval gate (the union of its groups) — NOT a parallel fan-out.
	// Two stages → two manual_approval steps, zero parallel steps.
	stepTypes := map[string]int{}
	byID := map[string]domain.Step{}
	for _, s := range def.Steps {
		stepTypes[string(s.Type)]++
		byID[s.ID] = s
	}
	if stepTypes["parallel"] != 0 {
		t.Fatalf("expected no parallel steps (quorum is per-user, not per-group), got %d", stepTypes["parallel"])
	}
	if stepTypes["manual_approval"] != 2 {
		t.Fatalf("expected one manual_approval per stage (2), got %d", stepTypes["manual_approval"])
	}
	// The gate's assignee is just a stage-marker placeholder — the approval model
	// is per-user (assignment rows), not per-group, so no group identifiers leak
	// into the compiled saga.
	s1 := byID["s0_task_0"]
	if a, _ := s1.Inputs["assignee"].(string); a != "stage:0" {
		t.Fatalf("S1 assignee: want stage marker \"stage:0\", got %q", a)
	}
	if _, leaked := s1.Inputs["group_ids"]; leaked {
		t.Fatal("compiled saga must not carry group_ids (approval model is per-user)")
	}
}

func TestCompile_ConditionalStage(t *testing.T) {
	wd := WorkflowDef{
		ID:      "wf-3",
		Name:    "Conditional",
		Version: 1,
		Stages: []Stage{
			{Name: "Always", Quorum: QuorumAny, ApproverIDs: []string{"g1"}},
			{Name: "HighOnly", Quorum: QuorumAny, ApproverIDs: []string{"g2"}, Condition: "sensitivity == 'HIGH'"},
		},
	}
	def, err := Compile(wd, CompileOptions{StageSLAHours: 72, StageReminderHours: 48})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	stepTypes := map[string]int{}
	for _, s := range def.Steps {
		stepTypes[string(s.Type)]++
	}
	// Conditional stage must use a "switch" step with CEL expr in inputs["expr"], NOT "condition".
	if stepTypes["switch"] == 0 {
		t.Fatal("expected a switch step for the conditional stage (CEL expr in inputs[\"expr\"])")
	}
	// Verify the switch step carries the expr input.
	for _, s := range def.Steps {
		if s.Type == "switch" {
			expr, _ := s.Inputs["expr"].(string)
			if expr == "" {
				t.Fatal("switch step missing inputs[\"expr\"]")
			}
			break
		}
	}
}

func TestCompile_RejectPath(t *testing.T) {
	wd := WorkflowDef{
		ID: "wf-4", Name: "Reject Path", Version: 1,
		Stages: []Stage{
			{Name: "Mgr", Quorum: QuorumAny, ApproverIDs: []string{"mgr"}},
		},
	}
	def, err := Compile(wd, CompileOptions{StageSLAHours: 72, StageReminderHours: 48})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	// The compiled definition must contain a set_status_rejected action step and a terminal end step
	// reachable from the reject branch.
	foundRejectAction := false
	foundEnd := false
	for _, s := range def.Steps {
		if s.Type == "action" && s.Action == "policy.set_status" {
			status, _ := s.Inputs["status"].(string)
			if status == "rejected" {
				foundRejectAction = true
			}
		}
		if s.Type == domain.StepTypeEnd {
			foundEnd = true
		}
	}
	if !foundRejectAction {
		t.Fatal("expected a set_status_rejected action step (Action=\"policy.set_status\", Inputs[\"status\"]=\"rejected\")")
	}
	if !foundEnd {
		t.Fatal("expected a domain.StepTypeEnd step reachable from reject branch")
	}
}

func TestCompile_InvalidDef(t *testing.T) {
	wd := WorkflowDef{Name: "bad"} // no stages
	if _, err := Compile(wd, CompileOptions{}); err == nil {
		t.Fatal("expected error for invalid WorkflowDef")
	}
}

func TestCompile_PerStageSLA(t *testing.T) {
	wd := WorkflowDef{ID: "wf", Name: "n", Version: 1, Stages: []Stage{
		// SLADays:5 → 120h, which is NOT the global default (72h), so this test
		// genuinely proves the per-stage path (not the global fallback).
		{Name: "S0", ApproverIDs: []string{"u1"}, Quorum: QuorumAny, SLADays: 5},
	}}
	def, err := Compile(wd, CompileOptions{})
	if err != nil {
		t.Fatal(err)
	}
	byID := map[string]domain.Step{}
	for _, s := range def.Steps {
		byID[s.ID] = s
	}
	if byID["s0_task_0"].Inputs["due_in"] != "120h" {
		t.Fatalf("due_in: got %v want 120h", byID["s0_task_0"].Inputs["due_in"])
	}
}

func TestCompile_RejectOnSLABreach(t *testing.T) {
	wd := WorkflowDef{ID: "wf", Name: "n", Version: 1, Stages: []Stage{
		{Name: "S0", ApproverIDs: []string{"u1"}, Quorum: QuorumAny, SLADays: 1, RejectOnSLABreach: true},
	}}
	def, _ := Compile(wd, CompileOptions{})
	byID := map[string]domain.Step{}
	for _, s := range def.Steps {
		byID[s.ID] = s
	}
	// timeout must NOT route to escalate; it must route to the reject action step.
	if byID["s0_task_0"].Branches["timeout"].Next == "s0_escalate" {
		t.Fatal("reject_on_sla_breach should bypass escalate")
	}
}
