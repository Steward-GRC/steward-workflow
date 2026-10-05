// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package builder_test

// End-to-end scenario matrix: compiles policy workflows with the real
// builder.Compile and drives them through the go-saga engine in-process. Covers
// single and multi stage, any/all/quorum, the request_changes loopback, and the
// SLA timeout to escalate.
//
//	go test ./internal/builder/ -run 'TestCompiledSaga|TestScenario' -v

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine"
	"github.com/Bugs5382/go-saga-orchestration/engine/verbs"
	"github.com/Bugs5382/go-saga-orchestration/saga"
	"github.com/Bugs5382/go-saga-orchestration/store"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
	"github.com/google/uuid"

	"github.com/Steward-GRC/steward-workflow/internal/builder"
)

type actionCall struct {
	Action string
	Status string
	StepID string
}

// policyWorker plays the policy worker against the embedded engine. The stage
// decision is supplied by decisionFor(stageIndex) so multi-stage flows can route
// differently per stage; set_status / escalate echo back.
type policyWorker struct {
	coord       *engine.Coordinator
	store       *memory.Store
	bg          context.Context
	wg          *sync.WaitGroup
	mu          *sync.Mutex
	log         *[]actionCall
	decisionFor func(stageIdx int) string
}

func (p *policyWorker) PublishSagaAdvance(_ context.Context, runID string) error {
	p.wg.Go(func() { ; _ = p.coord.Advance(p.bg, runID) })
	return nil
}

func (p *policyWorker) PublishActionDispatch(_ context.Context, _ string, body []byte) error {
	var payload verbs.ActionPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		return err
	}
	stageIdx := 0
	if f, ok := payload.Inputs["stage_index"].(float64); ok {
		stageIdx = int(f)
	}
	var result map[string]any
	recorded := ""
	switch payload.Action {
	case "policy.resolve_decision":
		d := p.decisionFor(stageIdx)
		result = map[string]any{"decision": d}
		recorded = d
	case "policy.set_status":
		status, _ := payload.Inputs["status"].(string)
		result = map[string]any{"ok": true, "status": status}
		recorded = status
	default: // policy.escalate_stage
		result = map[string]any{"ok": true}
		recorded = "escalated"
	}
	p.mu.Lock()
	*p.log = append(*p.log, actionCall{Action: payload.Action, Status: recorded, StepID: payload.StepID})
	p.mu.Unlock()

	runID, err := uuid.Parse(payload.RunID)
	if err != nil {
		return err
	}
	p.wg.Go(func() {
		if err := p.store.CompleteAction(p.bg, runID, payload.Attempt, result); err != nil {
			return
		}
		_ = p.coord.Advance(p.bg, runID.String())
	})
	return nil
}

// harness wires an embedded engine + policy worker around a compiled def.
type harness struct {
	s     *saga.Saga
	store *memory.Store
	clk   *clock.FakeClock
	calls *[]actionCall
	wg    *sync.WaitGroup
}

func newScenarioHarness(t *testing.T, decisionFor func(int) string) *harness {
	t.Helper()
	calls := &[]actionCall{}
	wg := &sync.WaitGroup{}
	bg := context.Background()
	st := memory.New()
	// Start the virtual clock well ahead of real wall-clock time. The memory
	// store stamps action-completion WakeupAt with real time.Now(); the engine
	// compares that against this (engine) clock, so the clock must be ahead for
	// completed actions to be considered "due" and wake the run.
	clk := clock.NewFakeClock(time.Date(2035, 1, 1, 12, 0, 0, 0, time.UTC))
	pub := &policyWorker{store: st, bg: bg, wg: wg, mu: &sync.Mutex{}, log: calls, decisionFor: decisionFor}
	s, err := saga.New(saga.Options{Store: st, Publisher: pub, Clock: clk})
	if err != nil {
		t.Fatalf("saga.New: %v", err)
	}
	pub.coord = s.Coordinator()
	return &harness{s: s, store: st, clk: clk, calls: calls, wg: wg}
}

// run drives a compiled def to a terminal state, submitting every user task that
// appears (each submission just unpauses the manual_approval; the actual decision
// is provided by the worker's resolve_decision). Returns the final run.
func (h *harness) run(t *testing.T, def domain.WorkflowDefinition, inputs map[string]any) domain.SagaRun {
	t.Helper()
	runID, err := h.s.Start(context.Background(), def.ID, inputs)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	submitted := map[string]bool{}
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		run, err := h.s.Get(context.Background(), runID)
		if err == nil && (run.State == domain.RunStateSucceeded || run.State == domain.RunStateFailed) {
			h.wg.Wait()
			run, _ = h.s.Get(context.Background(), runID)
			return run
		}
		// Sweep the parent AND any child runs (parallel stages spawn sub-sagas,
		// each with its own user task that must be submitted for the join).
		allRuns, _ := h.store.ListRuns(context.Background(), store.RunFilter{Limit: 500})
		acted := false
		for _, r := range allRuns {
			tasks, _ := h.store.ListUserTasksByRun(context.Background(), r.ID)
			for _, task := range tasks {
				if submitted[task.ID.String()] {
					continue
				}
				submitted[task.ID.String()] = true
				_ = h.store.SubmitUserTask(context.Background(), task.ID, "u-x", map[string]any{"ack": true})
				_ = h.s.Signal(context.Background(), r.ID, "user_task."+task.ID.String()+".submitted", nil)
				acted = true
			}
		}
		if !acted {
			time.Sleep(5 * time.Millisecond)
		}
	}
	run, _ := h.s.Get(context.Background(), runID)
	return run
}

func (h *harness) setStatuses() []string {
	out := []string{}
	h.wg.Wait()
	for _, c := range *h.calls {
		if c.Action == "policy.set_status" {
			out = append(out, c.Status)
		}
	}
	return out
}

func (h *harness) actionNames() []string {
	out := []string{}
	for _, c := range *h.calls {
		out = append(out, c.Action+"="+c.Status)
	}
	return out
}

func compileFor(t *testing.T, stages ...builder.Stage) domain.WorkflowDefinition {
	t.Helper()
	wd := builder.WorkflowDef{ID: "wf", Name: "wf", Version: 1, Published: true, Stages: stages}
	def, err := builder.Compile(wd, builder.CompileOptions{StageSLAHours: 72, StageReminderHours: 48})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return def
}

func ids(approvers ...string) []string {
	return approvers
}

// ---- Scenario matrix ----

func TestScenario_SingleStage(t *testing.T) {
	cases := []struct {
		name         string
		decision     string
		wantState    domain.RunState
		wantStatuses []string
	}{
		{"approve", "approve", domain.RunStateSucceeded, []string{"approved", "published"}},
		{"reject", "reject", domain.RunStateSucceeded, []string{"rejected"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newScenarioHarness(t, func(int) string { return tc.decision })
			def := compileFor(t, builder.Stage{Name: "S0", Quorum: builder.QuorumAny, ApproverIDs: ids("g0")})
			if err := h.s.Register(def); err != nil {
				t.Fatalf("register: %v", err)
			}
			run := h.run(t, def, map[string]any{"policy_version_id": "pv"})
			t.Logf("state=%s actions=%v", run.State, h.actionNames())
			if run.State != tc.wantState {
				t.Fatalf("state: got %s want %s", run.State, tc.wantState)
			}
			if got := h.setStatuses(); fmt.Sprint(got) != fmt.Sprint(tc.wantStatuses) {
				t.Fatalf("statuses: got %v want %v", got, tc.wantStatuses)
			}
		})
	}
}

func TestScenario_RequestChangesLoopback(t *testing.T) {
	// First time stage 0 is reached → request_changes (loops back to stage 0);
	// second time → approve. Proves the loopback wiring terminates.
	var visits int
	h := newScenarioHarness(t, func(int) string {
		visits++
		if visits == 1 {
			return "request_changes"
		}
		return "approve"
	})
	def := compileFor(t, builder.Stage{Name: "S0", Quorum: builder.QuorumAny, ApproverIDs: ids("g0")})
	if err := h.s.Register(def); err != nil {
		t.Fatalf("register: %v", err)
	}
	run := h.run(t, def, map[string]any{"policy_version_id": "pv"})
	t.Logf("state=%s visits=%d actions=%v", run.State, visits, h.actionNames())
	if run.State != domain.RunStateSucceeded {
		t.Fatalf("state: got %s want succeeded", run.State)
	}
	if got := h.setStatuses(); fmt.Sprint(got) != fmt.Sprint([]string{"approved", "published"}) {
		t.Fatalf("statuses: got %v", got)
	}
}

func TestScenario_MultiStageSequential(t *testing.T) {
	// 3 sequential single-group stages; approve all → published.
	h := newScenarioHarness(t, func(int) string { return "approve" })
	def := compileFor(t,
		builder.Stage{Name: "S0", Quorum: builder.QuorumAny, ApproverIDs: ids("g0")},
		builder.Stage{Name: "S1", Quorum: builder.QuorumAny, ApproverIDs: ids("g1")},
		builder.Stage{Name: "S2", Quorum: builder.QuorumAny, ApproverIDs: ids("g2")},
	)
	if err := h.s.Register(def); err != nil {
		t.Fatalf("register: %v", err)
	}
	run := h.run(t, def, map[string]any{"policy_version_id": "pv"})
	t.Logf("state=%s actions=%v", run.State, h.actionNames())
	if run.State != domain.RunStateSucceeded {
		t.Fatalf("state: got %s want succeeded", run.State)
	}
	if got := h.setStatuses(); fmt.Sprint(got) != fmt.Sprint([]string{"approved", "published"}) {
		t.Fatalf("statuses: got %v", got)
	}
}

func TestScenario_MultiStageRejectMidway(t *testing.T) {
	// Stage 0 approves, stage 1 rejects → rejected, never published.
	h := newScenarioHarness(t, func(stage int) string {
		if stage == 1 {
			return "reject"
		}
		return "approve"
	})
	def := compileFor(t,
		builder.Stage{Name: "S0", Quorum: builder.QuorumAny, ApproverIDs: ids("g0")},
		builder.Stage{Name: "S1", Quorum: builder.QuorumAny, ApproverIDs: ids("g1")},
	)
	if err := h.s.Register(def); err != nil {
		t.Fatalf("register: %v", err)
	}
	run := h.run(t, def, map[string]any{"policy_version_id": "pv"})
	t.Logf("state=%s actions=%v", run.State, h.actionNames())
	if run.State != domain.RunStateSucceeded {
		t.Fatalf("state: got %s want succeeded", run.State)
	}
	got := h.setStatuses()
	if fmt.Sprint(got) != fmt.Sprint([]string{"rejected"}) {
		t.Fatalf("statuses: got %v want [rejected]", got)
	}
}

func TestScenario_QuorumLoop_PendingThenApprove(t *testing.T) {
	// resolve_decision returns pending for the first two approvers, then approve.
	// Proves the compiler's outcome_switch "pending" branch loops back to the
	// stage task and the run still converges to published.
	var calls int
	h := newScenarioHarness(t, func(int) string {
		calls++
		if calls < 3 {
			return "pending"
		}
		return "approve"
	})
	def := compileFor(t, builder.Stage{Name: "S0", Quorum: builder.QuorumNofM, QuorumN: 3, ApproverIDs: ids("g0", "g1", "g2")})
	if err := h.s.Register(def); err != nil {
		t.Fatalf("register: %v", err)
	}
	run := h.run(t, def, map[string]any{"policy_version_id": "pv"})
	t.Logf("state=%s resolve_calls=%d actions=%v", run.State, calls, h.actionNames())
	if run.State != domain.RunStateSucceeded {
		t.Fatalf("state: got %s want succeeded", run.State)
	}
	if calls < 3 {
		t.Fatalf("expected the pending branch to loop (>=3 resolve calls), got %d", calls)
	}
	if got := h.setStatuses(); fmt.Sprint(got) != fmt.Sprint([]string{"approved", "published"}) {
		t.Fatalf("statuses: got %v", got)
	}
}

func TestScenario_QuorumAll_MultiGroup(t *testing.T) {
	h := newScenarioHarness(t, func(int) string { return "approve" })
	def := compileFor(t, builder.Stage{Name: "S0", Quorum: builder.QuorumAll, ApproverIDs: ids("g0", "g1")})
	if err := h.s.Register(def); err != nil {
		t.Fatalf("register: %v", err)
	}
	run := h.run(t, def, map[string]any{"policy_version_id": "pv"})
	t.Logf("state=%s actions=%v", run.State, h.actionNames())
	if run.State != domain.RunStateSucceeded {
		t.Fatalf("state: got %s want succeeded (multi-group quorum=all)", run.State)
	}
	if got := h.setStatuses(); fmt.Sprint(got) != fmt.Sprint([]string{"approved", "published"}) {
		t.Fatalf("statuses: got %v", got)
	}
}

func TestScenario_QuorumAny_MultiGroup(t *testing.T) {
	h := newScenarioHarness(t, func(int) string { return "approve" })
	def := compileFor(t, builder.Stage{Name: "S0", Quorum: builder.QuorumAny, ApproverIDs: ids("g0", "g1")})
	if err := h.s.Register(def); err != nil {
		t.Fatalf("register: %v", err)
	}
	run := h.run(t, def, map[string]any{"policy_version_id": "pv"})
	t.Logf("state=%s actions=%v", run.State, h.actionNames())
	if run.State != domain.RunStateSucceeded {
		t.Fatalf("state: got %s want succeeded (multi-group quorum=any)", run.State)
	}
}

func TestScenario_SLATimeoutEscalates(t *testing.T) {
	// Never submit the user task; advance the clock past the SLA so the
	// manual_approval times out → escalate → resolve_decision(approve) → published.
	h := newScenarioHarness(t, func(int) string { return "approve" })
	def := compileFor(t, builder.Stage{Name: "S0", Quorum: builder.QuorumAny, ApproverIDs: ids("g0")})
	if err := h.s.Register(def); err != nil {
		t.Fatalf("register: %v", err)
	}
	runID, err := h.s.Start(context.Background(), def.ID, map[string]any{"policy_version_id": "pv"})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	// Wait until paused at the task, then blow past the 72h SLA and re-advance.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		r, _ := h.s.Get(context.Background(), runID)
		if r.AwaitedSignal != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	h.clk.Advance(73 * time.Hour)
	_ = h.s.Coordinator().Advance(context.Background(), runID.String())

	final := time.Now().Add(3 * time.Second)
	for time.Now().Before(final) {
		r, _ := h.s.Get(context.Background(), runID)
		if r.State == domain.RunStateSucceeded || r.State == domain.RunStateFailed {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	h.wg.Wait()
	run, _ := h.s.Get(context.Background(), runID)
	t.Logf("state=%s actions=%v", run.State, h.actionNames())
	sawEscalate := false
	for _, c := range *h.calls {
		if c.Action == "policy.escalate_stage" {
			sawEscalate = true
		}
	}
	if !sawEscalate {
		t.Fatalf("expected policy.escalate_stage on SLA timeout, actions=%v", h.actionNames())
	}
	if run.State != domain.RunStateSucceeded {
		t.Fatalf("state: got %s want succeeded after escalate→approve", run.State)
	}
}
