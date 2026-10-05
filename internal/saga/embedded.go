// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package saga runs the go-saga engine in-process for the workflow service.
//
// The engine has no remote way to publish a definition, so workflow links it
// in over a Postgres store shared by every replica: a run started on one
// replica can be signalled on another. Actions run synchronously in-process on
// whichever replica advances the run. Only the SLA timer must be a singleton;
// RunTimer elects it with an advisory lock.
package saga

import (
	"context"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"time"

	log "github.com/Bugs5382/go-log"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine"
	"github.com/Bugs5382/go-saga-orchestration/engine/verbs"
	sagasdk "github.com/Bugs5382/go-saga-orchestration/saga"
	"github.com/Bugs5382/go-saga-orchestration/store"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
	"github.com/google/uuid"

	"github.com/Steward-GRC/steward-workflow/internal/grpcsvc"
	"github.com/Steward-GRC/steward-workflow/internal/worker"
)

// EmbeddedSagaClient is the grpcsvc.SagaClient on the in-process engine.
type EmbeddedSagaClient struct {
	sc    *sagasdk.Saga
	store store.Store
}

// NewEmbedded wraps a built engine.
func NewEmbedded(sc *sagasdk.Saga) *EmbeddedSagaClient {
	return &EmbeddedSagaClient{sc: sc, store: sc.Store()}
}

type stageDecider interface {
	ResolveStageDecision(ctx context.Context, runID string, stageIndex int) (string, error)
}

type stageAssigner interface {
	AssignStage(ctx context.Context, run domain.SagaRun, stageIndex int) error
}

// Build returns an engine over st with the policy actions registered as an
// in-process verb. A nil resolver sends resolve_decision to policyClient; a
// nil assigner makes assign_stage a no-op.
func Build(st store.Store, policyClient worker.PolicyServiceClient, resolver stageDecider, assigner stageAssigner, lg log.Logger) (*sagasdk.Saga, error) {
	sc, err := sagasdk.New(sagasdk.Options{Store: st})
	if err != nil {
		return nil, fmt.Errorf("saga.New: %w", err)
	}
	registerPolicyActionVerb(sc, policyClient, resolver, assigner, lg)
	return sc, nil
}

// BuildInMemory is Build over a fresh in-memory store, for tests only.
func BuildInMemory(policyClient worker.PolicyServiceClient) (*sagasdk.Saga, error) {
	return Build(memory.New(), policyClient, nil, nil, log.Nop())
}

// registerPolicyActionVerb replaces the engine's dispatching action verb with
// a synchronous handler, so an action's result (resolve_decision's
// "decision") is merged into the run before the next step.
func registerPolicyActionVerb(sc *sagasdk.Saga, policyClient worker.PolicyServiceClient, resolver stageDecider, assigner stageAssigner, lg log.Logger) {
	sc.RegisterVerb(string(domain.StepTypeAction), "common",
		verbs.HandlerFunc(func(ctx context.Context, run domain.SagaRun, step domain.Step) (out map[string]any, err error) {
			defer func() {
				if err != nil {
					status, _ := step.Inputs["status"].(string)
					lg.Ctx(ctx).Error(err, "saga action failed",
						log.F("run_id", run.ID.String()), log.F("step_id", step.ID), log.F("action", step.Action),
						log.F("status", status), log.F("policy_version_id", runValue(run, "policy_version_id")))
				}
			}()
			if step.Action == "" {
				return nil, fmt.Errorf("action: step.Action required")
			}
			stage := runIntInput(step.Inputs, "stage_index")
			switch {
			case step.Action == "policy.assign_stage":
				if assigner == nil {
					return map[string]any{}, nil
				}
				if err := assigner.AssignStage(ctx, run, stage); err != nil {
					return nil, err
				}
				return map[string]any{}, nil
			case step.Action == "policy.resolve_decision" && resolver != nil:
				d, err := resolver.ResolveStageDecision(ctx, run.ID.String(), stage)
				if err != nil {
					return nil, err
				}
				return map[string]any{"decision": d}, nil
			}
			// The engine doesn't interpolate run values into step inputs, so
			// the version is merged in here.
			inputs := make(map[string]any, len(step.Inputs)+1)
			maps.Copy(inputs, step.Inputs)
			if _, ok := inputs["policy_version_id"]; !ok {
				if pv := runValue(run, "policy_version_id"); pv != "" {
					inputs["policy_version_id"] = pv
				}
			}
			return worker.HandleAction(ctx, policyClient, step.Action, run.ID.String(), inputs)
		}))
}

// runValue reads a string run value: the variables first, then the inputs.
func runValue(run domain.SagaRun, key string) string {
	if s, ok := run.Variables[key].(string); ok && s != "" {
		return s
	}
	s, _ := run.Inputs[key].(string)
	return s
}

// PublishWorkflow upserts a compiled definition; idempotent on id and
// version.
func (c *EmbeddedSagaClient) PublishWorkflow(_ context.Context, def domain.WorkflowDefinition) error {
	return c.sc.Register(def)
}

// StartRun starts a run and returns its id.
func (c *EmbeddedSagaClient) StartRun(ctx context.Context, workflowID string, inputs map[string]any) (string, error) {
	id, err := c.sc.Start(ctx, workflowID, inputs)
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

// Signal unpauses a run. A manual_approval step waits for
// "user_task.<task_id>.submitted", so with a task_id this submits the task and
// sends that signal, carrying the decision for the record; the decision that
// counts is read back from the seats. Without a task_id the signal goes as
// named.
func (c *EmbeddedSagaClient) Signal(ctx context.Context, runID, signal string, inputs map[string]any) error {
	rid, err := uuid.Parse(runID)
	if err != nil {
		return fmt.Errorf("parse run id %q: %w", runID, err)
	}
	taskID, _ := inputs["task_id"].(string)
	if taskID == "" {
		return c.sc.Signal(ctx, rid, signal, inputs)
	}
	tid, err := uuid.Parse(taskID)
	if err != nil {
		return fmt.Errorf("parse task id %q: %w", taskID, err)
	}
	submittedBy, _ := inputs["actor_user_id"].(string)
	result := map[string]any{"decision": signal}
	if comment, ok := inputs["comment"].(string); ok && comment != "" {
		result["comment"] = comment
	}
	if err := c.store.SubmitUserTask(ctx, tid, submittedBy, result); err != nil {
		return fmt.Errorf("submit user task %q: %w", taskID, err)
	}
	return c.sc.Signal(ctx, rid, "user_task."+taskID+".submitted", result)
}

// ListPendingTasks returns every paused run's open task. The engine knows
// stages, not people; the seats narrow it to one approver.
func (c *EmbeddedSagaClient) ListPendingTasks(ctx context.Context, _ string) ([]grpcsvc.PendingTaskInfo, error) {
	runs, err := c.store.ListRuns(ctx, store.RunFilter{State: string(domain.RunStatePaused), Limit: 500})
	if err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	var out []grpcsvc.PendingTaskInfo
	for _, run := range runs {
		tasks, err := c.store.ListUserTasksByRun(ctx, run.ID)
		if err != nil {
			return nil, fmt.Errorf("list tasks for run %s: %w", run.ID, err)
		}
		pvID, _ := run.Inputs["policy_version_id"].(string)
		for _, task := range tasks {
			// A run keeps its earlier stages' submitted tasks; only an
			// unsubmitted one is waiting. Signalling an old one would never
			// unpause the run.
			if task.SubmittedAt != nil {
				continue
			}
			var due *time.Time
			if task.DueAt != nil {
				d := task.DueAt.UTC()
				due = &d
			}
			out = append(out, grpcsvc.PendingTaskInfo{
				TaskID:          task.ID.String(),
				RunID:           run.ID.String(),
				PolicyVersionID: pvID,
				StageIndex:      toInt32(stageOfStep(task.StepID)),
				DueAt:           due,
			})
		}
	}
	return out, nil
}

// CancelRun ends a run with the engine's Cancel, which closes its open tasks
// and clears its wakeups. A finished run is left alone.
func (c *EmbeddedSagaClient) CancelRun(ctx context.Context, runID string) error {
	return c.cancelRun(ctx, runID, "withdrawn")
}

func (c *EmbeddedSagaClient) cancelRun(ctx context.Context, runID, reason string) error {
	rid, err := uuid.Parse(runID)
	if err != nil {
		return fmt.Errorf("parse run id %q: %w", runID, err)
	}
	if err := c.sc.Cancel(ctx, rid, reason); err != nil {
		return fmt.Errorf("cancel run %q: %w", runID, err)
	}
	return nil
}

// CancelActiveRunsForPolicyVersion cancels every unfinished run of the
// version, so a resubmit never leaves an earlier run's task in an inbox. It
// returns how many it cancelled.
func (c *EmbeddedSagaClient) CancelActiveRunsForPolicyVersion(ctx context.Context, policyVersionID string) (int, error) {
	if policyVersionID == "" {
		return 0, fmt.Errorf("policy_version_id is required")
	}
	seen := make(map[uuid.UUID]struct{})
	var cancelled int
	for _, st := range []domain.RunState{domain.RunStatePending, domain.RunStateRunning, domain.RunStatePaused} {
		runs, err := c.store.ListRuns(ctx, store.RunFilter{State: string(st), Limit: 500})
		if err != nil {
			return cancelled, fmt.Errorf("list %s runs: %w", st, err)
		}
		for _, run := range runs {
			if _, dup := seen[run.ID]; dup {
				continue
			}
			seen[run.ID] = struct{}{}
			if pv, _ := run.Inputs["policy_version_id"].(string); pv != policyVersionID {
				continue
			}
			if err := c.cancelRun(ctx, run.ID.String(), "superseded by re-submit"); err != nil {
				return cancelled, err
			}
			cancelled++
		}
	}
	return cancelled, nil
}

// stageOfStep reads the stage from a step id like "s2_task_0"; -1 when the
// step isn't a stage's.
func stageOfStep(stepID string) int {
	rest, ok := strings.CutPrefix(stepID, "s")
	if !ok {
		return -1
	}
	end := strings.IndexByte(rest, '_')
	if end <= 0 {
		return -1
	}
	n, err := strconv.Atoi(rest[:end])
	if err != nil {
		return -1
	}
	return n
}

// advancePublisher advances each due run the timer finds, in the background.
type advancePublisher struct {
	coord *engine.Coordinator
}

func (p advancePublisher) PublishSagaAdvance(ctx context.Context, runID string) error {
	go func() { _ = p.coord.Advance(context.WithoutCancel(ctx), runID) }()
	return nil
}

var (
	_ grpcsvc.SagaClient    = (*EmbeddedSagaClient)(nil)
	_ engine.TimerPublisher = advancePublisher{}
)
