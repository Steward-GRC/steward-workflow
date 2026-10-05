// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package builder

import (
	"fmt"
	"time"

	"github.com/Bugs5382/go-saga-orchestration/domain"
)

// CompileOptions carries the deployment's timing, applied at compile time.
type CompileOptions struct {
	// StageSLAHours is the SLA per stage; zero means 72.
	StageSLAHours int
	// StageReminderHours is when the reminder falls.
	StageReminderHours int
}

func (o CompileOptions) slaHours() int {
	if o.StageSLAHours > 0 {
		return o.StageSLAHours
	}
	return 72
}

// StageSLA returns a stage's SLA deadline and reminder time from now: the
// stage's SLADays when set, else the service-wide hours. Submit, stage entry
// and reassignment all use it, so their deadlines agree.
func (o CompileOptions) StageSLA(stage Stage, now time.Time) (slaDeadline, reminder time.Time) {
	slaDeadline = now.Add(time.Duration(o.StageSLAHours) * time.Hour)
	reminder = now.Add(time.Duration(o.StageReminderHours) * time.Hour)
	if stage.SLADays > 0 {
		slaDeadline = now.Add(time.Duration(stage.SLADays) * 24 * time.Hour)
		reminder = slaDeadline
	}
	return slaDeadline, reminder
}

// Compile validates wd and compiles it to a published saga workflow.
//
// Each stage i becomes:
//
//	s{i}_switch            the stage's CEL condition, when it has one
//	s{i}_assign            policy.assign_stage: creates the stage's seats on entry
//	s{i}_task_0            manual_approval: the stage's gate, with its SLA as due_in
//	s{i}_resolve_decision  policy.resolve_decision: sets the run variable "decision"
//	s{i}_outcome_switch    approve: next stage; reject: set_status_rejected;
//	                       request_changes: stage 0; pending: back to s{i}_task_0
//	s{i}_escalate          policy.escalate_stage, on the SLA timeout branch
//
// then set_status_approved, publish_now, set_status_rejected and end.
//
// The engine drops the payload a person submits with a manual_approval task,
// so the stage outcome is read back from the seats by policy.resolve_decision,
// and the outcome switch branches on the variable it returns. A future
// effective date can't be a wait_until step, because the engine doesn't
// interpolate run variables into step inputs; the set_status worker handles it.
func Compile(wd WorkflowDef, opts CompileOptions) (domain.WorkflowDefinition, error) {
	if err := wd.Validate(); err != nil {
		return domain.WorkflowDefinition{}, fmt.Errorf("invalid WorkflowDef: %w", err)
	}

	var steps []domain.Step
	slaDuration := fmt.Sprintf("%dh", opts.slaHours())

	type stageEntry struct {
		entryID     string
		outcomeID   string
		taskEntryID string
	}
	entries := make([]stageEntry, 0, len(wd.Stages))

	const (
		rejectActionID   = "set_status_rejected"
		approvedActionID = "set_status_approved"
		publishNowID     = "publish_now"
		endID            = "end"
	)

	for i, stage := range wd.Stages {
		pre := fmt.Sprintf("s%d", i)
		switchStepID := pre + "_switch"
		assignStepID := pre + "_assign"
		taskID := pre + "_task_0"
		resolveID := pre + "_resolve_decision"
		outcomeID := pre + "_outcome_switch"
		escalateID := pre + "_escalate"

		entryID := assignStepID
		if stage.Condition != "" {
			steps = append(steps, domain.Step{
				ID:     switchStepID,
				Type:   domain.StepTypeSwitch,
				Inputs: map[string]any{"expr": stage.Condition},
				Branches: map[string]domain.Branch{
					"true":  {Next: assignStepID},
					"false": {Next: ""},
				},
			})
			entryID = switchStepID
		}

		// Idempotent, so the request_changes loopback and engine retries can
		// run it again; the pending loop goes back to the task, not here.
		steps = append(steps, domain.Step{
			ID:     assignStepID,
			Type:   domain.StepTypeAction,
			Action: "policy.assign_stage",
			Inputs: map[string]any{"stage_index": i},
			Next:   taskID,
		})

		dueIn := slaDuration
		if stage.SLADays > 0 {
			dueIn = fmt.Sprintf("%dh", stage.SLADays*24)
		}
		timeoutNext := escalateID
		if stage.RejectOnSLABreach {
			timeoutNext = rejectActionID
		}
		// The engine needs an assignee, but the seats are the real approvers,
		// so this is only a stage marker.
		steps = append(steps, domain.Step{
			ID:   taskID,
			Type: domain.StepTypeManualApproval,
			Inputs: map[string]any{
				"assignee":    fmt.Sprintf("stage:%d", i),
				"stage_name":  stage.Name,
				"stage_index": i,
				"due_in":      dueIn,
			},
			Branches: map[string]domain.Branch{
				"timeout": {Next: timeoutNext},
			},
			Next: resolveID,
		})

		steps = append(steps, domain.Step{
			ID:     resolveID,
			Type:   domain.StepTypeAction,
			Action: "policy.resolve_decision",
			Inputs: map[string]any{"stage_index": i},
			Next:   outcomeID,
		})

		steps = append(steps, domain.Step{
			ID:     outcomeID,
			Type:   domain.StepTypeSwitch,
			Inputs: map[string]any{"expr": `decision`},
			Branches: map[string]domain.Branch{
				"approve":         {Next: ""},
				"reject":          {Next: rejectActionID},
				"request_changes": {Next: ""},
				"pending":         {Next: ""},
			},
		})

		steps = append(steps, domain.Step{
			ID:     escalateID,
			Type:   domain.StepTypeAction,
			Action: "policy.escalate_stage",
			Inputs: map[string]any{"stage_index": i},
			Next:   resolveID,
		})

		entries = append(entries, stageEntry{entryID: entryID, outcomeID: outcomeID, taskEntryID: taskID})
	}

	stage0Entry := ""
	if len(entries) > 0 {
		stage0Entry = entries[0].entryID
	}

	for i, e := range entries {
		nextID := approvedActionID
		if i+1 < len(entries) {
			nextID = entries[i+1].entryID
		}
		switchStepID := fmt.Sprintf("s%d_switch", i)
		for k := range steps {
			switch steps[k].ID {
			case switchStepID:
				steps[k].Branches["false"] = domain.Branch{Next: nextID}
			case e.outcomeID:
				steps[k].Branches["approve"] = domain.Branch{Next: nextID}
				steps[k].Branches["request_changes"] = domain.Branch{Next: stage0Entry}
				steps[k].Branches["pending"] = domain.Branch{Next: e.taskEntryID}
			}
		}
	}

	// The set_status worker may make this "approved_scheduled" for a future
	// effective date, and then skips the publish below.
	steps = append(steps,
		domain.Step{
			ID:     approvedActionID,
			Type:   domain.StepTypeAction,
			Action: "policy.set_status",
			Inputs: map[string]any{"status": "approved"},
			Next:   publishNowID,
		},
		domain.Step{
			ID:     publishNowID,
			Type:   domain.StepTypeAction,
			Action: "policy.set_status",
			Inputs: map[string]any{"status": "published"},
			Next:   endID,
		},
		domain.Step{
			ID:     rejectActionID,
			Type:   domain.StepTypeAction,
			Action: "policy.set_status",
			Inputs: map[string]any{"status": "rejected"},
			Next:   endID,
		},
		domain.Step{ID: endID, Type: domain.StepTypeEnd},
	)

	return domain.WorkflowDefinition{
		ID:        wd.ID,
		Version:   wd.Version,
		Name:      wd.Name,
		Published: true,
		Start:     stage0Entry,
		Steps:     steps,
	}, nil
}
