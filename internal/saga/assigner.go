// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package saga

import (
	"context"
	"fmt"
	"time"

	log "github.com/Bugs5382/go-log"
	"github.com/Bugs5382/go-saga-orchestration/domain"

	"github.com/Steward-GRC/steward-workflow/internal/access"
	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/store"
)

// assignmentCreator creates one seat; re-creating a seat resets it.
type assignmentCreator interface {
	CreateAssignment(ctx context.Context, row store.AssignmentRow, actorUserID string) (store.AssignmentRow, error)
}

// stageEntryRecorder notes that a run entered a stage; it must be idempotent.
type stageEntryRecorder interface {
	RecordStageEntry(ctx context.Context, runID string, stageIdx int, at time.Time) error
}

// approverFilter drops the designated approvers the category rules do not let
// approve the policy, and supplies the category's owners and root as the
// backstop. The backstop is asked for only when the quorum can't be met.
type approverFilter interface {
	Eligible(ctx context.Context, policyVersionID string, candidates []string) (eligible []string, err error)
	Backstop(ctx context.Context, policyVersionID string) (backstop []string, err error)
}

type approvalNotifier interface {
	NotifyApprovalRequested(ctx context.Context, e ApprovalRequestedEvent) error
}

// ApprovalRequestedEvent asks obligations to tell an approver a decision is
// waiting for them. It goes to the "jobs" exchange as JSON on
// workflow.approval_requested. TaskID is "<policy_version_id>:<stage_index>";
// obligations de-duplicates on (TaskID, ApproverUserID), so a re-entered
// stage never notifies the same approver twice.
type ApprovalRequestedEvent struct {
	EventType         string `json:"event_type"`
	TaskID            string `json:"task_id"`
	PolicyID          string `json:"policy_id"`
	PolicyVersionID   string `json:"policy_version_id"`
	StageIndex        int    `json:"stage_index"`
	ApproverUserID    string `json:"approver_user_id"`
	RequestedByUserID string `json:"requested_by_user_id"`
	WorkflowName      string `json:"workflow_name"`
	// DueBy is the stage's SLA deadline in RFC 3339.
	DueBy string `json:"due_by,omitempty"`
}

// StageAssigner creates a stage's seats when the saga enters the stage, from
// the run's resolved individual pool (run input "approvers_s{i}") and the
// pinned definition's groups. It may run again (the request-changes loop,
// engine retries); re-creating a seat resets it.
type StageAssigner struct {
	defs     defLookup
	creator  assignmentCreator
	opts     builder.CompileOptions
	filter   approverFilter
	notifier approvalNotifier
	entries  stageEntryRecorder
	log      log.Logger
}

// NewStageAssigner returns a StageAssigner whose deadlines match Submit's.
func NewStageAssigner(defs defLookup, creator assignmentCreator, opts builder.CompileOptions) *StageAssigner {
	return &StageAssigner{defs: defs, creator: creator, opts: opts, log: log.Nop()}
}

// WithAccessFilter turns on the read filter and the owner backstop.
func (a *StageAssigner) WithAccessFilter(f approverFilter) *StageAssigner {
	a.filter = f
	return a
}

// WithApprovalNotifier turns on the approval-requested events.
func (a *StageAssigner) WithApprovalNotifier(n approvalNotifier) *StageAssigner {
	a.notifier = n
	return a
}

// WithStageEntries records every stage the saga enters, seated or not, so
// GetStatus can tell a stage waiting on nobody from one not reached yet.
func (a *StageAssigner) WithStageEntries(r stageEntryRecorder) *StageAssigner {
	a.entries = r
	return a
}

// WithLogger sets the logger.
func (a *StageAssigner) WithLogger(l log.Logger) *StageAssigner {
	a.log = l
	return a
}

type seatPlan struct {
	userID, groupID string
	veto            bool
}

// AssignStage creates the stage's seats. It reads the version and the pinned
// definition from the run inputs, not approval_runs: the engine runs the first
// stage's assign step inside StartRun, before Submit records the run.
//
// A stage with no one to seat is not an error: the run waits, and a warning
// says why.
func (a *StageAssigner) AssignStage(ctx context.Context, run domain.SagaRun, stageIndex int) error {
	lg := a.log.Ctx(ctx).With(log.F("run_id", run.ID.String()), log.F("stage_index", stageIndex))
	pvID := runStringInput(run.Inputs, "policy_version_id")
	defID := runStringInput(run.Inputs, "workflow_def_id")
	version := runIntInput(run.Inputs, "workflow_version")
	if pvID == "" || defID == "" {
		return fmt.Errorf("assign stage %d: run inputs missing policy_version_id/workflow_def_id", stageIndex)
	}
	wd, err := a.defs.GetVersion(ctx, defID, version)
	if err != nil {
		return fmt.Errorf("assign stage %d: get def %q@v%d: %w", stageIndex, defID, version, err)
	}
	if stageIndex < 0 || stageIndex >= len(wd.Stages) {
		return fmt.Errorf("assign stage %d: out of range (def has %d stages)", stageIndex, len(wd.Stages))
	}
	stage := wd.Stages[stageIndex]

	// Before any early return: a stage with nobody to seat is still entered.
	if a.entries != nil {
		if err := a.entries.RecordStageEntry(ctx, run.ID.String(), stageIndex, time.Now().UTC()); err != nil {
			lg.Error(err, "assign_stage: record stage entry failed", log.F("policy_version_id", pvID))
			return fmt.Errorf("assign stage %d: %w", stageIndex, err)
		}
		lg.Debug("assign_stage: stage entry recorded", log.F("policy_version_id", pvID))
	}

	individuals := poolFromInputs(run.Inputs, stageIndex)
	units := make(map[string][]string, len(stage.GroupUnits))
	for _, u := range stage.GroupUnits {
		units[u.GroupID] = u.Members
	}
	if len(individuals) == 0 && countMembers(units) == 0 {
		lg.Warn("assign_stage: empty approver pool; the run waits with no one to act", log.F("policy_version_id", pvID))
		return nil
	}

	// The approver pool is who the stage seats before any owner backstop joins:
	// the individuals and group members the access filter left. Only people in
	// it can hold a veto.
	pool := vetoPool(individuals, units)
	if a.filter != nil {
		individuals, units, pool, err = a.applyFilter(ctx, pvID, stage, individuals, units)
		if err != nil {
			return fmt.Errorf("assign stage %d: access filter: %w", stageIndex, err)
		}
		if len(individuals) == 0 && countMembers(units) == 0 {
			lg.Warn("assign_stage: no eligible approver and no owner backstop; the run waits with no one to act", log.F("policy_version_id", pvID))
			return nil
		}
	}

	// The stage's named veto people who are in the pool hold the veto on every
	// seat they hold. A named person outside the pool holds nothing and is not
	// an error. Owners who joined only to make up a quorum, site admin and root
	// are not in the pool, so they never hold one. No workflow switch gates this.
	holders, dropped := vetoHolders(stage.VetoUserIDs, pool)
	if len(dropped) > 0 {
		lg.Debug("assign_stage: veto people not in the approver pool hold no veto",
			log.F("policy_version_id", pvID), log.F("not_seated", len(dropped)))
	}
	var plan []seatPlan
	for _, uid := range individuals {
		plan = append(plan, seatPlan{userID: uid, veto: holders[uid]})
	}
	for _, u := range stage.GroupUnits {
		for _, m := range units[u.GroupID] {
			plan = append(plan, seatPlan{userID: m, groupID: u.GroupID, veto: holders[m]})
		}
	}
	lg.Debug("assign_stage: veto marks", log.F("policy_version_id", pvID),
		log.F("veto_named", len(stage.VetoUserIDs)), log.F("veto_holders", len(holders)), log.F("veto_seats", countVeto(plan)))

	now := time.Now().UTC()
	slaDeadline, reminder := a.opts.StageSLA(stage, now)
	actor := runStringInput(run.Inputs, "submitted_by")
	for _, p := range plan {
		if _, err := a.creator.CreateAssignment(ctx, store.AssignmentRow{
			PolicyVersionID: pvID,
			StageIndex:      stageIndex,
			UserID:          p.userID,
			GroupID:         p.groupID,
			Veto:            p.veto,
			AssignedAt:      now,
			SLADeadlineAt:   slaDeadline,
			ReminderAt:      reminder,
		}, actor); err != nil {
			return fmt.Errorf("assign stage %d: create assignment %q: %w", stageIndex, p.userID, err)
		}
	}
	lg.Debug("assign_stage: seats created", log.F("policy_version_id", pvID), log.F("seats", len(plan)))

	// Best effort: the seats are already recorded, and stalling the run over a
	// notice would be worse than a missed email.
	if a.notifier != nil {
		dueBy := ""
		if !slaDeadline.IsZero() {
			dueBy = slaDeadline.Format(time.RFC3339)
		}
		policyID := runStringInput(run.Inputs, "policy_id")
		taskID := fmt.Sprintf("%s:%d", pvID, stageIndex)
		seen := map[string]bool{}
		for _, p := range plan {
			if seen[p.userID] {
				continue
			}
			seen[p.userID] = true
			if err := a.notifier.NotifyApprovalRequested(ctx, ApprovalRequestedEvent{
				EventType:         "workflow.approval_requested",
				TaskID:            taskID,
				PolicyID:          policyID,
				PolicyVersionID:   pvID,
				StageIndex:        stageIndex,
				ApproverUserID:    p.userID,
				RequestedByUserID: actor,
				WorkflowName:      wd.Name,
				DueBy:             dueBy,
			}); err != nil {
				lg.Warn("assign_stage: emit workflow.approval_requested failed; the approver may not get the notice",
					log.F("policy_version_id", pvID), log.F("approver_user_id", p.userID), log.F("error", err.Error()))
			}
		}
	}
	return nil
}

// applyFilter drops individuals and group members the category rules don't let
// approve the policy. When what is left can't meet the stage quorum, the
// category's owners and root join as individual seats.
func (a *StageAssigner) applyFilter(ctx context.Context, pvID string, stage builder.Stage, individuals []string, units map[string][]string) ([]string, map[string][]string, map[string]bool, error) {
	candidates := append([]string(nil), individuals...)
	for _, u := range stage.GroupUnits {
		candidates = append(candidates, units[u.GroupID]...)
	}
	eligible, err := a.filter.Eligible(ctx, pvID, dedupe(candidates))
	if err != nil {
		return nil, nil, nil, err
	}
	approvable := make(map[string]bool, len(eligible))
	for _, u := range eligible {
		approvable[u] = true
	}
	keep := func(ids []string) []string {
		out := make([]string, 0, len(ids))
		for _, id := range ids {
			if approvable[id] {
				out = append(out, id)
			}
		}
		return out
	}
	individuals = keep(individuals)
	filtered := make(map[string][]string, len(units))
	votes := len(individuals)
	for g, members := range units {
		filtered[g] = keep(members)
		if len(filtered[g]) > 0 {
			votes++
		}
	}
	// Taken before the backstop joins: the veto belongs to the pool only.
	pool := vetoPool(individuals, filtered)
	if !access.MeetsQuorum(string(stage.Quorum), stage.QuorumN, votes) {
		seen := make(map[string]bool, len(individuals))
		for _, u := range individuals {
			seen[u] = true
		}
		backstop, err := a.filter.Backstop(ctx, pvID)
		if err != nil {
			return nil, nil, nil, err
		}
		for _, o := range backstop {
			if !seen[o] {
				seen[o] = true
				individuals = append(individuals, o)
			}
		}
	}
	return individuals, filtered, pool, nil
}

// vetoPool is the people a stage seats from its own approver lists: the
// individuals and every group unit's members.
func vetoPool(individuals []string, units map[string][]string) map[string]bool {
	pool := make(map[string]bool, len(individuals))
	for _, u := range individuals {
		pool[u] = true
	}
	for _, members := range units {
		for _, m := range members {
			pool[m] = true
		}
	}
	return pool
}

// vetoHolders keeps the named people who are in the pool, and answers the rest.
func vetoHolders(named []string, pool map[string]bool) (holders map[string]bool, dropped []string) {
	holders = make(map[string]bool, len(named))
	for _, id := range named {
		if pool[id] {
			holders[id] = true
		} else {
			dropped = append(dropped, id)
		}
	}
	return holders, dropped
}

func countVeto(plan []seatPlan) int {
	n := 0
	for _, p := range plan {
		if p.veto {
			n++
		}
	}
	return n
}

func countMembers(units map[string][]string) int {
	n := 0
	for _, m := range units {
		n += len(m)
	}
	return n
}

func dedupe(ids []string) []string {
	seen := make(map[string]bool, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out
}

// runIntInput reads an int run input; JSON numbers arrive as float64.
func runIntInput(inputs map[string]any, key string) int {
	switch n := inputs[key].(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	default:
		return 0
	}
}

// poolFromInputs reads a stage's individual pool. The saga store round-trips
// it through JSON, so it comes back as []any.
func poolFromInputs(inputs map[string]any, stageIndex int) []string {
	switch v := inputs[fmt.Sprintf("approvers_s%d", stageIndex)].(type) {
	case []string:
		return v
	case []any:
		out := make([]string, 0, len(v))
		for _, e := range v {
			if s, ok := e.(string); ok && s != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func runStringInput(inputs map[string]any, key string) string {
	s, _ := inputs[key].(string)
	return s
}
