// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package grpcsvc serves steward.workflow.v1.WorkflowService.
package grpcsvc

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	log "github.com/Bugs5382/go-log"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	workflowv1 "github.com/Steward-GRC/steward-workflow/gen/go/steward/workflow/v1"
	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/eligibility"
	"github.com/Steward-GRC/steward-workflow/internal/errcodes"
	"github.com/Steward-GRC/steward-workflow/internal/store"
	"github.com/Steward-GRC/steward-workflow/internal/tally"
)

// PendingTaskInfo is one open saga task, in the engine-neutral shape the
// SagaClient returns.
type PendingTaskInfo struct {
	TaskID          string
	RunID           string
	PolicyVersionID string
	StageIndex      int32
	DueAt           *time.Time
}

// WorkflowResolver resolves a policy's workflow.
type WorkflowResolver interface {
	Resolve(ctx context.Context, policyID string, ancestorCategoryIDs []string) (string, error)
}

// WorkflowDefStoreReader reads definitions.
type WorkflowDefStoreReader interface {
	Get(ctx context.Context, id string) (builder.WorkflowDef, error)
	Publish(ctx context.Context, id string) error
	GetVersion(ctx context.Context, id string, version int) (builder.WorkflowDef, error)
	GCVersion(ctx context.Context, defID string, version int) error
}

// RunStoreWriter records approval runs.
type RunStoreWriter interface {
	TrackRun(ctx context.Context, r store.ApprovalRun) error
	AbortActiveRuns(ctx context.Context, policyVersionID string) (int64, error)
	GetRun(ctx context.Context, policyVersionID string) (store.ApprovalRun, error)
	GetRunByRunID(ctx context.Context, runID string) (store.ApprovalRun, error)
	UpdateRunStatus(ctx context.Context, policyVersionID, status string) error
	ListActiveRuns(ctx context.Context) ([]store.ApprovalRun, error)
}

// SagaClient is the engine surface the service uses.
type SagaClient interface {
	// PublishWorkflow is idempotent on the definition's id and version.
	PublishWorkflow(ctx context.Context, def domain.WorkflowDefinition) error
	StartRun(ctx context.Context, workflowID string, inputs map[string]any) (string, error)
	Signal(ctx context.Context, runID, signal string, inputs map[string]any) error
	ListPendingTasks(ctx context.Context, approverUserID string) ([]PendingTaskInfo, error)
	// CancelRun ends a run and clears its open tasks; a finished run is left
	// alone.
	CancelRun(ctx context.Context, runID string) error
	CancelActiveRunsForPolicyVersion(ctx context.Context, policyVersionID string) (int, error)
}

// AuditEmitter emits audit events. Emit is for events with no person
// behind them.
type AuditEmitter interface {
	Emit(ctx context.Context, action, subject string)
	EmitActor(ctx context.Context, action, subject, actorUserID string)
	EmitActorAttrs(ctx context.Context, action, subject, actorUserID, groupID string, attrs map[string]string)
}

// CoreStatusSetter writes a version's stored status to core. Withdraw needs
// it to return the version to draft.
type CoreStatusSetter interface {
	SetVersionStatus(ctx context.Context, policyVersionID, status, actorUserID string) error
}

// AdminChecker reports whether a user may reassign any approval as an admin.
type AdminChecker interface {
	IsWorkflowAdmin(ctx context.Context, userID string) (bool, error)
}

// AssignmentStore is the seat store surface the service uses.
type AssignmentStore interface {
	CreateAssignment(ctx context.Context, row store.AssignmentRow, actorUserID string) (store.AssignmentRow, error)
	GetAssignment(ctx context.Context, pvID string, stageIdx int, userID, groupID string) (store.AssignmentRow, error)
	HasPendingAssignment(ctx context.Context, pvID string, stageIdx int, userID string) (bool, error)
	PendingSeats(ctx context.Context, pvID, userID string) ([]store.AssignmentRow, error)
	ListStageAssignments(ctx context.Context, pvID string, stageIdx int) ([]store.AssignmentRow, error)
	CurrentStageIndex(ctx context.Context, pvID string) (int, bool, error)
	DecideAssignment(ctx context.Context, in store.DecideAssignmentInput) error
	SupersedePendingPeers(ctx context.Context, pvID string, stageIdx int, excludeAssignmentID string, actorUserID string) (int64, error)
	TerminatePendingAssignments(ctx context.Context, pvID, state, actorUserID, reason string) (int64, error)
	Swap(ctx context.Context, in store.SwapInput) (store.AssignmentRow, error)
	ListHistory(ctx context.Context, pvID string, stageIdx int) ([]store.HistoryEntry, error)
	ListActiveRunsByDef(ctx context.Context, defID string, version int) ([]store.ApprovalRun, error)
	ReassignUserItems(ctx context.Context, fromUserID, toUserID, actorUserID string, dryRun bool) (store.ReassignResult, error)
}

// ActorClaims is the effective caller: during act-as, the target, because
// every check it feeds (the initiator role, eligibility, whose seat is
// decided) must evaluate as them. Who really acted is recorded separately
// from the act-as actor in the context.
type ActorClaims struct {
	UserID string
	// Roles holds "admin" when the caller is already known to be a workflow
	// admin; otherwise the AdminChecker decides.
	Roles []string
}

// ActorExtractor reads the caller from the request context.
type ActorExtractor func(ctx context.Context) (ActorClaims, bool)

// WorkflowServer implements workflowv1.WorkflowServiceServer.
type WorkflowServer struct {
	workflowv1.UnimplementedWorkflowServiceServer
	resolver       WorkflowResolver
	defStore       WorkflowDefStoreReader
	runStore       RunStoreWriter
	sagaClient     SagaClient
	auditEmit      AuditEmitter
	assignments    AssignmentStore
	actorExtractor ActorExtractor
	admins         AdminChecker
	coreStatus     CoreStatusSetter
	runNotifier    runNotifier
	compileOpts    builder.CompileOptions
	log            log.Logger
}

// WorkflowServerOpts are the server's dependencies.
type WorkflowServerOpts struct {
	Resolver       WorkflowResolver
	DefStore       WorkflowDefStoreReader
	RunStore       RunStoreWriter
	SagaClient     SagaClient
	AuditEmit      AuditEmitter
	Assignments    AssignmentStore
	ActorExtractor ActorExtractor
	Admins         AdminChecker
	CoreStatus     CoreStatusSetter
	// RunNotifier sends the workflow-started and workflow-denied events; nil
	// sends none.
	RunNotifier runNotifier
	CompileOpts builder.CompileOptions
	Logger      log.Logger
}

// NewWorkflowServer returns a WorkflowServer. With no ActorExtractor every
// call that needs a caller is refused.
func NewWorkflowServer(o WorkflowServerOpts) *WorkflowServer {
	if o.ActorExtractor == nil {
		o.ActorExtractor = func(context.Context) (ActorClaims, bool) { return ActorClaims{}, false }
	}
	if o.Logger == nil {
		o.Logger = log.Nop()
	}
	return &WorkflowServer{
		resolver:       o.Resolver,
		defStore:       o.DefStore,
		runStore:       o.RunStore,
		sagaClient:     o.SagaClient,
		auditEmit:      o.AuditEmit,
		assignments:    o.Assignments,
		actorExtractor: o.ActorExtractor,
		admins:         o.Admins,
		coreStatus:     o.CoreStatus,
		runNotifier:    o.RunNotifier,
		compileOpts:    o.CompileOpts,
		log:            o.Logger,
	}
}

// internalErr codes an unexpected failure (6000); its cause is only logged.
func internalErr(ctx context.Context, format string, args ...any) error {
	return errcodes.Error(ctx, fmt.Errorf(format, args...))
}

// ResolveWorkflow returns the workflow Submit would use; "" means the policy
// can't be submitted.
func (s *WorkflowServer) ResolveWorkflow(ctx context.Context, req *workflowv1.ResolveWorkflowRequest) (*workflowv1.ResolveWorkflowResponse, error) {
	defID, err := s.resolver.Resolve(ctx, req.GetPolicyId(), req.GetAncestorCategoryIds())
	if err != nil {
		return nil, internalErr(ctx, "resolve workflow: %w", err)
	}
	return &workflowv1.ResolveWorkflowResponse{WorkflowDefId: defID}, nil
}

// Submit resolves the policy's workflow, compiles and publishes it, starts a
// run on the definition's current version and records it. Each stage's
// individual pool is resolved here, for the stage's assign step to seat;
// group seats come from the pinned definition.
func (s *WorkflowServer) Submit(ctx context.Context, req *workflowv1.SubmitRequest) (*workflowv1.SubmitResponse, error) {
	lg := s.log.Ctx(ctx).With(log.F("policy_version_id", req.GetPolicyVersionId()))
	defID, err := s.resolver.Resolve(ctx, req.GetPolicyId(), req.GetAncestorCategoryIds())
	if err != nil {
		return nil, internalErr(ctx, "resolve workflow: %w", err)
	}
	if defID == "" {
		s.auditEmit.Emit(ctx, "workflow.submit.no_workflow", "policy_version:"+req.GetPolicyVersionId())
		return nil, status.Error(codes.FailedPrecondition,
			"no approval workflow is attached to this policy's category; attach one before submitting")
	}

	wd, err := s.defStore.Get(ctx, defID)
	if err != nil {
		return nil, internalErr(ctx, "get workflow def: %w", err)
	}
	sagaDef, err := builder.Compile(wd, s.compileOpts)
	if err != nil {
		return nil, internalErr(ctx, "compile workflow: %w", err)
	}
	if err := s.sagaClient.PublishWorkflow(ctx, sagaDef); err != nil {
		return nil, internalErr(ctx, "publish saga workflow: %w", err)
	}

	// The definition id and version ride the run inputs because the engine
	// runs the first assign step inside StartRun, before TrackRun below.
	runInputs := map[string]any{
		"policy_version_id": req.GetPolicyVersionId(),
		"policy_id":         req.GetPolicyId(),
		"group_id":          req.GetCategoryId(),
		"submitted_by":      req.GetSubmittedBy(),
		"workflow_def_id":   defID,
		"workflow_version":  wd.Version,
	}
	for i, stage := range wd.Stages {
		pool := eligibility.StagePool(stage, req.GetAncestorCategoryIds())
		votes, members := len(pool), 0
		for _, u := range stage.GroupUnits {
			members += len(u.Members)
			if len(u.Members) > 0 {
				votes++
			}
		}
		// A definition may be saved with an empty stage, but a run on one
		// would never finish.
		if len(pool) == 0 && members == 0 {
			s.auditEmit.Emit(ctx, "workflow.stage.unstaffed", fmt.Sprintf("policy_version:%s stage:%d", req.GetPolicyVersionId(), i))
			return nil, errcodes.New(ctx, errcodes.CodeStageNotStaffed, "stage", strconv.Itoa(i), "stage_name", stage.Name)
		}
		if stage.Quorum == builder.QuorumNofM && stage.QuorumN > votes {
			s.auditEmit.Emit(ctx, "workflow.stage.misconfigured", fmt.Sprintf("policy_version:%s stage:%d", req.GetPolicyVersionId(), i))
			return nil, status.Errorf(codes.FailedPrecondition, "stage %d: quorum_n=%d exceeds pool size %d", i, stage.QuorumN, votes)
		}
		runInputs[fmt.Sprintf("approvers_s%d", i)] = pool
	}

	// A resubmit cancels the version's earlier runs and keeps their records,
	// so the inbox holds one run per version.
	if _, err := s.sagaClient.CancelActiveRunsForPolicyVersion(ctx, req.GetPolicyVersionId()); err != nil {
		return nil, internalErr(ctx, "cancel prior runs: %w", err)
	}
	if _, err := s.runStore.AbortActiveRuns(ctx, req.GetPolicyVersionId()); err != nil {
		return nil, internalErr(ctx, "abort prior runs: %w", err)
	}

	runID, err := s.sagaClient.StartRun(ctx, sagaDef.ID, runInputs)
	if err != nil {
		return nil, internalErr(ctx, "start saga run: %w", err)
	}
	if err := s.runStore.TrackRun(ctx, store.ApprovalRun{
		PolicyVersionID: req.GetPolicyVersionId(),
		RunID:           runID,
		WorkflowDefID:   defID,
		WorkflowVersion: wd.Version,
		Status:          "in_review",
		HomeCategoryID:  req.GetCategoryId(),
		SubmittedBy:     req.GetSubmittedBy(),
		PolicyID:        req.GetPolicyId(),
	}); err != nil {
		return nil, internalErr(ctx, "track run: %w", err)
	}
	lg.Info("submitted", log.F("run_id", runID), log.F("workflow_def_id", defID), log.F("workflow_version", wd.Version))

	s.auditEmit.EmitActorAttrs(ctx, "workflow.submitted", "policy_version:"+req.GetPolicyVersionId(),
		req.GetSubmittedBy(), req.GetCategoryId(), map[string]string{"run_id": runID, "workflow_def_id": defID})

	if s.runNotifier != nil && req.GetSubmittedBy() != "" {
		var currentStep string
		if len(wd.Stages) > 0 {
			currentStep = wd.Stages[0].Name
		}
		if err := s.runNotifier.NotifyRunStarted(ctx, RunStartedEvent{
			EventType:         "workflow.started",
			RunID:             runID,
			PolicyID:          req.GetPolicyId(),
			PolicyVersionID:   req.GetPolicyVersionId(),
			SubmittedByUserID: req.GetSubmittedBy(),
			WorkflowName:      wd.Name,
			CurrentStep:       currentStep,
		}); err != nil {
			lg.Warn("submit: emit workflow.started failed; the submitter may not get the notice",
				log.F("run_id", runID), log.F("error", err.Error()))
		}
	}
	return &workflowv1.SubmitResponse{RunId: runID}, nil
}

// Signal records a decision and unpauses the run. Approve and reject need a
// comment. A decision always applies to the version's current run, whatever
// run_id the caller holds.
func (s *WorkflowServer) Signal(ctx context.Context, req *workflowv1.SignalRequest) (*workflowv1.SignalResponse, error) {
	name := signalName(req.GetSignal())
	run, err := s.resolveCurrentRun(ctx, req)
	if err != nil {
		return nil, err
	}

	if name == "approve" || name == "reject" {
		if req.GetComment() == "" {
			return nil, status.Error(codes.InvalidArgument, "comment is required for approve/reject")
		}
		// Durable before the saga moves: resolve_decision reads it back.
		if err := s.recordDecision(ctx, req, name, run); err != nil {
			return nil, err
		}
	}
	if name == "withdraw" {
		if err := s.recordWithdraw(ctx, run, req.GetActorUserId()); err != nil {
			return nil, err
		}
	}

	taskID := req.GetTaskId()
	if name == "approve" || name == "reject" {
		cur, ok, err := s.currentPendingTaskID(ctx, run)
		if err != nil {
			return nil, err
		}
		if ok {
			taskID = cur
		}
	}
	if err := s.sagaClient.Signal(ctx, run.RunID, name, map[string]any{
		"task_id":       taskID,
		"actor_user_id": req.GetActorUserId(),
		"comment":       req.GetComment(),
	}); err != nil {
		return nil, internalErr(ctx, "saga signal: %w", err)
	}
	if name == "reject" {
		if err := s.afterReject(ctx, run, req.GetActorUserId(), req.GetComment()); err != nil {
			return nil, err
		}
	}
	s.auditEmit.EmitActorAttrs(ctx, "workflow.signal."+name, "policy_version:"+run.PolicyVersionID,
		req.GetActorUserId(), run.HomeCategoryID, map[string]string{"run_id": run.RunID, "signal": name})
	return &workflowv1.SignalResponse{}, nil
}

// afterReject makes sure a rejected run is finished, in case the engine left
// it parked, and tells the submitter. A group-seat rejection that left the
// stage open changes nothing.
func (s *WorkflowServer) afterReject(ctx context.Context, run store.ApprovalRun, reviewer, reason string) error {
	stageRejected, err := s.stageRejected(ctx, run)
	if err != nil {
		return err
	}
	if !stageRejected {
		return nil
	}
	if err := s.sagaClient.CancelRun(ctx, run.RunID); err != nil {
		return internalErr(ctx, "reject: ensure run terminal: %w", err)
	}
	if s.runNotifier != nil && run.SubmittedBy != "" {
		if err := s.runNotifier.NotifyRunDenied(ctx, RunDeniedEvent{
			EventType:         "workflow.denied",
			RunID:             run.RunID,
			PolicyID:          run.PolicyID,
			PolicyVersionID:   run.PolicyVersionID,
			SubmittedByUserID: run.SubmittedBy,
			ReviewedByUserID:  reviewer,
			WorkflowName:      s.workflowName(ctx, run),
			Reason:            reason,
		}); err != nil {
			s.log.Ctx(ctx).Warn("reject: emit workflow.denied failed; the submitter may not get the notice",
				log.F("run_id", run.RunID), log.F("error", err.Error()))
		}
	}
	return nil
}

// stageRejected reports whether the run's current stage is rejected. It is
// when no seat on the run is pending any more after the rejection, or the
// stage's tally says so.
func (s *WorkflowServer) stageRejected(ctx context.Context, run store.ApprovalRun) (bool, error) {
	if s.assignments == nil {
		return true, nil
	}
	idx, pending, err := s.assignments.CurrentStageIndex(ctx, run.PolicyVersionID)
	if err != nil {
		return false, internalErr(ctx, "current stage: %w", err)
	}
	if !pending {
		return true, nil
	}
	stage, ok := s.pinnedStage(ctx, run, idx)
	if !ok {
		return true, nil
	}
	rows, err := s.assignments.ListStageAssignments(ctx, run.PolicyVersionID, idx)
	if err != nil {
		return false, internalErr(ctx, "list stage assignments: %w", err)
	}
	return tally.Resolve(stage, rows) == tally.DecisionReject, nil
}

// pinnedStage returns the run's stage from the definition version it was
// submitted under.
func (s *WorkflowServer) pinnedStage(ctx context.Context, run store.ApprovalRun, idx int) (builder.Stage, bool) {
	if s.defStore == nil || run.WorkflowDefID == "" {
		return builder.Stage{}, false
	}
	wd, err := s.defStore.GetVersion(ctx, run.WorkflowDefID, run.WorkflowVersion)
	if err != nil || idx < 0 || idx >= len(wd.Stages) {
		return builder.Stage{}, false
	}
	return wd.Stages[idx], true
}

// resolveCurrentRun returns the version's current run, from
// policy_version_id, else from the run_id. An unknown identifier is a
// FailedPrecondition so the client refreshes.
func (s *WorkflowServer) resolveCurrentRun(ctx context.Context, req *workflowv1.SignalRequest) (store.ApprovalRun, error) {
	pvID := req.GetPolicyVersionId()
	if pvID == "" {
		byRun, err := s.runStore.GetRunByRunID(ctx, req.GetRunId())
		if err != nil {
			s.log.Ctx(ctx).Warn("resolve current run: unknown run id", log.F("run_id", req.GetRunId()), log.F("error", err.Error()))
			return store.ApprovalRun{}, status.Errorf(codes.FailedPrecondition,
				"this approval is no longer active; refresh your inbox (run_id %q)", req.GetRunId())
		}
		pvID = byRun.PolicyVersionID
	}
	run, err := s.runStore.GetRun(ctx, pvID)
	if err != nil {
		s.log.Ctx(ctx).Warn("resolve current run: no run", log.F("policy_version_id", pvID), log.F("error", err.Error()))
		return store.ApprovalRun{}, status.Errorf(codes.FailedPrecondition,
			"this approval is no longer active; refresh your inbox (policy_version %q)", pvID)
	}
	return run, nil
}

// currentPendingTaskID finds the run's open saga task; ok is false when the
// engine has none for it.
func (s *WorkflowServer) currentPendingTaskID(ctx context.Context, run store.ApprovalRun) (string, bool, error) {
	tasks, err := s.sagaClient.ListPendingTasks(ctx, "")
	if err != nil {
		return "", false, internalErr(ctx, "resolve current pending task: %w", err)
	}
	for _, t := range tasks {
		if t.RunID == run.RunID {
			return t.TaskID, true, nil
		}
	}
	return "", false, nil
}

// workflowName is the run's definition name for the notices, "" when it
// can't be read.
func (s *WorkflowServer) workflowName(ctx context.Context, run store.ApprovalRun) string {
	if s.defStore == nil || run.WorkflowDefID == "" {
		return ""
	}
	wd, err := s.defStore.Get(ctx, run.WorkflowDefID)
	if err != nil {
		s.log.Ctx(ctx).Debug("workflow name lookup failed", log.F("workflow_def_id", run.WorkflowDefID), log.F("error", err.Error()))
		return ""
	}
	return wd.Name
}

// pickSeat chooses the seat a decision applies to from the caller's pending
// seats: the named group's, else the individual seat, else their only group
// seat.
func pickSeat(seats []store.AssignmentRow, groupID string) (store.AssignmentRow, error) {
	if len(seats) == 0 {
		return store.AssignmentRow{}, pgx.ErrNoRows
	}
	current := seats[0].StageIndex
	var onStage []store.AssignmentRow
	for _, a := range seats {
		if a.StageIndex == current {
			onStage = append(onStage, a)
		}
	}
	if groupID != "" {
		for _, a := range onStage {
			if a.GroupID == groupID {
				return a, nil
			}
		}
		return store.AssignmentRow{}, pgx.ErrNoRows
	}
	for _, a := range onStage {
		if a.GroupID == "" {
			return a, nil
		}
	}
	if len(onStage) == 1 {
		return onStage[0], nil
	}
	return store.AssignmentRow{}, errAmbiguousSeat
}

var errAmbiguousSeat = errors.New("the caller holds seats in several groups")

// recordDecision records the decision on the caller's seat. An individual
// rejection, or a group rejection that leaves the group short of its quorum,
// rejects the stage and supersedes its other pending seats.
func (s *WorkflowServer) recordDecision(ctx context.Context, req *workflowv1.SignalRequest, name string, run store.ApprovalRun) error {
	if s.assignments == nil {
		return status.Error(codes.Unimplemented, "assignment store not wired")
	}
	if req.GetActorUserId() == "" {
		return status.Error(codes.InvalidArgument, "actor_user_id is required to record a decision")
	}
	seats, err := s.assignments.PendingSeats(ctx, run.PolicyVersionID, req.GetActorUserId())
	if err != nil {
		return internalErr(ctx, "pending seats: %w", err)
	}
	seat, err := pickSeat(seats, req.GetGroupId())
	if errors.Is(err, errAmbiguousSeat) {
		return status.Error(codes.InvalidArgument, "group_id is required: you hold seats in several groups on this stage")
	}
	if err != nil {
		s.log.Ctx(ctx).Info("record decision: no pending seat", log.F("actor_user_id", req.GetActorUserId()),
			log.F("policy_version_id", run.PolicyVersionID), log.F("group_id", req.GetGroupId()))
		return status.Errorf(codes.FailedPrecondition,
			"this approval is no longer active; refresh your inbox (no pending assignment for actor %q on policy_version %q)",
			req.GetActorUserId(), run.PolicyVersionID)
	}
	decision := "approved"
	if name == "reject" {
		decision = "rejected"
	}
	if err := s.assignments.DecideAssignment(ctx, store.DecideAssignmentInput{
		AssignmentID: seat.ID, Decision: decision, Comment: req.GetComment(), ActorUserID: req.GetActorUserId(),
	}); err != nil {
		return internalErr(ctx, "record decision: %w", err)
	}
	if decision == "rejected" {
		if err := s.supersedeIfRejected(ctx, run, seat, req.GetActorUserId()); err != nil {
			return err
		}
	}
	attrs := map[string]string{"stage": strconv.Itoa(seat.StageIndex), "decision": decision}
	if seat.GroupID != "" {
		attrs["seat_group_id"] = seat.GroupID
	}
	s.auditEmit.EmitActorAttrs(ctx, "workflow.decision."+decision, "policy_version:"+run.PolicyVersionID,
		req.GetActorUserId(), run.HomeCategoryID, attrs)
	return nil
}

// supersedeIfRejected supersedes the stage's other pending seats when a
// rejection rejects the stage: always for an individual seat, and for a group
// seat once the group can't reach its quorum.
func (s *WorkflowServer) supersedeIfRejected(ctx context.Context, run store.ApprovalRun, seat store.AssignmentRow, actor string) error {
	if seat.GroupID != "" {
		stage, ok := s.pinnedStage(ctx, run, seat.StageIndex)
		if ok {
			rows, err := s.assignments.ListStageAssignments(ctx, run.PolicyVersionID, seat.StageIndex)
			if err != nil {
				return internalErr(ctx, "list stage assignments: %w", err)
			}
			if tally.Resolve(stage, rows) != tally.DecisionReject {
				return nil
			}
		}
	}
	if _, err := s.assignments.SupersedePendingPeers(ctx, run.PolicyVersionID, seat.StageIndex, seat.ID, actor); err != nil {
		return internalErr(ctx, "supersede pending peers: %w", err)
	}
	return nil
}

// recordWithdraw ends every pending seat of the version, marks the run
// withdrawn, cancels the saga run and returns the version to draft in core.
// Without the core write the version would stay in review, so a missing
// setter is a wiring fault, not a skip.
func (s *WorkflowServer) recordWithdraw(ctx context.Context, run store.ApprovalRun, actor string) error {
	if s.assignments == nil {
		return status.Error(codes.Unimplemented, "assignment store not wired")
	}
	if s.coreStatus == nil {
		return status.Error(codes.Unimplemented, "core status setter not wired")
	}
	if _, err := s.assignments.TerminatePendingAssignments(ctx, run.PolicyVersionID, "withdrawn", actor, "policy_withdrawn"); err != nil {
		return internalErr(ctx, "withdraw: terminate pending assignments: %w", err)
	}
	if err := s.runStore.UpdateRunStatus(ctx, run.PolicyVersionID, "withdrawn"); err != nil {
		return internalErr(ctx, "withdraw: update run status: %w", err)
	}
	// The workflow has no withdraw step, so the paused run would never end.
	if err := s.sagaClient.CancelRun(ctx, run.RunID); err != nil {
		return internalErr(ctx, "withdraw: cancel saga run: %w", err)
	}
	if err := s.coreStatus.SetVersionStatus(ctx, run.PolicyVersionID, "draft", actor); err != nil {
		return internalErr(ctx, "withdraw: set core version status to draft: %w", err)
	}
	s.auditEmit.EmitActor(ctx, "workflow.withdrawn", "policy_version:"+run.PolicyVersionID, actor)
	return nil
}

// GetStatus reads the run from the local store.
func (s *WorkflowServer) GetStatus(ctx context.Context, req *workflowv1.GetStatusRequest) (*workflowv1.GetStatusResponse, error) {
	run, err := s.runStore.GetRun(ctx, req.GetPolicyVersionId())
	if err != nil {
		if errors.Is(err, store.ErrRunNotFound) {
			return nil, status.Errorf(codes.NotFound, "no approval run for policy version %q", req.GetPolicyVersionId())
		}
		return nil, internalErr(ctx, "get run: %w", err)
	}
	var stageNames []string
	var wd builder.WorkflowDef
	haveDef := false
	if s.defStore != nil && run.WorkflowDefID != "" {
		if d, derr := s.defStore.GetVersion(ctx, run.WorkflowDefID, run.WorkflowVersion); derr == nil {
			wd, haveDef = d, true
			for _, st := range d.Stages {
				stageNames = append(stageNames, st.Name)
			}
		}
	}
	// With nothing pending the run is past its last stage.
	currentStageIdx := len(stageNames)
	if s.assignments != nil {
		if idx, pending, cerr := s.assignments.CurrentStageIndex(ctx, req.GetPolicyVersionId()); cerr == nil && pending {
			currentStageIdx = idx
		}
	}
	var stageAssignees []*workflowv1.StageAssignees
	var stageUnitProgress []*workflowv1.StageUnitProgressList
	if s.assignments != nil {
		for i := range stageNames {
			rows, aerr := s.assignments.ListStageAssignments(ctx, req.GetPolicyVersionId(), i)
			stage := wd.Stages[i]
			upl := &workflowv1.StageUnitProgressList{}
			for _, unit := range stage.GroupUnits {
				members, state := tally.UnitSeats(unit.GroupID, rows)
				if len(rows) == 0 {
					members = unit.Members
				}
				t := builder.TallyUnit(members, string(unit.InternalQuorum), state)
				upl.Units = append(upl.Units, &workflowv1.StageUnitProgress{
					GroupId:   unit.GroupID,
					Quorum:    quorumToProtoString(builder.Quorum(t.Quorum)),
					Required:  toInt32(t.Required),
					Approvals: toInt32(t.Approvals),
					Pending:   toInt32(t.Pending),
					Roster:    toInt32(t.Roster),
					Status:    t.Status,
				})
			}
			stageUnitProgress = append(stageUnitProgress, upl)

			sa := &workflowv1.StageAssignees{}
			if aerr == nil && len(rows) > 0 {
				for _, r := range rows {
					entry := &workflowv1.StageAssignee{UserId: r.UserID, State: r.State, GroupId: r.GroupID}
					if r.DecidedComment != nil {
						entry.Comment = *r.DecidedComment
					}
					if r.DecidedAt != nil {
						entry.DecidedAt = timestamppb.New(*r.DecidedAt)
					}
					sa.Assignees = append(sa.Assignees, entry)
				}
			} else if haveDef {
				// Not entered yet, so no seats: show who will approve.
				for _, uid := range stage.ApproverIDs {
					sa.Assignees = append(sa.Assignees, &workflowv1.StageAssignee{UserId: uid, State: "pending"})
				}
				for _, unit := range stage.GroupUnits {
					for _, uid := range unit.Members {
						sa.Assignees = append(sa.Assignees, &workflowv1.StageAssignee{UserId: uid, State: "pending", GroupId: unit.GroupID})
					}
				}
			}
			stageAssignees = append(stageAssignees, sa)
		}
	}
	return &workflowv1.GetStatusResponse{
		RunId:             run.RunID,
		Status:            statusToProto(run.Status),
		StageNames:        stageNames,
		CurrentStageIdx:   toInt32(currentStageIdx),
		StageAssignees:    stageAssignees,
		StageUnitProgress: stageUnitProgress,
	}, nil
}

// ListPendingTasks returns the open tasks the approver holds a pending seat
// on. The engine lists every paused run's task; the seats narrow it.
func (s *WorkflowServer) ListPendingTasks(ctx context.Context, req *workflowv1.ListPendingTasksRequest) (*workflowv1.ListPendingTasksResponse, error) {
	tasks, err := s.sagaClient.ListPendingTasks(ctx, req.GetApproverUserId())
	if err != nil {
		return nil, internalErr(ctx, "list pending tasks: %w", err)
	}
	out := make([]*workflowv1.PendingTask, 0, len(tasks))
	for _, t := range tasks {
		if s.assignments != nil && req.GetApproverUserId() != "" {
			ok, err := s.assignments.HasPendingAssignment(ctx, t.PolicyVersionID, int(t.StageIndex), req.GetApproverUserId())
			if err != nil {
				return nil, internalErr(ctx, "filter pending task for approver: %w", err)
			}
			if !ok {
				continue
			}
		}
		pt := &workflowv1.PendingTask{
			TaskId:          t.TaskID,
			RunId:           t.RunID,
			PolicyVersionId: t.PolicyVersionID,
			StageIndex:      t.StageIndex,
		}
		if t.DueAt != nil {
			pt.DueAt = timestamppb.New(*t.DueAt)
		}
		out = append(out, pt)
	}
	return &workflowv1.ListPendingTasksResponse{Tasks: out}, nil
}

// ListUpcomingTasks returns active runs where the approver sits on a stage
// that hasn't started: an individual approver or a group member.
func (s *WorkflowServer) ListUpcomingTasks(ctx context.Context, req *workflowv1.ListUpcomingTasksRequest) (*workflowv1.ListUpcomingTasksResponse, error) {
	out := make([]*workflowv1.UpcomingTask, 0)
	if s.runStore == nil || s.defStore == nil || req.GetApproverUserId() == "" {
		return &workflowv1.ListUpcomingTasksResponse{Tasks: out}, nil
	}
	runs, err := s.runStore.ListActiveRuns(ctx)
	if err != nil {
		return nil, internalErr(ctx, "list active runs: %w", err)
	}
	for _, run := range runs {
		wd, derr := s.defStore.GetVersion(ctx, run.WorkflowDefID, run.WorkflowVersion)
		if derr != nil {
			continue
		}
		current := -1
		if s.assignments != nil {
			if idx, pending, cerr := s.assignments.CurrentStageIndex(ctx, run.PolicyVersionID); cerr == nil && pending {
				current = idx
			}
		}
		for i := current + 1; i < len(wd.Stages); i++ {
			if sitsOn(wd.Stages[i], req.GetApproverUserId()) {
				out = append(out, &workflowv1.UpcomingTask{
					PolicyVersionId: run.PolicyVersionID,
					StageIndex:      toInt32(i),
					StageName:       wd.Stages[i].Name,
				})
			}
		}
	}
	return &workflowv1.ListUpcomingTasksResponse{Tasks: out}, nil
}

func sitsOn(stage builder.Stage, userID string) bool {
	if slices.Contains(stage.ApproverIDs, userID) {
		return true
	}
	for _, u := range stage.GroupUnits {
		if slices.Contains(u.Members, userID) {
			return true
		}
	}
	return false
}

// SwapAssignee reassigns a pending seat.
func (s *WorkflowServer) SwapAssignee(ctx context.Context, req *workflowv1.SwapAssigneeRequest) (*workflowv1.SwapAssigneeResponse, error) {
	if s.assignments == nil {
		return nil, status.Error(codes.Unimplemented, "assignment store not wired")
	}
	if req.GetReason() == "" {
		return nil, errcodes.New(ctx, errcodes.CodeSwapReasonRequired)
	}
	if req.GetCurrentUserId() == "" || req.GetNewUserId() == "" {
		return nil, errcodes.New(ctx, errcodes.CodeSwapUsersRequired)
	}
	if req.GetCurrentUserId() == req.GetNewUserId() {
		return nil, errcodes.New(ctx, errcodes.CodeSwapSameUser)
	}
	actor, ok := s.actorExtractor(ctx)
	if !ok || actor.UserID == "" {
		return nil, status.Error(codes.Unauthenticated, "no actor on context")
	}
	roleStr, err := s.validateInitiatorRole(ctx, req, actor)
	if err != nil {
		return nil, err
	}
	stage, err := s.lookupStage(ctx, req.GetPolicyVersionId(), int(req.GetStageIndex()))
	if err != nil {
		return nil, err
	}
	outOfElig, err := s.checkEligibility(ctx, stage, req, roleStr)
	if err != nil {
		return nil, err
	}

	stageStr := strconv.Itoa(int(req.GetStageIndex()))
	old, err := s.assignments.GetAssignment(ctx, req.GetPolicyVersionId(), int(req.GetStageIndex()), req.GetCurrentUserId(), req.GetGroupId())
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errcodes.New(ctx, errcodes.CodeSwapAssignmentNotFound, "stage", stageStr, "current_user_id", req.GetCurrentUserId())
		}
		return nil, internalErr(ctx, "lookup old assignment: %w", err)
	}
	if old.State != "pending" {
		return nil, errcodes.New(ctx, errcodes.CodeSwapAssignmentNotPending,
			"stage", stageStr, "current_user_id", req.GetCurrentUserId(), "state", old.State)
	}

	slaDeadline, reminder := s.compileOpts.StageSLA(stage, time.Now().UTC())
	newRow, err := s.assignments.Swap(ctx, store.SwapInput{
		OldAssignmentID:  old.ID,
		NewUserID:        req.GetNewUserId(),
		NewSLADeadlineAt: slaDeadline,
		NewReminderAt:    reminder,
		ActorUserID:      actor.UserID,
		ActorRole:        roleStr,
		Reason:           req.GetReason(),
		OutOfEligibility: outOfElig,
	})
	if err != nil {
		return nil, internalErr(ctx, "swap: %w", err)
	}

	action := "workflow.assignment.swapped"
	if outOfElig {
		action = "workflow.assignment.swapped.out_of_eligibility"
	}
	attrs := map[string]string{
		"stage":            stageStr,
		"actor_role":       roleStr,
		"previous_user_id": req.GetCurrentUserId(),
		"new_user_id":      req.GetNewUserId(),
	}
	if req.GetGroupId() != "" {
		attrs["seat_group_id"] = req.GetGroupId()
	}
	s.auditEmit.EmitActorAttrs(ctx, action, "policy_version:"+req.GetPolicyVersionId(), actor.UserID, "", attrs)
	return &workflowv1.SwapAssigneeResponse{
		NewAssignmentId:  newRow.ID,
		NewSlaDeadlineAt: timestamppb.New(newRow.SLADeadlineAt),
	}, nil
}

// BulkDecide decides several of the caller's seats. Every decision needs a
// comment; the successful ones share a bulk_batch_id.
func (s *WorkflowServer) BulkDecide(ctx context.Context, req *workflowv1.BulkDecideRequest) (*workflowv1.BulkDecideResponse, error) {
	if s.assignments == nil {
		return nil, status.Error(codes.Unimplemented, "assignment store not wired")
	}
	actor, ok := s.actorExtractor(ctx)
	if !ok || actor.UserID == "" {
		return nil, status.Error(codes.Unauthenticated, "no actor on context")
	}
	if len(req.GetDecisions()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "decisions must not be empty")
	}
	for i, d := range req.GetDecisions() {
		if d.GetComment() == "" {
			return nil, status.Errorf(codes.InvalidArgument, "decisions[%d]: comment is required", i)
		}
		if d.GetDecision() == workflowv1.DecisionType_DECISION_TYPE_UNSPECIFIED {
			return nil, status.Errorf(codes.InvalidArgument, "decisions[%d]: decision must be APPROVE or REJECT", i)
		}
	}

	batchID := uuid.NewString()
	resp := &workflowv1.BulkDecideResponse{BulkBatchId: batchID, Results: make([]*workflowv1.DecisionResult, 0, len(req.GetDecisions()))}
	for _, d := range req.GetDecisions() {
		dr := &workflowv1.DecisionResult{PolicyVersionId: d.GetPolicyVersionId(), StageIndex: d.GetStageIndex(), GroupId: d.GetGroupId()}
		resp.Results = append(resp.Results, dr)
		seat, err := s.assignments.GetAssignment(ctx, d.GetPolicyVersionId(), int(d.GetStageIndex()), actor.UserID, d.GetGroupId())
		if err != nil {
			dr.Error = "lookup: " + err.Error()
			continue
		}
		decision := "approved"
		if d.GetDecision() == workflowv1.DecisionType_DECISION_TYPE_REJECT {
			decision = "rejected"
		}
		if err := s.assignments.DecideAssignment(ctx, store.DecideAssignmentInput{
			AssignmentID: seat.ID, Decision: decision, Comment: d.GetComment(), ActorUserID: actor.UserID, BulkBatchID: &batchID,
		}); err != nil {
			dr.Error = err.Error()
			continue
		}
		if decision == "rejected" {
			run, rerr := s.runStore.GetRun(ctx, d.GetPolicyVersionId())
			if rerr != nil {
				run = store.ApprovalRun{PolicyVersionID: d.GetPolicyVersionId()}
			}
			if err := s.supersedeIfRejected(ctx, run, seat, actor.UserID); err != nil {
				dr.Error = "supersede: " + err.Error()
				continue
			}
		}
		dr.Ok = true
		dr.AssignmentId = seat.ID
		attrs := map[string]string{"stage": strconv.Itoa(int(d.GetStageIndex())), "decision": decision, "bulk_batch_id": batchID}
		if d.GetGroupId() != "" {
			attrs["seat_group_id"] = d.GetGroupId()
		}
		s.auditEmit.EmitActorAttrs(ctx, "workflow.bulk."+decision, "policy_version:"+d.GetPolicyVersionId(), actor.UserID, "", attrs)
	}
	return resp, nil
}

// GetAssignmentHistory returns one stage's log.
func (s *WorkflowServer) GetAssignmentHistory(ctx context.Context, req *workflowv1.GetAssignmentHistoryRequest) (*workflowv1.GetAssignmentHistoryResponse, error) {
	if s.assignments == nil {
		return nil, status.Error(codes.Unimplemented, "assignment store not wired")
	}
	entries, err := s.assignments.ListHistory(ctx, req.GetPolicyVersionId(), int(req.GetStageIndex()))
	if err != nil {
		return nil, internalErr(ctx, "list history: %w", err)
	}
	out := make([]*workflowv1.AssignmentHistoryEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, &workflowv1.AssignmentHistoryEntry{
			Id:                 e.ID,
			AssignmentId:       e.AssignmentID,
			Event:              e.Event,
			ActorUserId:        e.ActorUserID,
			ActorRole:          e.ActorRole,
			PreviousUserId:     e.PreviousUserID,
			NewUserId:          e.NewUserID,
			OutOfEligibility:   e.OutOfEligibility,
			Reason:             e.Reason,
			CreatedAt:          timestamppb.New(e.CreatedAt),
			ImpersonatedUserId: e.ImpersonatedUserID,
		})
	}
	return &workflowv1.GetAssignmentHistoryResponse{Entries: out}, nil
}

// GetStageEligiblePool returns a seat's pool from the same stage definition
// SwapAssignee checks. Like GetStatus it does no access check; the caller
// gates it.
func (s *WorkflowServer) GetStageEligiblePool(ctx context.Context, req *workflowv1.GetStageEligiblePoolRequest) (*workflowv1.GetStageEligiblePoolResponse, error) {
	stage, err := s.lookupStage(ctx, req.GetPolicyVersionId(), int(req.GetStageIndex()))
	if err != nil {
		return nil, err
	}
	return &workflowv1.GetStageEligiblePoolResponse{
		StageIndex:      req.GetStageIndex(),
		StageName:       stage.Name,
		EligibleUserIds: seatPool(stage, req.GetGroupId()),
	}, nil
}

// seatPool is who may hold a seat: the group's members for a group seat, the
// stage's approvers for an individual one.
func seatPool(stage builder.Stage, groupID string) []string {
	if groupID == "" {
		return slices.Clone(stage.ApproverIDs)
	}
	for _, u := range stage.GroupUnits {
		if u.GroupID == groupID {
			return slices.Clone(u.Members)
		}
	}
	return []string{}
}

// ReassignUserWorkflowItems moves a merged account's in-flight items. The
// actor comes from the request: identity calls it during the merge.
func (s *WorkflowServer) ReassignUserWorkflowItems(ctx context.Context, req *workflowv1.ReassignUserWorkflowItemsRequest) (*workflowv1.ReassignUserWorkflowItemsResponse, error) {
	if s.assignments == nil {
		return nil, status.Error(codes.Unimplemented, "assignment store not wired")
	}
	from, to := req.GetFromUserId(), req.GetToUserId()
	if from == "" || to == "" {
		return nil, status.Error(codes.InvalidArgument, "from_user_id and to_user_id are required")
	}
	if from == to {
		return nil, status.Error(codes.InvalidArgument, "from_user_id and to_user_id are the same")
	}
	res, err := s.assignments.ReassignUserItems(ctx, from, to, req.GetActorUserId(), req.GetDryRun())
	if err != nil {
		return nil, internalErr(ctx, "reassign user workflow items: %w", err)
	}
	if !req.GetDryRun() && res.Total() > 0 {
		s.auditEmit.EmitActorAttrs(ctx, "workflow.user.reassigned", "user:"+from, req.GetActorUserId(), "", map[string]string{
			"from_user_id":           from,
			"to_user_id":             to,
			"assignments_reassigned": strconv.Itoa(res.AssignmentsReassigned),
			"assignments_deduped":    strconv.Itoa(res.AssignmentsDeduped),
			"runs_reassigned":        strconv.Itoa(res.RunsReassigned),
			"merge_operation_id":     req.GetMergeOperationId(),
		})
	}
	return &workflowv1.ReassignUserWorkflowItemsResponse{
		AssignmentsReassigned: toInt32(res.AssignmentsReassigned),
		AssignmentsDeduped:    toInt32(res.AssignmentsDeduped),
		RunsReassigned:        toInt32(res.RunsReassigned),
		Total:                 toInt32(res.Total()),
	}, nil
}

// ResolveRun marks a run terminal and, when its version is no longer the
// definition's current one and no other active run pins it, removes the kept
// version. The cleanup is best effort.
func (s *WorkflowServer) ResolveRun(ctx context.Context, policyVersionID, runStatus string) error {
	run, err := s.runStore.GetRun(ctx, policyVersionID)
	if err != nil {
		return fmt.Errorf("resolve run: get run %q: %w", policyVersionID, err)
	}
	if err := s.runStore.UpdateRunStatus(ctx, policyVersionID, runStatus); err != nil {
		return fmt.Errorf("resolve run: update status: %w", err)
	}
	if s.defStore == nil || s.assignments == nil {
		return nil
	}
	subject := fmt.Sprintf("def:%s version:%d", run.WorkflowDefID, run.WorkflowVersion)
	currentDef, err := s.defStore.Get(ctx, run.WorkflowDefID)
	if err != nil {
		s.auditEmit.Emit(ctx, "workflow.gc.skip.get_def_failed", subject)
		return nil
	}
	if run.WorkflowVersion == currentDef.Version {
		return nil
	}
	active, err := s.assignments.ListActiveRunsByDef(ctx, run.WorkflowDefID, run.WorkflowVersion)
	if err != nil {
		s.auditEmit.Emit(ctx, "workflow.gc.skip.list_active_failed", subject)
		return nil
	}
	if len(active) > 0 {
		return nil
	}
	if err := s.defStore.GCVersion(ctx, run.WorkflowDefID, run.WorkflowVersion); err != nil {
		s.log.Ctx(ctx).Warn("gc of a kept definition version failed", log.F("workflow_def_id", run.WorkflowDefID),
			log.F("workflow_version", run.WorkflowVersion), log.F("error", err.Error()))
		s.auditEmit.Emit(ctx, "workflow.gc.failed", subject)
		return nil
	}
	s.auditEmit.Emit(ctx, "workflow.gc.version", subject)
	return nil
}

// lookupStage returns a run's stage from the definition version it runs on.
func (s *WorkflowServer) lookupStage(ctx context.Context, policyVersionID string, stageIdx int) (builder.Stage, error) {
	run, err := s.runStore.GetRun(ctx, policyVersionID)
	if err != nil {
		return builder.Stage{}, approvalRunNotFound(ctx, policyVersionID)
	}
	wd, err := s.defStore.GetVersion(ctx, run.WorkflowDefID, run.WorkflowVersion)
	if err != nil {
		return builder.Stage{}, internalErr(ctx, "get workflow def: %w", err)
	}
	if stageIdx < 0 || stageIdx >= len(wd.Stages) {
		return builder.Stage{}, errcodes.New(ctx, errcodes.CodeStageIndexOutOfRange,
			"stage", strconv.Itoa(stageIdx), "stage_count", strconv.Itoa(len(wd.Stages)))
	}
	return wd.Stages[stageIdx], nil
}

func approvalRunNotFound(ctx context.Context, policyVersionID string) error {
	return errcodes.New(ctx, errcodes.CodeApprovalRunNotFound, "policy_version_id", policyVersionID)
}

// validateInitiatorRole checks the claimed role against the caller and
// returns its name for the history: admin, self_delegate or workflow_author.
func (s *WorkflowServer) validateInitiatorRole(ctx context.Context, req *workflowv1.SwapAssigneeRequest, actor ActorClaims) (string, error) {
	switch req.GetInitiatorRole() {
	case workflowv1.InitiatorRole_INITIATOR_ROLE_ADMIN:
		admin, err := s.isAdmin(ctx, actor)
		if err != nil {
			return "", err
		}
		if !admin {
			return "", errcodes.New(ctx, errcodes.CodeSwapAssigneeForbidden, "role", "admin")
		}
		return "admin", nil
	case workflowv1.InitiatorRole_INITIATOR_ROLE_SELF_DELEGATE:
		if actor.UserID != req.GetCurrentUserId() {
			return "", errcodes.New(ctx, errcodes.CodeSwapAssigneeForbidden, "role", "self_delegate")
		}
		return "self_delegate", nil
	case workflowv1.InitiatorRole_INITIATOR_ROLE_WORKFLOW_AUTHOR:
		run, err := s.runStore.GetRun(ctx, req.GetPolicyVersionId())
		if err != nil {
			return "", approvalRunNotFound(ctx, req.GetPolicyVersionId())
		}
		wd, err := s.defStore.Get(ctx, run.WorkflowDefID)
		if err != nil {
			return "", internalErr(ctx, "get workflow def: %w", err)
		}
		if wd.AuthorUserID == "" || wd.AuthorUserID != actor.UserID {
			return "", errcodes.New(ctx, errcodes.CodeSwapAssigneeForbidden, "role", "workflow_author")
		}
		return "workflow_author", nil
	default:
		return "", errcodes.New(ctx, errcodes.CodeSwapInitiatorRoleRequired)
	}
}

func (s *WorkflowServer) isAdmin(ctx context.Context, actor ActorClaims) (bool, error) {
	if slices.Contains(actor.Roles, "admin") {
		return true, nil
	}
	if s.admins == nil {
		return false, nil
	}
	ok, err := s.admins.IsWorkflowAdmin(ctx, actor.UserID)
	if err != nil {
		return false, internalErr(ctx, "check admin: %w", err)
	}
	return ok, nil
}

// checkEligibility returns whether the swap is out of eligibility: only an
// admin with out_of_eligibility_ack may pick someone outside the seat's pool.
func (s *WorkflowServer) checkEligibility(ctx context.Context, stage builder.Stage, req *workflowv1.SwapAssigneeRequest, roleStr string) (bool, error) {
	if eligibility.Contains(seatPool(stage, req.GetGroupId()), req.GetNewUserId()) {
		return false, nil
	}
	if roleStr == "admin" && req.GetOutOfEligibilityAck() {
		return true, nil
	}
	return false, errcodes.New(ctx, errcodes.CodeSwapAssigneeNotEligible,
		"stage", strconv.Itoa(int(req.GetStageIndex())), "stage_name", stage.Name, "new_user_id", req.GetNewUserId())
}

// signalName maps a SignalType to the engine signal the outcome switch knows.
func signalName(st workflowv1.SignalType) string {
	switch st {
	case workflowv1.SignalType_SIGNAL_TYPE_APPROVE:
		return "approve"
	case workflowv1.SignalType_SIGNAL_TYPE_REJECT:
		return "reject"
	case workflowv1.SignalType_SIGNAL_TYPE_REQUEST_CHANGES:
		return "request_changes"
	case workflowv1.SignalType_SIGNAL_TYPE_WITHDRAW:
		return "withdraw"
	case workflowv1.SignalType_SIGNAL_TYPE_RETIRE:
		return "retire"
	default:
		return "unknown"
	}
}

// statusToProto maps a stored run status; an unknown one is UNSPECIFIED.
func statusToProto(s string) workflowv1.ApprovalStatus {
	switch s {
	case "draft":
		return workflowv1.ApprovalStatus_APPROVAL_STATUS_DRAFT
	case "in_review":
		return workflowv1.ApprovalStatus_APPROVAL_STATUS_IN_REVIEW
	case "approved":
		return workflowv1.ApprovalStatus_APPROVAL_STATUS_APPROVED
	case "scheduled":
		return workflowv1.ApprovalStatus_APPROVAL_STATUS_SCHEDULED
	case "published":
		return workflowv1.ApprovalStatus_APPROVAL_STATUS_PUBLISHED
	case "superseded":
		return workflowv1.ApprovalStatus_APPROVAL_STATUS_SUPERSEDED
	case "rejected":
		return workflowv1.ApprovalStatus_APPROVAL_STATUS_REJECTED
	case "withdrawn":
		return workflowv1.ApprovalStatus_APPROVAL_STATUS_WITHDRAWN
	case "archived":
		return workflowv1.ApprovalStatus_APPROVAL_STATUS_ARCHIVED
	default:
		return workflowv1.ApprovalStatus_APPROVAL_STATUS_UNSPECIFIED
	}
}
