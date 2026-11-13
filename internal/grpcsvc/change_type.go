// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc

import (
	"context"
	"slices"
	"strings"

	log "github.com/Bugs5382/go-log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	workflowv1 "github.com/Steward-GRC/steward-workflow/gen/go/steward/workflow/v1"
	"github.com/Steward-GRC/steward-workflow/internal/store"
	"github.com/Steward-GRC/steward-workflow/internal/tally"
)

// agreeingStage is the stage whose quorum agrees to lower a change type: the
// first one. Core records the lowering as a request while the version is in
// review; the people who sign off first are the ones who reviewed the change
// itself, and later stages see the type that stands. steward-workflow#10.
const agreeingStage = 0

// changeTypeName is the run's stored form of a change type: "major", "minor"
// or "fix", empty for none.
func changeTypeName(ct workflowv1.ChangeType) string {
	switch ct {
	case workflowv1.ChangeType_CHANGE_TYPE_MAJOR:
		return "major"
	case workflowv1.ChangeType_CHANGE_TYPE_MINOR:
		return "minor"
	case workflowv1.ChangeType_CHANGE_TYPE_FIX:
		return "fix"
	default:
		return ""
	}
}

// agreeChangeTypeOnQuorum records the approvers' agreement to a pending change
// type lowering when the approval about to be stored completes the first
// stage's quorum. It runs before the seat is decided, tallying the stage as it
// will stand once the seat approves, so a refusal from core stops the decision
// and nothing is half done. The agreement is written once per run.
//
// Core answering FAILED_PRECONDITION means no lowering is pending any more (the
// author dropped the pick, or it was applied): nothing is left to agree, so the
// run is marked done and the decision goes on. Any other refusal from core is
// returned to the approver as it came.
func (s *WorkflowServer) agreeChangeTypeOnQuorum(ctx context.Context, run store.ApprovalRun, seat store.AssignmentRow, actor string) error {
	lg := s.log.Ctx(ctx).With(log.F("policy_version_id", run.PolicyVersionID), log.F("run_id", run.RunID),
		log.F("stage", seat.StageIndex), log.F("actor_user_id", actor))
	if run.RequestedChangeType == "" || run.ChangeTypeAgreedAt != nil || seat.StageIndex != agreeingStage {
		lg.Debug("change type: no agreement due", log.F("requested", run.RequestedChangeType),
			log.F("already_agreed", run.ChangeTypeAgreedAt != nil))
		return nil
	}
	if s.coreStatus == nil {
		return status.Error(codes.Unimplemented, "core status setter not wired")
	}
	stage, ok := s.pinnedStage(ctx, run, seat.StageIndex)
	if !ok {
		lg.Warn("change type: the pinned stage can't be read, so no agreement is recorded")
		return nil
	}
	rows, err := s.assignments.ListStageAssignments(ctx, run.PolicyVersionID, seat.StageIndex)
	if err != nil {
		return internalErr(ctx, "list stage assignments: %w", err)
	}
	// The stage as it will stand once this seat approves.
	after := slices.Clone(rows)
	for i := range after {
		if after[i].ID == seat.ID {
			after[i].State = "approved"
		}
	}
	outcome := tally.Resolve(stage, after)
	lg.Debug("change type: tallied the first stage", log.F("outcome", outcome), log.F("requested", run.RequestedChangeType))
	if outcome != tally.DecisionApprove {
		return nil
	}

	var agreed []string
	for _, r := range after {
		if r.State == "approved" && !slices.Contains(agreed, r.UserID) {
			agreed = append(agreed, r.UserID)
		}
	}
	lg.Info("change type: the first stage's quorum agrees, recording it in core",
		log.F("requested", run.RequestedChangeType), log.F("approvers", len(agreed)))
	if err := s.coreStatus.RecordChangeTypeAgreement(ctx, run.PolicyVersionID, agreed, actor); err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			lg.Warn("change type: core holds no lowering, nothing to agree", log.F("error", err.Error()))
			if _, merr := s.runStore.MarkChangeTypeAgreed(ctx, run.PolicyVersionID); merr != nil {
				lg.Warn("change type: could not mark the run done", log.F("error", merr.Error()))
			}
			return nil
		}
		lg.Error(err, "change type: core refused the agreement")
		return passCoreRefusal(ctx, err, "record change type agreement")
	}
	if _, err := s.runStore.MarkChangeTypeAgreed(ctx, run.PolicyVersionID); err != nil {
		// Core already applied the lowering; the mark only stops a repeat.
		lg.Warn("change type: agreement recorded in core but the run could not be marked", log.F("error", err.Error()))
	}
	s.auditEmit.EmitActorAttrs(ctx, "workflow.change_type.agreed", "policy_version:"+run.PolicyVersionID,
		actor, run.HomeCategoryID, map[string]string{
			"run_id": run.RunID, "stage": "0", "change_type": run.RequestedChangeType,
			"agreed_by": strings.Join(agreed, ","),
		})
	return nil
}

// passCoreRefusal returns a refusal core made as the caller's own error, with
// its code and message, for the codes that mean the request itself was wrong.
// Anything else is an internal failure.
func passCoreRefusal(ctx context.Context, err error, what string) error {
	switch status.Code(err) {
	case codes.InvalidArgument, codes.FailedPrecondition, codes.NotFound, codes.PermissionDenied:
		return status.Errorf(status.Code(err), "%s: %s", what, status.Convert(err).Message())
	}
	return internalErr(ctx, what+": %w", err)
}
