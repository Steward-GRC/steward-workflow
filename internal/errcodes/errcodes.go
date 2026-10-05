// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package errcodes holds the workflow service's coded errors (band 6) and turns
// them into gRPC statuses through go-apperr.
package errcodes

import (
	"context"
	"sync"

	apperr "github.com/Bugs5382/go-apperr"
	"github.com/Bugs5382/go-apperr/apperrgrpc"
	log "github.com/Bugs5382/go-log"
)

// Domain is the ErrorInfo domain every workflow error carries.
const Domain = "workflow"

// The workflow service's codes.
const (
	CodeInternal                  = 6000
	CodeSwapAssigneeForbidden     = 6002
	CodeStageNotStaffed           = 6003
	CodeSwapReasonRequired        = 6004
	CodeSwapUsersRequired         = 6005
	CodeSwapSameUser              = 6006
	CodeSwapInitiatorRoleRequired = 6007
	CodeSwapAssignmentNotFound    = 6008
	CodeSwapAssignmentNotPending  = 6009
	CodeSwapAssigneeNotEligible   = 6010
	CodeStageIndexOutOfRange      = 6011
	CodeApprovalRunNotFound       = 6012
)

// Entries returns the registry entries.
func Entries() []apperr.Entry {
	return []apperr.Entry{
		{Code: CodeInternal, Symbol: "INTERNAL", Category: apperr.CategoryInternal,
			Title: "workflow", Cause: "an uncoded failure inside the workflow service"},
		{Code: CodeSwapAssigneeForbidden, Symbol: "SWAP_ASSIGNEE_FORBIDDEN", Category: apperr.CategoryPermissionDenied,
			Title: "reassign", Cause: "the caller doesn't hold the initiator role it claimed: not an admin, not the seat holder, or not the workflow's author",
			UserSafe: true, Message: "You don't have permission to reassign this approval."},
		{Code: CodeStageNotStaffed, Symbol: "STAGE_NOT_STAFFED", Category: apperr.CategoryFailedPrecondition,
			Title: "submit", Cause: "a stage has no approver once its individual approvers and group members are resolved, so a run would never finish",
			UserSafe: true, Message: "This workflow can't run yet: stage {stage} ({stage_name}) has no approver. Every required step needs at least one approver before the workflow can run."},
		{Code: CodeSwapReasonRequired, Symbol: "SWAP_REASON_REQUIRED", Category: apperr.CategoryInvalid,
			Title: "reassign", Cause: "the reassignment has no reason; every reassignment is audited with one",
			UserSafe: true, Message: "Give a reason for the reassignment."},
		{Code: CodeSwapUsersRequired, Symbol: "SWAP_USERS_REQUIRED", Category: apperr.CategoryInvalid,
			Title: "reassign", Cause: "the reassignment names no current or no new approver",
			UserSafe: true, Message: "Choose both the current approver and the person to reassign to."},
		{Code: CodeSwapSameUser, Symbol: "SWAP_SAME_USER", Category: apperr.CategoryInvalid,
			Title: "reassign", Cause: "the new approver is the current one",
			UserSafe: true, Message: "That person already holds this approval. Choose someone else."},
		{Code: CodeSwapInitiatorRoleRequired, Symbol: "SWAP_INITIATOR_ROLE_REQUIRED", Category: apperr.CategoryInvalid,
			Title: "reassign", Cause: "the request didn't set initiator_role; the gateway always sets it, so this is a client bug"},
		{Code: CodeSwapAssignmentNotFound, Symbol: "SWAP_ASSIGNMENT_NOT_FOUND", Category: apperr.CategoryNotFound,
			Title: "reassign", Cause: "the person being replaced holds no seat on the stage, usually because the run moved on since the page loaded",
			UserSafe: true, Message: "The person you're replacing isn't assigned to stage {stage} any more. Refresh and try again."},
		{Code: CodeSwapAssignmentNotPending, Symbol: "SWAP_ASSIGNMENT_NOT_PENDING", Category: apperr.CategoryFailedPrecondition,
			Title: "reassign", Cause: "the seat has already been acted on; only a pending seat can be reassigned",
			UserSafe: true, Message: "This approval is already {state}, so it can't be reassigned."},
		{Code: CodeSwapAssigneeNotEligible, Symbol: "SWAP_ASSIGNEE_NOT_ELIGIBLE", Category: apperr.CategoryFailedPrecondition,
			Title: "reassign", Cause: "the new approver isn't in the seat's pool and the request isn't an acknowledged admin reassignment",
			UserSafe: true, Message: "That person isn't an eligible approver for stage {stage} ({stage_name})."},
		{Code: CodeStageIndexOutOfRange, Symbol: "STAGE_INDEX_OUT_OF_RANGE", Category: apperr.CategoryInvalid,
			Title: "stage", Cause: "the stage index is below zero or past the run's stage count, usually from a page loaded before the definition changed",
			UserSafe: true, Message: "Stage {stage} isn't part of this workflow. Refresh and try again."},
		{Code: CodeApprovalRunNotFound, Symbol: "APPROVAL_RUN_NOT_FOUND", Category: apperr.CategoryNotFound,
			Title: "stage", Cause: "the policy version has no approval run, usually because it was never submitted",
			UserSafe: true, Message: "This policy version has no approval run. Refresh and try again."},
	}
}

var (
	regOnce sync.Once
	reg     *apperr.Registry
)

// Registry returns the service registry. Coded errors are logged through
// go-log with the trace of the request they failed.
func Registry() *apperr.Registry {
	regOnce.Do(func() {
		r, err := apperr.NewRegistry(Entries(), apperr.WithService(6), apperr.WithCodeDigits(4),
			apperr.WithLogger(logSink{log.NewLogger("workflow")}))
		if err != nil {
			panic(err)
		}
		reg = r
	})
	return reg
}

// Error turns err into the gRPC error a handler returns.
func Error(ctx context.Context, err error) error {
	return apperrgrpc.Error(ctx, Registry(), err, CodeInternal, Domain)
}

// New returns the gRPC error for code with the given metadata pairs, in
// key, value order.
func New(ctx context.Context, code int, kv ...string) error {
	pairs := make([]apperr.MetaPair, 0, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		pairs = append(pairs, apperr.Meta(kv[i], kv[i+1]))
	}
	entry, _ := Registry().Describe(code)
	return Error(ctx, apperr.WithMeta(apperr.Coded(code, refusal(entry.Symbol)), pairs...))
}

// Doc is the Markdown body of docs/error-codes.md.
func Doc() string {
	return "# Error codes\n\nEvery coded gRPC error from the workflow service carries an `ErrorInfo` with the symbol as\n" +
		"its reason, the domain `" + Domain + "` and the code in `codeNum`. Only user-safe messages reach\n" +
		"the caller; every other code is sent as `Code N: Internal Error`.\n\n" + Registry().Markdown()
}

type refusal string

func (r refusal) Error() string { return "workflow: " + string(r) }

type logSink struct{ l log.Logger }

func (s logSink) LogCoded(ctx context.Context, code int, err error) {
	s.l.Ctx(ctx).Debug("coded error", log.F("code", code), log.F("error", err.Error()))
}
