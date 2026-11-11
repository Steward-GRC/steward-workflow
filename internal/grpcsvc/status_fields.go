// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package grpcsvc

import (
	"google.golang.org/protobuf/types/known/timestamppb"

	workflowv1 "github.com/Steward-GRC/steward-workflow/gen/go/steward/workflow/v1"
	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/store"
)

// stageRuleName is the stage rule as GetStatus names it: one, majority, all or
// nofm. The n-of-m rule keeps its own name because it carries a number.
func stageRuleName(q builder.Quorum) string {
	if q == builder.QuorumNofM {
		return "nofm"
	}
	return quorumToProtoString(q)
}

// stageRequired is the individual approvals a stage needs under its rule out of
// a pool of individual seats. It is the same rule tally.Resolve scores with.
func stageRequired(stage builder.Stage, pool int) int {
	switch stage.Quorum {
	case builder.QuorumAll:
		return pool
	case builder.QuorumAny:
		return 1
	case builder.QuorumNofM:
		return stage.QuorumN
	default:
		return pool/2 + 1
	}
}

// stageProgress builds one stage's due date, rule and individual approval
// count. rows are the stage's seats. A stage not yet entered has no due date
// (the deadline is fixed when the seats are made) and its pool is
// unenteredPool, the individual approvers the run's categories resolve; an
// entered stage counts the seats made, none when nobody was seated. Group seats
// are not counted here: StageUnitProgress carries each group's own tally.
func stageProgress(stage builder.Stage, rows []store.AssignmentRow, entered bool, unenteredPool int) *workflowv1.StageProgress {
	out := &workflowv1.StageProgress{Quorum: stageRuleName(stage.Quorum)}
	if stage.Quorum == builder.QuorumNofM {
		out.QuorumN = toInt32(stage.QuorumN)
	}
	approvals, pool := 0, 0
	for _, r := range rows {
		if r.State == "superseded" || r.State == "swapped_out" {
			continue
		}
		if !r.SLADeadlineAt.IsZero() && (out.DueAt == nil || r.SLADeadlineAt.Before(out.DueAt.AsTime())) {
			out.DueAt = timestamppb.New(r.SLADeadlineAt)
		}
		if r.GroupID != "" {
			continue
		}
		pool++
		if r.State == "approved" {
			approvals++
		}
	}
	if !entered {
		pool = unenteredPool
	}
	out.Approvals = toInt32(approvals)
	out.Required = toInt32(stageRequired(stage, pool))
	return out
}
