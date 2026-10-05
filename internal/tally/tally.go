// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package tally decides a stage from its seats.
package tally

import (
	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/store"
)

// The stage outcomes.
const (
	DecisionApprove = "approve"
	DecisionReject  = "reject"
	DecisionPending = "pending"
)

// Resolve tallies a stage from its seats.
//
// Each individual seat is one vote, and a rejection on one rejects the stage.
// Each approve-as-group unit is one vote once its own quorum of yes votes on
// its members' group seats is met; it rejects the stage once that quorum can
// no longer be met. An individual seat never counts toward a group, and a group
// seat never counts as an individual vote. A unit with no seated member (all
// filtered out at assignment) takes no part.
func Resolve(stage builder.Stage, rows []store.AssignmentRow) string {
	if len(stage.GroupUnits) == 0 {
		return resolveFlat(stage, rows)
	}
	satisfied, total := 0, 0
	for _, a := range rows {
		if a.GroupID != "" {
			continue
		}
		switch a.State {
		case "superseded", "swapped_out":
			continue
		case "rejected":
			return DecisionReject
		case "approved":
			satisfied++
		}
		total++
	}
	for _, unit := range stage.GroupUnits {
		members, state := UnitSeats(unit.GroupID, rows)
		if len(members) == 0 {
			continue
		}
		total++
		switch builder.TallyUnit(members, string(unit.InternalQuorum), state).Status {
		case builder.UnitSatisfied:
			satisfied++
		case builder.UnitRejected:
			return DecisionReject
		}
	}
	if builder.EvaluateQuorum(stage.Quorum, stage.QuorumN, satisfied, total) {
		return DecisionApprove
	}
	return DecisionPending
}

// UnitSeats returns a group's seat holders and their seat states. A seat
// swapped to someone else is replaced by the new holder's seat.
func UnitSeats(groupID string, rows []store.AssignmentRow) (members []string, state map[string]string) {
	state = map[string]string{}
	for _, a := range rows {
		if a.GroupID != groupID || a.State == "swapped_out" {
			continue
		}
		members = append(members, a.UserID)
		state[a.UserID] = a.State
	}
	return members, state
}

func resolveFlat(stage builder.Stage, rows []store.AssignmentRow) string {
	var approved, pool int
	for _, a := range rows {
		switch a.State {
		case "rejected":
			return DecisionReject
		case "superseded", "swapped_out":
			continue
		case "approved":
			approved++
			pool++
		default:
			pool++
		}
	}
	if builder.EvaluateQuorum(stage.Quorum, stage.QuorumN, approved, pool) {
		return DecisionApprove
	}
	return DecisionPending
}
