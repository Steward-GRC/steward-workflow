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

// Resolve tallies a stage from its seats, by the mock server's rule.
//
// A rejected veto seat ends the stage at once, before any quorum is scored. A
// seat holds the veto when its person is one of the stage's named veto people
// and was seated in its approver pool; any one holder rejecting ends the run
// (steward-workflow#26, #64).
//
// The individual seats are scored against the stage quorum on their own. Each
// one is a vote, and a rejection on one is a vote against, not the end of the
// stage: it only takes that seat's approval out of reach. The individual seats
// end the stage rejected once the approvals still possible can no longer reach
// the stage quorum, so a majority of three survives one rejection while a
// majority of two does not.
//
// Every approve-as-group unit must also be met, each on its own quorum of yes
// votes over its members' group seats; the stage quorum does not apply to the
// units. A unit that can no longer reach its quorum rejects the stage. An
// individual seat never counts toward a group, and a group seat never counts as
// an individual vote. A unit with no seated member (all filtered out at
// assignment) takes no part.
func Resolve(stage builder.Stage, rows []store.AssignmentRow) string {
	// approved is the individual seats already for; pool is every individual
	// seat (rejected ones included, as a majority is of the whole stage); lost
	// is the seats that can no longer approve.
	approved, pool, lost := 0, 0, 0
	if VetoedBy(rows) != "" {
		// A reject from a veto seat ends the stage at once, whatever the quorum.
		return DecisionReject
	}
	for _, a := range rows {
		if a.GroupID != "" {
			continue
		}
		switch a.State {
		case "superseded", "swapped_out":
			continue
		case "rejected":
			lost++
		case "approved":
			approved++
		}
		pool++
	}
	unitsMet, units := true, 0
	for _, unit := range stage.GroupUnits {
		members, state := UnitSeats(unit.GroupID, rows)
		if len(members) == 0 {
			continue
		}
		units++
		switch builder.TallyUnit(members, string(unit.InternalQuorum), state).Status {
		case builder.UnitSatisfied:
		case builder.UnitRejected:
			// A group that can no longer reach its own quorum ends the stage.
			return DecisionReject
		default:
			unitsMet = false
		}
	}
	// Reject only on evidence: a rejection was cast and, with every other open
	// individual seat approving, the stage quorum is still short.
	if lost > 0 && !builder.EvaluateQuorum(stage.Quorum, stage.QuorumN, pool-lost, pool) {
		return DecisionReject
	}
	if pool == 0 && units == 0 {
		return DecisionPending
	}
	if unitsMet && (pool == 0 || builder.EvaluateQuorum(stage.Quorum, stage.QuorumN, approved, pool)) {
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

// VetoedBy returns the user whose reject from a veto seat ended the stage, or
// "" when no veto seat has rejected. Seats carry the mark only for a stage's
// named veto people who were seated, so a stage without any never reports one.
func VetoedBy(rows []store.AssignmentRow) string {
	for _, a := range rows {
		if a.Veto && a.State == "rejected" {
			return a.UserID
		}
	}
	return ""
}
