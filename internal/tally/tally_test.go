// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package tally

import (
	"testing"

	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/store"
)

// seatsOf builds one group's seats from a user->state map.
func seatsOf(group string, states map[string]string) []store.AssignmentRow {
	out := make([]store.AssignmentRow, 0, len(states))
	for u, s := range states {
		out = append(out, store.AssignmentRow{UserID: u, GroupID: group, State: s})
	}
	return out
}

func unitStage(q builder.Quorum, units ...builder.GroupUnit) builder.Stage {
	return builder.Stage{Quorum: q, GroupUnits: units}
}

func gu(id string, iq builder.Quorum, members ...string) builder.GroupUnit {
	return builder.GroupUnit{GroupID: id, InternalQuorum: iq, Members: members}
}

// One group under QuorumAll, so the stage outcome is the group's: satisfied
// approves, unreachable rejects, otherwise pending.
func TestResolve_GroupUnitMatrix(t *testing.T) {
	cases := []struct {
		name   string
		iq     builder.Quorum
		states map[string]string
		want   string
	}{
		{"one satisfied", builder.QuorumAny, map[string]string{"a": "approved", "b": "pending", "c": "pending"}, DecisionApprove},
		{"one pending", builder.QuorumAny, map[string]string{"a": "pending", "b": "pending", "c": "pending"}, DecisionPending},
		{"one unreachable", builder.QuorumAny, map[string]string{"a": "rejected", "b": "rejected", "c": "rejected"}, DecisionReject},
		{"majority satisfied", builder.QuorumMajority, map[string]string{"a": "approved", "b": "approved", "c": "pending"}, DecisionApprove},
		{"majority pending", builder.QuorumMajority, map[string]string{"a": "approved", "b": "pending", "c": "pending"}, DecisionPending},
		{"majority unreachable", builder.QuorumMajority, map[string]string{"a": "approved", "b": "rejected", "c": "rejected"}, DecisionReject},
		{"all satisfied", builder.QuorumAll, map[string]string{"a": "approved", "b": "approved", "c": "approved"}, DecisionApprove},
		{"all pending", builder.QuorumAll, map[string]string{"a": "approved", "b": "approved", "c": "pending"}, DecisionPending},
		{"all unreachable", builder.QuorumAll, map[string]string{"a": "approved", "b": "approved", "c": "rejected"}, DecisionReject},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stage := unitStage(builder.QuorumAll, gu("g", tc.iq, "a", "b", "c"))
			if got := Resolve(stage, seatsOf("g", tc.states)); got != tc.want {
				t.Fatalf("iq=%s states=%v: got %q want %q", tc.iq, tc.states, got, tc.want)
			}
		})
	}
}

// An individual seat plus a group under QuorumAny: one satisfied group
// approves the stage, and an individual rejection rejects it.
func TestResolve_StageQuorumOverUnits(t *testing.T) {
	stage := unitStage(builder.QuorumAny, gu("g", builder.QuorumAll, "a", "b"))
	group := seatsOf("g", map[string]string{"a": "approved", "b": "approved"})

	pending := append(group, store.AssignmentRow{UserID: "ind", State: "pending"})
	if got := Resolve(stage, pending); got != DecisionApprove {
		t.Fatalf("got %q want approve (one group satisfied under any)", got)
	}
	rejected := append(seatsOf("g", map[string]string{"a": "approved", "b": "approved"}), store.AssignmentRow{UserID: "ind", State: "rejected"})
	if got := Resolve(stage, rejected); got != DecisionReject {
		t.Fatalf("got %q want reject (individual seat rejected)", got)
	}
}

// The owner's rule: individual and group approvals are separate. Carol is an
// individual approver and the only member of a group on the same stage.
func TestResolve_IndividualApprovalNeverCountsForTheGroup(t *testing.T) {
	stage := unitStage(builder.QuorumAll, gu("finance-team", builder.QuorumAny, "user-carol"))
	rows := []store.AssignmentRow{
		{UserID: "user-carol", State: "approved"},
		{UserID: "user-carol", GroupID: "finance-team", State: "pending"},
	}
	if got := Resolve(stage, rows); got != DecisionPending {
		t.Fatalf("got %q want pending: the group hasn't voted", got)
	}

	rows[1].State = "approved"
	if got := Resolve(stage, rows); got != DecisionApprove {
		t.Fatalf("got %q want approve once both seats approve", got)
	}
}

func TestResolve_GroupApprovalNeverCountsAsTheIndividualVote(t *testing.T) {
	stage := unitStage(builder.QuorumAll, gu("finance-team", builder.QuorumAny, "user-carol"))
	rows := []store.AssignmentRow{
		{UserID: "user-carol", State: "pending"},
		{UserID: "user-carol", GroupID: "finance-team", State: "approved"},
	}
	if got := Resolve(stage, rows); got != DecisionPending {
		t.Fatalf("got %q want pending: the individual seat hasn't voted", got)
	}
}

// A person in two groups votes in each group separately.
func TestResolve_EachGroupSeatVotesSeparately(t *testing.T) {
	stage := unitStage(builder.QuorumAll, gu("gA", builder.QuorumAny, "user-carol"), gu("gB", builder.QuorumAny, "user-carol"))
	rows := []store.AssignmentRow{
		{UserID: "user-carol", GroupID: "gA", State: "approved"},
		{UserID: "user-carol", GroupID: "gB", State: "pending"},
	}
	if got := Resolve(stage, rows); got != DecisionPending {
		t.Fatalf("got %q want pending: group B hasn't voted", got)
	}
}

// A member's rejection only removes their yes vote: the group can still
// reach its quorum, so the stage isn't rejected yet.
func TestResolve_GroupMemberRejectionKeepsTheGroupOpen(t *testing.T) {
	stage := unitStage(builder.QuorumAll, gu("g", builder.QuorumMajority, "a", "b", "c"))
	rows := seatsOf("g", map[string]string{"a": "rejected", "b": "approved", "c": "pending"})
	if got := Resolve(stage, rows); got != DecisionPending {
		t.Fatalf("got %q want pending", got)
	}
}

func TestResolve_SwappedSeatIsReplaced(t *testing.T) {
	stage := unitStage(builder.QuorumAll, gu("g", builder.QuorumAll, "a", "b"))
	rows := []store.AssignmentRow{
		{UserID: "a", GroupID: "g", State: "swapped_out"},
		{UserID: "z", GroupID: "g", State: "approved"},
		{UserID: "b", GroupID: "g", State: "approved"},
	}
	if got := Resolve(stage, rows); got != DecisionApprove {
		t.Fatalf("got %q want approve: the replacement holds the seat", got)
	}
}

func TestResolve_GroupWithNoSeatsTakesNoPart(t *testing.T) {
	stage := unitStage(builder.QuorumAll, gu("g", builder.QuorumAll, "a"))
	rows := []store.AssignmentRow{{UserID: "ind", State: "approved"}}
	if got := Resolve(stage, rows); got != DecisionApprove {
		t.Fatalf("got %q want approve", got)
	}
}
