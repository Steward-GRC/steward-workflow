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
// approves the stage, and an individual rejection is a lost vote.
func TestResolve_StageQuorumOverUnits(t *testing.T) {
	stage := unitStage(builder.QuorumAny, gu("g", builder.QuorumAll, "a", "b"))
	group := seatsOf("g", map[string]string{"a": "approved", "b": "approved"})

	pending := append(group, store.AssignmentRow{UserID: "ind", State: "pending"})
	if got := Resolve(stage, pending); got != DecisionApprove {
		t.Fatalf("got %q want approve (one group satisfied under any)", got)
	}
	// An individual rejection is a lost vote, not the end of the stage: the
	// satisfied group already meets the quorum of one.
	rejected := append(seatsOf("g", map[string]string{"a": "approved", "b": "approved"}), store.AssignmentRow{UserID: "ind", State: "rejected"})
	if got := Resolve(stage, rejected); got != DecisionApprove {
		t.Fatalf("got %q want approve (group satisfied, individual seat rejected is one lost vote)", got)
	}
	// With the group still open, the same rejection leaves the quorum reachable.
	open := append(seatsOf("g", map[string]string{"a": "pending", "b": "pending"}), store.AssignmentRow{UserID: "ind", State: "rejected"})
	if got := Resolve(stage, open); got != DecisionPending {
		t.Fatalf("got %q want pending (the group can still satisfy the quorum of one)", got)
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

// seatRows builds individual seats from states in order.
func seatRows(states ...string) []store.AssignmentRow {
	rows := make([]store.AssignmentRow, 0, len(states))
	for i, s := range states {
		rows = append(rows, store.AssignmentRow{UserID: string(rune('a' + i)), State: s})
	}
	return rows
}

// A rejection is a vote: the stage ends rejected only when the approvals still
// possible cannot reach the quorum. Mirrors the mock server's author-loop
// tests for deciding an approval (#39).
func TestResolve_RejectIsAVote(t *testing.T) {
	cases := []struct {
		name  string
		stage builder.Stage
		seats []string
		want  string
	}{
		{"majority of two, one reject: out of reach", builder.Stage{Quorum: builder.QuorumMajority}, []string{"rejected", "pending"}, DecisionReject},
		{"majority of two, approve then reject: out of reach", builder.Stage{Quorum: builder.QuorumMajority}, []string{"approved", "rejected"}, DecisionReject},
		{"majority of three, one reject: still reachable", builder.Stage{Quorum: builder.QuorumMajority}, []string{"rejected", "pending", "pending"}, DecisionPending},
		{"majority of three, reject then two approve: met", builder.Stage{Quorum: builder.QuorumMajority}, []string{"rejected", "approved", "approved"}, DecisionApprove},
		{"majority of three, two rejects: out of reach", builder.Stage{Quorum: builder.QuorumMajority}, []string{"rejected", "rejected", "pending"}, DecisionReject},
		{"one seat, rejected: quorum of one out of reach", builder.Stage{Quorum: builder.QuorumAny}, []string{"rejected"}, DecisionReject},
		{"any of two, one reject: still reachable", builder.Stage{Quorum: builder.QuorumAny}, []string{"rejected", "pending"}, DecisionPending},
		{"any of two, both rejected: out of reach", builder.Stage{Quorum: builder.QuorumAny}, []string{"rejected", "rejected"}, DecisionReject},
		{"all of three, one reject: out of reach", builder.Stage{Quorum: builder.QuorumAll}, []string{"approved", "rejected", "pending"}, DecisionReject},
		{"2-of-3, one reject: still reachable", builder.Stage{Quorum: builder.QuorumNofM, QuorumN: 2}, []string{"rejected", "pending", "pending"}, DecisionPending},
		{"2-of-3, two rejects: out of reach", builder.Stage{Quorum: builder.QuorumNofM, QuorumN: 2}, []string{"rejected", "rejected", "pending"}, DecisionReject},
		{"paused seat still counts as open", builder.Stage{Quorum: builder.QuorumMajority}, []string{"rejected", "paused", "pending"}, DecisionPending},
		{"superseded seat is not in the pool", builder.Stage{Quorum: builder.QuorumAny}, []string{"rejected", "superseded"}, DecisionReject},
		{"no rejection and no seats stays pending", builder.Stage{Quorum: builder.QuorumAll}, nil, DecisionPending},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Resolve(tc.stage, seatRows(tc.seats...)); got != tc.want {
				t.Fatalf("seats=%v: got %q want %q", tc.seats, got, tc.want)
			}
		})
	}
}

// With group units on the stage an individual rejection is still just a lost
// vote in the combined pool of individual seats and units.
func TestResolve_RejectIsAVote_WithGroupUnits(t *testing.T) {
	// all of: one individual and one group; the individual rejects: out of reach.
	allStage := unitStage(builder.QuorumAll, gu("g", builder.QuorumAny, "m"))
	rows := append([]store.AssignmentRow{{UserID: "ind", State: "rejected"}}, seatsOf("g", map[string]string{"m": "pending"})...)
	if got := Resolve(allStage, rows); got != DecisionReject {
		t.Fatalf("all over ind+group, ind rejected: got %q want reject", got)
	}
	// majority of three votes (two individuals, one group): one individual
	// rejects, two approvals are still possible.
	majStage := unitStage(builder.QuorumMajority, gu("g", builder.QuorumAny, "m"))
	rows = append([]store.AssignmentRow{{UserID: "i1", State: "rejected"}, {UserID: "i2", State: "pending"}},
		seatsOf("g", map[string]string{"m": "pending"})...)
	if got := Resolve(majStage, rows); got != DecisionPending {
		t.Fatalf("majority of three votes, one lost: got %q want pending", got)
	}
}
