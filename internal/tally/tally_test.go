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

// A mixed stage follows the mock server (#42): the individual seats are scored
// against the stage quorum on their own, and every group unit must be met on
// its own quorum. A satisfied group never stands in for the individual seats.
func TestResolve_MixedStage_GroupMetWaitsForTheIndividualSeat(t *testing.T) {
	// Mock: "satisfies a group once enough members approve, and the run waits
	// for the other group and the user".
	stage := unitStage(builder.QuorumAny, gu("g", builder.QuorumAll, "a", "b"))
	group := seatsOf("g", map[string]string{"a": "approved", "b": "approved"})
	rows := append(group, store.AssignmentRow{UserID: "ind", State: "pending"})
	if got := Resolve(stage, rows); got != DecisionPending {
		t.Fatalf("got %q want pending: the individual seat still has to meet the quorum", got)
	}
	rows[len(rows)-1].State = "approved"
	if got := Resolve(stage, rows); got != DecisionApprove {
		t.Fatalf("got %q want approve: individual quorum met and the group satisfied", got)
	}
}

func TestResolve_MixedStage_IndividualsMetWaitForEveryGroup(t *testing.T) {
	stage := unitStage(builder.QuorumAny,
		gu("g1", builder.QuorumAny, "a"), gu("g2", builder.QuorumAny, "b"))
	rows := append(seatsOf("g1", map[string]string{"a": "approved"}),
		append(seatsOf("g2", map[string]string{"b": "pending"}),
			store.AssignmentRow{UserID: "ind", State: "approved"})...)
	if got := Resolve(stage, rows); got != DecisionPending {
		t.Fatalf("got %q want pending: group g2 has not voted", got)
	}
	rows[1] = store.AssignmentRow{UserID: "b", GroupID: "g2", State: "approved"}
	if got := Resolve(stage, rows); got != DecisionApprove {
		t.Fatalf("got %q want approve: every group met", got)
	}
}

// Mock: "reports a half-voted group and a group nobody has voted in".
func TestResolve_MixedStage_HalfVotedAndUntouchedGroupsStayPending(t *testing.T) {
	stage := unitStage(builder.QuorumAll,
		gu("half", builder.QuorumAll, "a", "b"), gu("none", builder.QuorumAny, "c"))
	rows := append(seatsOf("half", map[string]string{"a": "approved", "b": "pending"}),
		append(seatsOf("none", map[string]string{"c": "pending"}),
			store.AssignmentRow{UserID: "ind", State: "approved"})...)
	if got := Resolve(stage, rows); got != DecisionPending {
		t.Fatalf("got %q want pending", got)
	}
}

// The individual seats use the stage quorum over themselves only: a majority
// of three individuals ignores how many group units sit beside them.
func TestResolve_MixedStage_QuorumCountsIndividualSeatsOnly(t *testing.T) {
	stage := unitStage(builder.QuorumMajority, gu("g", builder.QuorumAny, "m"))
	group := seatsOf("g", map[string]string{"m": "approved"})
	two := append(group, seatRows("approved", "approved", "pending")...)
	if got := Resolve(stage, two); got != DecisionApprove {
		t.Fatalf("got %q want approve: 2 of 3 individuals plus the group", got)
	}
	one := append(seatsOf("g", map[string]string{"m": "approved"}), seatRows("approved", "pending", "pending")...)
	if got := Resolve(stage, one); got != DecisionPending {
		t.Fatalf("got %q want pending: 1 of 3 individuals is not a majority", got)
	}
}

// Mock rejectIfQuorum: the stage ends when the individual seats cannot reach
// the quorum, or when any group cannot reach its own.
func TestResolve_MixedStage_RejectArithmetic(t *testing.T) {
	stage := unitStage(builder.QuorumMajority, gu("g", builder.QuorumAny, "m"))
	open := func() []store.AssignmentRow { return seatsOf("g", map[string]string{"m": "pending"}) }

	// majority of three individuals, one rejects: two approvals still possible.
	if got := Resolve(stage, append(open(), seatRows("rejected", "pending", "pending")...)); got != DecisionPending {
		t.Fatalf("one of three rejected: got %q want pending", got)
	}
	// two of three reject: the individual quorum is out of reach, the open group does not save it.
	if got := Resolve(stage, append(open(), seatRows("rejected", "rejected", "pending")...)); got != DecisionReject {
		t.Fatalf("two of three rejected: got %q want reject", got)
	}
	// majority of two individuals, one rejects: out of reach even with a satisfied group.
	sat := seatsOf("g", map[string]string{"m": "approved"})
	if got := Resolve(stage, append(sat, seatRows("rejected", "pending")...)); got != DecisionReject {
		t.Fatalf("majority of two, one rejected: got %q want reject", got)
	}
	// a group that cannot reach its own quorum ends the stage whatever the individuals did.
	dead := seatsOf("g", map[string]string{"m": "rejected"})
	if got := Resolve(stage, append(dead, seatRows("approved", "approved", "approved")...)); got != DecisionReject {
		t.Fatalf("group out of reach: got %q want reject", got)
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

// With group units on the stage an individual rejection is still a lost vote,
// counted among the individual seats only.
func TestResolve_RejectIsAVote_WithGroupUnits(t *testing.T) {
	// all of: one individual and one group; the individual rejects: out of reach.
	allStage := unitStage(builder.QuorumAll, gu("g", builder.QuorumAny, "m"))
	rows := append([]store.AssignmentRow{{UserID: "ind", State: "rejected"}}, seatsOf("g", map[string]string{"m": "pending"})...)
	if got := Resolve(allStage, rows); got != DecisionReject {
		t.Fatalf("all over ind+group, ind rejected: got %q want reject", got)
	}
	// majority of three individuals: one rejects, two approvals are still possible.
	majStage := unitStage(builder.QuorumMajority, gu("g", builder.QuorumAny, "m"))
	rows = append([]store.AssignmentRow{{UserID: "i1", State: "rejected"}, {UserID: "i2", State: "pending"}, {UserID: "i3", State: "pending"}},
		seatsOf("g", map[string]string{"m": "pending"})...)
	if got := Resolve(majStage, rows); got != DecisionPending {
		t.Fatalf("majority of three individuals, one lost: got %q want pending", got)
	}
}
