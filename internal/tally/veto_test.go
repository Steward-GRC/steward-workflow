// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package tally

import (
	"testing"

	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/store"
)

// The veto seat (steward-workflow#26). Each test names the mock server test it
// mirrors (steward-mock-server src/product/veto.test.ts).

func vetoSeats(veto string, states map[string]string) []store.AssignmentRow {
	out := make([]store.AssignmentRow, 0, len(states))
	for u, s := range states {
		out = append(out, store.AssignmentRow{UserID: u, State: s, Veto: u == veto})
	}
	return out
}

// Mock: "a reject by a veto approver ends the version as rejected although the
// quorum could still be reached".
func TestResolve_VetoRejectEndsStageDespiteQuorum(t *testing.T) {
	stage := builder.Stage{Quorum: builder.QuorumAny}
	rows := vetoSeats("dave", map[string]string{"charlie": "pending", "alice": "pending", "dave": "rejected"})
	if got := Resolve(stage, rows); got != DecisionReject {
		t.Fatalf("got %q, want %q", got, DecisionReject)
	}
}

// Mock: "a reject by anyone else is only a vote while the quorum can still be
// reached".
func TestResolve_NonVetoRejectStaysAVote(t *testing.T) {
	stage := builder.Stage{Quorum: builder.QuorumAny}
	rows := vetoSeats("dave", map[string]string{"charlie": "rejected", "alice": "pending", "dave": "pending"})
	if got := Resolve(stage, rows); got != DecisionPending {
		t.Fatalf("got %q, want %q", got, DecisionPending)
	}
}

// A veto seat that approves, or has not decided, vetoes nothing.
func TestResolve_VetoSeatThatApprovesOrWaitsDoesNotVeto(t *testing.T) {
	stage := builder.Stage{Quorum: builder.QuorumAll}
	rows := vetoSeats("dave", map[string]string{"charlie": "pending", "dave": "approved"})
	if got := Resolve(stage, rows); got != DecisionPending {
		t.Fatalf("approved veto seat: got %q, want pending", got)
	}
}

// A group seat can carry the mark too: its member's reject ends the stage even
// when the group's own quorum could still be met.
func TestResolve_VetoGroupSeatRejectEndsStage(t *testing.T) {
	stage := builder.Stage{Quorum: builder.QuorumAny, GroupUnits: []builder.GroupUnit{gu("g", builder.QuorumAny, "a", "b")}}
	rows := []store.AssignmentRow{
		{UserID: "a", GroupID: "g", State: "rejected", Veto: true},
		{UserID: "b", GroupID: "g", State: "pending", Veto: true},
		{UserID: "c", State: "pending"},
	}
	if got := Resolve(stage, rows); got != DecisionReject {
		t.Fatalf("got %q, want %q", got, DecisionReject)
	}
}

// Mock: "cancels the other seats" and the run record names whose veto it was.
func TestVetoedBy(t *testing.T) {
	rows := vetoSeats("dave", map[string]string{"charlie": "superseded", "dave": "rejected"})
	if got := VetoedBy(rows); got != "dave" {
		t.Fatalf("got %q, want dave", got)
	}
	plain := []store.AssignmentRow{{UserID: "charlie", State: "rejected"}}
	if got := VetoedBy(plain); got != "" {
		t.Fatalf("got %q, want empty", got)
	}
}
