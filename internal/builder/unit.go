// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package builder

// The statuses of an approve-as-group unit, as StageUnitProgress reports them.
const (
	UnitSatisfied = "SATISFIED"
	UnitPending   = "PENDING"
	UnitRejected  = "REJECTED"
)

// UnitTally is one group's live tally and status.
type UnitTally struct {
	Approvals int
	// Pending is the members who can still vote yes but haven't.
	Pending  int
	Roster   int
	Required int
	Quorum   string
	Status   string
}

// TallyUnit resolves one group from its members and the states of their
// group seats. SATISFIED once the yes votes meet the internal quorum; REJECTED
// once the yes votes plus the still-pending seats can't meet it any more;
// PENDING otherwise.
func TallyUnit(members []string, internalQuorum string, state map[string]string) UnitTally {
	iq := Quorum(internalQuorum)
	if iq == "" {
		iq = QuorumAny
	}
	roster := len(members)
	approvals, reachable := 0, 0
	for _, m := range members {
		switch state[m] {
		case "approved":
			approvals++
			reachable++
		case "rejected", "superseded", "swapped_out":
		default:
			reachable++
		}
	}
	status := UnitPending
	switch {
	case EvaluateQuorum(iq, 0, approvals, roster):
		status = UnitSatisfied
	case !EvaluateQuorum(iq, 0, reachable, roster):
		status = UnitRejected
	}
	return UnitTally{
		Approvals: approvals,
		Pending:   reachable - approvals,
		Roster:    roster,
		Required:  requiredForQuorum(iq, roster),
		Quorum:    string(iq),
		Status:    status,
	}
}

func requiredForQuorum(q Quorum, roster int) int {
	switch q {
	case QuorumAll:
		return roster
	case QuorumMajority:
		return roster/2 + 1
	default:
		return 1
	}
}
