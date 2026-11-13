// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package builder holds the admin-authored workflow model, its quorum rules and
// the compiler that turns a definition into a go-saga workflow.
package builder

import (
	"fmt"
	"strings"

	"github.com/Steward-GRC/steward-workflow/internal/workcal"
)

// Quorum controls how many votes a stage needs before it advances.
type Quorum string

const (
	// QuorumAll needs every vote.
	QuorumAll Quorum = "all"
	// QuorumAny needs one vote.
	QuorumAny Quorum = "any"
	// QuorumMajority needs strictly more than half of the votes: 4 of 6, 3 of
	// 5, 1 of 1.
	QuorumMajority Quorum = "majority"
	// QuorumNofM needs QuorumN votes. QuorumN above the pool at submit is a
	// misconfiguration and the submit is refused.
	QuorumNofM Quorum = "nofm"
)

// Stage is one ordered step of a workflow. Stages run one after another. A
// non-empty Condition (CEL) decides whether the stage runs at all; it sees
// policy_id, group_id and sensitivity.
type Stage struct {
	ID          string   `json:"id,omitempty"`
	Name        string   `json:"name"`
	ApproverIDs []string `json:"approver_ids"`
	Quorum      Quorum   `json:"quorum"`
	// QuorumN is required when Quorum is QuorumNofM and ignored otherwise.
	QuorumN           int    `json:"quorum_n,omitempty"`
	SLADays           int    `json:"sla_days,omitempty"`
	RejectOnSLABreach bool   `json:"reject_on_sla_breach,omitempty"`
	PinnedLast        bool   `json:"pinned_last,omitempty"`
	Condition         string `json:"condition,omitempty"`

	// ApproversByCategory holds the individual approvers per category. A
	// policy uses its home category's entry, else the nearest ancestor's (see
	// eligibility.ResolveByCategory), else ApproverIDs.
	ApproversByCategory map[string][]string `json:"approvers_by_category,omitempty"`

	// GroupUnits are the groups that approve as a whole. Their members hold
	// group seats, separate from any individual seat they also hold.
	GroupUnits []GroupUnit `json:"group_units,omitempty"`

	// VetoUserIDs names the people on this stage whose single reject ends the
	// version at once. Each must be in the stage's explicit approver lists
	// (ValidateVetoUsers); at submit only those seated in the approver pool
	// hold the veto. There is no workflow-wide switch.
	VetoUserIDs []string `json:"veto_user_ids,omitempty"`

	// RemindEveryDays and EscalateAfterDays are whole working days counted past
	// the due date; zero is off (steward-workflow#72). See ValidateOverdue.
	RemindEveryDays   int `json:"remind_every_days,omitempty"`
	EscalateAfterDays int `json:"escalate_after_days,omitempty"`
}

// GroupUnit is a group that approves as a whole: satisfied once
// InternalQuorum of its members vote yes on their group seats, and then one
// vote in the stage's quorum.
type GroupUnit struct {
	GroupID string `json:"group_id"`
	// InternalQuorum is the group's own quorum over its members.
	InternalQuorum Quorum   `json:"internal_quorum"`
	Members        []string `json:"members,omitempty"`
}

// Validate returns a non-nil error if the Stage is structurally invalid.
func (s Stage) Validate() error {
	if s.Name == "" {
		return fmt.Errorf("stage name is required")
	}
	if !validQuorums[s.Quorum] {
		return fmt.Errorf("stage %q: quorum must be one of all/any/majority/nofm, got %q", s.Name, s.Quorum)
	}
	if s.Quorum == QuorumNofM && s.QuorumN < 1 {
		return fmt.Errorf("stage %q: quorum_n must be >= 1 when quorum is %q", s.Name, QuorumNofM)
	}
	if s.Quorum != QuorumNofM && s.QuorumN != 0 {
		return fmt.Errorf("stage %q: quorum_n only valid when quorum is %q", s.Name, QuorumNofM)
	}
	// A stage may be saved with no approvers, as a draft; submit refuses an
	// unstaffed stage. So the count check only applies once there are some.
	if s.Quorum == QuorumNofM && len(s.ApproverIDs) > 0 && s.QuorumN > len(s.ApproverIDs) {
		return fmt.Errorf("stage %q: quorum_n (%d) exceeds approver count (%d)", s.Name, s.QuorumN, len(s.ApproverIDs))
	}
	return nil
}

// ValidateVetoUsers checks the save-time rule for a stage's veto people: each
// must appear in the stage's explicit approver lists (ApproverIDs, any
// ApproversByCategory list, or a group unit's Members). Someone in none of them
// could never be seated, and site admin or root does not count. For a pool
// derived from the policy's category or from a group the check is at seating,
// where a named person who is not seated simply holds no veto.
func (s Stage) ValidateVetoUsers() error {
	if len(s.VetoUserIDs) == 0 {
		return nil
	}
	named := make(map[string]bool)
	for _, id := range s.ApproverIDs {
		named[id] = true
	}
	for _, ids := range s.ApproversByCategory {
		for _, id := range ids {
			named[id] = true
		}
	}
	for _, u := range s.GroupUnits {
		for _, id := range u.Members {
			named[id] = true
		}
	}
	var strangers []string
	for _, id := range s.VetoUserIDs {
		if !named[id] {
			strangers = append(strangers, id)
		}
	}
	if len(strangers) > 0 {
		return fmt.Errorf("stage %q: a veto person must be one of the stage's approvers (%s)", s.Name, strings.Join(strangers, ", "))
	}
	return nil
}

// ValidateOverdue checks the save-time rule for a stage's overdue settings.
// RemindEveryDays and EscalateAfterDays are whole working days from 0 (off) to
// workcal.MaxWorkingDays. Both count from the due date, so they need SLADays,
// and a stage that rejects at its due date (RejectOnSLABreach) is never
// overdue, so a setting that could not fire is refused rather than stored.
func (s Stage) ValidateOverdue() error {
	check := func(name string, v int) error {
		if v < 0 || v > workcal.MaxWorkingDays {
			return fmt.Errorf("stage %q: %s must be from 0 to %d working days, got %d", s.Name, name, workcal.MaxWorkingDays, v)
		}
		return nil
	}
	if err := check("remind_every_days", s.RemindEveryDays); err != nil {
		return err
	}
	if err := check("escalate_after_days", s.EscalateAfterDays); err != nil {
		return err
	}
	if s.RemindEveryDays == 0 && s.EscalateAfterDays == 0 {
		return nil
	}
	if s.SLADays <= 0 {
		return fmt.Errorf("stage %q: reminders and escalation count from the due date, so the stage needs a due date (sla_days)", s.Name)
	}
	if s.RejectOnSLABreach {
		return fmt.Errorf("stage %q: the stage rejects at its due date, so it is never overdue and reminders or escalation would never fire", s.Name)
	}
	return nil
}

// WorkflowDef is the admin-authored approval workflow, compiled to a saga
// workflow at submit.
type WorkflowDef struct {
	ID          string  `json:"id,omitempty"`
	Name        string  `json:"name"`
	Description string  `json:"description,omitempty"`
	Version     int     `json:"version,omitempty"`
	Stages      []Stage `json:"stages"`
	Published   bool    `json:"published,omitempty"`
	// AuthorUserID created or last edited the definition, and may reassign
	// its runs' approvals as the workflow author.
	AuthorUserID string `json:"author_user_id,omitempty"`
}

var validQuorums = map[Quorum]bool{
	QuorumAll:      true,
	QuorumAny:      true,
	QuorumMajority: true,
	QuorumNofM:     true,
}

// Validate returns a non-nil error if the WorkflowDef is structurally invalid.
func (w WorkflowDef) Validate() error {
	if w.Name == "" {
		return fmt.Errorf("workflow name is required")
	}
	if len(w.Stages) == 0 {
		return fmt.Errorf("workflow must have at least one stage")
	}
	for i, s := range w.Stages {
		if err := s.Validate(); err != nil {
			return fmt.Errorf("stage[%d]: %w", i, err)
		}
	}
	return nil
}

// EvaluateQuorum reports whether approved votes out of pool satisfy q. A
// rejection stays in the pool and is never an approval; the caller decides
// whether the approvals still possible can reach q (see tally.Resolve).
func EvaluateQuorum(q Quorum, n, approved, pool int) bool {
	switch q {
	case QuorumAll:
		return approved >= pool && pool > 0
	case QuorumAny:
		return approved >= 1
	case QuorumMajority:
		return pool > 0 && approved*2 > pool
	case QuorumNofM:
		return approved >= n
	default:
		return false
	}
}
