// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// OverdueSeat is a pending seat past its due date, with the run it belongs to.
type OverdueSeat struct {
	AssignmentRow
	RunID           string
	WorkflowDefID   string
	WorkflowVersion int
	PolicyID        string
	SubmittedBy     string
}

// ListOverdueSeats returns the pending seats whose due date is at or before
// now, on runs still in review. A decided, paused, superseded or withdrawn
// seat, and a seat of a finished run, are left out. The sweeper decides from
// the stage's own settings which of them to notify about.
func (a *Assignments) ListOverdueSeats(ctx context.Context, now time.Time) ([]OverdueSeat, error) {
	rows, err := a.db.Querier().Query(ctx,
		`SELECT a.id, a.policy_version_id, a.stage_index, a.user_id, a.group_id, a.assigned_at,
		        a.sla_deadline_at, a.reminder_at, a.state, a.decided_at, a.decided_comment, a.bulk_batch_id,
		        a.paused_remaining_sla_seconds, a.paused_remaining_reminder_seconds, a.veto, a.reminders_sent,
		        r.run_id, r.workflow_def_id::text, r.workflow_version, COALESCE(r.policy_id, ''), COALESCE(r.submitted_by, '')
		   FROM approval_assignments a
		   JOIN approval_runs r ON r.policy_version_id = a.policy_version_id AND r.status = 'in_review'
		  WHERE a.state = 'pending' AND a.sla_deadline_at <= $1
		  ORDER BY a.policy_version_id, a.stage_index, a.assigned_at, a.group_id, a.user_id`,
		now)
	if err != nil {
		return nil, fmt.Errorf("list overdue seats: %w", err)
	}
	defer rows.Close()
	var out []OverdueSeat
	for rows.Next() {
		var s OverdueSeat
		if err := rows.Scan(&s.ID, &s.PolicyVersionID, &s.StageIndex, &s.UserID, &s.GroupID, &s.AssignedAt,
			&s.SLADeadlineAt, &s.ReminderAt, &s.State, &s.DecidedAt, &s.DecidedComment, &s.BulkBatchID,
			&s.PausedRemainingSLASec, &s.PausedRemainingReminderS, &s.Veto, &s.RemindersSent,
			&s.RunID, &s.WorkflowDefID, &s.WorkflowVersion, &s.PolicyID, &s.SubmittedBy); err != nil {
			return nil, fmt.Errorf("scan overdue seat: %w", err)
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// RecordReminderSent notes that reminder n was published for the seat and moves
// reminder_at to when the next one falls. It records only forward and only
// while the seat is pending, and reports whether it did: the same or an older
// number, or a seat decided in the meantime, records nothing.
func (a *Assignments) RecordReminderSent(ctx context.Context, assignmentID string, n int, nextAt time.Time) (bool, error) {
	tag, err := a.db.Querier().Exec(ctx,
		`UPDATE approval_assignments SET reminders_sent = $2, reminder_at = $3
		  WHERE id = $1 AND state = 'pending' AND reminders_sent < $2`,
		assignmentID, n, nextAt)
	if err != nil {
		return false, fmt.Errorf("record reminder %d for seat %q: %w", n, assignmentID, err)
	}
	return tag.RowsAffected() == 1, nil
}

// StageEscalated reports whether the run's stage was already escalated.
func (a *Assignments) StageEscalated(ctx context.Context, runID string, stageIdx int) (bool, error) {
	var done bool
	if err := a.db.Querier().QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM stage_escalations WHERE run_id = $1 AND stage_index = $2)`,
		runID, stageIdx,
	).Scan(&done); err != nil {
		return false, fmt.Errorf("stage %d of run %q escalated: %w", stageIdx, runID, err)
	}
	return done, nil
}

// RecordStageEscalation notes that the run's stage was escalated, once: it
// reports false when the stage was already recorded.
func (a *Assignments) RecordStageEscalation(ctx context.Context, runID, policyVersionID string, stageIdx int, at time.Time) (bool, error) {
	var recorded bool
	err := a.db.Querier().QueryRow(ctx,
		`INSERT INTO stage_escalations (run_id, stage_index, policy_version_id, escalated_at)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (run_id, stage_index) DO NOTHING
		 RETURNING true`,
		runID, stageIdx, policyVersionID, at,
	).Scan(&recorded)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("record escalation of stage %d of run %q: %w", stageIdx, runID, err)
	}
	return recorded, nil
}
