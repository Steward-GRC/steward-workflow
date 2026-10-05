// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	postgres "github.com/Bugs5382/go-postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ErrSeatNotPending means the seat was no longer pending when a decision or
// swap tried to change it: someone else got there first, or the run moved on.
var ErrSeatNotPending = errors.New("seat is not pending")

// AssignmentRow is one seat on a stage. GroupID is empty for an individual
// seat and names the approve-as-group unit for a group seat.
type AssignmentRow struct {
	ID                       string
	PolicyVersionID          string
	StageIndex               int
	UserID                   string
	GroupID                  string
	AssignedAt               time.Time
	SLADeadlineAt            time.Time
	ReminderAt               time.Time
	State                    string
	DecidedAt                *time.Time
	DecidedComment           *string
	BulkBatchID              *string
	PausedRemainingSLASec    *int64
	PausedRemainingReminderS *int64
}

// HistoryEntry is one assignment_history row.
type HistoryEntry struct {
	ID           int64
	AssignmentID string
	Event        string
	// ActorUserID is who really acted: during act-as, the admin.
	ActorUserID string
	// ImpersonatedUserID is the account ActorUserID acted as; empty outside
	// act-as.
	ImpersonatedUserID string
	ActorRole          string
	PreviousUserID     string
	NewUserID          string
	OutOfEligibility   bool
	Reason             string
	CreatedAt          time.Time
}

// historyRow is a history row to append, with the effective actor;
// appendHistory applies act-as attribution.
type historyRow struct {
	AssignmentID     string
	Event            string
	ActorUserID      string
	ActorRole        string
	PreviousUserID   string
	NewUserID        string
	OutOfEligibility bool
	Reason           string
}

// appendHistory is the one statement that writes assignment_history, so no
// history write can skip act-as attribution.
func appendHistory(ctx context.Context, tx pgx.Tx, in historyRow) error {
	actor, impersonated := Attribution(ctx, in.ActorUserID)
	if _, err := tx.Exec(ctx,
		`INSERT INTO assignment_history
		   (assignment_id, event, actor_user_id, impersonated_user_id, actor_role,
		    previous_user_id, new_user_id, out_of_eligibility, reason)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		in.AssignmentID, in.Event, actor, impersonated,
		nullableText(in.ActorRole), nullableText(in.PreviousUserID),
		nullableText(in.NewUserID), in.OutOfEligibility, nullableText(in.Reason),
	); err != nil {
		return fmt.Errorf("insert %s history: %w", in.Event, err)
	}
	return nil
}

// RunStateRow is a workflow_run_state row.
type RunStateRow struct {
	PolicyVersionID string
	State           string
	PausedAt        *time.Time
	PausedReason    *string
	ResumedAt       *time.Time
	UpdatedAt       time.Time
}

// Assignments persists seats, their history and the run states.
type Assignments struct{ db *postgres.DB }

// NewAssignments returns an Assignments store on db.
func NewAssignments(db *postgres.DB) *Assignments { return &Assignments{db: db} }

const seatColumns = `id, policy_version_id, stage_index, user_id, group_id, assigned_at, sla_deadline_at, reminder_at,
	state, decided_at, decided_comment, bulk_batch_id, paused_remaining_sla_seconds, paused_remaining_reminder_seconds`

func scanSeat(row pgx.Row) (AssignmentRow, error) {
	var r AssignmentRow
	err := row.Scan(&r.ID, &r.PolicyVersionID, &r.StageIndex, &r.UserID, &r.GroupID,
		&r.AssignedAt, &r.SLADeadlineAt, &r.ReminderAt,
		&r.State, &r.DecidedAt, &r.DecidedComment, &r.BulkBatchID,
		&r.PausedRemainingSLASec, &r.PausedRemainingReminderS)
	return r, err
}

func (a *Assignments) listSeats(ctx context.Context, sql string, args ...any) ([]AssignmentRow, error) {
	rows, err := a.db.Querier().Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("list seats: %w", err)
	}
	defer rows.Close()
	var out []AssignmentRow
	for rows.Next() {
		r, err := scanSeat(rows)
		if err != nil {
			return nil, fmt.Errorf("scan seat: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// CreateAssignment creates a pending seat with a "created" history row. A seat
// that already exists, from an earlier submission or a re-entered stage, is
// reset to pending in place, so it keeps its id and history.
func (a *Assignments) CreateAssignment(ctx context.Context, row AssignmentRow, actorUserID string) (AssignmentRow, error) {
	if row.ID == "" {
		row.ID = uuid.NewString()
	}
	if row.State == "" {
		row.State = "pending"
	}
	err := a.db.RunInTx(ctx, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx,
			`INSERT INTO approval_assignments
			   (id, policy_version_id, stage_index, user_id, group_id, assigned_at, sla_deadline_at, reminder_at, state)
			 VALUES ($1, $2, $3, $4, $5, COALESCE($6, now()), $7, $8, $9)
			 ON CONFLICT (policy_version_id, stage_index, user_id, group_id) DO UPDATE SET
			   assigned_at = COALESCE($6, now()),
			   sla_deadline_at = $7,
			   reminder_at = $8,
			   state = $9,
			   decided_at = NULL,
			   decided_comment = NULL,
			   bulk_batch_id = NULL
			 RETURNING id`,
			row.ID, row.PolicyVersionID, row.StageIndex, row.UserID, row.GroupID,
			nullableTime(row.AssignedAt), row.SLADeadlineAt, row.ReminderAt, row.State,
		).Scan(&row.ID); err != nil {
			return fmt.Errorf("upsert assignment: %w", err)
		}
		return appendHistory(ctx, tx, historyRow{AssignmentID: row.ID, Event: "created", ActorUserID: actorUserID})
	})
	if err != nil {
		return AssignmentRow{}, err
	}
	return row, nil
}

// GetAssignment returns one seat. It returns pgx.ErrNoRows when there is none.
func (a *Assignments) GetAssignment(ctx context.Context, pvID string, stageIdx int, userID, groupID string) (AssignmentRow, error) {
	return scanSeat(a.db.Querier().QueryRow(ctx,
		`SELECT `+seatColumns+` FROM approval_assignments
		  WHERE policy_version_id=$1 AND stage_index=$2 AND user_id=$3 AND group_id=$4`,
		pvID, stageIdx, userID, groupID,
	))
}

// PendingSeats returns the user's pending seats on a version, lowest stage
// first and the individual seat before group seats. Only the current stage
// has pending seats.
func (a *Assignments) PendingSeats(ctx context.Context, pvID, userID string) ([]AssignmentRow, error) {
	return a.listSeats(ctx,
		`SELECT `+seatColumns+` FROM approval_assignments
		  WHERE policy_version_id=$1 AND user_id=$2 AND state='pending'
		  ORDER BY stage_index, group_id`,
		pvID, userID)
}

// CurrentStageIndex returns the stage a run is waiting on: the lowest stage
// with a pending seat. ok is false when nothing is pending.
func (a *Assignments) CurrentStageIndex(ctx context.Context, pvID string) (int, bool, error) {
	var idx *int
	if err := a.db.Querier().QueryRow(ctx,
		`SELECT MIN(stage_index) FROM approval_assignments WHERE policy_version_id=$1 AND state='pending'`,
		pvID,
	).Scan(&idx); err != nil {
		return 0, false, fmt.Errorf("current stage of %q: %w", pvID, err)
	}
	if idx == nil {
		return 0, false, nil
	}
	return *idx, true, nil
}

// HasPendingAssignment reports whether the user holds any pending seat on the
// stage.
func (a *Assignments) HasPendingAssignment(ctx context.Context, pvID string, stageIdx int, userID string) (bool, error) {
	var exists bool
	err := a.db.Querier().QueryRow(ctx,
		`SELECT EXISTS (
		   SELECT 1 FROM approval_assignments
		    WHERE policy_version_id=$1 AND stage_index=$2 AND user_id=$3 AND state='pending'
		 )`,
		pvID, stageIdx, userID,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("has pending assignment: %w", err)
	}
	return exists, nil
}

// ListStageAssignments returns every seat of a stage, in any state.
func (a *Assignments) ListStageAssignments(ctx context.Context, pvID string, stageIdx int) ([]AssignmentRow, error) {
	return a.listSeats(ctx,
		`SELECT `+seatColumns+` FROM approval_assignments
		  WHERE policy_version_id=$1 AND stage_index=$2 ORDER BY assigned_at, group_id, user_id`,
		pvID, stageIdx)
}

// DecideAssignmentInput is one decision on a seat.
type DecideAssignmentInput struct {
	AssignmentID string
	// Decision is "approved" or "rejected".
	Decision    string
	Comment     string
	ActorUserID string
	// BulkBatchID is nil for a single decision.
	BulkBatchID *string
}

// DecideAssignment records a decision on a pending seat with its history row.
// A seat that is no longer pending returns ErrSeatNotPending.
func (a *Assignments) DecideAssignment(ctx context.Context, in DecideAssignmentInput) error {
	if in.Decision != "approved" && in.Decision != "rejected" {
		return fmt.Errorf("invalid decision %q", in.Decision)
	}
	if in.Comment == "" {
		return fmt.Errorf("decided_comment is required")
	}
	return a.db.RunInTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`UPDATE approval_assignments
			    SET state=$2, decided_at=now(), decided_comment=$3, bulk_batch_id=$4
			  WHERE id=$1 AND state='pending'`,
			in.AssignmentID, in.Decision, in.Comment, in.BulkBatchID,
		)
		if err != nil {
			return fmt.Errorf("update assignment: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("assignment %q: %w", in.AssignmentID, ErrSeatNotPending)
		}
		// The seat stays the target's during act-as; only the history names
		// the admin.
		return appendHistory(ctx, tx, historyRow{
			AssignmentID: in.AssignmentID,
			Event:        in.Decision,
			ActorUserID:  in.ActorUserID,
			Reason:       in.Comment,
		})
	})
}

// TerminatePendingAssignments moves every pending seat of a version, on every
// stage, to state (withdrawn, say) with a history row each, so the version
// leaves every approver's inbox. It returns how many it moved.
func (a *Assignments) TerminatePendingAssignments(ctx context.Context, pvID, state, actorUserID, reason string) (int64, error) {
	if state == "" {
		return 0, fmt.Errorf("terminal state is required")
	}
	var n int64
	err := a.db.RunInTx(ctx, func(tx pgx.Tx) error {
		ids, err := updateReturningIDs(ctx, tx,
			`UPDATE approval_assignments SET state=$2, decided_at=now()
			  WHERE policy_version_id=$1 AND state='pending'
			  RETURNING id`,
			pvID, state)
		if err != nil {
			return fmt.Errorf("terminate pending: %w", err)
		}
		for _, id := range ids {
			if err := appendHistory(ctx, tx, historyRow{AssignmentID: id, Event: state, ActorUserID: actorUserID, Reason: reason}); err != nil {
				return err
			}
		}
		n = int64(len(ids))
		return nil
	})
	return n, err
}

// SupersedePendingPeers marks every other pending seat of the stage
// superseded: the stage was rejected. It returns how many it marked.
func (a *Assignments) SupersedePendingPeers(ctx context.Context, pvID string, stageIdx int, excludeAssignmentID string, actorUserID string) (int64, error) {
	var n int64
	err := a.db.RunInTx(ctx, func(tx pgx.Tx) error {
		ids, err := updateReturningIDs(ctx, tx,
			`UPDATE approval_assignments SET state='superseded', decided_at=now()
			  WHERE policy_version_id=$1 AND stage_index=$2 AND id <> $3 AND state='pending'
			  RETURNING id`,
			pvID, stageIdx, excludeAssignmentID)
		if err != nil {
			return fmt.Errorf("supersede update: %w", err)
		}
		for _, id := range ids {
			if err := appendHistory(ctx, tx, historyRow{AssignmentID: id, Event: "rejected", ActorUserID: actorUserID, Reason: "stage_rejected"}); err != nil {
				return err
			}
		}
		n = int64(len(ids))
		return nil
	})
	return n, err
}

func updateReturningIDs(ctx context.Context, tx pgx.Tx, sql string, args ...any) ([]string, error) {
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// SwapInput is one reassignment.
type SwapInput struct {
	OldAssignmentID  string
	NewUserID        string
	NewSLADeadlineAt time.Time
	NewReminderAt    time.Time
	ActorUserID      string
	// ActorRole is admin, self_delegate or workflow_author.
	ActorRole        string
	Reason           string
	OutOfEligibility bool
}

// Swap retires a pending seat (swapped_out) and creates the same seat, in the
// same group, for the new user, with both history rows, in one transaction.
func (a *Assignments) Swap(ctx context.Context, in SwapInput) (AssignmentRow, error) {
	if in.Reason == "" {
		return AssignmentRow{}, fmt.Errorf("swap reason is required")
	}
	var newRow AssignmentRow
	err := a.db.RunInTx(ctx, func(tx pgx.Tx) error {
		var old AssignmentRow
		if err := tx.QueryRow(ctx,
			`SELECT id, policy_version_id, stage_index, user_id, group_id FROM approval_assignments
			  WHERE id=$1 AND state='pending' FOR UPDATE`,
			in.OldAssignmentID,
		).Scan(&old.ID, &old.PolicyVersionID, &old.StageIndex, &old.UserID, &old.GroupID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("assignment %q: %w", in.OldAssignmentID, ErrSeatNotPending)
			}
			return fmt.Errorf("lock old assignment: %w", err)
		}
		if _, err := tx.Exec(ctx,
			`UPDATE approval_assignments SET state='swapped_out', decided_at=now() WHERE id=$1`, in.OldAssignmentID,
		); err != nil {
			return fmt.Errorf("swap out: %w", err)
		}
		swap := historyRow{
			ActorUserID:      in.ActorUserID,
			ActorRole:        in.ActorRole,
			PreviousUserID:   old.UserID,
			NewUserID:        in.NewUserID,
			OutOfEligibility: in.OutOfEligibility,
			Reason:           in.Reason,
		}
		swappedOut := swap
		swappedOut.AssignmentID, swappedOut.Event = in.OldAssignmentID, "swapped_out"
		if err := appendHistory(ctx, tx, swappedOut); err != nil {
			return err
		}
		newRow = AssignmentRow{
			ID:              uuid.NewString(),
			PolicyVersionID: old.PolicyVersionID,
			StageIndex:      old.StageIndex,
			UserID:          in.NewUserID,
			GroupID:         old.GroupID,
			AssignedAt:      time.Now().UTC(),
			SLADeadlineAt:   in.NewSLADeadlineAt,
			ReminderAt:      in.NewReminderAt,
			State:           "pending",
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO approval_assignments
			   (id, policy_version_id, stage_index, user_id, group_id, sla_deadline_at, reminder_at, state)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending')`,
			newRow.ID, newRow.PolicyVersionID, newRow.StageIndex, newRow.UserID, newRow.GroupID,
			newRow.SLADeadlineAt, newRow.ReminderAt,
		); err != nil {
			return fmt.Errorf("insert new assignment: %w", err)
		}
		swappedIn := swap
		swappedIn.AssignmentID, swappedIn.Event = newRow.ID, "swapped_in"
		return appendHistory(ctx, tx, swappedIn)
	})
	if err != nil {
		return AssignmentRow{}, err
	}
	return newRow, nil
}

// ListHistory returns one stage's log across every seat it ever had.
func (a *Assignments) ListHistory(ctx context.Context, pvID string, stageIdx int) ([]HistoryEntry, error) {
	rows, err := a.db.Querier().Query(ctx,
		`SELECT h.id, h.assignment_id, h.event, h.actor_user_id,
		        COALESCE(h.impersonated_user_id,''),
		        COALESCE(h.actor_role,''), COALESCE(h.previous_user_id,''),
		        COALESCE(h.new_user_id,''), h.out_of_eligibility, COALESCE(h.reason,''), h.created_at
		   FROM assignment_history h
		   JOIN approval_assignments a ON a.id = h.assignment_id
		  WHERE a.policy_version_id=$1 AND a.stage_index=$2
		  ORDER BY h.id`,
		pvID, stageIdx,
	)
	if err != nil {
		return nil, fmt.Errorf("list history: %w", err)
	}
	defer rows.Close()
	var out []HistoryEntry
	for rows.Next() {
		var h HistoryEntry
		if err := rows.Scan(&h.ID, &h.AssignmentID, &h.Event, &h.ActorUserID, &h.ImpersonatedUserID,
			&h.ActorRole, &h.PreviousUserID, &h.NewUserID, &h.OutOfEligibility, &h.Reason, &h.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan history: %w", err)
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

// PauseAllPending pauses every pending seat, keeping the SLA and reminder time
// left at now. It returns the ids paused.
func (a *Assignments) PauseAllPending(ctx context.Context, now time.Time, actorUserID, reason string) ([]string, error) {
	var ids []string
	err := a.db.RunInTx(ctx, func(tx pgx.Tx) error {
		var err error
		ids, err = updateReturningIDs(ctx, tx,
			`UPDATE approval_assignments
			    SET state='paused',
			        paused_remaining_sla_seconds = EXTRACT(EPOCH FROM (sla_deadline_at - $1))::bigint,
			        paused_remaining_reminder_seconds = EXTRACT(EPOCH FROM (reminder_at - $1))::bigint
			  WHERE state='pending'
			 RETURNING id`,
			now)
		if err != nil {
			return fmt.Errorf("pause pending: %w", err)
		}
		for _, id := range ids {
			if err := appendHistory(ctx, tx, historyRow{AssignmentID: id, Event: "paused", ActorUserID: actorUserID, Reason: reason}); err != nil {
				return err
			}
		}
		return nil
	})
	return ids, err
}

// ResumeAllPaused returns paused seats to pending, with deadlines moved to now
// plus the time they had left. It returns the ids resumed.
func (a *Assignments) ResumeAllPaused(ctx context.Context, now time.Time, actorUserID string) ([]string, error) {
	var ids []string
	err := a.db.RunInTx(ctx, func(tx pgx.Tx) error {
		var err error
		ids, err = updateReturningIDs(ctx, tx,
			`UPDATE approval_assignments
			    SET state='pending',
			        sla_deadline_at = ($1::timestamptz) + make_interval(secs => COALESCE(paused_remaining_sla_seconds, 0)),
			        reminder_at     = ($1::timestamptz) + make_interval(secs => COALESCE(paused_remaining_reminder_seconds, 0)),
			        paused_remaining_sla_seconds = NULL,
			        paused_remaining_reminder_seconds = NULL
			  WHERE state='paused'
			 RETURNING id`,
			now)
		if err != nil {
			return fmt.Errorf("resume paused: %w", err)
		}
		for _, id := range ids {
			if err := appendHistory(ctx, tx, historyRow{AssignmentID: id, Event: "resumed", ActorUserID: actorUserID, Reason: "identity_svc_resumed"}); err != nil {
				return err
			}
		}
		return nil
	})
	return ids, err
}

// UpsertRunState writes a version's run state.
func (a *Assignments) UpsertRunState(ctx context.Context, row RunStateRow) error {
	_, err := a.db.Querier().Exec(ctx,
		`INSERT INTO workflow_run_state (policy_version_id, state, paused_at, paused_reason, resumed_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, now())
		 ON CONFLICT (policy_version_id) DO UPDATE
		   SET state=EXCLUDED.state, paused_at=EXCLUDED.paused_at,
		       paused_reason=EXCLUDED.paused_reason, resumed_at=EXCLUDED.resumed_at,
		       updated_at=now()`,
		row.PolicyVersionID, row.State, row.PausedAt, row.PausedReason, row.ResumedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert run state: %w", err)
	}
	return nil
}

// SetAllRunsState moves every run state not already at to to it; the outage
// reconciler pauses and resumes everything at once.
func (a *Assignments) SetAllRunsState(ctx context.Context, to string, reason string, now time.Time) error {
	if to != "running" && to != "paused_external_dep" && to != "complete" {
		return fmt.Errorf("invalid target state %q", to)
	}
	var paused, resumed *time.Time
	switch to {
	case "paused_external_dep":
		paused = &now
	case "running":
		resumed = &now
	}
	_, err := a.db.Querier().Exec(ctx,
		`UPDATE workflow_run_state
		    SET state=$1, paused_at = CASE WHEN $1='paused_external_dep' THEN $2 ELSE paused_at END,
		        paused_reason = CASE WHEN $1='paused_external_dep' THEN $3 ELSE paused_reason END,
		        resumed_at = CASE WHEN $1='running' THEN $4 ELSE resumed_at END,
		        updated_at = now()
		  WHERE state <> $1`,
		to, paused, reason, resumed,
	)
	if err != nil {
		return fmt.Errorf("set all runs state: %w", err)
	}
	return nil
}

// ListActiveRunsByDef returns the active runs pinned to a definition version.
func (a *Assignments) ListActiveRunsByDef(ctx context.Context, defID string, version int) ([]ApprovalRun, error) {
	return (&AssignmentStore{db: a.db}).ListActiveRunsByDef(ctx, defID, version)
}

// CountPausedRuns counts the runs the outage reconciler has paused.
func (a *Assignments) CountPausedRuns(ctx context.Context) (int, error) {
	var n int
	if err := a.db.Querier().QueryRow(ctx,
		`SELECT COUNT(*) FROM workflow_run_state WHERE state='paused_external_dep'`,
	).Scan(&n); err != nil {
		return 0, fmt.Errorf("count paused runs: %w", err)
	}
	return n, nil
}

func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}
