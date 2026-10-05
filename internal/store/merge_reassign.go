// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// reassignableAssignmentStates are the in-flight seat states an account merge
// moves. Decided, swapped, superseded and withdrawn seats stay as they were,
// for the record.
const reassignableAssignmentStates = `('pending','paused')`

// ReassignResult counts what an account merge moved.
type ReassignResult struct {
	// AssignmentsReassigned is seats moved in place, timers kept.
	AssignmentsReassigned int
	// AssignmentsDeduped is seats retired because the target already held
	// the same seat.
	AssignmentsDeduped int
	// RunsReassigned is active runs whose submitter moved.
	RunsReassigned int
}

// Total is the one "workflow items" count the merge preview shows.
func (r ReassignResult) Total() int {
	return r.AssignmentsReassigned + r.AssignmentsDeduped + r.RunsReassigned
}

// ReassignUserItems moves a merged account's in-flight items to the account it
// was merged into, in one transaction: each pending or paused seat is moved in
// place, or retired when the target already holds that seat (same version,
// stage and group), and the active runs' submitter moves. Each seat gets a
// "reassigned" history row. A second run finds nothing to move. With dryRun it
// only counts.
func (a *Assignments) ReassignUserItems(ctx context.Context, fromUserID, toUserID, actorUserID string, dryRun bool) (ReassignResult, error) {
	var res ReassignResult
	if fromUserID == "" || toUserID == "" {
		return res, fmt.Errorf("from and to user ids are required")
	}
	if fromUserID == toUserID {
		return res, fmt.Errorf("from and to user ids are the same")
	}

	err := a.db.RunInTx(ctx, func(tx pgx.Tx) error {
		res = ReassignResult{}
		type seat struct {
			id, pvID, groupID string
			stage             int
		}
		rows, err := tx.Query(ctx,
			`SELECT id, policy_version_id, stage_index, group_id
			   FROM approval_assignments
			  WHERE user_id=$1 AND state IN `+reassignableAssignmentStates+`
			  ORDER BY policy_version_id, stage_index, group_id
			  FOR UPDATE`,
			fromUserID,
		)
		if err != nil {
			return fmt.Errorf("list source assignments: %w", err)
		}
		var seats []seat
		for rows.Next() {
			var s seat
			if err := rows.Scan(&s.id, &s.pvID, &s.stage, &s.groupID); err != nil {
				rows.Close()
				return fmt.Errorf("scan source assignment: %w", err)
			}
			seats = append(seats, s)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate source assignments: %w", err)
		}

		for _, s := range seats {
			var targetHasSeat bool
			if err := tx.QueryRow(ctx,
				`SELECT EXISTS (
				   SELECT 1 FROM approval_assignments
				    WHERE policy_version_id=$1 AND stage_index=$2 AND user_id=$3 AND group_id=$4
				 )`,
				s.pvID, s.stage, toUserID, s.groupID,
			).Scan(&targetHasSeat); err != nil {
				return fmt.Errorf("check target seat: %w", err)
			}

			if targetHasSeat {
				res.AssignmentsDeduped++
				if dryRun {
					continue
				}
				if _, err := tx.Exec(ctx,
					`UPDATE approval_assignments SET state='swapped_out', decided_at=now() WHERE id=$1`, s.id,
				); err != nil {
					return fmt.Errorf("retire source seat: %w", err)
				}
				if err := insertReassignHistory(ctx, tx, s.id, actorUserID, fromUserID, toUserID,
					"account_merge: target already assigned on this stage"); err != nil {
					return err
				}
				continue
			}

			res.AssignmentsReassigned++
			if dryRun {
				continue
			}
			if _, err := tx.Exec(ctx, `UPDATE approval_assignments SET user_id=$2 WHERE id=$1`, s.id, toUserID); err != nil {
				return fmt.Errorf("re-point source seat: %w", err)
			}
			if err := insertReassignHistory(ctx, tx, s.id, actorUserID, fromUserID, toUserID, "account_merge"); err != nil {
				return err
			}
		}

		if dryRun {
			if err := tx.QueryRow(ctx,
				`SELECT COUNT(*) FROM approval_runs WHERE submitted_by=$1 AND status IN `+activeRunStatuses,
				fromUserID,
			).Scan(&res.RunsReassigned); err != nil {
				return fmt.Errorf("count source runs: %w", err)
			}
			return nil
		}
		tag, err := tx.Exec(ctx,
			`UPDATE approval_runs SET submitted_by=$2 WHERE submitted_by=$1 AND status IN `+activeRunStatuses,
			fromUserID, toUserID,
		)
		if err != nil {
			return fmt.Errorf("re-point source runs: %w", err)
		}
		res.RunsReassigned = int(tag.RowsAffected())
		return nil
	})
	return res, err
}

func insertReassignHistory(ctx context.Context, tx pgx.Tx, assignmentID, actorUserID, fromUserID, toUserID, reason string) error {
	return appendHistory(ctx, tx, historyRow{
		AssignmentID:   assignmentID,
		Event:          "reassigned",
		ActorUserID:    actorUserID,
		PreviousUserID: fromUserID,
		NewUserID:      toUserID,
		Reason:         reason,
	})
}
