// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	postgres "github.com/Bugs5382/go-postgres"
	"github.com/jackc/pgx/v5"
)

// ErrRunNotFound means no approval run matched.
var ErrRunNotFound = errors.New("approval run not found")

// AssignmentStore holds the policy overrides and the approval runs. A
// category's default workflow lives in core.
type AssignmentStore struct{ db *postgres.DB }

// NewAssignmentStore returns an AssignmentStore on db.
func NewAssignmentStore(db *postgres.DB) *AssignmentStore { return &AssignmentStore{db: db} }

// SetPolicyOverride sets a policy's own workflow; "" means the policy needs no
// approval.
func (s *AssignmentStore) SetPolicyOverride(ctx context.Context, policyID, workflowDefID string) error {
	var defID any
	if workflowDefID != "" {
		defID = workflowDefID
	}
	_, err := s.db.Querier().Exec(ctx,
		`INSERT INTO policy_workflow_overrides (policy_id, workflow_def_id, updated_at)
		 VALUES ($1, $2, now())
		 ON CONFLICT (policy_id) DO UPDATE SET workflow_def_id = $2, updated_at = now()`,
		policyID, defID,
	)
	if err != nil {
		return fmt.Errorf("set policy override %q: %w", policyID, err)
	}
	return nil
}

// GetPolicyOverride returns a policy's override. exists is false with no
// override; exists with defID "" means the override is "no workflow".
func (s *AssignmentStore) GetPolicyOverride(ctx context.Context, policyID string) (defID string, exists bool, err error) {
	var ptr *string
	err = s.db.Querier().QueryRow(ctx,
		`SELECT workflow_def_id FROM policy_workflow_overrides WHERE policy_id = $1`, policyID,
	).Scan(&ptr)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("get policy override %q: %w", policyID, err)
	}
	if ptr == nil {
		return "", true, nil
	}
	return *ptr, true, nil
}

// ApprovalRun is one approval run of a policy version.
type ApprovalRun struct {
	PolicyVersionID string
	RunID           string
	WorkflowDefID   string
	WorkflowVersion int
	Status          string
	StartedAt       time.Time
	ResolvedAt      *time.Time
	// HomeCategoryID is the policy's home category at submit, for the audit
	// events of later decisions.
	HomeCategoryID string
	// SubmittedBy is the effective submitter, even during act-as: the
	// approval notices go to them and an account merge moves it.
	SubmittedBy string
	// SubmittedByActorID is the real admin who submitted during act-as; empty
	// otherwise. TrackRun reads it from the context.
	SubmittedByActorID string
	PolicyID           string
}

// activeRunStatuses are the statuses that hold a version's one active slot.
const activeRunStatuses = `('in_review', 'scheduled')`

const runColumns = `policy_version_id, run_id, workflow_def_id, workflow_version, status, started_at, resolved_at,
	COALESCE(home_category_id, ''), COALESCE(submitted_by, ''), COALESCE(submitted_by_actor_id, ''), COALESCE(policy_id, '')`

func scanRun(row pgx.Row) (ApprovalRun, error) {
	var r ApprovalRun
	err := row.Scan(&r.PolicyVersionID, &r.RunID, &r.WorkflowDefID, &r.WorkflowVersion, &r.Status, &r.StartedAt, &r.ResolvedAt,
		&r.HomeCategoryID, &r.SubmittedBy, &r.SubmittedByActorID, &r.PolicyID)
	return r, err
}

// TrackRun records a new run. The caller first drives any active run of the
// version terminal (AbortActiveRuns), or the one-active-run index refuses it.
func (s *AssignmentStore) TrackRun(ctx context.Context, r ApprovalRun) error {
	status := r.Status
	if status == "" {
		status = "in_review"
	}
	var actor any
	if admin, ok := ActingAdmin(ctx); ok {
		actor = admin
	}
	_, err := s.db.Querier().Exec(ctx,
		`INSERT INTO approval_runs (policy_version_id, run_id, workflow_def_id, workflow_version, status,
		                            home_category_id, submitted_by, submitted_by_actor_id, policy_id)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		r.PolicyVersionID, r.RunID, r.WorkflowDefID, r.WorkflowVersion, status,
		nullableText(r.HomeCategoryID), nullableText(r.SubmittedBy), actor, nullableText(r.PolicyID),
	)
	if err != nil {
		return fmt.Errorf("track run %q: %w", r.RunID, err)
	}
	return nil
}

// AbortActiveRuns marks the version's active runs aborted, keeping them as the
// record of the earlier attempt and freeing the slot for a resubmit.
func (s *AssignmentStore) AbortActiveRuns(ctx context.Context, policyVersionID string) (int64, error) {
	tag, err := s.db.Querier().Exec(ctx,
		`UPDATE approval_runs SET status = 'aborted', resolved_at = now()
		  WHERE policy_version_id = $1 AND status IN `+activeRunStatuses,
		policyVersionID,
	)
	if err != nil {
		return 0, fmt.Errorf("abort active runs for %q: %w", policyVersionID, err)
	}
	return tag.RowsAffected(), nil
}

// GetRun returns the version's current run: the active one, else the latest.
// It wraps ErrRunNotFound when the version has none.
func (s *AssignmentStore) GetRun(ctx context.Context, policyVersionID string) (ApprovalRun, error) {
	r, err := scanRun(s.db.Querier().QueryRow(ctx,
		`SELECT `+runColumns+` FROM approval_runs WHERE policy_version_id = $1
		  ORDER BY (status IN `+activeRunStatuses+`) DESC, started_at DESC
		  LIMIT 1`,
		policyVersionID,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return r, fmt.Errorf("policy version %q: %w", policyVersionID, ErrRunNotFound)
	}
	if err != nil {
		return r, fmt.Errorf("get run for %q: %w", policyVersionID, err)
	}
	return r, nil
}

// GetRunByRunID returns the run with the saga run id.
func (s *AssignmentStore) GetRunByRunID(ctx context.Context, runID string) (ApprovalRun, error) {
	r, err := scanRun(s.db.Querier().QueryRow(ctx, `SELECT `+runColumns+` FROM approval_runs WHERE run_id = $1`, runID))
	if errors.Is(err, pgx.ErrNoRows) {
		return r, fmt.Errorf("run id %q: %w", runID, ErrRunNotFound)
	}
	if err != nil {
		return r, fmt.Errorf("get run %q: %w", runID, err)
	}
	return r, nil
}

// ListActiveRunsByDef returns the active runs pinned to a definition version.
func (s *AssignmentStore) ListActiveRunsByDef(ctx context.Context, defID string, version int) ([]ApprovalRun, error) {
	return s.listRuns(ctx,
		`SELECT `+runColumns+` FROM approval_runs
		  WHERE workflow_def_id = $1 AND workflow_version = $2 AND status IN `+activeRunStatuses,
		defID, version)
}

// ListActiveRuns returns every active run, one per live version.
func (s *AssignmentStore) ListActiveRuns(ctx context.Context) ([]ApprovalRun, error) {
	return s.listRuns(ctx, `SELECT `+runColumns+` FROM approval_runs WHERE status IN `+activeRunStatuses)
}

func (s *AssignmentStore) listRuns(ctx context.Context, sql string, args ...any) ([]ApprovalRun, error) {
	rows, err := s.db.Querier().Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	defer rows.Close()
	var out []ApprovalRun
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, fmt.Errorf("scan run: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// UpdateRunStatus sets the status of the version's current run, stamping
// resolved_at for a terminal one.
func (s *AssignmentStore) UpdateRunStatus(ctx context.Context, policyVersionID, status string) error {
	terminal := status == "approved" || status == "rejected" || status == "withdrawn"
	resolvedAt := "resolved_at"
	if terminal {
		resolvedAt = "now()"
	}
	_, err := s.db.Querier().Exec(ctx,
		`UPDATE approval_runs SET status = $1, resolved_at = `+resolvedAt+`
		  WHERE run_id = (
		    SELECT run_id FROM approval_runs
		     WHERE policy_version_id = $2
		     ORDER BY (status IN `+activeRunStatuses+`) DESC, started_at DESC
		     LIMIT 1
		  )`,
		status, policyVersionID,
	)
	if err != nil {
		return fmt.Errorf("update run status for %q: %w", policyVersionID, err)
	}
	return nil
}
