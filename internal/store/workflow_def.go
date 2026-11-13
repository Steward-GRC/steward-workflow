// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Package store holds workflow's Postgres stores: definitions and their
// versions, policy overrides, approval runs, seats and their history.
package store

import (
	"context"
	"encoding/json"
	"fmt"

	postgres "github.com/Bugs5382/go-postgres"
	"github.com/jackc/pgx/v5"

	"github.com/Steward-GRC/steward-workflow/internal/builder"
)

// MigrationsTable is the table workflow's migrations are recorded in. The saga
// engine shares the database and uses golang-migrate's default table, so the
// two version sequences must not share one.
const MigrationsTable = "workflow_schema_migrations"

// WorkflowDefStore persists workflow definitions.
type WorkflowDefStore struct{ db *postgres.DB }

// NewWorkflowDefStore returns a WorkflowDefStore on db.
func NewWorkflowDefStore(db *postgres.DB) *WorkflowDefStore { return &WorkflowDefStore{db: db} }

// Create inserts a definition at version 1 and returns its id.
func (s *WorkflowDefStore) Create(ctx context.Context, wd builder.WorkflowDef) (string, error) {
	stages, err := json.Marshal(wd.Stages)
	if err != nil {
		return "", fmt.Errorf("marshal stages: %w", err)
	}
	var id string
	err = s.db.Querier().QueryRow(ctx,
		`INSERT INTO workflow_defs (name, description, stages_json, author_user_id) VALUES ($1, $2, $3, $4) RETURNING id`,
		wd.Name, wd.Description, stages, wd.AuthorUserID,
	).Scan(&id)
	if err != nil {
		return "", fmt.Errorf("insert workflow_def: %w", err)
	}
	return id, nil
}

// Get returns the definition's live version.
func (s *WorkflowDefStore) Get(ctx context.Context, id string) (builder.WorkflowDef, error) {
	var wd builder.WorkflowDef
	var stagesJSON []byte
	err := s.db.Querier().QueryRow(ctx,
		`SELECT id, name, description, version, stages_json, published, author_user_id FROM workflow_defs WHERE id = $1`,
		id,
	).Scan(&wd.ID, &wd.Name, &wd.Description, &wd.Version, &stagesJSON, &wd.Published, &wd.AuthorUserID)
	if err != nil {
		return wd, fmt.Errorf("get workflow_def %q: %w", id, err)
	}
	if err := json.Unmarshal(stagesJSON, &wd.Stages); err != nil {
		return wd, fmt.Errorf("unmarshal stages: %w", err)
	}
	return wd, nil
}

// List returns the definitions that aren't archived, by name.
func (s *WorkflowDefStore) List(ctx context.Context) ([]builder.WorkflowDef, error) {
	rows, err := s.db.Querier().Query(ctx,
		`SELECT id, name, description, version, stages_json, published, author_user_id
		   FROM workflow_defs
		  WHERE archived_at IS NULL
		  ORDER BY name`,
	)
	if err != nil {
		return nil, fmt.Errorf("list workflow_defs: %w", err)
	}
	defer rows.Close()

	var out []builder.WorkflowDef
	for rows.Next() {
		var wd builder.WorkflowDef
		var stagesJSON []byte
		if err := rows.Scan(&wd.ID, &wd.Name, &wd.Description, &wd.Version, &stagesJSON, &wd.Published, &wd.AuthorUserID); err != nil {
			return nil, fmt.Errorf("scan workflow_def: %w", err)
		}
		if err := json.Unmarshal(stagesJSON, &wd.Stages); err != nil {
			return nil, fmt.Errorf("unmarshal stages: %w", err)
		}
		out = append(out, wd)
	}
	return out, rows.Err()
}

// Update keeps the live version in workflow_def_versions, then overwrites the
// live row and bumps its version. It returns the new live version.
//
// A category's default workflow lives in core, so archiving or replacing a
// definition never touches it here; the gateway detaches it through core.
func (s *WorkflowDefStore) Update(ctx context.Context, id string, wd builder.WorkflowDef) (builder.WorkflowDef, error) {
	stages, err := json.Marshal(wd.Stages)
	if err != nil {
		return builder.WorkflowDef{}, fmt.Errorf("marshal stages: %w", err)
	}
	err = s.db.RunInTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`INSERT INTO workflow_def_versions (def_id, version, name, description, stages_json)
			 SELECT id, version, name, description, stages_json FROM workflow_defs WHERE id = $1`,
			id,
		); err != nil {
			return fmt.Errorf("copy to versions: %w", err)
		}
		if _, err := tx.Exec(ctx,
			`UPDATE workflow_defs
			    SET name = $2, description = $3, stages_json = $4, author_user_id = $5,
			        version = version + 1, updated_at = now()
			  WHERE id = $1`,
			id, wd.Name, wd.Description, stages, wd.AuthorUserID,
		); err != nil {
			return fmt.Errorf("update workflow_def: %w", err)
		}
		return nil
	})
	if err != nil {
		return builder.WorkflowDef{}, err
	}
	return s.Get(ctx, id)
}

// Archive hides a definition from List. Runs pinned to it keep reading it.
func (s *WorkflowDefStore) Archive(ctx context.Context, id string) error {
	if _, err := s.db.Querier().Exec(ctx, `UPDATE workflow_defs SET archived_at = now() WHERE id = $1`, id); err != nil {
		return fmt.Errorf("archive workflow_def %q: %w", id, err)
	}
	return nil
}

// GetVersion returns the definition at version: the live row when it is the
// live version, else the kept copy.
func (s *WorkflowDefStore) GetVersion(ctx context.Context, id string, version int) (builder.WorkflowDef, error) {
	var currentVersion int
	if err := s.db.Querier().QueryRow(ctx, `SELECT version FROM workflow_defs WHERE id = $1`, id).Scan(&currentVersion); err != nil {
		return builder.WorkflowDef{}, fmt.Errorf("get current version for %q: %w", id, err)
	}
	if version == currentVersion {
		return s.Get(ctx, id)
	}

	var wd builder.WorkflowDef
	var stagesJSON []byte
	err := s.db.Querier().QueryRow(ctx,
		`SELECT def_id, name, description, stages_json FROM workflow_def_versions WHERE def_id = $1 AND version = $2`,
		id, version,
	).Scan(&wd.ID, &wd.Name, &wd.Description, &stagesJSON)
	if err != nil {
		return wd, fmt.Errorf("get workflow_def_version %q@v%d: %w", id, version, err)
	}
	wd.Version = version
	if err := json.Unmarshal(stagesJSON, &wd.Stages); err != nil {
		return wd, fmt.Errorf("unmarshal stages: %w", err)
	}
	return wd, nil
}

// GCVersion deletes a kept version no active run pins any more.
func (s *WorkflowDefStore) GCVersion(ctx context.Context, defID string, version int) error {
	if _, err := s.db.Querier().Exec(ctx,
		`DELETE FROM workflow_def_versions WHERE def_id = $1 AND version = $2`, defID, version,
	); err != nil {
		return fmt.Errorf("gc workflow_def_version %q@v%d: %w", defID, version, err)
	}
	return nil
}

// Publish marks a definition published.
func (s *WorkflowDefStore) Publish(ctx context.Context, id string) error {
	if _, err := s.db.Querier().Exec(ctx,
		`UPDATE workflow_defs SET published = true, updated_at = now() WHERE id = $1`, id,
	); err != nil {
		return fmt.Errorf("publish workflow_def %q: %w", id, err)
	}
	return nil
}
