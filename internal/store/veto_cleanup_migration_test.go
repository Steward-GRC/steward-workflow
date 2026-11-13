// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package store_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	pg "github.com/Bugs5382/go-postgres"

	"github.com/Steward-GRC/steward-workflow/internal/store"
)

// Migration 0006 removes what the per-stage veto people replaced: the
// workflow_defs.veto_enabled column and the old veto_approver_ids and
// veto_group_ids keys in every stored stage. Stage veto_user_ids and the
// approval_assignments.veto seat mark stay.

// schemaBefore0006 returns a migrations dir holding everything before 0006.
func schemaBefore0006(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	files, err := filepath.Glob("../../migrations/000[1-5]_*.sql")
	require.NoError(t, err)
	require.NotEmpty(t, files)
	for _, f := range files {
		b, err := os.ReadFile(f)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, filepath.Base(f)), b, 0o600))
	}
	return dir
}

func migration0006(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile("../../migrations/0006_drop_workflow_veto." + name + ".sql")
	require.NoError(t, err)
	return string(b)
}

const stagesWithOldAndNewVeto = `[
 {"id":"s1","name":"Finance","approver_ids":["u1","u2"],"quorum":"any",
  "veto_user_ids":["u1"],"veto_approver_ids":["u1"],"veto_group_ids":["g1"]},
 {"id":"s2","name":"Legal","approver_ids":["u2"],"quorum":"any","veto_approver_ids":["u2"]},
 {"id":"s3","name":"Plain","approver_ids":["u2"],"quorum":"any"}
]`

func columnExists(t *testing.T, q pg.Querier, table, column string) bool {
	t.Helper()
	var n int
	require.NoError(t, q.QueryRow(context.Background(),
		`SELECT count(*) FROM information_schema.columns WHERE table_name = $1 AND column_name = $2`,
		table, column).Scan(&n))
	return n > 0
}

func TestMigration0006_DropsTheOldVetoColumnAndKeys(t *testing.T) {
	ctx := context.Background()
	db := newTestDBIn(t, schemaBefore0006(t))
	q := db.Querier()
	const id = "00000000-0000-0000-0000-0000000000a1"
	_, err := q.Exec(ctx,
		`INSERT INTO workflow_defs (id, name, version, stages_json, veto_enabled) VALUES ($1, 'wf', 2, $2::jsonb, true)`, id, stagesWithOldAndNewVeto)
	require.NoError(t, err)
	_, err = q.Exec(ctx,
		`INSERT INTO workflow_def_versions (def_id, version, name, stages_json) VALUES ($1, 1, 'old', $2::jsonb)`, id, stagesWithOldAndNewVeto)
	require.NoError(t, err)

	_, err = q.Exec(ctx, migration0006(t, "up"))
	require.NoError(t, err)

	require.False(t, columnExists(t, q, "workflow_defs", "veto_enabled"), "the workflow-wide switch is gone")
	require.True(t, columnExists(t, q, "approval_assignments", "veto"), "the seat mark stays")

	defs := store.NewWorkflowDefStore(db)
	for _, version := range []int{2, 1} { // the live row, then the kept copy
		got, err := defs.GetVersion(ctx, id, version)
		require.NoError(t, err, "version %d", version)
		require.Len(t, got.Stages, 3)
		require.Equal(t, []string{"u1"}, got.Stages[0].VetoUserIDs, "the people are untouched, version %d", version)
		require.Empty(t, got.Stages[1].VetoUserIDs)
		require.Empty(t, got.Stages[2].VetoUserIDs)
	}
	for _, table := range []string{"workflow_defs", "workflow_def_versions"} {
		var oldKeys int
		require.NoError(t, q.QueryRow(ctx,
			`SELECT count(*) FROM `+table+`, jsonb_array_elements(stages_json) AS s(stage)
			  WHERE s.stage ? 'veto_approver_ids' OR s.stage ? 'veto_group_ids'`).Scan(&oldKeys))
		require.Zero(t, oldKeys, table)
	}
	var order string
	require.NoError(t, q.QueryRow(ctx,
		`SELECT string_agg(s.stage ->> 'id', ',' ORDER BY s.pos) FROM workflow_defs, jsonb_array_elements(stages_json) WITH ORDINALITY AS s(stage, pos)`).Scan(&order))
	require.Equal(t, "s1,s2,s3", order, "stage order is kept")
}

func TestMigration0006_DownRestoresTheColumnOff(t *testing.T) {
	ctx := context.Background()
	db := newTestDBIn(t, schemaBefore0006(t))
	q := db.Querier()
	const id = "00000000-0000-0000-0000-0000000000a1"
	_, err := q.Exec(ctx,
		`INSERT INTO workflow_defs (id, name, stages_json, veto_enabled) VALUES ($1, 'wf', $2::jsonb, true)`, id, stagesWithOldAndNewVeto)
	require.NoError(t, err)

	_, err = q.Exec(ctx, migration0006(t, "up"))
	require.NoError(t, err)
	_, err = q.Exec(ctx, migration0006(t, "down"))
	require.NoError(t, err)

	var on bool
	require.NoError(t, q.QueryRow(ctx, `SELECT veto_enabled FROM workflow_defs WHERE id = $1`, id).Scan(&on))
	require.False(t, on, "the old values are not recoverable; the column returns off")
}
