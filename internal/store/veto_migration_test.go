// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

package store_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	pg "github.com/Bugs5382/go-postgres"

	"github.com/Steward-GRC/steward-workflow/internal/store"
)

// Migration 0005 moves the workflow-wide veto switch and the per-stage marks
// onto people (steward-workflow#64): veto_user_ids on each stage of an enabled
// workflow, nothing for a disabled one.

// schemaBefore0005 returns a migrations dir holding everything before 0005.
func schemaBefore0005(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	files, err := filepath.Glob("../../migrations/000[1-4]_*.sql")
	require.NoError(t, err)
	require.NotEmpty(t, files)
	for _, f := range files {
		b, err := os.ReadFile(f)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, filepath.Base(f)), b, 0o600))
	}
	return dir
}

func migration0005(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile("../../migrations/0005_stage_veto_people." + name + ".sql")
	require.NoError(t, err)
	return string(b)
}

// oldKey returns the raw JSON of a key on the first stage of a stored version
// (the live row when version is the definition's current one).
func oldKey(t *testing.T, q pg.Querier, defID string, version int, key string) string {
	t.Helper()
	var raw string
	require.NoError(t, q.QueryRow(context.Background(),
		`SELECT stage -> $3::text FROM (
		   SELECT stages_json -> 0 AS stage FROM workflow_defs WHERE id = $1 AND version = $2
		   UNION ALL
		   SELECT stages_json -> 0 FROM workflow_def_versions WHERE def_id = $1 AND version = $2) x`,
		defID, version, key).Scan(&raw))
	return raw
}

const legacyStages = `[
 {"id":"s1","name":"Finance","approver_ids":["u1","u2","u3"],"quorum":"any",
  "approvers_by_category":{"cat-1":["u4"]},
  "group_units":[{"group_id":"g1","internal_quorum":"any","members":["m1","m2","u1"]},
                 {"group_id":"g2","internal_quorum":"any","members":["m3"]}],
  "veto_approver_ids":["u4","u1","ghost"],"veto_group_ids":["g1"]},
 {"id":"s2","name":"Legal","approver_ids":["u2"],"quorum":"any"},
 {"id":"s3","name":"Only a group","approver_ids":["u2"],"quorum":"any",
  "group_units":[{"group_id":"g2","internal_quorum":"any","members":["m3"]}],
  "veto_group_ids":["g2","g-none"]}
]`

func TestMigration0005_MovesVetoMarksOntoPeople(t *testing.T) {
	ctx := context.Background()
	db := newTestDBIn(t, schemaBefore0005(t))
	q := db.Querier()

	const enabled, disabled = "00000000-0000-0000-0000-0000000000a1", "00000000-0000-0000-0000-0000000000b2"
	for _, d := range []struct {
		id      string
		on      bool
		version int
	}{{enabled, true, 2}, {disabled, false, 2}} {
		_, err := q.Exec(ctx,
			`INSERT INTO workflow_defs (id, name, version, stages_json, veto_enabled) VALUES ($1, $2, $3, $4::jsonb, $5)`,
			d.id, "wf-"+d.id[len(d.id)-2:], d.version, legacyStages, d.on)
		require.NoError(t, err)
		_, err = q.Exec(ctx,
			`INSERT INTO workflow_def_versions (def_id, version, name, stages_json) VALUES ($1, 1, 'old', $2::jsonb)`,
			d.id, legacyStages)
		require.NoError(t, err)
	}

	_, err := q.Exec(ctx, migration0005(t, "up"))
	require.NoError(t, err)

	defs := store.NewWorkflowDefStore(db)
	for _, version := range []int{2, 1} { // the live row, then the kept copy
		got, err := defs.GetVersion(ctx, enabled, version)
		require.NoError(t, err, "version %d", version)
		require.Len(t, got.Stages, 3)
		// Individuals first, then the veto groups' members; kept to the stage's
		// explicit lists (ghost is dropped) and without duplicates (u1 once).
		require.Equal(t, []string{"u4", "u1", "m1", "m2"}, got.Stages[0].VetoUserIDs, "version %d", version)
		require.Empty(t, got.Stages[1].VetoUserIDs, "a stage with no marks stays without people")
		require.Equal(t, []string{"m3"}, got.Stages[2].VetoUserIDs, "a veto group with no unit adds nobody")
		// The old keys are kept in the JSON until migration 0006.
		require.JSONEq(t, `["u4","u1","ghost"]`, oldKey(t, q, enabled, version, "veto_approver_ids"))
		require.JSONEq(t, `["g1"]`, oldKey(t, q, enabled, version, "veto_group_ids"))
	}

	// A switch that was off activates nothing.
	for _, version := range []int{2, 1} {
		got, err := defs.GetVersion(ctx, disabled, version)
		require.NoError(t, err)
		for i, st := range got.Stages {
			require.Empty(t, st.VetoUserIDs, "disabled workflow, stage %d, version %d", i, version)
		}
		require.JSONEq(t, `["u4","u1","ghost"]`, oldKey(t, q, disabled, version, "veto_approver_ids"), "marks kept, inert")
	}
}

func TestMigration0005_DownRemovesThePeople(t *testing.T) {
	ctx := context.Background()
	db := newTestDBIn(t, schemaBefore0005(t))
	q := db.Querier()
	const id = "00000000-0000-0000-0000-0000000000a1"
	_, err := q.Exec(ctx,
		`INSERT INTO workflow_defs (id, name, stages_json, veto_enabled) VALUES ($1, 'wf', $2::jsonb, true)`, id, legacyStages)
	require.NoError(t, err)

	_, err = q.Exec(ctx, migration0005(t, "up"))
	require.NoError(t, err)
	_, err = q.Exec(ctx, migration0005(t, "down"))
	require.NoError(t, err)

	got, err := store.NewWorkflowDefStore(db).Get(ctx, id)
	require.NoError(t, err)
	for _, st := range got.Stages {
		require.Empty(t, st.VetoUserIDs)
	}
	var raw string
	require.NoError(t, q.QueryRow(ctx, `SELECT stages_json::text FROM workflow_defs WHERE id = $1`, id).Scan(&raw))
	require.False(t, strings.Contains(raw, "veto_user_ids"))
	require.Contains(t, raw, "veto_approver_ids")
}

// Rows written before the migration, or saved by an older build, load with the
// new field empty, and a stored stage that still has the old keys loads.
func TestWorkflowDefStore_OldStoredStagesLoadWithoutVetoUsers(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	const id = "00000000-0000-0000-0000-0000000000c3"
	_, err := db.Querier().Exec(ctx,
		`INSERT INTO workflow_defs (id, name, stages_json) VALUES ($1, 'old', $2::jsonb)`, id, legacyStages)
	require.NoError(t, err)

	got, err := store.NewWorkflowDefStore(db).Get(ctx, id)
	require.NoError(t, err)
	require.Empty(t, got.Stages[0].VetoUserIDs)
}
