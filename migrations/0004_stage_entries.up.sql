-- Copyright 2026 The Steward Authors
-- SPDX-License-Identifier: Apache-2.0

-- Every stage a run has entered, written when the saga enters the stage, with
-- or without anyone to seat. GetStatus reads it to tell a stage waiting on
-- nobody from one not reached yet. There is no foreign key: the first stage is
-- entered inside StartRun, before the run row is recorded. A run from before
-- this table has no rows and is read from its seats.
CREATE TABLE approval_stage_entries (
    run_id      text        NOT NULL,
    stage_index int         NOT NULL,
    entered_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, stage_index)
);

-- The policy's category lineage at submit, home first, so a stage not yet
-- entered can show its per-category approvers.
ALTER TABLE approval_runs ADD COLUMN ancestor_category_ids text[] NOT NULL DEFAULT '{}';
